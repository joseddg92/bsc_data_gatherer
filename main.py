import itertools
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

from web3.contract import Contract

from data_models import DecentralizedExchange, DecentralizedExchangeType, DexTradePair
from ddbb_manager import DDBBManager
from entity_factory import EntityFactory
from web3_utils import get_w3, get_contract, WEB3_PROVIDERS

BLOCK_FOR_THE_FIRST_LP = 6810423
BLOCK_LENGTH = 5000
MAX_THREADS = int(os.getenv("THREADS", len(WEB3_PROVIDERS)))

LOG_FORMAT_STR = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FORMAT = logging.Formatter(LOG_FORMAT_STR)
LOGGERS_CONF = {
    "ddbb_manager": logging.DEBUG,
    "web3_utils": logging.DEBUG,
    "main": logging.DEBUG
}

logger = logging.getLogger("main")

# Sometimes a BSC node will throw this error.
# When a filter is created, it gets assign an id. Later requests about that filter use that id to identify it.
# My assumption is that the BSC nodes cache those filter ids for some time, and they eventually forget about them if
# no-one interacts with them for a while.
# In any case, I get these errors from time to time, and just retrying seems to (eventually) work just fine.
FILTER_NOT_FOUND_ERR_MSG = '{\'code\': -32000, \'message\': \'filter not found\'}'
MAX_FNF_RETRIES_FOR_WARNING = 50
FNF_ERROR_WAIT_SECONDS = 10

FORBIDEN_ERROR_MSG = "403 Client Error: Forbidden for url"
FORBIDDEN_ERROR_WAIT_SECONDS = 5 * 60

# The rate limit of BSC endpoint on Testnet and Mainnet is 10K/5min (https://docs.binance.org/smart-chain/developer/rpc.html#rate-limit)
RATE_LIMIT_WAIT_TIME = 10 / (10000 / (5*60))

def setup_loggers():
    file_handler = logging.FileHandler('main.log', mode='w', encoding='utf-16')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(LOG_FORMAT)

    class OneLineExceptionFormatter(logging.Formatter):
        def format(self, record):
            if record.exc_info:
                # Replace record.msg with the string representation of the message
                # use repr() to prevent printing it to multiple lines
                record.msg += f": {record.exc_info[1]}"
                record.exc_info = None
                record.exc_text = None
            result = super().format(record)
            return result

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(OneLineExceptionFormatter(LOG_FORMAT_STR, '%m/%d/%Y %I:%M:%S %p'))

    for logger_name, log_level in LOGGERS_CONF.items():
        this_logger = logging.getLogger(logger_name)

        this_logger.addHandler(file_handler)
        this_logger.addHandler(stdout_handler)
        this_logger.setLevel(log_level)

def __handle_exception_from_w3_provider(retry: int, e: Exception):
    if retry == 0:
        error_text = str(e)
        if FORBIDEN_ERROR_MSG in error_text:
            logger.warning("Got forbidden, 403, waiting...")
            time.sleep(FORBIDDEN_ERROR_WAIT_SECONDS)
    else:
        logger.exception(f"ERROR (retry #{retry})")

def get_new_pairs(
        dex_factories: Dict[DecentralizedExchangeType, Contract],
        start_block: int,
        e_factory: EntityFactory
) -> List[DexTradePair]:
    new_pairs = []
    for dex, factory_contract in dex_factories.items():
        for retry in itertools.count():
            try:
                logger.info(f"\tGetting pairs for {dex.dex_name}...")
                pair_logs = factory_contract.events.PairCreated.getLogs(
                    fromBlock=start_block - 1,
                    toBlock=start_block + BLOCK_LENGTH - 1
                )

                with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                    def __process_pair(pair_for_worker):
                        pair = e_factory.get_DexTradePair(dex, pair_for_worker)
                        if pair:
                            logger.debug(f"{threading.current_thread().name} got {pair}")

                        return pair

                    new_pairs.extend(executor.map(__process_pair, pair_logs))
                break
            except (Exception,) as e:
                __handle_exception_from_w3_provider(retry, e)

    return list(filter(None, new_pairs))


def find_and_persist_trades(
        indexed_pair: Tuple[int, DexTradePair],
        total_pairs: int,
        start_block: int,
        e_factory: EntityFactory,
        ddbb_manager: DDBBManager
) -> int:
    index, pair = indexed_pair
    for retry in itertools.count():
        try:
            sync_logs = pair.pair_contract().events.Sync.getLogs(
                fromBlock=start_block - 1,
                toBlock=start_block + BLOCK_LENGTH - 1
            )

            for sync in sync_logs:
                ddbb_manager.persist(e_factory.get_DexTradeSync(sync, pair))

            logger.debug(f"{threading.current_thread().name} ({index}/{total_pairs}) got {len(sync_logs)} swaps for {pair}")
            return len(sync_logs)
        except (Exception,) as e:
            __handle_exception_from_w3_provider(retry, e)


def main():
    import faulthandler

    faulthandler.enable()

    db_manager = DDBBManager(os.getenv("DDBB_STRING"))
    e_factory = EntityFactory(db_manager)
    w3 = get_w3()

    logger.info("Reading last block...")
    last_block = db_manager.get_last_block()
    start_block = last_block.number if last_block else BLOCK_FOR_THE_FIRST_LP - 10
    dex_factories = {
        dex.value: get_contract(w3, dex.value.factory_addr) for dex in DecentralizedExchange
    }
    logger.info("Reading pairs...")
    pairs = db_manager.get_all_pairs()
    logger.info("Done!")

    start_time = time.time()
    logger.info(f"Starting in block {start_block}, {len(pairs)} pairs so far.")

    block = start_block
    while True:
        logger.info(f"Importing blocks {block}-{block + BLOCK_LENGTH}...")

        new_pairs = get_new_pairs(dex_factories, block, e_factory)
        if len(new_pairs) > 0:
            logger.info(f"\tGot {len(new_pairs)} new pairs")
            pairs.extend(new_pairs)
            for pair in new_pairs:
                db_manager.persist(pair)

        logger.info("\tLooking for trades...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            trades_found = sum(
                executor.map(
                    lambda pair_for_worker: find_and_persist_trades(pair_for_worker, len(pairs), block, e_factory, db_manager),
                    enumerate(pairs)
                )
            )
        logger.info(f"\tGot {trades_found} new trades")

        first_block = BLOCK_FOR_THE_FIRST_LP
        last_block = get_w3().eth.get_block_number()
        seconds_so_far = time.time() - start_time
        blocks_processed = block + BLOCK_LENGTH - start_block
        blocks_per_second = blocks_processed / seconds_so_far
        remaining_blocks = last_block - block + BLOCK_LENGTH
        remaining_seconds = remaining_blocks / blocks_per_second
        progress_percent = 100 * (block - first_block) / (last_block - first_block)

        try:
            start_persist_time = time.time()
            db_manager.commit_changes()
            logger.info(f"\tNew entities commited in {time.time() - start_persist_time:.2f} seconds!")
        except (Exception,):
            logger.exception(f"Error committing")

        logger.info(
            f"Progress update: {progress_percent:.2f}% "
            f"({blocks_per_second:.2f} block/second, {remaining_seconds / 3600:.2f} hours remaining)"
            "\n\n"
        )

        block += BLOCK_LENGTH


if __name__ == '__main__':
    setup_loggers()
    main()
