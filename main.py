import concurrent
import itertools
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from web3 import Web3
from web3.contract import Contract

from data_models import DecentralizedExchange, DecentralizedExchangeType, DexTradePair
from ddbb_manager import DDBBManager
from entity_factory import EntityFactory
from web3_utils import get_w3, get_contract, get_lptoken_contract

BLOCK_FOR_THE_FIRST_LP = 6810423
BLOCK_LENGTH = 5000
N_WORKERS = 4

def get_new_pairs(
        dex_factories: Dict[DecentralizedExchangeType, Contract],
        start_block: int,
        e_factory: EntityFactory
) -> List[DexTradePair]:
    new_pairs = []
    for dex, factory_contract in dex_factories.items():
        for retry in itertools.count():
            try:
                print(f"\tGetting pairs for {dex.dex_name}...")
                new_pairs_filter = factory_contract.events.PairCreated.createFilter(
                    fromBlock=start_block - 1,
                    toBlock=start_block + BLOCK_LENGTH - 1
                )

                for pair_created in new_pairs_filter.get_all_entries():
                    pair = e_factory.get_DexTradePair(dex, pair_created)
                    if not pair:
                        continue

                    new_pairs.append(pair)
                break
            except (Exception,) as e:
                if retry > 1:
                    print(f"ERROR (retry #{retry}): {e}")

    return new_pairs

def find_and_persist_trades(
        pair: DexTradePair,
        start_block: int,
        e_factory: EntityFactory,
        ddbb_manager: DDBBManager
) -> int:
    for retry in itertools.count():
        try:
            swaps_filter = pair.pair_contract().events.Swap.createFilter(
                fromBlock=start_block - 1,
                toBlock=start_block + BLOCK_LENGTH - 1
            )

            # Iterate through .get_all_entries() fast, otherwise the Web3 provider will forget about our
            # filter id, and we won't be able to finish to iterate the swap entries (exception will be raisen)
            swaps = [swap for swap in swaps_filter.get_all_entries()]
            for swap in swaps:
                ddbb_manager.persist(e_factory.get_DexTrade(swap, pair))

            print(f"{threading.get_ident()} got {len(swaps)} swaps for pair {pair}")
            return len(swaps)
        except (Exception,) as e:
            if retry > 1:
                print(f"ERROR (retry #{retry}): {e}")

def main():
    db_manager = DDBBManager(os.getenv("DDBB_STRING"))
    e_factory = EntityFactory(db_manager)
    w3 = get_w3()

    print("Reading last block...")
    last_block = db_manager.get_last_block()
    start_block = last_block.number if last_block else BLOCK_FOR_THE_FIRST_LP - 10
    dex_factories = {
        dex.value: get_contract(w3, dex.value.factory_addr) for dex in DecentralizedExchange
    }
    print("Reading pairs...")
    pairs = db_manager.get_all_pairs()
    print("Done!")

    start_time = time.time()
    print(f"Starting in block {start_block}, {len(pairs)} pairs so far.")

    block = start_block
    while True:
        print(f"Import blocks {block}-{block + BLOCK_LENGTH}...")

        new_pairs = get_new_pairs(dex_factories, block, e_factory)
        if len(new_pairs) > 0:
            print(f"\tGot {len(new_pairs)} new pairs")
            pairs.extend(new_pairs)

        print("\tLooking for swaps...")
        with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
            trades_found = sum(
                executor.map(
                    lambda pair_for_worker: find_and_persist_trades(pair_for_worker, block, e_factory, db_manager),
                    pairs
                )
            )

        seconds_so_far = time.time() - start_time
        blocks_processed = block + BLOCK_LENGTH - start_block
        blocks_per_second = blocks_processed / seconds_so_far
        remaining_blocks = get_w3().eth.get_block_number() - block + BLOCK_LENGTH
        remaining_seconds = remaining_blocks / blocks_per_second

        print(
            f"Processed blocks {block}-{block + BLOCK_LENGTH} "
            f"({blocks_per_second:.2f} block/second, {remaining_seconds / 3600:.2f} hours remaining)")
        print(f"\tTotal pairs: {len(pairs)}")
        print(f"\tNew swaps: {trades_found}")
        print(f"")
        block += BLOCK_LENGTH

        try:
            start_persist_time = time.time()
            db_manager.commit_changes()
            print(f"New entities commited in {time.time() - start_persist_time} seconds!")
        except (Exception,) as e:
            print(f"Error committing: {e}")



if __name__ == '__main__':
    main()
