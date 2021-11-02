import itertools
import os
import time

from data_models import DexTradePair, get_token, get_tx, get_DexTrade, DecentralizedExchange
from ddbb_manager import DDBBManager
from web3_utils import get_w3, get_contract, WBNB_ADDRESS


def create_pair_if_wbnb(dex, pair_created):
    if WBNB_ADDRESS not in (pair_created.args.values()):
        return None

    is_token0_wbnb = WBNB_ADDRESS == pair_created.args.token0
    token_addr = pair_created.args.token1 if is_token0_wbnb else pair_created.args.token0
    token = get_token(token_addr)

    return DexTradePair(
        dex=dex, token=token, creator_tx=get_tx(pair_created.transactionHash),
        is_token0_wbnb=is_token0_wbnb
    )


BLOCK_FOR_THE_FIRST_LP = 6810423
BLOCK_LENGTH = 1000


def main():
    db_manager = DDBBManager(os.getenv("DDBB_STRING"), prune_schema=True)

    w3 = get_w3()

    start_block = BLOCK_FOR_THE_FIRST_LP - 10
    dex_factories = {
        dex.value: get_contract(w3, dex.value.factory_addr) for dex in DecentralizedExchange
    }
    pairs = {}
    n_swaps = 0
    start_time = time.time()

    block = start_block
    while True:
        # Find new pairs
        entities_to_persist = []
        for dex, factory_contract in dex_factories.items():
            for retry in itertools.count():
                try:
                    print(f"Getting pairs for {dex.dex_name}, blocks {block}-{block + BLOCK_LENGTH}...", end="",
                          flush=True)
                    new_pairs_filter = factory_contract.events.PairCreated.createFilter(
                        fromBlock=block - 1,
                        toBlock=block + BLOCK_LENGTH - 1
                    )

                    pairs_found = 0
                    for pair_created in new_pairs_filter.get_all_entries():
                        pair = create_pair_if_wbnb(dex, pair_created)
                        if not pair:
                            continue

                        pair_contract = get_contract(w3, pair_created.args['pair'])
                        pairs[pair] = pair_contract
                        entities_to_persist.append(pair)
                        pairs_found += 1
                    print(f" Found {pairs_found} pairs")
                    break
                except (Exception, ) as e:
                    if retry > 1:
                        print(f"ERROR (retry #{retry}): {e}")

        # Find new swaps for existing pairs
        print("Looking for swaps...")
        for pair, pair_contract in pairs.items():
            for retry in itertools.count():
                try:
                    swaps_filter = pair_contract.events.Swap.createFilter(
                        fromBlock=block - 1,
                        toBlock=block + BLOCK_LENGTH - 1
                    )

                    # Iterate through .get_all_entries() fast, otherwise the Web3 provider will forget about our
                    # filter id, and we won't be able to finish to iterate the swap entries (exception will be raisen)
                    swaps = [swap for swap in swaps_filter.get_all_entries()]
                    swaps = [get_DexTrade(swap, pair) for swap in swaps]

                    n_swaps += len(swaps)
                    entities_to_persist.extend(swaps)
                    if len(swaps) > 0:
                        print(f"\t{pair.dex.dex_name} {pair.token}: got {len(swaps)} swaps.")
                    break
                except (Exception, ) as e:
                    if retry > 1:
                        print(f"ERROR (retry #{retry}): {e}")

        seconds_so_far = time.time() - start_time
        blocks_processed = block + BLOCK_LENGTH - start_block
        blocks_per_second = blocks_processed / seconds_so_far
        remaining_blocks = get_w3().eth.get_block_number() - block + BLOCK_LENGTH
        remaining_seconds = remaining_blocks / blocks_per_second

        print(
            f"Processed blocks {block}-{block + BLOCK_LENGTH} "
            f"({blocks_per_second:.2f} block/second, {remaining_seconds / 3600:.2f} hours remaining)")
        print(f"\tPairs: {len(pairs)}")
        print(f"\tSwaps: {n_swaps}")
        print(f"")
        block += BLOCK_LENGTH

        if entities_to_persist:
            print(f"Persisting...")
            try:
                start_persist_time = time.time()
                db_manager.persist(entities_to_persist, commit=True)
                print(f" {len(entities_to_persist)} persisted in {time.time() - start_persist_time} seconds!")
            except (Exception,) as e:
                print(f" Error persisting: {e}")


if __name__ == '__main__':
    main()
