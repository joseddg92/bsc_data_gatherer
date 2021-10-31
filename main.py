from web3.types import BlockData

from ParsedTx import ParsedTx
from data_models import DexTradePair, get_token, get_tx, get_DexTrade, DecentralizedExchange
from web3_utils import get_w3, get_contract, WBNB_ADDRESS


def process_block(block_data: BlockData):
    for tx in block_data['transactions']:
        parsed_tx = ParsedTx(tx)
        if parsed_tx.is_simple_wbnb_tx():
            print(parsed_tx)


def main_events():
    w3 = get_w3()
    last_block = w3.eth.get_block_number()

    for dex in DecentralizedExchange:
        contract = get_contract(w3, dex.factory_addr)
        new_pairs_filter = contract.events.PairCreated.createFilter(fromBlock=last_block-5000, toBlock=last_block)
        for pair_created in new_pairs_filter.get_all_entries():
            print(pair_created)
            pair_contract = get_contract(w3, pair_created.args['pair'])
            if WBNB_ADDRESS not in (pair_created.args.values()):
                print(f"Ignoring not-WBNB pair: {pair_created.args}")
                continue


            is_token0_wbnb = WBNB_ADDRESS == pair_created.args.token0
            token_addr = pair_created.args.token1 if is_token0_wbnb else pair_created.args.token0
            token = get_token(token_addr)

            pair = DexTradePair(dex, token, get_tx(pair_created.transactionHash), is_token0_wbnb)

            swaps_filter = pair_contract.events.Swap.createFilter(fromBlock=last_block-5000, toBlock=last_block)
            for swap in swaps_filter.get_all_entries():
                print(get_DexTrade(swap, pair))
        print(contract)


def main():
    w3 = get_w3()

    last_block = w3.eth.get_block_number()
    current_block = last_block

    while True:
        block_data = w3.eth.get_block(current_block, full_transactions=True)
        process_block(block_data)

        current_block -= 1


if __name__ == '__main__':
    main_events()
    main()
