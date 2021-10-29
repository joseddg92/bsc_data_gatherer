from web3.types import BlockData

from ParsedTx import ParsedTx
from web3_utils import get_w3


def process_block(block_data: BlockData):
    for tx in block_data['transactions']:
        parsed_tx = ParsedTx(tx)
        if parsed_tx.is_dex_tx():
            print(f"Got DEX transaction in block {block_data['number']}: {parsed_tx}")
    pass


def main():
    w3 = get_w3()

    last_block = w3.eth.get_block_number()
    current_block = last_block

    while True:
        block_data = w3.eth.get_block(current_block, full_transactions=True)
        process_block(block_data)

        current_block -= 1


if __name__ == '__main__':
    main()
