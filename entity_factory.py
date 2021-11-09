from datetime import datetime

from eth_typing import ChecksumAddress
from hexbytes import HexBytes
from web3 import Web3
from web3.types import TxData, TxReceipt

from data_models import Token, Block, Tx, DexTradePair, DexTrade
from ddbb_manager import DDBBManager
from web3_utils import get_erc20_contract, get_w3, WBNB_ADDRESS


class EntityFactory:

    def __init__(self, dbm: DDBBManager = None):
        self.dbm = dbm

    def get_token(self, token_addr: ChecksumAddress) -> Token:
        if isinstance(token_addr, str):
            token_addr = Web3.toChecksumAddress(token_addr)

        if self.dbm:
            entity = self.dbm.get_entity_by_pl(Token, token_addr)
            if entity:
                return entity

        contract = get_erc20_contract(get_w3(), token_addr)
        name = contract.functions.name().call()
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()

        return Token(address=token_addr, name=name, symbol=symbol, decimals=decimals)

    def get_block(self, block_number: int) -> Block:
        if self.dbm:
            entity = self.dbm.get_entity_by_pl(Block, block_number)
            if entity:
                return entity

        block_data = get_w3().eth.get_block(block_number)

        return Block(
            number=block_number,
            timestamp=datetime.fromtimestamp(block_data['timestamp'])
        )

    def get_tx(self, tx_hash: str) -> Tx:
        if not tx_hash:
            raise ValueError("tx_hash cannot be None")

        if isinstance(tx_hash, HexBytes):
            tx_hash = tx_hash.hex()

        if self.dbm:
            entity = self.dbm.get_entity_by_pl(Tx, tx_hash)
            if entity:
                return entity

        tx_data: TxData = get_w3().eth.get_transaction(tx_hash)

        return Tx(
            hash=tx_hash,
            block=self.get_block(tx_data['blockNumber']),
            transaction_index=tx_data['transactionIndex'],
            gas_price=tx_data['gasPrice']
        )

    def get_DexTradePair(self, dex, pair_created, none_on_not_wbnb_pair=True) -> DexTradePair:
        if none_on_not_wbnb_pair and WBNB_ADDRESS not in (pair_created.args.values()):
            return None

        if self.dbm:
            entity = self.dbm.get_entity_by_pl(DexTradePair, pair_created.args.pair)
            if entity:
                return entity

        is_token0_wbnb = WBNB_ADDRESS == pair_created.args.token0
        token_addr = pair_created.args.token1 if is_token0_wbnb else pair_created.args.token0
        token = self.get_token(token_addr)

        return DexTradePair(
            pair_addr=pair_created.args.pair,
            dex=dex, token=token, creator_tx=self.get_tx(pair_created.transactionHash),
            is_token0_wbnb=is_token0_wbnb
        )

    def get_DexTrade(self, swap_info: TxReceipt, dex_pair: DexTradePair) -> DexTrade:
        # No cache in DDBB for this entity
        if dex_pair.is_token0_wbnb:
            token_in = swap_info.args['amount1In']
            token_out = swap_info.args['amount1Out']
            wbnb_in = swap_info.args['amount0In']
            wbnb_out = swap_info.args['amount0Out']
        else:
            token_in = swap_info.args['amount0In']
            token_out = swap_info.args['amount0Out']
            wbnb_in = swap_info.args['amount1In']
            wbnb_out = swap_info.args['amount1Out']

        return DexTrade(
            dex_pair=dex_pair,
            tx=self.get_tx(swap_info.transactionHash),
            log_index=swap_info.logIndex,
            token_delta=token_in - token_out,
            wbnb_delta=wbnb_in - wbnb_out
        )