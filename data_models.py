from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from eth_typing import ChecksumAddress, Hash32
from hexbytes import HexBytes
from web3 import Web3
from web3.types import TxData

from web3_utils import get_erc20_contract, get_w3


@dataclass
class DecentralizedExchangeType:
    dex_name: str
    router_addr: ChecksumAddress
    factory_addr: ChecksumAddress


class DecentralizedExchange(DecentralizedExchangeType, Enum):
    PANCAKESWAP = "pancakeswap", \
                  Web3.toChecksumAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E"), \
                  Web3.toChecksumAddress("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73")

    APESWAP = "apeswap", \
              Web3.toChecksumAddress("0xcF0feBd3f17CEf5b47b0cD257aCf6025c5BFf3b7"), \
              Web3.toChecksumAddress("0x0841BD0B734E4F5853f0dD8d7Ea041c241fb0Da6")


@dataclass
class Token:
    address: ChecksumAddress
    name: str
    symbol: str
    decimals: int


def get_token(token_addr: ChecksumAddress) -> Token:
    if isinstance(token_addr, str):
        token_addr = Web3.toChecksumAddress(token_addr)

    contract = get_erc20_contract(get_w3(), token_addr)
    name = contract.functions.name().call()
    symbol = contract.functions.symbol().call()
    decimals = contract.functions.decimals().call()

    return Token(token_addr, name, symbol, decimals)


@dataclass
class Block:
    number: int
    timestamp: datetime


def get_block(block_number: int) -> Block:
    block_data = get_w3().eth.get_block(block_number)

    return Block(block_number, datetime.fromtimestamp(block_data['timestamp']))


@dataclass
class Tx:
    hash: Hash32
    block: Block
    gas_price: int


def get_tx(tx_hash: HexBytes) -> Tx:
    tx_data: TxData = get_w3().eth.get_transaction(tx_hash)

    return Tx(tx_hash, get_block(tx_data['blockNumber']), tx_data['gasPrice'])


@dataclass
class DexTradePair:
    dex: DecentralizedExchange
    token: Token
    creator_tx: Tx
    is_token0_wbnb: bool


@dataclass
class DexTrade:
    dex_pair: DexTradePair
    token_in: int
    token_out: int
    wbnb_in: int
    wbnb_out: int


def get_DexTrade(swap_info, dex_pair: DexTradePair) -> DexTrade:
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
        dex_pair,
        token_in,
        token_out,
        wbnb_in,
        wbnb_out
    )
