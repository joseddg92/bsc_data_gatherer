from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from eth_typing import ChecksumAddress, Hash32
from hexbytes import HexBytes
from sqlalchemy import Column, Table, String, Integer, BigInteger, DateTime, ForeignKey, Boolean, Numeric
from web3 import Web3
from web3.types import TxData

from web3_utils import get_erc20_contract, get_w3

from sqlalchemy.orm import registry, relationship

mapper_registry = registry()


@dataclass
@mapper_registry.mapped
class DecentralizedExchangeType:
    __table__ = Table(
        "dex",
        mapper_registry.metadata,
        Column("dex_name", String(), primary_key=True),
        Column("router_addr", String(), nullable=False),
        Column("factory_addr", String(), nullable=False),
    )

    dex_name: str
    router_addr: ChecksumAddress
    factory_addr: ChecksumAddress


class DecentralizedExchange(Enum):
    PANCAKESWAP = DecentralizedExchangeType(
        dex_name="pancakeswap",
        router_addr=Web3.toChecksumAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E"),
        factory_addr=Web3.toChecksumAddress("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73")
    )

    APESWAP = DecentralizedExchangeType(
        dex_name="apeswap",
        router_addr=Web3.toChecksumAddress("0xcF0feBd3f17CEf5b47b0cD257aCf6025c5BFf3b7"),
        factory_addr=Web3.toChecksumAddress("0x0841BD0B734E4F5853f0dD8d7Ea041c241fb0Da6")
    )


@dataclass
@mapper_registry.mapped
class Token:
    __table__ = Table(
        "token",
        mapper_registry.metadata,
        Column("address", String(), primary_key=True),
        Column("name", String(), nullable=False),
        Column("symbol", String(), nullable=False),
        Column("decimals", Integer(), nullable=False),
    )

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

    return Token(address=token_addr, name=name, symbol=symbol, decimals=decimals)


@dataclass
@mapper_registry.mapped
class Block:
    __table__ = Table(
        "block",
        mapper_registry.metadata,
        Column("number", BigInteger(), primary_key=True),
        Column("timestamp", DateTime(), nullable=False)
    )
    number: int
    timestamp: datetime


def get_block(block_number: int) -> Block:
    block_data = get_w3().eth.get_block(block_number)

    return Block(
        number=block_number,
        timestamp=datetime.fromtimestamp(block_data['timestamp'])
    )


@dataclass
@mapper_registry.mapped
class Tx:
    __table__ = Table(
        "tx",
        mapper_registry.metadata,
        Column("hash", String(), primary_key=True, nullable=False),
        Column("block_number", BigInteger(), ForeignKey("block.number"), nullable=False),
        Column("gas_price", BigInteger(), nullable=False),
    )

    __mapper_args__ = {  # type: ignore
        "properties": {
            "block": relationship("Block")
        }
    }

    hash: Hash32
    block: Block
    gas_price: int


def get_tx(tx_hash: HexBytes) -> Tx:
    tx_data: TxData = get_w3().eth.get_transaction(tx_hash)

    return Tx(
        hash=tx_hash,
        block=get_block(tx_data['blockNumber']),
        gas_price=tx_data['gasPrice']
    )


@dataclass
@mapper_registry.mapped
class DexTradePair:
    __table__ = Table(
        "dex_trade_pair",
        mapper_registry.metadata,
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("dex_name", String(), ForeignKey("dex.dex_name"), nullable=False),
        Column("token_address", String(), ForeignKey("token.address"), nullable=False),
        Column("creator_tx_hash", String(), ForeignKey("tx.hash"), nullable=False),
        Column("is_token0_wbnb", Boolean(), nullable=False)
    )

    __mapper_args__ = {  # type: ignore
        "properties": {
            "dex": relationship("DecentralizedExchangeType"),
            "token": relationship("Token"),
            "creator_tx": relationship("Tx")
        }
    }

    dex: DecentralizedExchange
    token: Token
    creator_tx: Tx
    is_token0_wbnb: bool


@dataclass
@mapper_registry.mapped
class DexTrade:
    __table__ = Table(
        "dex_trade",
        mapper_registry.metadata,
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("dex_pair_id", BigInteger(), ForeignKey("dex_trade_pair.id"), nullable=False),
        Column("token_in", Numeric(precision=40, scale=0), nullable=False),
        Column("token_out", Numeric(precision=40, scale=0), nullable=False),
        Column("wbnb_in", Numeric(precision=40, scale=0), nullable=False),
        Column("wbnb_out", Numeric(precision=40, scale=0), nullable=False),
    )

    __mapper_args__ = {  # type: ignore
        "properties": {
            "dex_pair": relationship("DexTradePair"),
        }
    }

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
        dex_pair=dex_pair,
        token_in=token_in,
        token_out=token_out,
        wbnb_in=wbnb_in,
        wbnb_out=wbnb_out
    )
