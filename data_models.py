from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from eth_typing import ChecksumAddress
from sqlalchemy import Column, Table, String, Integer, BigInteger, DateTime, ForeignKey, Boolean, Numeric, Sequence
from web3 import Web3
from web3.contract import Contract

from web3_utils import get_w3, get_lptoken_contract

from sqlalchemy.orm import registry, relationship

mapper_registry = registry()


@dataclass(unsafe_hash=True)
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

    def __str__(self):
        return self.dex_name


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


@dataclass(unsafe_hash=True)
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

    def __str__(self):
        return f"{self.name} ({self.symbol})"


@dataclass(unsafe_hash=True)
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

    def __str__(self):
        return f"Block {self.number}"


@dataclass(unsafe_hash=True)
@mapper_registry.mapped
class Tx:
    __table__ = Table(
        "tx",
        mapper_registry.metadata,
        Column("hash", String(), primary_key=True),
        Column("block_number", BigInteger(), ForeignKey("block.number"), nullable=False),
        Column("transaction_index", Integer(), nullable=False),
        Column("gas_price", BigInteger(), nullable=False),
    )

    __mapper_args__ = {  # type: ignore
        "properties": {
            "block": relationship("Block")
        }
    }

    hash: str
    block: Block
    transaction_index: int
    gas_price: int

    def __str__(self):
        return f"tx{self.hash[:4]}...{self.hash[-4:]}"


@dataclass(unsafe_hash=True)
@mapper_registry.mapped
class DexTradePair:
    __table__ = Table(
        "dex_trade_pair",
        mapper_registry.metadata,
        Column("id", BigInteger(), Sequence('dex_trade_pair_id'), unique=True),
        Column("pair_addr", String(), primary_key=True),
        Column("token_address", String(), ForeignKey("token.address")),
        Column("dex_name", String(), ForeignKey("dex.dex_name")),
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

    pair_addr: str
    dex: DecentralizedExchange
    token: Token
    creator_tx: Tx
    is_token0_wbnb: bool

    __pair_contract: Optional[Contract] = field(default=None, init=False, repr=False, hash=False, compare=False)


    def get_pair_addr(self) -> ChecksumAddress:
        return Web3.toChecksumAddress(self.pair_addr)

    def pair_contract(self) -> Contract:
        if not self.__pair_contract:
            self.__pair_contract = get_lptoken_contract(get_w3(), self.get_pair_addr())
        return self.__pair_contract

    def __str__(self):
        return f"Pair for {self.token} ({self.dex})"



@dataclass(unsafe_hash=True)
@mapper_registry.mapped
class DexTrade:
    __table__ = Table(
        "dex_trade",
        mapper_registry.metadata,
        Column("id", BigInteger(), Sequence('dex_trade_id'), primary_key=True),
        Column("dex_pair_id", BigInteger(), ForeignKey("dex_trade_pair.id"), nullable=False),
        Column("tx_hash", String(), ForeignKey("tx.hash"), nullable=False),
        Column("log_index", Integer(), nullable=False),
        Column("token_in", Numeric(precision=78, scale=0), nullable=False),
        Column("token_out", Numeric(precision=78, scale=0), nullable=False),
        Column("wbnb_in", Numeric(precision=78, scale=0), nullable=False),
        Column("wbnb_out", Numeric(precision=78, scale=0), nullable=False),
    )

    __mapper_args__ = {  # type: ignore
        "properties": {
            "dex_pair": relationship("DexTradePair"),
            "tx": relationship("Tx"),
        }
    }

    dex_pair: DexTradePair
    tx: Tx
    log_index: int
    token_in: int
    token_out: int
    wbnb_in: int
    wbnb_out: int

    def __str__(self):
        return f"trade for {self.dex_pair}"
