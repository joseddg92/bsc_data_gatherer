from collections import namedtuple
from enum import Enum
from typing import Optional, Union

from hexbytes import HexBytes
from web3 import Web3
from web3.types import TxData

from web3_utils import get_w3

DexInfo = namedtuple("DexInfo", ["name", "router_addr", "factory_addr"])


class Dex(DexInfo, Enum):
    PANCAKESWAP = DexInfo(
        name="pancakeswap",
        router_addr=Web3.toChecksumAddress("0x10ED43C718714eb63d5aA57B78B54704E256024E"),
        factory_addr=Web3.toChecksumAddress("0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73")
    )

    APESWAP = DexInfo(
        name="apeswap",
        router_addr=Web3.toChecksumAddress("0xcF0feBd3f17CEf5b47b0cD257aCf6025c5BFf3b7"),
        factory_addr=Web3.toChecksumAddress("0x0841BD0B734E4F5853f0dD8d7Ea041c241fb0Da6")
    )


def get_dex(tx_data: TxData) -> Optional[Dex]:
    for dex in Dex:
        if tx_data['to'] == dex.router_addr:
            return dex
    return None


class ParsedTx:
    w3 = get_w3()

    def __init__(self, tx: Union[HexBytes, TxData]):
        self.tx_data: TxData = self.w3.eth.get_transaction(tx) if isinstance(tx, HexBytes) else tx
        self.tx_hash: HexBytes = self.tx_data['hash']

        self.dex = get_dex(self.tx_data)

    def is_dex_tx(self):
        return self.dex is not None

    def __str__(self):
        return f"{self.tx_data}"
