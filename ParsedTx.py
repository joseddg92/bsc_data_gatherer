from collections import namedtuple
from enum import Enum, auto
from typing import Optional, Any, Dict

from eth_typing import ChecksumAddress
from web3 import Web3
from web3.contract import ContractFunction
from web3.types import TxData, TxReceipt

from web3_utils import get_w3, get_router_contract, get_erc20_contract, WBNB_ADDRESS

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


class OrderType(Enum):
    ADD_LIQUIDITY = auto()
    REMOVE_LIQUIDITY = auto()
    BUY = auto()
    SELL = auto()


class DexContractFunctionType:
    def __init__(self, token_path_index: int, order_type: OrderType = None):
        self.token_path_index = token_path_index
        self.order_type = order_type


# See https://docs.pancakeswap.finance/code/smart-contracts/pancakeswap-exchange/router-v2
# Only adding ETH-related functions (really WBNB-related, for the BSC)
class DexContractFunction(DexContractFunctionType, Enum):
    swapETHForExactTokens = DexContractFunctionType(-1, OrderType.BUY)
    swapExactETHForTokens = DexContractFunctionType(-1, OrderType.BUY)
    swapExactETHForTokensSupportingFeeOnTransferTokens = DexContractFunctionType(-1, OrderType.BUY)
    swapExactTokensForETH = DexContractFunctionType(0, OrderType.SELL)
    swapExactTokensForETHSupportingFeeOnTransferTokens = DexContractFunctionType(0, OrderType.SELL)


def get_DexContractFunction_by_name(name: str) -> Optional[DexContractFunction]:
    for dex_contract_fn in DexContractFunction:
        if name == dex_contract_fn.name:
            return dex_contract_fn
    return None


def get_dex(tx_data: TxData) -> Optional[Dex]:
    for dex in Dex:
        if tx_data['to'] == dex.router_addr:
            return dex
    return None


class Token:
    w3 = get_w3()

    def __init__(self, addr: ChecksumAddress):
        if isinstance(addr, str):
            addr = Web3.toChecksumAddress(addr)

        self.addr = addr
        self.contract = get_erc20_contract(self.w3, self.addr)
        self.name = self.contract.functions.name().call()
        self.symbol = self.contract.functions.symbol().call()

    def __str__(self):
        return f"[{self.name} ({self.symbol}) at {self.addr[:4]}...{self.addr[-4:]}]"


class ParsedTx:
    w3 = get_w3()
    _router_contract = get_router_contract(w3)

    def __init__(self, tx: TxData, get_receipt=False):
        self.tx_data = tx
        self.tx_hash = self.tx_data['hash']

        self.tx_receipt: Optional[TxReceipt] = None
        self.reverted: Optional[bool] = None

        self.fn: Optional[ContractFunction] = None
        self.fn_args: Optional[Dict[str, Any]] = None
        self.dex_fn_type: Optional[DexContractFunction] = None
        self.token: Optional[Token] = None

        self.dex = get_dex(self.tx_data)
        if self.dex:
            self.fn, self.fn_args = self._router_contract.decode_function_input(self.tx_data['input'])
            self.dex_fn_type = get_DexContractFunction_by_name(self.fn.fn_name)
            if self.dex_fn_type:
                self.token = Token(self.fn_args['path'][self.dex_fn_type.value.token_path_index])

        if self.dex or get_receipt:
            self.tx_receipt: TxReceipt = self.w3.eth.get_transaction_receipt(self.tx_hash)
            self.reverted = self.tx_receipt['status'] != 1

    def is_dex_tx(self):
        return self.dex is not None

    def is_simple_wbnb_tx(self):
        return self.is_dex_tx() and \
               self.dex_fn_type is not None and \
               len(self.fn_args['path']) == 2 and \
               WBNB_ADDRESS in self.fn_args['path']

    def __str__(self):
        if self.is_simple_wbnb_tx():
            tx_data_str = f"{self.dex_fn_type.value.order_type.name} {self.token}"
        elif self.is_dex_tx():
            tx_data_str = f"{self.dex.name} tx at {self.tx_hash.hex()}"
        else:
            tx_data_str = f"TX at {self.tx_hash.hex()}"

        status_str = 'FAILED! ' if self.reverted else ''

        return f"[{status_str}{tx_data_str}]"
