# Most of this taken from the Uniswap python library
import json
import logging
import threading
import random
import time
from functools import lru_cache
from typing import Union, Optional, Tuple
from urllib import request

from eth_account.signers.local import LocalAccount
from eth_typing import Address, ChecksumAddress
from hexbytes import HexBytes
from web3 import Web3
from web3.contract import ContractFunction, Contract
from web3.exceptions import TransactionNotFound
from web3.middleware import geth_poa_middleware
from web3.types import Wei, TxParams

TEST_MODE_DRY_RUN = False

AddressLike = Union[Address, ChecksumAddress]

ERC20_ABI = '[  {    "constant": true,    "inputs": [],    "name": "name",    "outputs": [      {        "name": "",        "type": "string"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": true,    "inputs": [],    "name": "symbol",    "outputs": [      {        "name": "",        "type": "string"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": true,    "inputs": [],    "name": "decimals",    "outputs": [      {        "name": "",        "type": "uint8"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "name": "spender",        "type": "address"      },      {        "name": "value",        "type": "uint256"      }    ],    "name": "approve",    "outputs": [      {        "name": "",        "type": "bool"      }    ],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  },  {    "constant": true,    "inputs": [],    "name": "totalSupply",    "outputs": [      {        "name": "",        "type": "uint256"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "name": "from",        "type": "address"      },      {        "name": "to",        "type": "address"      },      {        "name": "value",        "type": "uint256"      }    ],    "name": "transferFrom",    "outputs": [      {        "name": "",        "type": "bool"      }    ],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "name": "spender",        "type": "address"      },      {        "name": "addedValue",        "type": "uint256"      }    ],    "name": "increaseAllowance",    "outputs": [      {        "name": "",        "type": "bool"      }    ],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  },  {    "constant": true,    "inputs": [      {        "name": "owner",        "type": "address"      }    ],    "name": "balanceOf",    "outputs": [      {        "name": "",        "type": "uint256"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "name": "spender",        "type": "address"      },      {        "name": "subtractedValue",        "type": "uint256"      }    ],    "name": "decreaseAllowance",    "outputs": [      {        "name": "",        "type": "bool"      }    ],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "name": "to",        "type": "address"      },      {        "name": "value",        "type": "uint256"      }    ],    "name": "transfer",    "outputs": [      {        "name": "",        "type": "bool"      }    ],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  },  {    "constant": true,    "inputs": [      {        "name": "owner",        "type": "address"      },      {        "name": "spender",        "type": "address"      }    ],    "name": "allowance",    "outputs": [      {        "name": "",        "type": "uint256"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "anonymous": false,    "inputs": [      {        "indexed": true,        "name": "from",        "type": "address"      },      {        "indexed": true,        "name": "to",        "type": "address"      },      {        "indexed": false,        "name": "value",        "type": "uint256"      }    ],    "name": "Transfer",    "type": "event"  },  {    "anonymous": false,    "inputs": [      {        "indexed": true,        "name": "owner",        "type": "address"      },      {        "indexed": true,        "name": "spender",        "type": "address"      },      {        "indexed": false,        "name": "value",        "type": "uint256"      }    ],    "name": "Approval",    "type": "event"  }]'

BSCSCAN_APIKEY = "1JPUBHDJQRM6XDCCM89JIZ2MR1IFMIK1Q5"
BNB_TO_DECIMALS = 10 ** 18
GWEI = 1000000000

WBNB_ADDRESS = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
TESTNET_WBNB_ADDRESS = "0xae13d989daC2f0dEbFf460aC112a837C89BAa7cd"
EMPTY_CONTRACT = '0x0000000000000000000000000000000000000000'

DEFAULT_GAS_LIMIT = 1500000
PANCAKE_SWAP_ROUTER = "0x10ED43C718714eb63d5aA57B78B54704E256024E"
PANCAKE_SWAP_ROUTER_ABI = '[{"inputs":[{"internalType":"address","name":"_factory","type":"address"},{"internalType":"address","name":"_WETH","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"WETH","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"amountADesired","type":"uint256"},{"internalType":"uint256","name":"amountBDesired","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"amountTokenDesired","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"addLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"},{"internalType":"uint256","name":"liquidity","type":"uint256"}],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountIn","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"reserveIn","type":"uint256"},{"internalType":"uint256","name":"reserveOut","type":"uint256"}],"name":"getAmountOut","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsIn","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],"name":"getAmountsOut","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"reserveA","type":"uint256"},{"internalType":"uint256","name":"reserveB","type":"uint256"}],"name":"quote","outputs":[{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETH","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"removeLiquidityETHSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermit","outputs":[{"internalType":"uint256","name":"amountToken","type":"uint256"},{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"token","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountTokenMin","type":"uint256"},{"internalType":"uint256","name":"amountETHMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityETHWithPermitSupportingFeeOnTransferTokens","outputs":[{"internalType":"uint256","name":"amountETH","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint256","name":"liquidity","type":"uint256"},{"internalType":"uint256","name":"amountAMin","type":"uint256"},{"internalType":"uint256","name":"amountBMin","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bool","name":"approveMax","type":"bool"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"removeLiquidityWithPermit","outputs":[{"internalType":"uint256","name":"amountA","type":"uint256"},{"internalType":"uint256","name":"amountB","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapETHForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactETHForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForETHSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMin","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapExactTokensForTokensSupportingFeeOnTransferTokens","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactETH","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMax","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"swapTokensForExactTokens","outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],"stateMutability":"nonpayable","type":"function"},{"stateMutability":"payable","type":"receive"}]'
PANCAKE_SWAP_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKE_SWAP_FACTORY_ABI = '[  {    "anonymous": false,    "inputs": [      {        "indexed": true,        "internalType": "address",        "name": "token0",        "type": "address"      },      {        "indexed": true,        "internalType": "address",        "name": "token1",        "type": "address"      },      {        "indexed": false,        "internalType": "address",        "name": "pair",        "type": "address"      },      {        "indexed": false,        "internalType": "uint256",        "name": "",        "type": "uint256"      }    ],    "name": "PairCreated",    "type": "event"  },  {    "constant": true,    "inputs": [      {        "internalType": "uint256",        "name": "",        "type": "uint256"      }    ],    "name": "allPairs",    "outputs": [      {        "internalType": "address",        "name": "pair",        "type": "address"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": true,    "inputs": [],    "name": "allPairsLength",    "outputs": [      {        "internalType": "uint256",        "name": "",        "type": "uint256"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "internalType": "address",        "name": "tokenA",        "type": "address"      },      {        "internalType": "address",        "name": "tokenB",        "type": "address"      }    ],    "name": "createPair",    "outputs": [      {        "internalType": "address",        "name": "pair",        "type": "address"      }    ],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  },  {    "constant": true,    "inputs": [],    "name": "feeTo",    "outputs": [      {        "internalType": "address",        "name": "",        "type": "address"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": true,    "inputs": [],    "name": "feeToSetter",    "outputs": [      {        "internalType": "address",        "name": "",        "type": "address"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": true,    "inputs": [      {        "internalType": "address",        "name": "tokenA",        "type": "address"      },      {        "internalType": "address",        "name": "tokenB",        "type": "address"      }    ],    "name": "getPair",    "outputs": [      {        "internalType": "address",        "name": "pair",        "type": "address"      }    ],    "payable": false,    "stateMutability": "view",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "internalType": "address",        "name": "",        "type": "address"      }    ],    "name": "setFeeTo",    "outputs": [],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  },  {    "constant": false,    "inputs": [      {        "internalType": "address",        "name": "",        "type": "address"      }    ],    "name": "setFeeToSetter",    "outputs": [],    "payable": false,    "stateMutability": "nonpayable",    "type": "function"  }]'

PANCAKE_SWAP_LP_ABI = '[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]'

TESTNET_PANCAKE_SWAP_ROUTER = "0x9Ac64Cc6e4415144C455BD8E4837Fea55603e5c3"
TESTNET_PANCAKE_SWAP_FACTORY = "0xB7926C0430Afb07AA7DEfDE6DA862aE0Bde767bc"

MAX_APPROVAL_INT = int(f"0x{64 * 'f'}", 16)

logger = logging.getLogger("my_app")

# Sorted from best to worst.
WEB3_PROVIDERS = [
    Web3.HTTPProvider("https://bsc-dataseed.binance.org/"),
    Web3.HTTPProvider("https://bsc-dataseed1.defibit.io/"),
    Web3.HTTPProvider("https://bsc-dataseed1.ninicoin.io/"),
    Web3.HTTPProvider("https://bsc-dataseed2.defibit.io/"),
    Web3.HTTPProvider("https://bsc-dataseed3.defibit.io/"),
    Web3.HTTPProvider("https://bsc-dataseed4.defibit.io/"),
    Web3.HTTPProvider("https://bsc-dataseed2.ninicoin.io/"),
    Web3.HTTPProvider("https://bsc-dataseed3.ninicoin.io/"),
    Web3.HTTPProvider("https://bsc-dataseed4.ninicoin.io/"),
    Web3.HTTPProvider("https://bsc-dataseed1.binance.org/"),
    Web3.HTTPProvider("https://bsc-dataseed2.binance.org/"),
    Web3.HTTPProvider("https://bsc-dataseed3.binance.org/"),
    Web3.HTTPProvider("https://bsc-dataseed4.binance.org/"),
]

WEB3_TESTNET_PROVIDERS = [
    Web3.HTTPProvider("https://data-seed-prebsc-2-s1.binance.org:8545/")
]

__providers_used = {
    False: {provider: (0, index) for index, provider, in enumerate(WEB3_PROVIDERS)}, # For mainnet
    True: {provider: (0, index) for index, provider in enumerate(WEB3_TESTNET_PROVIDERS)}, # For testnet
}
__provider_lock = threading.Lock()
MAX_RETRIES = 5 * sum(len(candidates) for candidates in __providers_used.values())

@lru_cache(maxsize=None)
def _create_best_provider(thread: threading.Thread, testnet=False) -> Web3:
    with __provider_lock:
        logger.debug(f"Creating WEB3 instance for {thread}")

        for retries in range(MAX_RETRIES):
            best_provider = min(__providers_used[testnet], key=__providers_used[testnet].get)
            try:
                web3 = Web3(best_provider)
                web3.middleware_onion.inject(geth_poa_middleware, layer=0)
                if not web3.isConnected():
                    raise ValueError("Not connected!")

                logger.debug(f"{threading.current_thread().name} got provider: {best_provider}")
                return web3
            except (Exception,) as e:
                logger.warning(f"Skipping {best_provider}: {e}")
            finally:
                n_used, priority = __providers_used[testnet][best_provider]
                __providers_used[testnet][best_provider] = n_used + 1, priority

        raise ValueError(f"No provider available after {MAX_RETRIES} tries!")


def get_w3(testnet=False) -> Web3:
    return _create_best_provider(threading.current_thread(), testnet)


def _deadline() -> int:
    return int(time.time()) + 60 * 10  # 10 minutes


def _addr_to_str(a: AddressLike) -> str:
    if isinstance(a, bytes):
        # Address or ChecksumAddress
        addr: str = Web3.toChecksumAddress("0x" + bytes(a).hex())
        return addr
    elif isinstance(a, str) and a.startswith("0x"):
        addr = Web3.toChecksumAddress(a)
        return addr

    raise ValueError(f"Invalid _addr_to_str: {a}")


def if_liquidity_tx_get_args(w3, tx):
    if not tx.to == PANCAKE_SWAP_ROUTER:
        return None

    try:
        fn, args = decode_tx_input(w3, tx)
    except (Exception,):
        return None

    if fn.fn_name == 'addLiquidityETH':
        return args

    return None


def if_buy_get_args(w3, tx):
    if not tx.to == PANCAKE_SWAP_ROUTER:
        return None

    try:
        fn, args = decode_tx_input(w3, tx)
    except (Exception,):
        return None

    if (fn.fn_name.startswith('swapExactETH') or fn.fn_name.startswith('swapETH')) \
            and args['path'][0] == WBNB_ADDRESS and len(args['path']) > 1:
        args['token'] = args['path'][-1]
        return args

    return None


def if_sell_get_args(w3: Web3, tx):
    if not tx.to == PANCAKE_SWAP_ROUTER:
        return None

    try:
        fn, args = decode_tx_input(w3, tx)
    except (Exception,):
        return None

    if (fn.fn_name.startswith('swapExactTokens') or fn.fn_name.startswith('swapTokens')) and \
            'path' in args and \
            len(args['path']) > 1 and \
            args['path'][-1] == WBNB_ADDRESS:
        args['token'] = args['path'][0]
        return args

    return None


WORD_LISTS = [
    ("set", "trading", "enabled"),
    ("sales", "begin"),
    ("open", "gates"),
    ("open", "trading"),
    ("trading", "status"),
    ("setKillSniper",),  # 0x27ca4a34EE832dbf4EBCf4a43c2a40BBE44b4A48   17/08/2021
    ('enable', 'trading'),
    ('setBuyBackEnabled',)  # 0x10f1148a023D7f6F41d3c4F39B0E9e96c6fdCf31 17/08/2021
]


def looks_like_enable_trade_name(function_name):
    fn = function_name.lower()

    for wordlist in WORD_LISTS:
        all_in = True
        for word in wordlist:
            if word.lower() not in fn:
                all_in = False
                break
        if all_in:
            return True

    return False


# Doesnt always work..
def is_erc20(w3: Web3, tx):
    try:
        get_erc20_contract(w3, tx.to).functions.balanceOf("0xF7E3Dc977963800D27a32B89E54c35E57753E0c6").call()
        return True
    except (Exception,):
        return False


def enables_trade_for_token(w3, tx, token):
    if token and tx.to != token:
        return False
    if tx.to in (None, PANCAKE_SWAP_ROUTER, PANCAKE_SWAP_FACTORY):
        return False
    if tx.value > 0 or len(tx.input) <= 4:
        return False
    if not is_erc20(w3, tx):
        return False

    try:
        fn, args = decode_tx_input(w3, tx)
    except (Exception,) as e:
        logger.warning(f"No se pudo decodificar la transaccion: {tx.hash.hex()}: {e}")
        return False

    return looks_like_enable_trade_name(fn.fn_name)


def is_confirmed_buy_for_token(w3, tx, token):
    try:
        tx_reversed = get_w3().eth.get_transaction_receipt(tx.hash).status == 0
        if tx_reversed:
            return False
        else:
            args = if_buy_get_args(w3, tx)
            return args and args['token'] == token
    except TransactionNotFound:
        return False


def get_contract_abi(contract):
    for retries in range(5):
        response = None
        try:
            url = f"https://api.bscscan.com/api?module=contract&action=getabi&address={contract}&apikey={BSCSCAN_APIKEY}"
            f = request.urlopen(url)
            response = json.load(f)
            return json.loads(response["result"])
        except (Exception,):
            if "source code not verified" in response.get('result', ''):
                raise ValueError("Source code not verified")
            logger.exception(f"Error on get_contract_abi({contract}): {response}")
            time.sleep(1)

    return None


def get_nowbnb_token(token0, token1):
    if token0 == WBNB_ADDRESS:
        return token1
    elif token1 == WBNB_ADDRESS:
        return token0
    return None


def get_contract(w3, addr: Union[str, AddressLike], abi=None) -> Contract:
    return w3.eth.contract(address=addr, abi=abi if abi else get_contract_abi(addr))


@lru_cache()
def get_erc20_contract(w3, addr) -> Contract:
    return get_contract(w3, addr, abi=ERC20_ABI)


def decode_tx_input(web_provider, tx):
    contract = get_contract(web_provider, tx.to)
    return contract.decode_function_input(tx.input)

def get_lptoken_contract(w3, addr: ChecksumAddress):
    return w3.eth.contract(address=addr, abi=PANCAKE_SWAP_LP_ABI)

def get_router_contract(w3, testnet=False) -> Contract:
    router_addr = TESTNET_PANCAKE_SWAP_ROUTER if testnet else PANCAKE_SWAP_ROUTER
    return w3.eth.contract(address=router_addr, abi=PANCAKE_SWAP_ROUTER_ABI)


def get_factory_contract(w3):
    return get_contract(w3, w3.toChecksumAddress(PANCAKE_SWAP_FACTORY))


@lru_cache()
def get_erc20_wbnb_pair_contract(web3, token, testnet=False):
    factory_abi = json.loads(PANCAKE_SWAP_FACTORY_ABI)
    factory_address = TESTNET_PANCAKE_SWAP_FACTORY if testnet else PANCAKE_SWAP_FACTORY
    factory_contract = web3.eth.contract(address=factory_address, abi=factory_abi)

    return factory_contract.functions.getPair(
        web3.toChecksumAddress(token),
        web3.toChecksumAddress(TESTNET_WBNB_ADDRESS if testnet else WBNB_ADDRESS)
    ).call()


def has_liquidity(w3, token) -> bool:
    wbnb_balance, _, _ = get_pool_info(w3, token, only_bnb=True)
    return wbnb_balance and wbnb_balance > 0


def get_pool_info(w3, token, only_bnb=False, testnet=False) -> Tuple[Optional[int], Optional[int], Optional[float]]:
    wbnb_address = w3.toChecksumAddress(TESTNET_WBNB_ADDRESS if testnet else WBNB_ADDRESS)
    erc20_wbnb = get_erc20_contract(w3, wbnb_address)

    owner = get_erc20_wbnb_pair_contract(w3, token, testnet=testnet)
    if owner == EMPTY_CONTRACT:
        return None, None, None

    bnb_balance: int = erc20_wbnb.functions.balanceOf(owner).call()
    if only_bnb:
        return bnb_balance, None, None

    token_address = w3.toChecksumAddress(token)
    erc20 = get_erc20_contract(w3, token_address)

    token_balance: int = erc20.functions.balanceOf(owner).call()

    price = .0 if token_balance == 0 else float(bnb_balance / token_balance)  # price of token

    return bnb_balance, token_balance, price


def approve_account(
        w3: Web3,
        token: AddressLike,
        account: LocalAccount,
        allowed_spender: AddressLike = PANCAKE_SWAP_ROUTER
) -> Optional[HexBytes]:
    return approve(w3, token, account.address, account.privateKey, allowed_spender)


def approve(
        w3: Web3,
        token: AddressLike,
        my_address: AddressLike,
        private_key,
        allowed_spender: AddressLike = PANCAKE_SWAP_ROUTER
) -> Optional[HexBytes]:
    if TEST_MODE_DRY_RUN:
        return None

    token = w3.toChecksumAddress(token)
    token_contract = get_erc20_contract(w3, token)

    # Check not approved already
    amount = token_contract.functions.allowance(my_address, allowed_spender).call()
    if amount > 0:
        # Already approved
        return None

    function = token_contract.functions.approve(allowed_spender, MAX_APPROVAL_INT)
    tx = _build_and_send_tx(w3, function,
                            {
                                "from": _addr_to_str(my_address),
                                "value": 0,
                                "gas": DEFAULT_GAS_LIMIT,
                            },
                            my_address,
                            private_key=private_key,
                            )

    return tx


def wait_and_validate_tx(w3: Web3, tx: HexBytes, poll_latency=1, timeout=120):
    tx_receipt = w3.eth.wait_for_transaction_receipt(tx, timeout=timeout, poll_latency=poll_latency)

    if tx_receipt['status'] != 1:
        raise ValueError(f"Tx Failed! {tx.hex()}")

    return tx_receipt


TX_STANDARD_PRICE = 21000


def send_funds(w3: Web3, origin: LocalAccount, destination: ChecksumAddress, amount: int = None, all_funds=False,
               testnet=False, validate_tx=True):
    if amount is None and not all_funds:
        raise ValueError("Specify either amount or all_funds")

    raw_tx = {
        "from": origin.address,
        "nonce": max(last_nonce.get(origin.address, -1) + 1, w3.eth.getTransactionCount(origin.address)),
        "gasPrice": 10000000000 if testnet else 5000000000,
        "gas": TX_STANDARD_PRICE,
        "to": destination,
        "value": amount
    }

    if all_funds:
        raw_tx['value'] = w3.eth.get_balance(origin.address) - TX_STANDARD_PRICE * raw_tx['gasPrice']

    signed_txn = w3.eth.account.sign_transaction(
        raw_tx, private_key=origin.privateKey
    )

    try:
        tx = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        if validate_tx:
            wait_and_validate_tx(w3, tx)
        return tx
    finally:
        last_nonce[origin.address] = raw_tx['nonce']


def quick_buy(
        w3,
        token_to_buy: AddressLike,
        qty: Union[int, Wei],
        recipient: AddressLike,
        private_key,
        gas_limit: int = None,
        gas_price: int = None,
        testnet: bool = False
) -> HexBytes:
    if not recipient:
        raise ValueError("No recipient?!")

    qty = Wei(qty)

    return _build_and_send_tx(
        w3,
        get_router_contract(w3, testnet=testnet).functions.swapExactETHForTokens(
            0,  # Amount min
            [TESTNET_WBNB_ADDRESS if testnet else WBNB_ADDRESS, token_to_buy],
            recipient,
            _deadline(),
        ),
        {
            "from": _addr_to_str(recipient),
            "value": int(qty),
            "gas": DEFAULT_GAS_LIMIT,
        },
        my_address=recipient,
        private_key=private_key,
        gas_price=gas_price,
        gas_limit=gas_limit
    )


def quick_sell(
        w3,
        token_to_sell: AddressLike,
        qty: Union[int, Wei],
        recipient: AddressLike,
        private_key,
        gas_limit: int = None,
        gas_price: int = None
) -> HexBytes:
    if not recipient:
        raise ValueError("No recipient?!")

    qty = Wei(qty)

    return _build_and_send_tx(
        w3,
        get_router_contract(w3).functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
            0,  # amountIn
            qty,  # amountOutMin
            [WBNB_ADDRESS, token_to_sell],
            recipient,
            _deadline(),
        ),
        {
            "from": _addr_to_str(recipient),
            "gas": DEFAULT_GAS_LIMIT,
        },
        my_address=recipient,
        private_key=private_key,
        gas_price=gas_price,
        gas_limit=gas_limit
    )


last_nonce = {}

DRY_RUN_OK_PROBABLITY = 0.75  # 75% chance to generate an OK tx
TEST_TX_OK = HexBytes('0x52ad8cd961d31c0885332d1921506afe2a3dc3aac7d7ae3af755d861b2bee05e')
TEST_TX_FAIL = HexBytes('0x6fec0c67390b8d8183863a6cf92f4eeccb88de5af79583ce62def1f7f9b78e09')


def _build_and_send_tx(
        w3,
        function: ContractFunction,
        tx_params: TxParams,
        my_address: AddressLike,
        private_key,
        gas_price: int = None,
        gas_limit: int = None
) -> HexBytes:
    global last_nonce

    tx_params['nonce'] = max(last_nonce.get(my_address, -1) + 1, w3.eth.getTransactionCount(my_address))

    if gas_price:
        tx_params['gasPrice'] = gas_price
    if gas_limit:
        tx_params['gas'] = gas_limit

    transaction = function.buildTransaction(tx_params)
    signed_txn = w3.eth.account.sign_transaction(
        transaction, private_key=private_key
    )

    if TEST_MODE_DRY_RUN:
        tx = TEST_TX_OK if random.random() < DRY_RUN_OK_PROBABLITY else TEST_TX_FAIL
        logger.warning(f"DRY RUN _build_and_send_tx: generating {'GOOD' if tx == TEST_TX_OK else 'FAILED'} tx")
        return tx

    try:
        return w3.eth.send_raw_transaction(signed_txn.rawTransaction)
    finally:
        last_nonce[my_address] = tx_params['nonce']


if __name__ == '__main__':
    print("TEST web3_utils")
    test_words = [
        'setTradingIsEnabled',
        'SetupEnableTrading'
    ]
    for test in test_words:
        result = looks_like_enable_trade_name(test)
        print(f"{test}: {result}")

    test_abi = get_contract_abi('0xe766DcB4b71b313a465Fe00868a24555257aF77F')
    print(test_abi)
