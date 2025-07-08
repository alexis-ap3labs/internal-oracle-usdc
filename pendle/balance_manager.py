import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import json
from web3 import Web3
from typing import Dict, Any, Tuple
from decimal import Decimal
import requests
import time
from datetime import datetime
from utils.retry import Web3Retry, APIRetry

"""
Pendle balance manager module.
Handles balance fetching and USDC valuation for Pendle Principal Tokens (PT).
Integrates with Pendle's API for accurate price discovery and fallback mechanisms.
"""

# Add parent directory to PYTHONPATH
root_path = str(Path(__file__).parent.parent)
sys.path.append(root_path)

# Load environment variables from parent directory
load_dotenv(Path(root_path) / '.env')

from config.networks import RPC_URLS, CHAIN_IDS
from cowswap.cow_client import get_quote
from pendle.pool import get_all_pools, get_pool_info

# USDC token configuration
USDC_TOKENS = {
    "ethereum": {
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "decimals": 6,
        "name": "USD Coin",
        "symbol": "USDC"
    },
    "base": {
        "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "decimals": 6,
        "name": "USD Coin",
        "symbol": "USDC"
    }
}

# Replace PT_ABI with minimal ABI
MINIMAL_PT_ABI = [
    {
        "constant": True,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            }
        ],
        "name": "balanceOf",
        "outputs": [
            {
                "name": "balance",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

# Add LP token ABI
MINIMAL_LP_ABI = [
    {
        "constant": True,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            }
        ],
        "name": "balanceOf",
        "outputs": [
            {
                "name": "balance",
                "type": "uint256"
            }
        ],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

# Add PendleRouterV4 ABI
PENDLE_ROUTER_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "address", "name": "receiver", "type": "address"}
        ],
        "name": "claimRewards",
        "outputs": [
            {"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}
        ],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "address", "name": "user", "type": "address"}
        ],
        "name": "getRewardTokens",
        "outputs": [
            {"internalType": "address[]", "name": "tokens", "type": "address[]"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "address", "name": "user", "type": "address"},
            {"internalType": "address", "name": "token", "type": "address"}
        ],
        "name": "getRewardAmount",
        "outputs": [
            {"internalType": "uint256", "name": "amount", "type": "uint256"}
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "address", "name": "receiver", "type": "address"},
            {"internalType": "uint256", "name": "netLpToRemove", "type": "uint256"},
            {"internalType": "uint256", "name": "minPtOut", "type": "uint256"},
            {"internalType": "uint256", "name": "minSyOut", "type": "uint256"}
        ],
        "name": "removeLiquidityDualSyAndPt",
        "outputs": [
            {"internalType": "uint256", "name": "netPtOut", "type": "uint256"},
            {"internalType": "uint256", "name": "netSyOut", "type": "uint256"}
        ],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

class PendleBalanceManager:
    """
    Unified manager for Pendle positions handling:
    - Smart contract interactions
    - Balance fetching
    - Price discovery
    - USDC conversion
    """
    
    API_CONFIG = {
        "base_url": "https://api-v2.pendle.finance/core/v1/sdk",
        "default_slippage": "0.01",  # Changed to decimal percentage (0.01 = 1%)
        "enable_aggregator": "true"
    }
    
    MAX_PRICE_IMPACT = 0.05  # 5%
    
    def __init__(self):
        # Initialize Web3 connections
        self.eth_w3 = Web3(Web3.HTTPProvider(RPC_URLS['ethereum']))
        self.base_w3 = Web3(Web3.HTTPProvider(RPC_URLS['base']))
        
        # Initialize contracts
        self.contracts = self._init_contracts()
        
        # Initialize PendleRouterV4
        self.router = {
            'ethereum': self.eth_w3.eth.contract(
                address=Web3.to_checksum_address('0x888888888889758f76e7103c6cbf23abbf58f946'),
                abi=PENDLE_ROUTER_ABI
            ),
            'base': self.base_w3.eth.contract(
                address=Web3.to_checksum_address('0x888888888889758f76e7103c6cbf23abbf58f946'),
                abi=PENDLE_ROUTER_ABI
            )
        }
        
        # Initialize current timestamp
        self.current_timestamp = int(datetime.now().timestamp())

    def _init_contracts(self) -> Dict:
        """Initialize Web3 contract instances for all Pendle PT tokens"""
        contracts = {}
        all_pools = get_all_pools()
        
        for network, tokens in all_pools.items():
            contracts[network] = {}
            w3 = self.eth_w3 if network == 'ethereum' else self.base_w3
            
            for token_symbol, token_data in tokens.items():
                contracts[network][token_symbol] = w3.eth.contract(
                    address=Web3.to_checksum_address(token_data['address']),
                    abi=MINIMAL_PT_ABI
                )
        
        return contracts

    def is_pt_expired(self, token_data: Dict) -> bool:
        """
        Check if a PT token is expired
        
        Args:
            token_data: Token data from pool info
            
        Returns:
            bool: True if token is expired, False otherwise
        """
        expiry = token_data.get('expiry')
        if not expiry:
            return False
        return self.current_timestamp > expiry

    def _get_raw_balances(self, address: str) -> Dict:
        """Get raw balances from smart contracts"""
        try:
            checksum_address = Web3.to_checksum_address(address)
            balances = {}
            
            for network, network_contracts in self.contracts.items():
                if network_contracts:
                    balances[network] = {}
                    
                    for token_symbol, contract in network_contracts.items():
                        token_data = get_pool_info(network, token_symbol)[token_symbol]
                        balance = Web3Retry.call_contract_function(
                            contract.functions.balanceOf(checksum_address).call
                        )
                        
                        if balance > 0:
                            balances[network][token_symbol] = {
                                "amount": str(balance),
                                "decimals": token_data["decimals"]
                            }
            
            return balances
            
        except Exception as e:
            print(f"Error getting Pendle balances: {e}")
            return {}

    def _get_lp_balances(self, address: str) -> Dict:
        """Get LP token balances from smart contracts"""
        try:
            checksum_address = Web3.to_checksum_address(address)
            balances = {}
            
            for network, pools in get_all_pools().items():
                if network not in balances:
                    balances[network] = {}
                
                w3 = self.eth_w3 if network == 'ethereum' else self.base_w3
                
                for pt_symbol, pool_data in pools.items():
                    # Get market address which is the LP token
                    market_address = pool_data['market']
                    
                    # Create contract instance for LP token
                    lp_contract = w3.eth.contract(
                        address=Web3.to_checksum_address(market_address),
                        abi=MINIMAL_LP_ABI
                    )
                    
                    # Get LP balance
                    balance = Web3Retry.call_contract_function(
                        lp_contract.functions.balanceOf(checksum_address).call
                    )
                    
                    if balance > 0:
                        balances[network][pt_symbol] = {
                            "amount": str(balance),
                            "decimals": 18,  # LP tokens always have 18 decimals
                            "type": "lp"
                        }
            
            return balances
            
        except Exception as e:
            print(f"Error getting Pendle LP balances: {e}")
            return {}

    def _get_usdc_quote(self, network: str, token_symbol: str, amount_in_wei: str, user_address: str) -> Tuple[int, float, Dict]:
        """
        Get USDC conversion quote from Pendle SDK API.
        
        Args:
            network: Network identifier (ethereum/base)
            token_symbol: PT token symbol
            amount_in_wei: Amount to convert in wei (18 decimals)
            user_address: Address of the user making the request
            
        Returns:
            Tuple containing:
            - USDC amount (6 decimals)
            - Price impact percentage
            - Conversion details
        """
        print(f"\nAttempting to get quote for {token_symbol}:")
        
        try:
            token_data = get_pool_info(network, token_symbol)[token_symbol]
            if self.is_pt_expired(token_data):
                print(f"\nToken {token_symbol} is expired (matured)")
                
                underlying_token = next(iter(token_data['underlying'].values()))
                print(f"Converting directly to underlying {underlying_token['symbol']} token (1:1)")
                
                print(f"\nConverting {underlying_token['symbol']} to USDC via CoWSwap:")
                result = get_quote(
                    network=network,
                    sell_token=underlying_token['address'],
                    buy_token=USDC_TOKENS[network]['address'],
                    amount=amount_in_wei,
                    token_decimals=underlying_token['decimals'],
                    token_symbol=underlying_token['symbol']
                )
                
                if result["quote"]:
                    usdc_amount = int(result["quote"]["quote"]["buyAmount"])
                    price_impact = float(result["conversion_details"].get("price_impact", "0"))
                    if isinstance(price_impact, str) and price_impact == "N/A":
                        price_impact = 0
                        
                    # Modify conversion details to reflect complete process
                    result["conversion_details"].update({
                        "source": "Matured PT",
                        "note": f"PT token matured - Direct 1:1 conversion to {underlying_token['symbol']}, then {result['conversion_details']['source']} quote for USDC"
                    })
                    
                    return usdc_amount, price_impact/100, result["conversion_details"]
                
                raise Exception(f"Failed to convert {underlying_token['symbol']} to USDC")

            # If not expired, use Pendle API
            print(f"\nRequesting Pendle API quote...")
            
            # Define URL first
            url = f"{self.API_CONFIG['base_url']}/{CHAIN_IDS[network]}/markets/{token_data['market']}/swap"
            print(f"URL: {url}")
            
            # Use the test address as txOrigin since it will be the one executing the transaction
            tx_origin = Web3.to_checksum_address(user_address)
            print(f"\nUsing address as txOrigin and receiver: {tx_origin}")
            print("Note: Both txOrigin and receiver must match the address that will execute the transaction")
            
            params = {
                "receiver": tx_origin,  # Use same address as txOrigin
                "slippage": self.API_CONFIG["default_slippage"],
                "enableAggregator": self.API_CONFIG["enable_aggregator"],
                "tokenIn": token_data["address"],
                "tokenOut": USDC_TOKENS[network]["address"],
                "amountIn": amount_in_wei,
                "txOrigin": tx_origin
            }
            
            print("\nInitial request parameters:")
            print(json.dumps(params, indent=2))
            
            try:
                print("\nSending request to Pendle API...")
                response = APIRetry.get(url, params=params)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print("\n✗ Initial request failed")
                print(f"Error type: {type(e).__name__}")
                print(f"Error message: {str(e)}")
                
                # Try to get more details from the response if available
                if hasattr(e, 'response') and e.response is not None:
                    print(f"\nResponse status: {e.response.status_code}")
                    print(f"Response headers: {json.dumps(dict(e.response.headers), indent=2)}")
                    try:
                        error_details = e.response.json()
                        print("\nAPI Error details:")
                        print(json.dumps(error_details, indent=2))
                    except:
                        print(f"Raw response: {e.response.text}")
                
                if "400" in str(e) and "txOrigin" in str(e):
                    print("\nDetected txOrigin error - This usually means:")
                    print("1. The txOrigin address is not properly formatted")
                    print("2. The txOrigin address is not allowed by the API")
                    print("3. The txOrigin address is not a valid Ethereum address")
                    print(f"\nCurrent txOrigin: {tx_origin}")
                    print("Note: This address should be the one that will execute the transaction")
                    
                    print("\n" + "="*80)
                    print("FALLBACK MODE ACTIVATED")
                    print("="*80)
                    print(f"Error: {str(e)}")
                    print("\nAttempting fallback strategy: PT -> SY -> Underlying -> USDC")
                    
                    # Fallback: Try without aggregator and use SY token
                    if "sy_token" not in token_data:
                        print("✗ No SY token configured for fallback")
                        raise Exception("No SY token configured for fallback")
                    
                    print("\nStep 1: Converting PT to SY")
                    print(f"PT Token: {token_data['address']}")
                    print(f"SY Token: {token_data['sy_token']['address']}")
                    print(f"Amount: {amount_in_wei} wei")
                    
                    # First convert PT to SY
                    sy_params = {
                        "receiver": tx_origin,  # Use same address as txOrigin
                        "slippage": self.API_CONFIG["default_slippage"],
                        "enableAggregator": "false",
                        "tokenIn": token_data["address"],
                        "tokenOut": token_data["sy_token"]["address"],
                        "amountIn": amount_in_wei,
                        "txOrigin": tx_origin
                    }
                    
                    print("\nPT -> SY Request parameters:")
                    print(json.dumps(sy_params, indent=2))
                    
                    try:
                        print("\nSending PT -> SY request...")
                        sy_response = APIRetry.get(url, params=sy_params)
                        sy_response.raise_for_status()
                        sy_data = sy_response.json()
                        
                        if 'data' in sy_data and 'amountOut' in sy_data['data']:
                            sy_amount = sy_data['data']['amountOut']
                            print(f"\n✓ PT -> SY conversion successful")
                            print(f"Input: {Decimal(amount_in_wei)/Decimal(10**18)} PT")
                            print(f"Output: {Decimal(sy_amount)/Decimal(10**18)} SY")
                            
                            # Since 1 SY = 1 underlying, we can use the SY amount directly
                            underlying_token = next(iter(token_data['underlying'].values()))
                            print(f"\nStep 2: Converting {underlying_token['symbol']} to USDC via CoWSwap")
                            print(f"Underlying Token: {underlying_token['address']}")
                            print(f"USDC Token: {USDC_TOKENS[network]['address']}")
                            print(f"Amount: {sy_amount} wei")
                            
                            result = get_quote(
                                network=network,
                                sell_token=underlying_token['address'],
                                buy_token=USDC_TOKENS[network]['address'],
                                amount=sy_amount,
                                token_decimals=underlying_token['decimals'],
                                token_symbol=underlying_token['symbol']
                            )
                            
                            if result["quote"]:
                                usdc_amount = int(result["quote"]["quote"]["buyAmount"])
                                price_impact = float(result["conversion_details"].get("price_impact", "0"))
                                if isinstance(price_impact, str) and price_impact == "N/A":
                                    price_impact = 0
                                    
                                # Calculate rate for monitoring
                                amount_decimal = Decimal(amount_in_wei) / Decimal(10**18)
                                usdc_decimal = Decimal(usdc_amount) / Decimal(10**6)
                                rate = usdc_decimal / amount_decimal if amount_decimal else Decimal('0')
                                
                                print("\n✓ Fallback conversion complete")
                                print("Conversion path:")
                                print(f"1. PT: {amount_decimal} {token_symbol}")
                                print(f"2. SY: {Decimal(sy_amount)/Decimal(10**18)} SY")
                                print(f"3. USDC: {usdc_decimal} USDC")
                                print("\nFinal metrics:")
                                print(f"- Rate: {float(rate):.6f} USDC/{token_symbol}")
                                print(f"- Price impact: {price_impact:.4f}%")
                                print(f"- Source: Pendle SDK + CoWSwap")
                                
                                conversion_details = {
                                    "source": "Pendle SDK + CoWSwap",
                                    "price_impact": f"{price_impact:.6f}",
                                    "rate": f"{rate:.6f}",
                                    "fee_percentage": result["conversion_details"].get("fee_percentage", "0.0000%"),
                                    "fallback": True,
                                    "note": "PT -> SY (1:1) -> Underlying -> USDC via CoWSwap"
                                }
                                
                                return usdc_amount, price_impact, conversion_details
                            else:
                                print("\n✗ CoWSwap conversion failed")
                                print("Response:", json.dumps(result, indent=2))
                                raise Exception(f"Failed to convert {underlying_token['symbol']} to USDC via CoWSwap")
                        else:
                            print("\n✗ Invalid response from PT to SY conversion")
                            print("Response:", json.dumps(sy_data, indent=2))
                            raise Exception("Invalid response from PT to SY conversion")
                            
                    except Exception as fallback_error:
                        print("\n✗ Fallback process failed")
                        print(f"Error details: {str(fallback_error)}")
                        print("\nFull error chain:")
                        print(f"1. Original error: {str(e)}")
                        print(f"2. Fallback error: {str(fallback_error)}")
                        
                        # Try to get more details from the fallback error response
                        if hasattr(fallback_error, 'response') and fallback_error.response is not None:
                            print(f"\nFallback response status: {fallback_error.response.status_code}")
                            print(f"Fallback response headers: {json.dumps(dict(fallback_error.response.headers), indent=2)}")
                            try:
                                fallback_error_details = fallback_error.response.json()
                                print("\nFallback API Error details:")
                                print(json.dumps(fallback_error_details, indent=2))
                            except:
                                print(f"Raw fallback response: {fallback_error.response.text}")
                        
                        raise Exception(f"Both direct and fallback conversions failed: {str(e)} -> {str(fallback_error)}")
                else:
                    raise e
            
            if 'data' in data and 'amountOut' in data['data']:
                usdc_amount = int(data['data']['amountOut'])
                price_impact = float(data['data'].get('priceImpact', 0))
                
                # Calculate rate for monitoring
                amount_decimal = Decimal(amount_in_wei) / Decimal(10**18)
                usdc_decimal = Decimal(usdc_amount) / Decimal(10**6)
                rate = usdc_decimal / amount_decimal if amount_decimal else Decimal('0')
                
                print(f"✓ Quote successful:")
                print(f"  - Sell amount: {amount_decimal} {token_symbol}")
                print(f"  - Buy amount: {usdc_decimal} USDC")
                print(f"  - Rate: {float(rate):.6f} USDC/{token_symbol}")
                print(f"  - Price impact: {price_impact:.4f}%")
                
                conversion_details = {
                    "source": "Pendle SDK",
                    "price_impact": f"{price_impact:.6f}",
                    "rate": f"{rate:.6f}",
                    "fee_percentage": "0.0000%",
                    "fallback": False,
                    "note": "Direct Conversion using Pendle SDK"
                }
                
                return usdc_amount, price_impact, conversion_details
            
            print(f"✗ Invalid response from Pendle API: {data}")
            raise Exception("Invalid API response format")
                
        except Exception as e:
            print(f"✗ Technical error:")
            print(f"  {str(e)}")
            raise Exception(f"Failed to get Pendle quote: {str(e)}")

    def _get_lp_usdc_quote(self, network: str, token_symbol: str, amount_in_wei: str, user_address: str) -> Tuple[int, float, Dict]:
        """
        Get USDC conversion quote for LP tokens using Pendle's remove-liquidity endpoint.
        
        Args:
            network: Network identifier (ethereum/base)
            token_symbol: PT token symbol
            amount_in_wei: Amount to convert in wei (18 decimals)
            user_address: Address of the user making the request
            
        Returns:
            Tuple containing:
            - USDC amount (6 decimals)
            - Price impact percentage
            - Conversion details
        """
        print(f"\nAttempting to get remove-liquidity quote for {token_symbol}:")
        
        try:
            token_data = get_pool_info(network, token_symbol)[token_symbol]
            
            # Define URL for remove-liquidity endpoint
            url = f"{self.API_CONFIG['base_url']}/{CHAIN_IDS[network]}/markets/{token_data['market']}/remove-liquidity"
            print(f"URL: {url}")
            
            # Use the address as txOrigin since it will be the one executing the transaction
            tx_origin = Web3.to_checksum_address(user_address)
            print(f"\nUsing address as txOrigin and receiver: {tx_origin}")
            
            params = {
                "receiver": tx_origin,
                "slippage": self.API_CONFIG["default_slippage"],
                "enableAggregator": self.API_CONFIG["enable_aggregator"],
                "amountIn": amount_in_wei,
                "tokenOut": USDC_TOKENS[network]["address"],
                "txOrigin": tx_origin
            }
            
            print("\nRequest parameters:")
            print(json.dumps(params, indent=2))
            
            try:
                print("\nSending request to Pendle API...")
                response = APIRetry.get(url, params=params)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print("\n✗ Initial request failed")
                print(f"Error type: {type(e).__name__}")
                print(f"Error message: {str(e)}")
                
                # Try fallback strategy: LP -> PT/SY -> USDC
                print("\n" + "="*80)
                print("FALLBACK MODE ACTIVATED")
                print("="*80)
                print(f"Error: {str(e)}")
                print("\nAttempting fallback strategy: LP -> PT/SY -> USDC")
                
                # Get market contract to calculate exact SY/PT ratio
                w3 = self.eth_w3 if network == 'ethereum' else self.base_w3
                market_contract = w3.eth.contract(
                    address=Web3.to_checksum_address(token_data['market']),
                    abi=[
                        {
                            "inputs": [],
                            "name": "_storage",
                            "outputs": [
                                {"internalType": "int128", "name": "totalPt", "type": "int128"},
                                {"internalType": "int128", "name": "totalSy", "type": "int128"},
                                {"internalType": "uint96", "name": "lastLnImpliedRate", "type": "uint96"},
                                {"internalType": "uint16", "name": "observationIndex", "type": "uint16"},
                                {"internalType": "uint16", "name": "observationCardinality", "type": "uint16"},
                                {"internalType": "uint16", "name": "observationCardinalityNext", "type": "uint16"}
                            ],
                            "stateMutability": "view",
                            "type": "function"
                        },
                        {
                            "inputs": [],
                            "name": "totalSupply",
                            "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                            "stateMutability": "view",
                            "type": "function"
                        }
                    ]
                )
                
                # Get total PT and SY from market
                total_pt, total_sy, *_ = Web3Retry.call_contract_function(
                    market_contract.functions._storage().call
                )
                
                # Get total supply of LP tokens
                total_lp_supply = Web3Retry.call_contract_function(
                    market_contract.functions.totalSupply().call
                )
                
                print(f"\nMarket Information:")
                print(f"Total PT: {Decimal(total_pt)/Decimal(10**18)} {token_symbol}")
                print(f"Total SY: {Decimal(total_sy)/Decimal(10**18)} {token_data['sy_token']['symbol']}")
                print(f"Total LP Supply: {Decimal(total_lp_supply)/Decimal(10**18)} LP")
                
                # Calculate our share ratio
                our_share_ratio = Decimal(amount_in_wei) / Decimal(total_lp_supply)
                print(f"\nStep 1: Converting Pendle LP to PT and SY")
                print(f"Input LP: {Decimal(amount_in_wei)/Decimal(10**18)} Pendle LP")
                print(f"Total LP Supply: {Decimal(total_lp_supply)/Decimal(10**18)} Pendle LP")
                print(f"Share Ratio: {our_share_ratio:.6f} ({our_share_ratio*100:.4f}%)")
                
                # Calculate our PT and SY amounts based on our share
                pt_amount = int(Decimal(total_pt) * our_share_ratio)
                sy_amount = int(Decimal(total_sy) * our_share_ratio)
                
                print(f"\nInitial PT and SY amounts:")
                print(f"PT Amount: {Decimal(pt_amount)/Decimal(10**18)} {token_symbol}")
                print(f"SY Amount: {Decimal(sy_amount)/Decimal(10**18)} {token_data['sy_token']['symbol']}")
                
                # Convert PT to SY first
                print(f"\nStep 2: Converting PT to SY")
                try:
                    # Get PT -> SY quote from Pendle API
                    pt_to_sy_url = f"{self.API_CONFIG['base_url']}/{CHAIN_IDS[network]}/markets/{token_data['market']}/swap"
                    pt_to_sy_params = {
                        "receiver": tx_origin,
                        "slippage": self.API_CONFIG["default_slippage"],
                        "enableAggregator": "false",  # Disable aggregator for PT->SY
                        "tokenIn": token_data["address"],
                        "tokenOut": token_data["sy_token"]["address"],
                        "amountIn": str(pt_amount),
                        "txOrigin": tx_origin
                    }
                    
                    print("\nPT -> SY Request parameters:")
                    print(json.dumps(pt_to_sy_params, indent=2))
                    
                    print("\nSending PT -> SY request...")
                    pt_to_sy_response = APIRetry.get(pt_to_sy_url, params=pt_to_sy_params)
                    pt_to_sy_response.raise_for_status()
                    pt_to_sy_data = pt_to_sy_response.json()
                    
                    if 'data' in pt_to_sy_data and 'amountOut' in pt_to_sy_data['data']:
                        pt_to_sy_amount = int(pt_to_sy_data['data']['amountOut'])
                        print(f"\n✓ PT -> SY conversion successful")
                        print(f"Input: {Decimal(pt_amount)/Decimal(10**18)} PT")
                        print(f"Output: {Decimal(pt_to_sy_amount)/Decimal(10**18)} SY")
                        
                        # Add converted SY to our SY amount
                        total_sy_amount = sy_amount + pt_to_sy_amount
                        print(f"\nTotal SY calculation:")
                        print(f"Original SY amount: {Decimal(sy_amount)/Decimal(10**18)} SY")
                        print(f"PT converted to SY: {Decimal(pt_to_sy_amount)/Decimal(10**18)} SY")
                        print(f"Total SY: {Decimal(total_sy_amount)/Decimal(10**18)} SY")
                        
                        # Convert total SY to USDC
                        print(f"\nStep 3: Converting SY to USDC")
                        underlying_token = next(iter(token_data['underlying'].values()))
                        print(f"Converting {underlying_token['symbol']} to USDC via CoWSwap:")
                        result = get_quote(
                            network=network,
                            sell_token=underlying_token['address'],
                            buy_token=USDC_TOKENS[network]['address'],
                            amount=str(total_sy_amount),
                            token_decimals=underlying_token['decimals'],
                            token_symbol=underlying_token['symbol']
                        )
                        
                        if result["quote"]:
                            usdc_amount = int(result["quote"]["quote"]["buyAmount"])
                            price_impact = float(result["conversion_details"].get("price_impact", "0"))
                            if isinstance(price_impact, str) and price_impact == "N/A":
                                price_impact = 0
                                
                            # Calculate rate for monitoring
                            amount_decimal = Decimal(amount_in_wei) / Decimal(10**18)
                            usdc_decimal = Decimal(usdc_amount) / Decimal(10**6)
                            rate = usdc_decimal / amount_decimal if amount_decimal else Decimal('0')
                            
                            print("\n✓ Fallback conversion complete")
                            print("Conversion path:")
                            print(f"1. LP: {amount_decimal} Pendle LP")
                            print(f"2. PT: {Decimal(pt_amount)/Decimal(10**18)} {token_symbol}")
                            print(f"3. SY: {Decimal(total_sy_amount)/Decimal(10**18)} SY")
                            print(f"4. USDC: {usdc_decimal} USDC")
                            print("\nFinal metrics:")
                            print(f"- Rate: {float(rate):.6f} USDC/LP")
                            print(f"- Price impact: {price_impact:.4f}%")
                            print(f"- Source: Pendle SDK + CoWSwap")
                            
                            conversion_details = {
                                "source": "Pendle SDK + CoWSwap",
                                "price_impact": f"{price_impact:.6f}",
                                "rate": f"{rate:.6f}",
                                "fee_percentage": result["conversion_details"].get("fee_percentage", "0.0000%"),
                                "fallback": True,
                                "note": "LP -> PT/SY -> Underlying -> USDC via CoWSwap"
                            }
                            
                            return usdc_amount, price_impact, conversion_details
                        else:
                            print("\n✗ CoWSwap conversion failed")
                            print("Response:", json.dumps(result, indent=2))
                            raise Exception(f"Failed to convert {underlying_token['symbol']} to USDC via CoWSwap")
                    else:
                        print("\n✗ Invalid response from PT to SY conversion")
                        print("Response:", json.dumps(pt_to_sy_data, indent=2))
                        raise Exception("Invalid response from PT to SY conversion")
                        
                except Exception as fallback_error:
                    print("\n✗ Fallback process failed")
                    print(f"Error details: {str(fallback_error)}")
                    raise Exception(f"Both direct and fallback conversions failed: {str(e)} -> {str(fallback_error)}")
            
            if 'data' in data and 'amountOut' in data['data']:
                usdc_amount = int(data['data']['amountOut'])
                price_impact = float(data['data'].get('priceImpact', 0))
                
                # Calculate rate for monitoring
                amount_decimal = Decimal(amount_in_wei) / Decimal(10**18)
                usdc_decimal = Decimal(usdc_amount) / Decimal(10**6)
                rate = usdc_decimal / amount_decimal if amount_decimal else Decimal('0')
                
                print(f"✓ Quote successful:")
                print(f"  - Sell amount: {amount_decimal} LP")
                print(f"  - Buy amount: {usdc_decimal} USDC")
                print(f"  - Rate: {float(rate):.6f} USDC/LP")
                print(f"  - Price impact: {price_impact:.4f}%")
                
                conversion_details = {
                    "source": "Pendle SDK",
                    "price_impact": f"{price_impact:.6f}",
                    "rate": f"{rate:.6f}",
                    "fee_percentage": "0.0000%",
                    "fallback": False,
                    "note": "Direct Conversion using Pendle SDK remove-liquidity"
                }
                
                return usdc_amount, price_impact, conversion_details
            
            print(f"✗ Invalid response from Pendle API: {data}")
            raise Exception("Invalid API response format")
                
        except Exception as e:
            print(f"✗ Technical error:")
            print(f"  {str(e)}")
            raise Exception(f"Failed to get Pendle quote: {str(e)}")

    def _get_rewards(self, network: str, market_address: str, user_address: str) -> Dict[str, Any]:
        """
        Get rewards for a specific market and user using the market contract's userReward function
        """
        try:
            print(f"\nChecking rewards for market {market_address}")
            
            # Get market contract
            w3 = self.eth_w3 if network == 'ethereum' else self.base_w3
            market_contract = w3.eth.contract(
                address=Web3.to_checksum_address(market_address),
                abi=[
                    {
                        "inputs": [],
                        "name": "getRewardTokens",
                        "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
                        "stateMutability": "view",
                        "type": "function"
                    },
                    {
                        "inputs": [
                            {"internalType": "address", "name": "", "type": "address"},
                            {"internalType": "address", "name": "", "type": "address"}
                        ],
                        "name": "userReward",
                        "outputs": [
                            {"internalType": "uint128", "name": "index", "type": "uint128"},
                            {"internalType": "uint128", "name": "accrued", "type": "uint128"}
                        ],
                        "stateMutability": "view",
                        "type": "function"
                    }
                ]
            )
            
            # Get reward tokens
            print("\nGetting reward tokens...")
            reward_tokens = Web3Retry.call_contract_function(
                market_contract.functions.getRewardTokens().call
            )
            
            print(f"Reward tokens: {reward_tokens}")
            
            rewards = {}
            if reward_tokens and len(reward_tokens) > 0:
                for token_address in reward_tokens:
                    try:
                        # Get user's reward data
                        reward_data = Web3Retry.call_contract_function(
                            market_contract.functions.userReward(token_address, user_address).call
                        )
                        
                        # Web3.py returns a tuple with named fields
                        index = reward_data[0]  # First element is index
                        accrued = reward_data[1]  # Second element is accrued
                        
                        print(f"\nRaw reward data for {token_address}:")
                        print(f"Index: {index}")
                        print(f"Accrued: {accrued}")
                        
                        if accrued > 0:
                            # For Pendle token, we know it's PENDLE
                            token_symbol = "PENDLE"
                            
                            # Get USDC value for PENDLE
                            try:
                                print(f"\nGetting USDC quote for {token_symbol}...")
                                usdc_amount, price_impact, conversion_details = self._get_usdc_quote(
                                    network=network,
                                    token_symbol=token_symbol,
                                    amount_in_wei=str(accrued),
                                    user_address=user_address
                                )
                                
                                rewards[token_symbol] = {
                                    "amount": str(accrued),
                                    "decimals": 18,  # PENDLE uses 18 decimals
                                    "value": {
                                        "USDC": {
                                            "amount": str(usdc_amount),
                                            "decimals": 6,
                                            "conversion_details": conversion_details
                                        }
                                    }
                                }
                                
                                print(f"\nFound rewards for {token_symbol}:")
                                print(f"Amount: {Decimal(accrued)/Decimal(10**18):.6f} {token_symbol}")
                                print(f"USDC Value: {Decimal(usdc_amount)/Decimal(10**6):.6f} USDC")
                                print(f"Price Impact: {price_impact:.4f}%")
                            except Exception as quote_error:
                                print(f"Error getting USDC quote: {quote_error}")
                                # Still add the reward without USDC value
                                rewards[token_symbol] = {
                                    "amount": str(accrued),
                                    "decimals": 18,
                                    "value": {
                                        "USDC": {
                                            "amount": "0",
                                            "decimals": 6,
                                            "conversion_details": {
                                                "source": "Market Contract",
                                                "note": f"Rewards available to claim for {token_symbol}",
                                                "reward_index": str(index)
                                            }
                                        }
                                    }
                                }
                    except Exception as token_error:
                        print(f"Error getting reward for token {token_address}: {token_error}")
                        print(f"Error type: {type(token_error).__name__}")
                        print(f"Error details: {str(token_error)}")
                        continue
            
            if not rewards:
                print("\nNo rewards available to claim")
            
            return rewards
            
        except Exception as e:
            print(f"Error getting rewards for {network}.{market_address}: {e}")
            return {}

    def get_token_symbol(self, network: str, token_address: str) -> str:
        """
        Get token symbol from address using NETWORK_TOKENS
        """
        # First check USDC tokens
        for symbol, token_info in USDC_TOKENS[network].items():
            if token_info['address'].lower() == token_address.lower():
                return symbol
                
        # Then check all network tokens
        for symbol, token_info in NETWORK_TOKENS[network].items():
            if token_info['address'].lower() == token_address.lower():
                return symbol
                
        # If not found, return the address
        return token_address

    def get_balances(self, address: str) -> Dict[str, Any]:
        print("\n" + "="*80)
        print("PENDLE BALANCE MANAGER")
        print("="*80)
        
        checksum_address = Web3.to_checksum_address(address)
        
        # Fetch all balances first
        pt_balances = self._get_raw_balances(checksum_address)
        lp_balances = self._get_lp_balances(checksum_address)
        
        result = {"pendle": {}}
        total_usdc_wei = 0
        
        # Process only networks where we have positions
        networks_with_positions = set()
        for network in ["ethereum", "base"]:
            if (network in pt_balances and pt_balances[network]) or \
               (network in lp_balances and lp_balances[network]):
                networks_with_positions.add(network)
        
        # Process each network where we have positions
        for network in networks_with_positions:
            print(f"\nProcessing network: {network}")
            network_result = {}
            network_total = 0
            
            # Process each Pendle position
            for token_symbol, token_data in get_all_pools()[network].items():
                # Get PT balance
                pt_balance = int(pt_balances[network][token_symbol]['amount']) if network in pt_balances and token_symbol in pt_balances[network] else 0
                
                # Get LP balance
                lp_balance = int(lp_balances[network][token_symbol]['amount']) if network in lp_balances and token_symbol in lp_balances[network] else 0
                
                if pt_balance == 0 and lp_balance == 0:
                    continue
                
                print(f"\nProcessing position: {token_symbol}")
                
                # Contract information
                print(f"\nContract information:")
                print(f"  token: {token_data['address']} ({token_symbol})")
                print(f"  market: {token_data['market']}")
                if token_data.get('expiry'):
                    print(f"  expiry: {token_data['expiry']}")
                print(f"  underlying: {next(iter(token_data['underlying'].values()))['symbol']}")
                
                # Balance information
                print("\nQuerying balances:")
                if pt_balance > 0:
                    print(f"  PT Balance: {pt_balance} (decimals: {token_data['decimals']})")
                    print(f"  Formatted: {(Decimal(pt_balance) / Decimal(10**token_data['decimals'])):.6f} {token_symbol}")
                if lp_balance > 0:
                    print(f"  LP Balance: {lp_balance} (decimals: 18)")
                    print(f"  Formatted: {(Decimal(lp_balance) / Decimal(10**18)):.6f} LP")
                
                # Get rewards
                rewards = self._get_rewards(network, token_data['market'], checksum_address)
                if rewards:
                    print("\nFound rewards:")
                    for token, reward in rewards.items():
                        print(f"  {token}: {Decimal(reward['amount'])/Decimal(10**18):.6f}")
                
                # Get USDC valuation for PT
                pt_usdc_amount = 0
                pt_price_impact = 0
                pt_conversion_details = {}
                
                if pt_balance > 0:
                    try:
                        pt_usdc_amount, pt_price_impact, pt_conversion_details = self._get_usdc_quote(
                            network=network,
                            token_symbol=token_symbol,
                            amount_in_wei=str(pt_balance),
                            user_address=checksum_address
                        )
                        pt_usdc_normalized = Decimal(pt_usdc_amount) / Decimal(10**6)
                        
                        print(f"✓ PT Valuation successful:")
                        print(f"  - USDC value: {pt_usdc_normalized}")
                        print(f"  - Price impact: {pt_price_impact:.4f}%")
                        
                    except Exception as e:
                        print(f"✗ PT Valuation failed: {str(e)}")
                
                # Get USDC valuation for LP
                lp_usdc_amount = 0
                lp_price_impact = 0
                lp_conversion_details = {}
                
                if lp_balance > 0:
                    try:
                        lp_usdc_amount, lp_price_impact, lp_conversion_details = self._get_lp_usdc_quote(
                            network=network,
                            token_symbol=token_symbol,
                            amount_in_wei=str(lp_balance),
                            user_address=checksum_address
                        )
                        lp_usdc_normalized = Decimal(lp_usdc_amount) / Decimal(10**6)
                        
                        print(f"✓ LP Valuation successful:")
                        print(f"  - USDC value: {lp_usdc_normalized}")
                        print(f"  - Price impact: {lp_price_impact:.4f}%")
                        
                    except Exception as e:
                        print(f"✗ LP Valuation failed: {str(e)}")
                
                # Calculate position total
                position_total = pt_usdc_amount + lp_usdc_amount
                if position_total > 0:
                    network_total += position_total
                    
                    # Build position data
                    position_data = {
                        "market": token_data['market'],
                        "expiry": token_data.get('expiry'),
                        "underlying": next(iter(token_data['underlying'].values()))['symbol'],
                        "pt": {
                            "amount": str(pt_balance),
                            "decimals": token_data["decimals"],
                            "value": {
                                "USDC": {
                                    "amount": str(pt_usdc_amount),
                                    "decimals": 6,
                                    "conversion_details": pt_conversion_details
                                }
                            }
                        } if pt_balance > 0 else None,
                        "lp": {
                            "amount": str(lp_balance),
                            "decimals": 18,
                            "value": {
                                "USDC": {
                                    "amount": str(lp_usdc_amount),
                                    "decimals": 6,
                                    "conversion_details": lp_conversion_details
                                }
                            }
                        } if lp_balance > 0 else None,
                        "rewards": rewards,
                        "totals": {
                            "wei": position_total,
                            "formatted": f"{position_total/1e6:.6f}"
                        }
                    }
                    
                    network_result[token_symbol] = position_data
            
            if network_result:
                result["pendle"][network] = network_result
                # Add network-level totals
                result["pendle"][network]["totals"] = {
                    "wei": network_total,
                    "formatted": f"{network_total/1e6:.6f}"
                }
                total_usdc_wei += network_total
        
        # Add protocol-level totals
        if total_usdc_wei > 0:
            result["pendle"]["totals"] = {
                "wei": total_usdc_wei,
                "formatted": f"{total_usdc_wei/1e6:.6f}"
            }
        
        # Display detailed summary
        print("\n[Pendle] Calculation complete")
        
        # Display detailed positions
        for network in result["pendle"]:
            if network != "totals":
                for token, data in result["pendle"][network].items():
                    if token != "totals" and isinstance(data, dict) and "totals" in data:
                        amount = int(data["totals"]["wei"])
                        if amount > 0:
                            formatted_amount = amount / 10**6
                            print(f"pendle.{network}.{token}: {formatted_amount:.6f} USDC")
                            if "rewards" in data and data["rewards"]:
                                print(f"  Rewards:")
                                for reward_token, reward_data in data["rewards"].items():
                                    print(f"    {reward_token}: {Decimal(reward_data['amount'])/Decimal(10**18):.6f}")
        
        return result

    def _get_failed_position(self, position: Dict) -> Dict:
        """Create position data structure for failed conversions"""
        return {
            "amount": position["amount"],
            "decimals": position["decimals"],
            "value": {
                "USDC": {
                    "amount": "0",
                    "decimals": 6,
                    "conversion_details": {
                        "source": "Failed",
                        "price_impact": "0",
                        "rate": "0",
                        "fallback": True
                    }
                }
            }
        }

def format_position_data(positions_data):
    result = {"pendle": {}}
    
    # Group by chain
    total_usdc_wei = 0
    
    for network, positions in positions_data["pendle"].items():
        if network == "totals":
            continue  # Skip global totals during network processing
            
        formatted_positions = {}
        network_total = 0
        
        for position_name, position in positions.items():
            if position_name == "totals":
                continue  # Skip network totals during position processing
                
            try:
                # Get total from position data
                position_total = int(position["totals"]["wei"])
                network_total += position_total
                
                # Build formatted position data
                formatted_positions[position_name] = {
                    "market": position["market"],
                    "expiry": position["expiry"],
                    "underlying": position["underlying"],
                    "pt": position["pt"],
                    "lp": position["lp"],
                    "totals": {
                        "wei": position_total,
                        "formatted": f"{position_total/1e6:.6f}"
                    }
                }
            except Exception as e:
                print(f"Warning: Could not process position {position_name}: {str(e)}")
                continue
        
        # Add network data with positions and network total
        if formatted_positions:  # Only add network if it has positions
            result["pendle"][network] = formatted_positions
            result["pendle"][network]["totals"] = {
                "wei": network_total,
                "formatted": f"{network_total/1e6:.6f}"
            }
            total_usdc_wei += network_total
    
    # Add protocol total only if we have positions
    if total_usdc_wei > 0:
        result["pendle"]["totals"] = {
            "wei": total_usdc_wei,
            "formatted": f"{total_usdc_wei/1e6:.6f}"
        }
    
    return result

def main():
    """
    CLI utility for testing Pendle balance aggregation.
    Uses production address by default.
    """
    # Production address
    PRODUCTION_ADDRESS = "0xc6835323372A4393B90bCc227c58e82D45CE4b7d"
    
    # Use command line argument if provided, otherwise use production address
    test_address = sys.argv[1] if len(sys.argv) > 1 else PRODUCTION_ADDRESS
    
    manager = PendleBalanceManager()
    balances = manager.get_balances(test_address)
    formatted_balances = format_position_data(balances)
    
    print("\n" + "="*80)
    print("FINAL RESULT:")
    print("="*80 + "\n")
    print(json.dumps(formatted_balances, indent=2))

if __name__ == "__main__":
    main()