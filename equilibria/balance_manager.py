from web3 import Web3
import os
from dotenv import load_dotenv
import requests
from config.networks import NETWORK_TOKENS, CHAIN_IDS
from cowswap.cow_client import get_quote
import time
from decimal import Decimal
import json
from typing import Dict, Any
from datetime import datetime
from utils.retry import Web3Retry, APIRetry
from pendle.balance_manager import PendleBalanceManager
from equilibria.pool import get_pool_info, get_all_pools

# Load environment variables
load_dotenv()

# Web3 configuration with environment variables
ETHEREUM_RPC = os.getenv('ETHEREUM_RPC')
BASE_RPC = os.getenv('BASE_RPC')

# Network RPCs mapping
NETWORK_RPCS = {
    'ethereum': ETHEREUM_RPC,
    'base': BASE_RPC
}

# Zero address for API calls
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Production addresses
PRODUCTION_ADDRESS = Web3.to_checksum_address("0xc6835323372A4393B90bCc227c58e82D45CE4b7d")

# Get token addresses from network config
PENDLE_TOKEN_ADDRESS = NETWORK_TOKENS['ethereum']['PENDLE']['address']
CRV_TOKEN_ADDRESS = NETWORK_TOKENS['ethereum']['CRV']['address']
USDC_ADDRESS = NETWORK_TOKENS['ethereum']['USDC']['address']

# Pendle API base URL (without chain ID)
PENDLE_API_BASE_URL = "https://api-v2.pendle.finance/core/v1/sdk"

# BaseRewardPoolV2 ABI
BASE_REWARD_POOL_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "pid",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function"
    },
    {
        "inputs": [
            {"internalType": "address", "name": "_account", "type": "address"},
            {"internalType": "address", "name": "_rewardToken", "type": "address"}
        ],
        "name": "earned",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "getRewardTokens",
        "outputs": [{"name": "", "type": "address[]"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

# PendleBoosterMainchain ABI
PENDLE_BOOSTER_ABI = [
    {
        "name": "poolInfo",
        "inputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "outputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "address", "name": "token", "type": "address"},
            {"internalType": "address", "name": "rewardPool", "type": "address"},
            {"internalType": "bool", "name": "shutdown", "type": "bool"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

# Pendle Market ABI for _storage and other methods
PENDLE_MARKET_ABI = [
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

# Add fGHO ABI for convertToAssets
FGHO_ABI = [
    {
        "inputs": [{"internalType": "uint256", "name": "shares", "type": "uint256"}],
        "name": "convertToAssets",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

class BalanceManager:
    def __init__(self):
        # Initialize Web3 instances for each network
        self.w3_instances = {}
        for network, rpc in NETWORK_RPCS.items():
            if not rpc:
                raise ValueError(f"{network.upper()}_RPC not configured in .env file")
            self.w3_instances[network] = Web3(Web3.HTTPProvider(rpc))
        
        # Initialize contracts and pool info for each network and pool
        self.pools = {}
        all_pools = get_all_pools()
        for network, pools in all_pools.items():
            self.pools[network] = {}
            
            # Use different booster for Base network
            if network == "base":
                booster_address = "0x2583A2538272f31e9A15dD12A432B8C96Ab4821d"
                print(f"\nUsing Base booster: {booster_address}")
            else:
                booster_address = None
                
            for pool_id, config in pools.items():
                print(f"\nInitializing pool {network}.{pool_id}")
                print(f"Reward pool: {config['reward_pool_address']}")
                
                self.pools[network][pool_id] = {
                    'config': config,
                    'reward_pool': self.w3_instances[network].eth.contract(
                        address=self.w3_instances[network].to_checksum_address(config['reward_pool_address']),
                        abi=BASE_REWARD_POOL_ABI
                    )
                }
                
                # Use the correct booster for Base network
                if network == "base":
                    self.pools[network][pool_id]['pendle_booster'] = self.w3_instances[network].eth.contract(
                        address=self.w3_instances[network].to_checksum_address(booster_address),
                        abi=PENDLE_BOOSTER_ABI
                    )
                else:
                    self.pools[network][pool_id]['pendle_booster'] = self.w3_instances[network].eth.contract(
                        address=self.w3_instances[network].to_checksum_address(config['booster_address']),
                        abi=PENDLE_BOOSTER_ABI
                    )
                
                # Get pool info and reward tokens
                pool_info = self.get_pool_info(network, pool_id)
                if pool_info:
                    self.pools[network][pool_id]['pool_info'] = pool_info
                    print(f"Pool info: {pool_info}")
                    # Get reward tokens dynamically
                    reward_tokens = self.get_reward_tokens(network, pool_id)
                    if reward_tokens:
                        self.pools[network][pool_id]['reward_tokens'] = reward_tokens
                        print(f"Reward tokens: {reward_tokens}")

        self.current_timestamp = int(datetime.now().timestamp())

    def get_pool_info(self, network, pool_id):
        """
        Get pool information from PendleBoosterMainchain contract
        Returns tuple of (market, token, rewardPool, shutdown)
        """
        try:
            pool = self.pools[network][pool_id]
            pid = Web3Retry.call_contract_function(
                pool['reward_pool'].functions.pid().call
            )
            print(f"\nGetting pool info for {network}.{pool_id}")
            print(f"PID: {pid}")
            
            # For Base network, use the Base booster
            if network == "base":
                booster_address = "0x2583A2538272f31e9A15dD12A432B8C96Ab4821d"
                booster_contract = self.w3_instances[network].eth.contract(
                    address=self.w3_instances[network].to_checksum_address(booster_address),
                    abi=PENDLE_BOOSTER_ABI
                )
                pool_info = Web3Retry.call_contract_function(
                    booster_contract.functions.poolInfo(pid).call
                )
            else:
                pool_info = Web3Retry.call_contract_function(
                    pool['pendle_booster'].functions.poolInfo(pid).call
                )
            
            print(f"Pool info from booster: {pool_info}")
            return {
                'market': pool_info[0],
                'token': pool_info[1],
                'rewardPool': pool_info[2],
                'shutdown': pool_info[3]
            }
        except Exception as e:
            print(f"Error fetching pool info for {network}.{pool_id}: {e}")
            return None

    def get_reward_tokens(self, network, pool_id):
        """
        Get the list of reward tokens from the reward pool contract
        Returns a list of token addresses
        """
        try:
            pool = self.pools[network][pool_id]
            pool_info = pool.get('pool_info')
            if not pool_info:
                print(f"No pool info found for {network}.{pool_id}")
                return []
                
            # Use the reward pool from pool_info
            reward_pool_address = pool_info['rewardPool']
            print(f"\nGetting reward tokens for {network}.{pool_id}")
            print(f"Using reward pool: {reward_pool_address}")
            
            reward_pool = self.w3_instances[network].eth.contract(
                address=self.w3_instances[network].to_checksum_address(reward_pool_address),
                abi=BASE_REWARD_POOL_ABI
            )
            
            reward_tokens = Web3Retry.call_contract_function(
                reward_pool.functions.getRewardTokens().call
            )
            print(f"Found reward tokens: {reward_tokens}")
            return reward_tokens
        except Exception as e:
            print(f"Error fetching reward tokens for {network}.{pool_id}: {e}")
            return []

    def get_staked_balance(self, network, pool_id, address=None):
        """
        Get staked LP balance for a specific pool
        If no address is provided, uses production address
        Returns the raw balance in wei
        """
        if address is None:
            address = PRODUCTION_ADDRESS

        try:
            pool = self.pools[network][pool_id]
            balance = Web3Retry.call_contract_function(
                pool['reward_pool'].functions.balanceOf(
                    self.w3_instances[network].to_checksum_address(address)
                ).call
            )
            return balance
        except Exception as e:
            print(f"Error while fetching balance for {network}.{pool_id}: {e}")
            return 0

    def is_pt_expired(self, token_data: Dict) -> bool:
        """
        Check if a PT token is expired
        """
        expiry = token_data.get('expiry')
        if not expiry:
            return False
        return self.current_timestamp > expiry

    def get_remove_liquidity_data(self, network, pool_id, balance_wei):
        """
        Get remove liquidity data from Pendle API including amount out and price impact
        Returns a tuple of (amount_out, price_impact, method_used, direct_result, fallback_result)
        """
        pool = self.pools[network][pool_id]
        
        # Construct Pendle API URL with the correct chain ID for the network
        chain_id = CHAIN_IDS[network]
        pendle_api_url = f"{PENDLE_API_BASE_URL}/{chain_id}/markets"
        url = f"{pendle_api_url}/{pool['config']['market_address']}/remove-liquidity"
        
        # Validate and format parameters
        if not isinstance(balance_wei, (int, str)):
            raise ValueError(f"Invalid balance_wei type: {type(balance_wei)}")
        
        balance_str = str(balance_wei)
        if not balance_str.isdigit():
            raise ValueError(f"Invalid balance_wei value: {balance_str}")
        
        # Use the same address for both receiver and txOrigin
        tx_origin = self.w3_instances[network].to_checksum_address(PRODUCTION_ADDRESS)
        print(f"\nUsing address as txOrigin and receiver: {tx_origin}")
        print("Note: Both txOrigin and receiver must match the address that will execute the transaction")
        
        # Format parameters to match browser request
        params = {
            "receiver": tx_origin,
            "slippage": "0.01",
            "enableAggregator": "true",
            "amountIn": balance_str,
            "tokenOut": USDC_ADDRESS,
            "txOrigin": tx_origin
        }
        
        # Add headers to match browser request
        headers = {
            "accept": "application/json"
        }
        
        print(f"\nMaking Pendle API request:")
        print(f"URL: {url}")
        print(f"Headers: {json.dumps(headers, indent=2)}")
        print(f"Params: {json.dumps(params, indent=2)}")
        
        # Try direct method first (skip for yvBal-GHO-USR)
        print("\n" + "="*80)
        print("METHOD 1: DIRECT CONVERSION")
        print("="*80)
        
        direct_result = None
        if pool['config']['market_address'].lower() != '0x69efa3cd7fc773fe227b9cc4f41132dcde020a29000000000000000000000000':
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                amount_out = int(data["data"]["amountOut"])
                price_impact = float(data["data"]["priceImpact"])
                
                print("\n✓ Direct conversion successful")
                print(f"Input: {Decimal(balance_str)/Decimal(10**18)} LP")
                print(f"Output: {Decimal(amount_out)/Decimal(10**6)} USDC")
                print(f"Price impact: {price_impact:.4f}%")
                
                direct_result = {
                    "amount": amount_out,
                    "price_impact": price_impact,
                    "method": "direct"
                }
            except Exception as e:
                print(f"\n✗ Direct conversion failed: {str(e)}")
                print(f"Response status: {response.status_code}")
                print(f"Response body: {response.text}")
        else:
            print("\nSkipping direct conversion for yvBal-GHO-USR (using Yearn Vault method only)")
        
        # Now try fallback method
        print("\n" + "="*80)
        print("METHOD 2: FALLBACK CONVERSION")
        print("="*80)
        
        # Get token info
        sy_token = pool['config']['sy_token']
        pt_token = pool['config']['pt_token']
        underlying_token = next(iter(pool['config']['underlying'].values()))
        
        print(f"\nToken Information:")
        print(f"SY Token: {sy_token['symbol']} ({sy_token['address']})")
        print(f"PT Token: {pt_token['symbol']} ({pt_token['address']})")
        print(f"Underlying: {underlying_token['symbol']} ({underlying_token['address']})")
        
        # Get market contract to calculate exact SY/PT ratio
        market_contract = self.w3_instances[network].eth.contract(
            address=self.w3_instances[network].to_checksum_address(pool['config']['market_address']),
            abi=PENDLE_MARKET_ABI
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
        print(f"Total PT: {Decimal(total_pt)/Decimal(10**18)} {pt_token['symbol']}")
        print(f"Total SY: {Decimal(total_sy)/Decimal(10**18)} {sy_token['symbol']}")
        print(f"Total LP Supply: {Decimal(total_lp_supply)/Decimal(10**18)} LP")
        
        # Calculate our share ratio
        our_share_ratio = Decimal(balance_wei) / Decimal(total_lp_supply)
        print(f"\nStep 1: Converting Pendle LP to PT and SY")
        print(f"Input LP: {Decimal(balance_wei)/Decimal(10**18)} Pendle LP")
        print(f"Total LP Supply: {Decimal(total_lp_supply)/Decimal(10**18)} Pendle LP")
        print(f"Share Ratio: {our_share_ratio:.6f} ({our_share_ratio*100:.4f}%)")
        
        # Calculate our PT and SY amounts based on our share
        pt_amount = int(Decimal(total_pt) * our_share_ratio)
        sy_amount = int(Decimal(total_sy) * our_share_ratio)
        
        print(f"\nInitial PT and SY amounts:")
        print(f"PT Amount: {Decimal(pt_amount)/Decimal(10**18)} {pt_token['symbol']}")
        print(f"SY Amount: {Decimal(sy_amount)/Decimal(10**18)} {sy_token['symbol']}")
        
        # Convert PT to SY first
        print(f"\nStep 2: Converting PT to SY")
        try:
            # Get PT -> SY quote from Pendle API
            pt_to_sy_url = f"{pendle_api_url}/{pool['config']['market_address']}/swap"
            pt_to_sy_params = {
                "receiver": tx_origin,
                "slippage": "0.01",
                "enableAggregator": "false",  # Disable aggregator for PT->SY
                "tokenIn": pt_token['address'],
                "tokenOut": sy_token['address'],
                "amountIn": str(pt_amount),  # Already in wei from our calculation
                "txOrigin": tx_origin,
                "useAggregator": "false"  # Additional flag to ensure aggregator is disabled
            }
            
            print("\nPT -> SY Request parameters:")
            print(json.dumps(pt_to_sy_params, indent=2))
            
            print("\nSending PT -> SY request...")
            pt_to_sy_response = requests.get(pt_to_sy_url, params=pt_to_sy_params, headers=headers)
            
            # Print full response details if there's an error
            if pt_to_sy_response.status_code != 200:
                print(f"\nError Response Status: {pt_to_sy_response.status_code}")
                print(f"Error Response Headers: {dict(pt_to_sy_response.headers)}")
                print(f"Error Response Body: {pt_to_sy_response.text}")
                pt_to_sy_response.raise_for_status()
                
            pt_to_sy_data = pt_to_sy_response.json()
            
            # Get SY amount from PT
            pt_to_sy_amount = int(pt_to_sy_data["data"]["amountOut"])
            print(f"\n✓ PT -> SY conversion successful")
            print(f"Input: {Decimal(pt_amount)/Decimal(10**18)} PT")
            print(f"Output: {Decimal(pt_to_sy_amount)/Decimal(10**18)} SY")
            
            # Add converted SY to our SY amount
            total_sy_amount = sy_amount + pt_to_sy_amount
            print(f"\nTotal SY calculation:")
            print(f"Original SY amount: {Decimal(sy_amount)/Decimal(10**18)} SY")
            print(f"PT converted to SY: {Decimal(pt_to_sy_amount)/Decimal(10**18)} SY")
            print(f"Total SY after PT conversion: {Decimal(total_sy_amount)/Decimal(10**18)} SY")
            
        except Exception as e:
            print(f"\n✗ PT -> SY conversion failed: {str(e)}")
            print("Using original SY amount only")
            total_sy_amount = sy_amount
            print(f"Using original SY amount: {Decimal(total_sy_amount)/Decimal(10**18)} SY")
        
        # Convert SY to underlying tokens based on token type
        print(f"\nStep 3: Converting {underlying_token['symbol']} to underlying tokens")
        print(f"Underlying Token: {underlying_token['address']}")
        
        if underlying_token['symbol'] == 'fGHO':
            # Handle fGHO token
            fgho_contract = self.w3_instances[network].eth.contract(
                address=self.w3_instances[network].to_checksum_address(underlying_token['address']),
                abi=FGHO_ABI
            )
            
            # Convert fGHO to GHO
            gho_amount = Web3Retry.call_contract_function(
                fgho_contract.functions.convertToAssets(total_sy_amount).call
            )
            
            print(f"Input: {Decimal(total_sy_amount)/Decimal(10**18)} {underlying_token['symbol']}")
            print(f"Output: {Decimal(gho_amount)/Decimal(10**18)} GHO")
            
            # Convert GHO to USDC
            print(f"\nStep 4: Converting GHO to USDC via CoWSwap")
            print(f"GHO Token: {NETWORK_TOKENS[network]['GHO']['address']}")
            print(f"USDC Token: {USDC_ADDRESS}")
            print(f"Amount: {gho_amount} wei")
            
            result = get_quote(
                network=network,
                sell_token=NETWORK_TOKENS[network]['GHO']['address'],
                buy_token=USDC_ADDRESS,
                amount=str(gho_amount),
                token_decimals=NETWORK_TOKENS[network]['GHO']['decimals'],
                token_symbol="GHO"
            )
            
            if result["quote"]:
                fallback_usdc_amount = int(result["quote"]["quote"]["buyAmount"])
                fallback_price_impact = float(result["conversion_details"].get("price_impact", "0"))
                if isinstance(fallback_price_impact, str) and fallback_price_impact == "N/A":
                    fallback_price_impact = 0
                    
                print("\n✓ GHO -> USDC conversion successful")
                print(f"Input: {Decimal(gho_amount)/Decimal(10**18)} GHO")
                print(f"Output: {Decimal(fallback_usdc_amount)/Decimal(10**6)} USDC")
                print(f"Price impact: {fallback_price_impact:.4f}%")
                
                # Compare with direct method
                print("\n" + "="*80)
                print("COMPARISON OF METHODS")
                print("="*80)
                
                if direct_result and fallback_usdc_amount > 0:
                    print("\nDirect Method:")
                    print(f"Amount: {Decimal(direct_result['amount'])/Decimal(10**6)} USDC")
                    print(f"Price Impact: {direct_result['price_impact']:.4f}%")
                    
                    print("\nFallback Method:")
                    print(f"Amount: {Decimal(fallback_usdc_amount)/Decimal(10**6)} USDC")
                    print(f"Price Impact: {fallback_price_impact:.4f}%")
                    
                    print("\nDifference:")
                    diff_amount = abs(direct_result['amount'] - fallback_usdc_amount)
                    diff_percentage = (diff_amount / direct_result['amount']) * 100
                    print(f"Amount Difference: {Decimal(diff_amount)/Decimal(10**6)} USDC ({diff_percentage:.4f}%)")
                    
                    # Use the method with the higher amount
                    if direct_result['amount'] >= fallback_usdc_amount:
                        return direct_result['amount'], direct_result['price_impact'], "Direct", direct_result, {
                            "amount": fallback_usdc_amount,
                            "price_impact": fallback_price_impact,
                            "method": "fallback"
                        }
                    else:
                        return fallback_usdc_amount, fallback_price_impact, "Fallback", direct_result, {
                            "amount": fallback_usdc_amount,
                            "price_impact": fallback_price_impact,
                            "method": "fallback"
                        }
                elif direct_result:
                    return direct_result['amount'], direct_result['price_impact'], "Direct", direct_result, None
                elif fallback_usdc_amount > 0:
                    return fallback_usdc_amount, fallback_price_impact, "Fallback", None, {
                        "amount": fallback_usdc_amount,
                        "price_impact": fallback_price_impact,
                        "method": "fallback"
                    }
                else:
                    raise Exception("Both direct and fallback methods failed")
            else:
                print("\n✗ CoWSwap conversion failed")
                print("Response:", json.dumps(result, indent=2))
                if direct_result:
                    return direct_result['amount'], direct_result['price_impact'], "Direct", direct_result, None
                else:
                    raise Exception("Both direct and fallback methods failed")
                
        elif underlying_token['symbol'] == 'cUSDO':
            # Handle cUSDO token - direct conversion to USDC
            print(f"Input: {Decimal(total_sy_amount)/Decimal(10**18)} {underlying_token['symbol']}")
            
            # Convert cUSDO directly to USDC
            print(f"\nStep 4: Converting cUSDO to USDC via CoWSwap")
            print(f"cUSDO Token: {underlying_token['address']}")
            print(f"USDC Token: {USDC_ADDRESS}")
            print(f"Amount: {total_sy_amount} wei")
            
            result = get_quote(
                network=network,
                sell_token=underlying_token['address'],
                buy_token=USDC_ADDRESS,
                amount=str(total_sy_amount),
                token_decimals=underlying_token['decimals'],
                token_symbol="cUSDO"
            )
            
            if result["quote"]:
                fallback_usdc_amount = int(result["quote"]["quote"]["buyAmount"])
                fallback_price_impact = float(result["conversion_details"].get("price_impact", "0"))
                if isinstance(fallback_price_impact, str) and fallback_price_impact == "N/A":
                    fallback_price_impact = 0
                    
                print("\n✓ cUSDO -> USDC conversion successful")
                print(f"Input: {Decimal(total_sy_amount)/Decimal(10**18)} cUSDO")
                print(f"Output: {Decimal(fallback_usdc_amount)/Decimal(10**6)} USDC")
                print(f"Price impact: {fallback_price_impact:.4f}%")
                
                # Compare with direct method
                print("\n" + "="*80)
                print("COMPARISON OF METHODS")
                print("="*80)
                
                if direct_result and fallback_usdc_amount > 0:
                    print("\nDirect Method:")
                    print(f"Amount: {Decimal(direct_result['amount'])/Decimal(10**6)} USDC")
                    print(f"Price Impact: {direct_result['price_impact']:.4f}%")
                    
                    print("\nFallback Method:")
                    print(f"Amount: {Decimal(fallback_usdc_amount)/Decimal(10**6)} USDC")
                    print(f"Price Impact: {fallback_price_impact:.4f}%")
                    
                    print("\nDifference:")
                    diff_amount = abs(direct_result['amount'] - fallback_usdc_amount)
                    diff_percentage = (diff_amount / direct_result['amount']) * 100
                    print(f"Amount Difference: {Decimal(diff_amount)/Decimal(10**6)} USDC ({diff_percentage:.4f}%)")
                    
                    # Use the method with the higher amount
                    if direct_result['amount'] >= fallback_usdc_amount:
                        return direct_result['amount'], direct_result['price_impact'], "Direct", direct_result, {
                            "amount": fallback_usdc_amount,
                            "price_impact": fallback_price_impact,
                            "method": "fallback"
                        }
                    else:
                        return fallback_usdc_amount, fallback_price_impact, "Fallback", direct_result, {
                            "amount": fallback_usdc_amount,
                            "price_impact": fallback_price_impact,
                            "method": "fallback"
                        }
                elif direct_result:
                    return direct_result['amount'], direct_result['price_impact'], "Direct", direct_result, None
                elif fallback_usdc_amount > 0:
                    return fallback_usdc_amount, fallback_price_impact, "Fallback", None, {
                        "amount": fallback_usdc_amount,
                        "price_impact": fallback_price_impact,
                        "method": "fallback"
                    }
                else:
                    raise Exception("Both direct and fallback methods failed")
            else:
                print("\n✗ CoWSwap conversion failed")
                print("Response:", json.dumps(result, indent=2))
                if direct_result:
                    return direct_result['amount'], direct_result['price_impact'], "Direct", direct_result, None
                else:
                    raise Exception("Both direct and fallback methods failed")
                    
        elif underlying_token['symbol'] == 'yvBal-GHO-USR':
            # Handle yvBal token
            # Yearn V3 Vault ABI
            YEARN_VAULT_ABI = [
                {
                    "inputs": [{"name": "shares", "type": "uint256"}],
                    "name": "convertToAssets",
                    "outputs": [{"name": "", "type": "uint256"}],
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "inputs": [],
                    "name": "asset",
                    "outputs": [{"name": "", "type": "address"}],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]
            
            # Get Yearn Vault contract
            yearn_vault = self.w3_instances[network].eth.contract(
                address=self.w3_instances[network].to_checksum_address(underlying_token['address']),
                abi=YEARN_VAULT_ABI
            )
            
            # Get underlying LP token address
            lp_token_address = Web3Retry.call_contract_function(
                yearn_vault.functions.asset().call
            )
            
            print(f"\nYearn Vault Information:")
            print(f"Vault Address: {underlying_token['address']}")
            print(f"Underlying LP Token: {lp_token_address}")
            
            # Convert yvBal to LP using convertToAssets
            lp_amount = Web3Retry.call_contract_function(
                yearn_vault.functions.convertToAssets(total_sy_amount).call
            )
            
            print(f"\nYearn Vault Conversion:")
            print(f"Input: {Decimal(total_sy_amount)/Decimal(10**18)} {underlying_token['symbol']}")
            print(f"Output: {Decimal(lp_amount)/Decimal(10**18)} Balancer LP")
            
            # Get Balancer StablePool contract
            BALANCER_POOL_ABI = [
                {
                    "inputs": [],
                    "name": "getTokens",
                    "outputs": [{"internalType": "contract IERC20[]", "name": "tokens", "type": "address[]"}],
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "inputs": [],
                    "name": "totalSupply",
                    "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                    "stateMutability": "view",
                    "type": "function"
                },
                {
                    "inputs": [],
                    "name": "getStablePoolDynamicData",
                    "outputs": [{
                        "components": [
                            {"internalType": "uint256[]", "name": "balancesLiveScaled18", "type": "uint256[]"}
                        ],
                        "internalType": "struct StablePoolDynamicData",
                        "name": "data",
                        "type": "tuple"
                    }],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]
            
            # Get Balancer Pool contract
            balancer_pool = self.w3_instances[network].eth.contract(
                address=self.w3_instances[network].to_checksum_address(lp_token_address),
                abi=BALANCER_POOL_ABI
            )
            
            # Get pool tokens
            pool_tokens = Web3Retry.call_contract_function(
                balancer_pool.functions.getTokens().call
            )
            
            # Get Balancer pool total supply
            balancer_total_supply = Web3Retry.call_contract_function(
                balancer_pool.functions.totalSupply().call
            )
            
            # Get pool balances
            pool_data = Web3Retry.call_contract_function(
                balancer_pool.functions.getStablePoolDynamicData().call
            )
            pool_balances = pool_data[0]  # balancesLiveScaled18
            
            # Calculate our share of the Balancer pool using the LP amount from Yearn
            our_share = Decimal(lp_amount) / Decimal(balancer_total_supply)
            
            print(f"\nBalancer Pool Information:")
            print(f"Pool Address: {lp_token_address}")
            print(f"Total LP Supply: {Decimal(balancer_total_supply)/Decimal(10**18)} Balancer LP")
            print(f"Our LP Amount (from Yearn): {Decimal(lp_amount)/Decimal(10**18)} Balancer LP")
            print(f"Our Share: {our_share:.6f} ({our_share*100:.4f}%)")
            
            # Calculate amounts of underlying tokens we get
            underlying_amounts = []
            print("\nCalculating underlying token amounts:")
            for i, token in enumerate(pool_tokens):
                # Keep amounts in wei, only format for display
                balance = pool_balances[i]  # Already in wei
                amount = int(Decimal(balance) * our_share)
                underlying_amounts.append((token, amount))
                print(f"\nToken {i}: {token}")
                print(f"Pool Balance: {Decimal(balance)/Decimal(10**18)}")
                print(f"Our Amount: {Decimal(amount)/Decimal(10**18)}")
                print(f"Our Amount (wei): {amount}")
                
                # Try to identify the token
                token_symbol = None
                for symbol, token_info in NETWORK_TOKENS['base'].items():
                    if token_info['address'].lower() == token.lower():
                        token_symbol = symbol
                        break
                if token_symbol:
                    print(f"Identified as: {token_symbol}")
                else:
                    print("Unknown token")
            
            # Now convert both USR and waBasGHO to USDC
            print(f"\nStep 4: Converting underlying tokens to USDC")
            total_usdc = 0
            
            for token_address, amount in underlying_amounts:
                if amount == 0:
                    continue
                    
                # Get token symbol from Base network tokens
                token_symbol = None
                for symbol, token_info in NETWORK_TOKENS['base'].items():
                    if token_info['address'].lower() == token_address.lower():
                        token_symbol = symbol
                        break
                
                if not token_symbol:
                    print(f"Unknown token: {token_address}")
                    continue
                    
                print(f"\nConverting {token_symbol} to USDC")
                print(f"Amount: {Decimal(amount)/Decimal(10**18)} {token_symbol}")
                
                # Use Base network for USR and waBasGHO with correct token addresses
                if token_symbol in ["USR", "waBasGHO"]:
                    # Use Base network addresses for these tokens
                    base_token_address = NETWORK_TOKENS['base'][token_symbol]['address']
                    usdc_base_address = NETWORK_TOKENS['base']['USDC']['address']
                    
                    print(f"\nUsing Base network for {token_symbol}:")
                    print(f"Token address: {base_token_address}")
                    print(f"USDC address: {usdc_base_address}")
                    print(f"Amount: {amount} wei")
                    print(f"Decimals: {NETWORK_TOKENS['base'][token_symbol]['decimals']}")
                    
                    result = get_quote(
                        network="base",
                        sell_token=base_token_address,
                        buy_token=usdc_base_address,
                        amount=str(amount),
                        token_decimals=NETWORK_TOKENS['base'][token_symbol]['decimals'],
                        token_symbol=token_symbol
                    )
                    
                    # Check if we got a quote and calculate the rate
                    if result["quote"]:
                        usdc_amount = int(result["quote"]["quote"]["buyAmount"])
                        
                        # Calculate rate properly accounting for decimals
                        usdc_normalized = Decimal(usdc_amount) / Decimal(10**6)  # USDC to regular units
                        token_decimals = NETWORK_TOKENS['base'][token_symbol]['decimals']
                        token_normalized = Decimal(amount) / Decimal(10**token_decimals)
                        rate = usdc_normalized / token_normalized
                        
                        print(f"Base network conversion successful:")
                        print(f"Converted to: {Decimal(usdc_amount)/Decimal(10**6)} USDC")
                        print(f"Rate: {rate:.6f} USDC/{token_symbol}")
                        
                        # Special handling for USR: check if rate is too low (< 0.9)
                        if token_symbol == "USR" and rate < Decimal('0.9'):
                            print(f"\n⚠️  WARNING: USR/USDC rate on Base ({rate:.6f}) is below 0.9 threshold!")
                            print("🔄 This indicates a potential API issue with the Base network pair.")
                            print("🌐 Attempting fallback to Ethereum network for more reliable pricing...")
                            
                            # Try the same conversion on Ethereum network
                            if 'USR' in NETWORK_TOKENS.get('ethereum', {}):
                                ethereum_usr_address = NETWORK_TOKENS['ethereum']['USR']['address']
                                ethereum_usdc_address = NETWORK_TOKENS['ethereum']['USDC']['address']
                                
                                print(f"\nFallback to Ethereum network for {token_symbol}:")
                                print(f"Ethereum USR address: {ethereum_usr_address}")
                                print(f"Ethereum USDC address: {ethereum_usdc_address}")
                                print(f"Amount: {amount} wei")
                                print(f"Decimals: {NETWORK_TOKENS['ethereum'][token_symbol]['decimals']}")
                                
                                ethereum_result = get_quote(
                                    network="ethereum",
                                    sell_token=ethereum_usr_address,
                                    buy_token=ethereum_usdc_address,
                                    amount=str(amount),
                                    token_decimals=NETWORK_TOKENS['ethereum'][token_symbol]['decimals'],
                                    token_symbol=token_symbol
                                )
                                
                                if ethereum_result["quote"]:
                                    ethereum_usdc_amount = int(ethereum_result["quote"]["quote"]["buyAmount"])
                                    
                                    # Calculate Ethereum rate
                                    ethereum_usdc_normalized = Decimal(ethereum_usdc_amount) / Decimal(10**6)
                                    ethereum_token_decimals = NETWORK_TOKENS['ethereum'][token_symbol]['decimals']
                                    ethereum_token_normalized = Decimal(amount) / Decimal(10**ethereum_token_decimals)
                                    ethereum_rate = ethereum_usdc_normalized / ethereum_token_normalized
                                    
                                    print(f"\n✅ Ethereum network conversion successful:")
                                    print(f"Converted to: {Decimal(ethereum_usdc_amount)/Decimal(10**6)} USDC")
                                    print(f"Rate: {ethereum_rate:.6f} USDC/{token_symbol}")
                                    
                                    # Compare rates and choose the better one
                                    print(f"\n📊 RATE COMPARISON:")
                                    print(f"Base network rate:     {rate:.6f} USDC/{token_symbol}")
                                    print(f"Ethereum network rate: {ethereum_rate:.6f} USDC/{token_symbol}")
                                    
                                    rate_difference = abs(ethereum_rate - rate)
                                    rate_difference_pct = (rate_difference / max(rate, ethereum_rate)) * 100
                                    print(f"Difference: {rate_difference:.6f} ({rate_difference_pct:.2f}%)")
                                    
                                    if ethereum_rate >= rate:
                                        print(f"🎯 Using Ethereum rate as it's better or equal (threshold check passed: {ethereum_rate:.6f} >= 0.9)")
                                        usdc_amount = ethereum_usdc_amount
                                        rate = ethereum_rate
                                        print(f"📝 Final decision: Using Ethereum network result")
                                    else:
                                        print(f"🤔 Ethereum rate ({ethereum_rate:.6f}) is lower than Base rate ({rate:.6f})")
                                        print(f"📝 Final decision: Keeping Base network result despite low rate warning")
                                else:
                                    print(f"\n❌ Ethereum network conversion failed for {token_symbol}")
                                    print("📝 Final decision: Keeping Base network result despite low rate warning")
                                    print("Ethereum response:", json.dumps(ethereum_result, indent=2))
                            else:
                                print(f"\n❌ USR token not found in Ethereum network configuration")
                                print("📝 Final decision: Keeping Base network result despite low rate warning")
                        
                        total_usdc += usdc_amount
                        print(f"\n✅ Final result for {token_symbol}: {Decimal(usdc_amount)/Decimal(10**6)} USDC (rate: {rate:.6f})")
                        
                    else:
                        print(f"❌ Base network conversion failed for {token_symbol}")
                        print("Response:", json.dumps(result, indent=2))
                else:
                    result = get_quote(
                        network=network,
                        sell_token=token_address,
                        buy_token=NETWORK_TOKENS[network]['USDC']['address'],
                        amount=str(amount),
                        token_decimals=NETWORK_TOKENS[network][token_symbol]['decimals'],
                        token_symbol=token_symbol
                    )
                    
                    if result["quote"]:
                        usdc_amount = int(result["quote"]["quote"]["buyAmount"])
                        total_usdc += usdc_amount
                        print(f"Converted to: {Decimal(usdc_amount)/Decimal(10**6)} USDC")
                        # Calculate rate properly accounting for decimals (same logic as cow_client.py)
                        usdc_normalized = Decimal(usdc_amount) / Decimal(10**6)  # USDC to regular units
                        token_decimals = NETWORK_TOKENS[network][token_symbol]['decimals']
                        token_normalized = Decimal(amount) / Decimal(10**token_decimals)
                        rate = usdc_normalized / token_normalized
                        print(f"Rate: {rate:.6f} USDC/token")
                    else:
                        print(f"Failed to convert {token_symbol} to USDC")
                        print("Response:", json.dumps(result, indent=2))
            
            print(f"\nTotal USDC from all tokens: {Decimal(total_usdc)/Decimal(10**6)} USDC")
            print(f"Final conversion rate: {Decimal(total_usdc)/Decimal(balance_wei)*Decimal(10**12)} USDC/Pendle LP")
            
            # Calculate price impact (simplified for now)
            price_impact = 0  # TODO: Calculate actual price impact
            
            # Create conversion details for yvBal-GHO-USR
            conversion_details = {
                "step1_pendle_lp": {
                    "input": f"{Decimal(balance_wei)/Decimal(10**18)} Pendle LP",
                    "total_supply": f"{Decimal(total_lp_supply)/Decimal(10**18)} Pendle LP",
                    "share_ratio": f"{our_share_ratio:.6f} ({our_share_ratio*100:.4f}%)"
                },
                "step2_pt_sy": {
                    "initial_pt": f"{Decimal(pt_amount)/Decimal(10**18)} PT",
                    "initial_sy": f"{Decimal(sy_amount)/Decimal(10**18)} SY",
                    "pt_to_sy": f"{Decimal(pt_to_sy_amount)/Decimal(10**18)} SY",
                    "total_sy": f"{Decimal(total_sy_amount)/Decimal(10**18)} SY"
                },
                "step3_yearn_vault": {
                    "input": f"{Decimal(total_sy_amount)/Decimal(10**18)} yvBal",
                    "output": f"{Decimal(lp_amount)/Decimal(10**18)} Balancer LP",
                    "balancer_pool": lp_token_address,
                    "balancer_total_supply": f"{Decimal(balancer_total_supply)/Decimal(10**18)} Balancer LP",
                    "balancer_share": f"{our_share:.6f} ({our_share*100:.4f}%)"
                },
                "step4_underlying_tokens": {
                    "tokens": []
                },
                "step5_usdc": {
                    "total_usdc": f"{Decimal(total_usdc)/Decimal(10**6)} USDC",
                    "final_rate": f"{Decimal(total_usdc)/Decimal(balance_wei)*Decimal(10**12):.6f} USDC/Pendle LP"
                }
            }

            # Add details for each underlying token
            for token_address, amount in underlying_amounts:
                token_symbol = None
                for symbol, token_info in NETWORK_TOKENS['base'].items():
                    if token_info['address'].lower() == token_address.lower():
                        token_symbol = symbol
                        break
                
                if token_symbol:
                    token_details = {
                        "symbol": token_symbol,
                        "amount": f"{Decimal(amount)/Decimal(10**18)} {token_symbol}",
                        "address": token_address
                    }
                    conversion_details["step4_underlying_tokens"]["tokens"].append(token_details)
            
            return total_usdc, price_impact, "Fallback", None, {
                "amount": total_usdc,
                "price_impact": price_impact,
                "method": "fallback",
                "conversion_details": conversion_details
            }
        else:
            raise ValueError(f"Unsupported underlying token: {underlying_token['symbol']}")

    def get_earned_rewards(self, network, pool_id, address=None):
        """
        Get earned rewards for a specific pool
        If no address is provided, uses production address
        Returns a dict with raw reward amounts in wei
        """
        if address is None:
            address = PRODUCTION_ADDRESS

        rewards = {}
        try:
            pool = self.pools[network][pool_id]
            pool_info = pool.get('pool_info')
            if not pool_info:
                print(f"No pool info found for {network}.{pool_id}")
                return []
                
            # Use the reward pool from pool_info
            reward_pool_address = pool_info['rewardPool']
            print(f"\nGetting reward tokens for {network}.{pool_id}")
            print(f"Using reward pool: {reward_pool_address}")
            
            reward_pool = self.w3_instances[network].eth.contract(
                address=self.w3_instances[network].to_checksum_address(reward_pool_address),
                abi=BASE_REWARD_POOL_ABI
            )
            
            reward_tokens = Web3Retry.call_contract_function(
                reward_pool.functions.getRewardTokens().call
            )
            print(f"Found reward tokens: {reward_tokens}")
            
            for token_address in reward_tokens:
                earned = Web3Retry.call_contract_function(
                    reward_pool.functions.earned(
                        self.w3_instances[network].to_checksum_address(address),
                        self.w3_instances[network].to_checksum_address(token_address)
                    ).call
                )
                # Get token symbol from address
                token_symbol = self.get_token_symbol(network, token_address)
                if token_symbol:
                    rewards[token_symbol] = earned
                    print(f"Found {Decimal(earned)/Decimal(10**18)} {token_symbol} rewards")

            # Add Pendle rewards if not already included
            if "PENDLE" not in rewards:
                # Use the correct PENDLE token for the network
                pendle_address = NETWORK_TOKENS[network]["PENDLE"]["address"]
                print(f"\nChecking for PENDLE rewards using token: {pendle_address}")
                if pendle_address in reward_tokens:
                    pendle_earned = Web3Retry.call_contract_function(
                        reward_pool.functions.earned(
                            self.w3_instances[network].to_checksum_address(address),
                            self.w3_instances[network].to_checksum_address(pendle_address)
                        ).call
                    )
                    rewards["PENDLE"] = pendle_earned
                    print(f"Found {Decimal(pendle_earned)/Decimal(10**18)} PENDLE rewards")

            return rewards
        except Exception as e:
            print(f"Error fetching earned rewards for {network}.{pool_id}: {e}")
            print(f"Full error details: {str(e)}")
            return {}

    def get_token_symbol(self, network, token_address):
        """
        Get token symbol from address using NETWORK_TOKENS
        """
        for symbol, token_info in NETWORK_TOKENS[network].items():
            if token_info['address'].lower() == token_address.lower():
                return symbol
        return None

    def get_reward_value_in_usdc(self, token_symbol: str, amount: str) -> tuple:
        """
        Get USDC value for reward tokens using CoW Swap
        Returns tuple of (amount_out, price_impact, success, conversion_details)
        """
        print(f"\nAttempting to get quote for {token_symbol}:")
        
        result = get_quote(
            network="ethereum",
            sell_token=NETWORK_TOKENS['ethereum'][token_symbol]['address'],
            buy_token=USDC_ADDRESS,
            amount=amount,
            token_decimals=NETWORK_TOKENS['ethereum'][token_symbol]['decimals'],
            token_symbol=token_symbol
        )

        if result["quote"]:
            buy_amount = int(result["quote"]["quote"]["buyAmount"])
            price_impact_str = result["conversion_details"].get("price_impact", "0")
            
            # Handle case where price_impact is "N/A" (fallback case)
            if price_impact_str == "N/A":
                price_impact = 0
            else:
                price_impact = float(price_impact_str.rstrip("%"))
            
            return buy_amount, price_impact/100, True, result["conversion_details"]

        return 0, 0, False, {}

    def get_balances(self, address: str) -> Dict[str, Any]:
        print("\n" + "="*80)
        print("EQUILIBRIA BALANCE MANAGER")
        print("="*80)
        
        checksum_address = Web3.to_checksum_address(address)
        result = {"equilibria": {}}
        
        try:
            protocol_total = 0
            
            for network, pools in self.pools.items():
                print(f"\nProcessing network: {network}")
                result["equilibria"][network] = {}
                network_total = 0
                
                for pool_id, pool in pools.items():
                    print(f"\nProcessing position: {pool_id}")
                    
                    # Get staked balance
                    balance = self.get_staked_balance(network, pool_id, address)
                    if balance == 0:
                        continue
                    
                    # Get rewards
                    rewards = self.get_earned_rewards(network, pool_id, address)
                    
                    # Get LP value in USDC
                    usdc_amount, price_impact, used_method, direct_result, fallback_result = self.get_remove_liquidity_data(network, pool_id, balance)
                    
                    # Store method details
                    is_fallback = used_method == "Fallback"
                    method_note = "Direct conversion via Pendle SDK" if used_method == "Direct" else "LP -> PT/SY -> USDC conversion path"
                    
                    # Calculate rewards value in USDC
                    rewards_total = 0
                    rewards_data = {}
                    for token, amount in rewards.items():
                        if amount > 0:
                            token_usdc_amount, token_price_impact, success, conversion_details = self.get_reward_value_in_usdc(
                                token, str(amount)
                            )
                            rewards_total += token_usdc_amount
                            rewards_data[token] = {
                                "amount": str(amount),
                                "decimals": pool['config']['decimals'],
                                "value": {
                                    "USDC": {
                                        "amount": token_usdc_amount,
                                        "decimals": 6,
                                        "conversion_details": conversion_details
                                    }
                                }
                            }
                    
                    # Calculate position total
                    position_total = usdc_amount + rewards_total
                    network_total += position_total
                    
                    # Add position data to result
                    position_data = {
                        "staking_contract": pool['pool_info']['rewardPool'],
                        "amount": str(balance),
                        "decimals": pool['config']['decimals'],
                        "value": {
                            "USDC": {
                                "amount": usdc_amount,
                                "decimals": 6,
                                "conversion_details": {
                                    "source": "Pendle SDK",
                                    "price_impact": f"{price_impact:.6f}",
                                    "rate": f"{Decimal(usdc_amount)/Decimal(balance)*Decimal(10**12):.6f}",
                                    "fee_percentage": "0.0000%",
                                    "fallback": is_fallback,
                                    "note": f"Using {used_method} method: {method_note}"
                                }
                            }
                        },
                        "rewards": rewards_data,
                        "totals": {
                            "wei": position_total,
                            "formatted": f"{position_total/1e6:.6f}"
                        }
                    }

                    # Add conversion steps for yvBal-GHO-USR position
                    if pool_id == "yvBal-GHO-USR" and fallback_result and "conversion_details" in fallback_result:
                        position_data["conversion_steps"] = fallback_result["conversion_details"]

                    result["equilibria"][network][pool_id] = position_data

                    # Add network totals
                    result["equilibria"][network]["totals"] = {
                        "wei": network_total,
                        "formatted": f"{network_total/1e6:.6f}"
                    }
                    
                    protocol_total += network_total
            
            # Add protocol totals
            result["equilibria"]["totals"] = {
                "wei": protocol_total,
                "formatted": f"{protocol_total/1e6:.6f}"
            }
            
        except Exception as e:
            print(f"✗ Error fetching Equilibria positions: {str(e)}")
            return {"equilibria": {
                "totals": {
                    "wei": 0,
                    "formatted": "0.000000"
                }
            }}
        
        return result

# Code for direct testing
if __name__ == "__main__":
    import sys
    
    # Use command line argument if provided, otherwise use PRODUCTION_ADDRESS
    test_address = sys.argv[1] if len(sys.argv) > 1 else PRODUCTION_ADDRESS
    
    bm = BalanceManager()
    results = bm.get_balances(test_address)
    
    # Display position summary
    if results and "equilibria" in results:
        data = results["equilibria"]
        for network, pools in data.items():
            if "ethereum" in pools:
                for pool_id, pool in pools["ethereum"].items():
                    if "value" in pool and "USDC" in pool["value"]:
                        print(f"equilibria.{network}.{pool_id}: {pool['value']['USDC']['amount']/1e6:.6f} USDC")
                    if "rewards" in pool:
                        for token, reward in pool["rewards"].items():
                            if "value" in reward and "USDC" in reward["value"]:
                                print(f"equilibria.{network}.{pool_id}.rewards.{token}: {reward['value']['USDC']['amount']/1e6:.6f} USDC")
    
    print("\n" + "="*80)
    print("FINAL RESULT:")
    print("="*80 + "\n")
    print(json.dumps(results, indent=2))
