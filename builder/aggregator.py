import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import json
from decimal import Decimal
from web3 import Web3
from datetime import datetime, timezone
import asyncio
from functools import lru_cache
import time
from config.networks import RPC_URLS, NETWORK_TOKENS
from sky.balance_manager import BalanceManager as SkyBalanceManager
from pendle.balance_manager import PendleBalanceManager
from equilibria.balance_manager import BalanceManager as EquilibriaBalanceManager
from convex.balance_manager import ConvexBalanceManager
from cowswap.cow_client import get_quote
from spot.balance_manager import SpotBalanceManager

# Add parent directory to PYTHONPATH
root_path = str(Path(__file__).parent.parent)
sys.path.append(root_path)

from shares.supply_reader import SupplyReader

class BalanceAggregator:
    """
    Master aggregator that combines balances from multiple protocols.
    Currently supports:
    - Pendle (Ethereum + Base)
    - Sky Protocol (Ethereum + Base)
    - Equilibria (Ethereum + Base)
    """
    
    def __init__(self):
        # Initialize Web3 connections with connection pooling
        self.w3_eth = Web3(Web3.HTTPProvider(RPC_URLS["ethereum"], request_kwargs={'timeout': 30}))
        self.w3_base = Web3(Web3.HTTPProvider(RPC_URLS["base"], request_kwargs={'timeout': 30}))
        
        # Initialize protocol managers
        self.sky_manager = SkyBalanceManager()
        self.pendle_manager = PendleBalanceManager()
        self.equilibria_manager = EquilibriaBalanceManager()
        self.convex_manager = ConvexBalanceManager()
        self.spot_manager = SpotBalanceManager()
        
        # Initialize token contracts with caching
        self.token_contracts = {}
        self._init_token_contracts()

    def _init_token_contracts(self):
        """Initialize token contracts with caching"""
        for network, tokens in NETWORK_TOKENS.items():
            self.token_contracts[network] = {}
            for symbol, token_info in tokens.items():
                if symbol not in NETWORK_TOKENS[network]:
                    continue
                    
                w3 = self.w3_eth if network == "ethereum" else self.w3_base
                self.token_contracts[network][symbol] = w3.eth.contract(
                    address=Web3.to_checksum_address(token_info["address"]),
                    abi=self._get_erc20_abi()
                )

    @lru_cache(maxsize=128)
    def _get_erc20_abi(self) -> List:
        """Get the ERC20 ABI for token interactions with caching"""
        return [
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "type": "function"
            }
        ]

    @lru_cache(maxsize=256)
    def get_token_balance(self, network: str, token_symbol: str, address: str) -> float:
        """Get the balance of a specific token for an address with caching"""
        try:
            contract = self.token_contracts[network][token_symbol]
            balance = contract.functions.balanceOf(address).call()
            decimals = contract.functions.decimals().call()
            return float(balance) / (10 ** decimals)
        except Exception as e:
            print(f"Error getting balance for {token_symbol} on {network}: {str(e)}")
            return 0.0

    async def _get_protocol_balances(self, protocol_manager, address: str) -> Dict:
        """Get balances for a specific protocol asynchronously"""
        try:
            if hasattr(protocol_manager, 'get_balances_async'):
                return await protocol_manager.get_balances_async(address)
            else:
                return protocol_manager.get_balances(address)
        except Exception as e:
            print(f"✗ Error fetching {protocol_manager.__class__.__name__} positions: {str(e)}")
            return {}

    async def get_balances_async(self, address: str) -> Dict:
        """Get all balances for an address across all protocols asynchronously"""
        result = {}
        checksum_address = Web3.to_checksum_address(address)
        
        # Create tasks for all protocol balance fetches
        tasks = [
            self._get_protocol_balances(self.sky_manager, checksum_address),
            self._get_protocol_balances(self.pendle_manager, checksum_address),
            self._get_protocol_balances(self.equilibria_manager, checksum_address),
            self._get_protocol_balances(self.convex_manager, checksum_address),
            self._get_protocol_balances(self.spot_manager, checksum_address)
        ]
        
        # Wait for all tasks to complete
        protocol_results = await asyncio.gather(*tasks)
        
        # Process results
        for protocol_result in protocol_results:
            if protocol_result:
                result.update(protocol_result)
        
        # Get direct token balances
        for network, tokens in NETWORK_TOKENS.items():
            for symbol, token_info in tokens.items():
                if "protocol" in token_info:
                    continue
                    
                balance = self.get_token_balance(network, symbol, checksum_address)
                if balance > 0:
                    result[f"{network}_{symbol}"] = {
                        "amount": balance,
                        "token": token_info
                    }
        
        return result

    def get_balances(self, address: str) -> Dict:
        """Synchronous wrapper for get_balances_async"""
        return asyncio.run(self.get_balances_async(address))

    def get_total_value(self, balances: Dict) -> float:
        """Calculate total value in USDC for all positions"""
        total_value = 0.0
        
        # For protocols with direct totals (Sky, Pendle, Equilibria)
        for key, value in balances.items():
            if isinstance(value, dict) and "total_usdc" in value:
                total_value += value["total_usdc"]
        
        return total_value

async def build_overview_async(address: str) -> Dict:
    """Build a complete overview of all positions and their values asynchronously"""
    aggregator = BalanceAggregator()
    balances = await aggregator.get_balances_async(address)
    
    # Get total supply from shares
    supply_reader = SupplyReader()
    total_supply = supply_reader.get_total_supply()
    
    # Initialize variables
    total_value = 0
    detailed_positions = {}
    
    # Process protocol positions
    for protocol in ['equilibria', 'pendle', 'sky', 'convex']:
        if protocol in balances and balances[protocol]:  # Only process if protocol exists and has data
            has_positions = False
            for network, network_data in balances[protocol].items():
                for asset, asset_data in network_data.items():
                    if asset != 'totals' and isinstance(asset_data, dict):
                        # Calculate total value including all underlying tokens
                        position_value = 0
                        
                        # Special handling for Pendle positions
                        if protocol == 'pendle':
                            # Add PT value if present
                            if 'pt' in asset_data and asset_data['pt'] and 'value' in asset_data['pt']:
                                if 'USDC' in asset_data['pt']['value']:
                                    position_value += float(asset_data['pt']['value']['USDC']['amount']) / 1e6
                            
                            # Add LP value if present
                            if 'lp' in asset_data and asset_data['lp'] and 'value' in asset_data['lp']:
                                if 'USDC' in asset_data['lp']['value']:
                                    position_value += float(asset_data['lp']['value']['USDC']['amount']) / 1e6
                            
                            # Add rewards if present
                            if 'rewards' in asset_data:
                                for reward_token, reward_data in asset_data['rewards'].items():
                                    if 'value' in reward_data and 'USDC' in reward_data['value']:
                                        position_value += float(reward_data['value']['USDC']['amount']) / 1e6
                        elif protocol == 'convex':
                            # Use the total from rewards calculation for Convex positions
                            if 'rewards' in asset_data and 'totals' in asset_data['rewards']:
                                position_value = float(asset_data['rewards']['totals']['formatted'])
                            # Fallback to other totals if rewards total not available
                            elif 'totals' in asset_data:
                                position_value = float(asset_data['totals']['formatted'])
                            elif network in balances[protocol] and 'totals' in balances[protocol][network]:
                                position_value = float(balances[protocol][network]['totals']['formatted'])
                            elif 'totals' in balances[protocol]:
                                position_value = float(balances[protocol]['totals']['formatted'])
                        else:
                            # Handle other protocols as before
                            if 'value' in asset_data:
                                # Handle direct USDC value
                                if 'USDC' in asset_data['value']:
                                    position_value += float(asset_data['value']['USDC']['amount']) / 1e6
                                # Handle other token values that have been converted to USDC
                                for token, token_data in asset_data['value'].items():
                                    if token != 'USDC' and 'value' in token_data and 'USDC' in token_data['value']:
                                        position_value += float(token_data['value']['USDC']['amount']) / 1e6
                            
                            # Add rewards if present
                            if 'rewards' in asset_data:
                                for reward_token, reward_data in asset_data['rewards'].items():
                                    if 'value' in reward_data and 'USDC' in reward_data['value']:
                                        position_value += float(reward_data['value']['USDC']['amount']) / 1e6
                        
                        if position_value > 0:
                            total_value += position_value
                            has_positions = True
            
            # Only add to detailed_positions if there are actual positions
            if has_positions:
                detailed_positions[protocol] = balances[protocol]
    
    # Process spot positions
    spot_details = balances.get('spot', {})
    if 'totals' in spot_details:
        total_value += float(spot_details['totals']['formatted'])
    
    # Calculate share price using formatted total supply
    formatted_supply = float(total_supply) / 1e18
    share_price = total_value / formatted_supply if formatted_supply > 0 else 0
    
    # Generate positions dictionary at the end
    positions = {}
    
    # Add protocol positions
    for protocol in ['equilibria', 'pendle', 'sky', 'convex']:
        if protocol in detailed_positions:
            for network, network_data in detailed_positions[protocol].items():
                for asset, asset_data in network_data.items():
                    if asset != 'totals' and isinstance(asset_data, dict):
                        position_value = 0
                        
                        # Calculate position value based on protocol type
                        if protocol == 'pendle':
                            if 'pt' in asset_data and asset_data['pt'] and 'value' in asset_data['pt']:
                                position_value += float(asset_data['pt']['value']['USDC']['amount']) / 1e6
                            if 'lp' in asset_data and asset_data['lp'] and 'value' in asset_data['lp']:
                                position_value += float(asset_data['lp']['value']['USDC']['amount']) / 1e6
                            if 'rewards' in asset_data:
                                for reward_data in asset_data['rewards'].values():
                                    position_value += float(reward_data['value']['USDC']['amount']) / 1e6
                        elif protocol == 'convex':
                            if 'totals' in asset_data:
                                position_value = float(asset_data['totals']['formatted'])
                        else:
                            if 'value' in asset_data and 'USDC' in asset_data['value']:
                                position_value += float(asset_data['value']['USDC']['amount']) / 1e6
                            if 'rewards' in asset_data:
                                for reward_data in asset_data['rewards'].values():
                                    position_value += float(reward_data['value']['USDC']['amount']) / 1e6
                        
                        if position_value > 0:
                            positions[f"{protocol}.{network}.{asset}"] = f"{position_value:.6f}"
    
    # Add spot positions
    for network in ['ethereum', 'base']:
        if network in spot_details and 'totals' in spot_details[network]:
            positions[f"spot.{network}"] = spot_details[network]['totals']['formatted']
    
    return {
        "address": address,
        "nav": {
            "usdc": f"{total_value:.6f}",
            "share_price": f"{share_price:.6f}",
            "total_supply": str(total_supply)
        },
        "positions": positions,
        "protocols": detailed_positions,
        "spot": spot_details
    }

def build_overview(address: str) -> Dict:
    """Synchronous wrapper for build_overview_async"""
    return asyncio.run(build_overview_async(address))

async def main_async():
    """Async main function"""
    DEFAULT_ADDRESS = '0xc6835323372A4393B90bCc227c58e82D45CE4b7d'
    address = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ADDRESS
    
    if not Web3.is_address(address):
        print(f"Error: Invalid address format: {address}")
        return None
    
    start_time = time.time()
    overview = await build_overview_async(address)
    end_time = time.time()
    
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    from bson import ObjectId
    mongo_id = ObjectId()
    
    final_result = {
        **overview,
        "address": address,
        "created_at": created_at,
        "_id": str(mongo_id),
        "execution_time": f"{end_time - start_time:.2f} seconds"
    }
    
    print("\n" + "="*80)
    print("FINAL AGGREGATED RESULT")
    print("="*80 + "\n")
    print(json.dumps(final_result, indent=2))
    
    return final_result

def main():
    """Synchronous wrapper for main_async"""
    return asyncio.run(main_async())

if __name__ == "__main__":
    main()
