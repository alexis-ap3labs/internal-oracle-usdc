import sys
from pathlib import Path
# Add parent directory to PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))

from web3 import Web3
from decimal import Decimal
from typing import Dict, Any
from dotenv import load_dotenv
from config.networks import RPC_URLS, NETWORK_TOKENS
from utils.retry import Web3Retry, APIRetry

"""
Sky Protocol balance manager module.
Provides high-level interface for fetching Sky Protocol positions and balances.
Handles direct interaction with Sky Protocol contracts and balance aggregation.
"""

# Load environment variables
load_dotenv()

# Minimal ABI for sUSDS contract
MINIMAL_SUSDS_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"name": "shares", "type": "uint256"}],
        "name": "convertToAssets",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

# Production address
PRODUCTION_ADDRESS = "0xc6835323372A4393B90bCc227c58e82D45CE4b7d"

class BalanceManager:
    def __init__(self):
        # Initialize Web3 connections
        self.eth_w3 = Web3(Web3.HTTPProvider(RPC_URLS['ethereum']))
        self.base_w3 = Web3(Web3.HTTPProvider(RPC_URLS['base']))
        
        # Initialize contracts
        self.eth_contract = self.eth_w3.eth.contract(
            address=NETWORK_TOKENS['ethereum']['sUSDS']['address'],
            abi=MINIMAL_SUSDS_ABI
        )
        self.base_contract = self.base_w3.eth.contract(
            address=NETWORK_TOKENS['base']['sUSDS']['address'],
            abi=MINIMAL_SUSDS_ABI
        )

    def get_balances(self, address: str) -> Dict[str, Any]:
        """Get Sky Protocol balances for the given address"""
        print("\n" + "="*80)
        print("SKY PROTOCOL BALANCE MANAGER")
        print("="*80)
        
        checksum_address = Web3.to_checksum_address(address)
        balances = {"sky": {}}
        total_usdc_wei = 0

        # Process each network
        for network, contract in [("ethereum", self.eth_contract), ("base", self.base_contract)]:
            try:
                print(f"\nProcessing network: {network}")
                
                # Contract information
                print("\nContract information:")
                print(f"  token: {NETWORK_TOKENS[network]['sUSDS']['address']} (sUSDS)")
                print(f"  underlying: USDS")
                
                # Get balance
                print("\nQuerying sUSDS balance:")
                print(f"  Contract: {NETWORK_TOKENS[network]['sUSDS']['address']}")
                print("  Function: balanceOf(address) - Returns user's sUSDS balance")
                balance = Web3Retry.call_contract_function(
                    contract.functions.balanceOf(checksum_address).call
                )
                
                if balance > 0:
                    print(f"  Amount: {balance} (decimals: 18)")
                    print(f"  Formatted: {(Decimal(balance) / Decimal(10**18)):.6f} sUSDS")
                    
                    # Convert to USDS using Ethereum contract
                    print("\nConverting sUSDS to USDS:")
                    print(f"  Contract: {NETWORK_TOKENS['ethereum']['sUSDS']['address']}")
                    print("  Function: convertToAssets(uint256) - Returns equivalent USDS amount")
                    usds_value = Web3Retry.call_contract_function(
                        self.eth_contract.functions.convertToAssets(balance).call
                    )
                    print(f"  USDS value: {(Decimal(usds_value) / Decimal(10**18)):.6f} USDS")
                    
                    # Convert USDS to USDC via CoWSwap
                    print("\nConverting USDS to USDC via CoWSwap:")
                    from cowswap.cow_client import get_quote
                    
                    quote_result = get_quote(
                        network=network,
                        sell_token=NETWORK_TOKENS[network]['USDS']['address'],
                        buy_token=NETWORK_TOKENS[network]['USDC']['address'],
                        amount=str(usds_value),
                        token_decimals=18,
                        token_symbol='USDS'
                    )
                    
                    if quote_result["quote"]:
                        usdc_amount = int(quote_result["quote"]["quote"]["buyAmount"])
                        conversion_details = quote_result["conversion_details"]
                        
                        # Handle case where price_impact is "N/A"
                        if conversion_details.get("price_impact") == "N/A":
                            conversion_details["price_impact"] = "0.0000%"
                        
                        print(f"✓ Converted to USDC: {usdc_amount/1e6:.6f} USDC")
                        print(f"  Rate: {conversion_details['rate']} USDC/USDS")
                        print(f"  Source: {conversion_details['source']}")
                        print(f"  Note: {conversion_details['note']}")
                    
                        # Add to results
                        if network not in balances["sky"]:
                            balances["sky"][network] = {}
                        
                        balances["sky"][network]["sUSDS"] = {
                            "amount": str(balance),
                            "decimals": 18,
                            "value": {
                                "USDC": {
                                    "amount": str(usdc_amount),
                                    "decimals": 6,
                                    "conversion_details": conversion_details
                                }
                            }
                        }
                        
                        # Add position totals
                        balances["sky"][network]["sUSDS"]["totals"] = {
                            "wei": usdc_amount,
                            "formatted": f"{usdc_amount/1e6:.6f}"
                        }
                        
                        # Add network totals
                        if "totals" not in balances["sky"][network]:
                            balances["sky"][network]["totals"] = {
                                "wei": 0,
                                "formatted": "0.000000"
                            }
                        balances["sky"][network]["totals"]["wei"] += usdc_amount
                        balances["sky"][network]["totals"]["formatted"] = f"{balances['sky'][network]['totals']['wei']/1e6:.6f}"
                        
                        total_usdc_wei += usdc_amount

            except Exception as e:
                print(f"Error processing {network}: {str(e)}")
                continue

        # Add protocol total
        if total_usdc_wei > 0:
            balances["sky"]["totals"] = {
                "wei": total_usdc_wei,
                "formatted": f"{total_usdc_wei/1e6:.6f}"
            }
            
        print("\n[Sky] Calculation complete")
        
        # Print position summary
        if balances["sky"]:
            for network in ["ethereum", "base"]:
                if network in balances["sky"] and "sUSDS" in balances["sky"][network]:
                    usdc_value = int(balances["sky"][network]["sUSDS"]["totals"]["wei"])
                    print(f"sky.{network}.sUSDS: {usdc_value/1e6:.6f} USDC")

        return balances

def main():
    """CLI utility for testing"""
    import json
    
    # Use command line argument if provided, otherwise use production address
    test_address = sys.argv[1] if len(sys.argv) > 1 else PRODUCTION_ADDRESS
    
    manager = BalanceManager()
    balances = manager.get_balances(test_address)
    
    print("\n" + "="*80)
    print("FINAL RESULT:")
    print("="*80 + "\n")
    print(json.dumps(balances, indent=2))

if __name__ == "__main__":
    main() 