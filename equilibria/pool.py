from typing import Dict, Any
from datetime import datetime
from pendle.pool import get_pool_info as get_pendle_pool_info, PENDLE_POOLS

class EquilibriaPool:
    """
    Represents an Equilibria pool with its associated tokens and market information.
    Uses Pendle pool information for PT/SY tokens.
    """
    def __init__(
        self,
        network: str,
        market_address: str,
        reward_pool_address: str,
        booster_address: str,
        decimals: int,
        pendle_pt_symbol: str,  # Symbol of the PT token in Pendle
        underlying: Dict[str, Any]
    ):
        self.network = network
        self.market = market_address
        self.reward_pool = reward_pool_address
        self.booster = booster_address
        self.decimals = decimals
        self.pendle_pt_symbol = pendle_pt_symbol
        self.underlying = underlying

        # Get PT/SY information from Pendle
        pendle_info = get_pendle_pool_info(network, pendle_pt_symbol)[pendle_pt_symbol]
        self.pt = {
            'address': pendle_info['address'],
            'symbol': pendle_info['symbol'],
            'decimals': pendle_info['decimals']
        }
        self.sy = {
            'address': pendle_info['sy_token']['address'],
            'symbol': pendle_info['sy_token']['symbol'],
            'decimals': pendle_info['sy_token']['decimals']
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert pool information to dictionary format compatible with balance_manager.py"""
        return {
            'market_address': self.market,
            'reward_pool_address': self.reward_pool,
            'booster_address': self.booster,
            'decimals': self.decimals,
            'sy_token': self.sy,
            'pt_token': self.pt,
            'underlying': self.underlying
        }

# Define all Equilibria pools
EQUILIBRIA_POOLS = {
    'ethereum': {
        'fGHO': EquilibriaPool(
            network='ethereum',
            market_address='0xC64D59eb11c869012C686349d24e1D7C91C86ee2',
            reward_pool_address="0xba0928d9d0C2dA79522E45244CE859838999b21c",
            booster_address='0x4D32C8Ff2fACC771eC7Efc70d6A8468bC30C26bF',
            decimals=18,
            pendle_pt_symbol='PT-fGHO-31JUL2025',
            underlying={
                'fGHO': {
                    'address': '0x6A29A46E21C730DcA1d8b23d637c101cec605C5B',
                    'symbol': 'fGHO',
                    'decimals': 18
                }
            }
        )
    },
    'base': {
        'yvBal-GHO-USR': EquilibriaPool(
            network='base',
            market_address='0xA6b8cFE75Ca5e1b2A527AA255d10521FAaF24b61',  # Market LP
            reward_pool_address='0x785CD3813Ccd918104eEE4E58afAe5E12483eA66',  # Reward
            booster_address='0x2583A2538272f31e9A15dD12A432B8C96Ab4821d',  # Deposit
            decimals=18,
            pendle_pt_symbol='PT-yvBal-GHO-USR-25SEP2025',
            underlying={
                'yvBal-GHO-USR': {
                    'address': '0x69efa3cd7fc773fe227b9cc4f41132dcde020a29',
                    'symbol': 'yvBal-GHO-USR',
                    'decimals': 18
                }
            }
        )
    }
}

def get_pool_info(network: str, pool_id: str) -> Dict[str, Any]:
    """
    Get pool information for a specific pool.
    
    Args:
        network: Network identifier (ethereum/base)
        pool_id: Pool identifier
        
    Returns:
        Dictionary containing pool information
    """
    if network not in EQUILIBRIA_POOLS:
        raise ValueError(f"Network {network} not supported")
        
    if pool_id not in EQUILIBRIA_POOLS[network]:
        raise ValueError(f"Pool {pool_id} not found on {network}")
        
    return EQUILIBRIA_POOLS[network][pool_id].to_dict()

def get_all_pools() -> Dict[str, Dict[str, Any]]:
    """
    Get all Equilibria pools information.
    
    Returns:
        Dictionary containing all pools information organized by network
    """
    return {
        network: {
            pool_id: pool.to_dict()
            for pool_id, pool in pools.items()
        }
        for network, pools in EQUILIBRIA_POOLS.items()
    } 