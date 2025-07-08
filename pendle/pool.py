from typing import Dict, Any
from datetime import datetime

class PendlePool:
    """
    Represents a Pendle pool with its associated tokens (PT, SY) and market information.
    """
    def __init__(
        self,
        network: str,
        pt_address: str,
        pt_symbol: str,
        pt_name: str,
        market_address: str,
        expiry_timestamp: int,
        sy_address: str,
        sy_symbol: str,
        sy_name: str,
        underlying_token: Dict[str, Any]
    ):
        self.network = network
        self.pt = {
            "address": pt_address,
            "symbol": pt_symbol,
            "name": pt_name,
            "decimals": 18
        }
        self.market = market_address
        self.expiry = expiry_timestamp
        self.sy = {
            "address": sy_address,
            "symbol": sy_symbol,
            "name": sy_name,
            "decimals": 18
        }
        self.underlying = underlying_token

    @property
    def is_expired(self) -> bool:
        """Check if the PT token is expired based on current timestamp"""
        return datetime.now().timestamp() > self.expiry

    def to_dict(self) -> Dict[str, Any]:
        """Convert pool information to dictionary format compatible with networks.py"""
        return {
            self.pt["symbol"]: {
                "address": self.pt["address"],
                "decimals": self.pt["decimals"],
                "name": self.pt["name"],
                "symbol": self.pt["symbol"],
                "protocol": "pendle",
                "market": self.market,
                "underlying": {
                    self.underlying["symbol"]: self.underlying
                },
                "expiry": self.expiry,
                "sy_token": {
                    "address": self.sy["address"],
                    "decimals": self.sy["decimals"],
                    "name": self.sy["name"],
                    "symbol": self.sy["symbol"]
                }
            }
        }

# Define all Pendle pools
PENDLE_POOLS = {
    "ethereum": {
        "PT-fGHO-31JUL2025": PendlePool(
            network="ethereum",
            pt_address="0xaacae34960cac6f32826f81e15f855f8e8c7f39e",
            pt_symbol="PT-fGHO-31JUL2025",
            pt_name="PT fGHO 31JUL2025",
            market_address="0xC64D59eb11c869012C686349d24e1D7C91C86ee2",
            expiry_timestamp=1753920000,
            sy_address="0x4726fcb2fbe4398449bb7ce44eb458dbd7141191",
            sy_symbol="SY-fGHO",
            sy_name="SY fGHO",
            underlying_token={
                "address": "0x6A29A46E21C730DcA1d8b23d637c101cec605C5B",
                "decimals": 18,
                "name": "fGHO",
                "symbol": "fGHO"
            }
        ),
        "PT-csUSDL-31JUL2025": PendlePool(
            network="ethereum",
            pt_address="0xf10A134A987E22ffa9463570A6D1eb92a63Fc178",
            pt_symbol="PT-csUSDL-31JUL2025",
            pt_name="PT Coinshift USDL 31JUL2025",
            market_address="0x08bf93c8f85977c64069dd34c5da7b1c636e104f",
            expiry_timestamp=1753920000,
            sy_address="0x8077b6f34e9193d5bbb0ef06a73119060534d130",
            sy_symbol="SY-csUSDL",
            sy_name="SY Coinshift USDL",
            underlying_token={
                "address": "0x7751E2F4b8ae93EF6B79d86419d42FE3295A4559",
                "decimals": 18,
                "name": "Wrapped USDL",
                "symbol": "wUSDL"
            }
        )
    },
    "base": {
        "PT-USR-24APR2025": PendlePool(
            network="base",
            pt_address="0xec443e7E0e745348E500084892C89218B3ba4683",
            pt_symbol="PT-USR-24APR2025",
            pt_name="Pendle PT Resolv USD 24APR2025",
            market_address="0xe15578523937ed7f08e8f7a1fa8a021e07025a08",
            expiry_timestamp=1745452800,
            sy_address="",  # Add SY token address if available
            sy_symbol="SY-USR",
            sy_name="SY Resolv USD",
            underlying_token={
                "address": "0x35E5dB674D8e93a03d814FA0ADa70731efe8a4b9",
                "decimals": 18,
                "name": "Resolve USD",
                "symbol": "USR"
            }
        ),
        "PT-yvBal-GHO-USR-25SEP2025": PendlePool(
            network="base",
            pt_address="0xe4862da6e20c3a2a4e437a0ecdb823a2aebbf140",
            pt_symbol="PT-yvBal-GHO-USR-25SEP2025",
            pt_name="PT Balancer GHO-USR yVault 25SEP2025",
            market_address="0xa6b8cfe75ca5e1b2a527aa255d10521faaf24b61",
            expiry_timestamp=1756195200,  # 25 September 2025
            sy_address="0x66d32c781a5be5a78ec9e260cad288d75de80295",
            sy_symbol="SY-yvBal-GHO-USR",
            sy_name="SY Balancer GHO-USR yVault",
            underlying_token={
                "address": "0x69efa3cd7fc773fe227b9cc4f41132dcde020a29",
                "decimals": 18,
                "name": "Balancer GHO-USR yVault",
                "symbol": "yvBal-GHO-USR"
            }
        )
    }
}

def get_pool_info(network: str, pt_symbol: str) -> Dict[str, Any]:
    """
    Get pool information for a specific PT token.
    
    Args:
        network: Network identifier (ethereum/base)
        pt_symbol: PT token symbol
        
    Returns:
        Dictionary containing pool information
    """
    if network not in PENDLE_POOLS:
        raise ValueError(f"Network {network} not supported")
        
    if pt_symbol not in PENDLE_POOLS[network]:
        raise ValueError(f"PT token {pt_symbol} not found on {network}")
        
    return PENDLE_POOLS[network][pt_symbol].to_dict()

def get_all_pools() -> Dict[str, Dict[str, Any]]:
    """
    Get all Pendle pools information.
    
    Returns:
        Dictionary containing all pools information organized by network
    """
    return {
        network: {
            pt_symbol: pool.to_dict()[pt_symbol]
            for pt_symbol, pool in pools.items()
        }
        for network, pools in PENDLE_POOLS.items()
    } 