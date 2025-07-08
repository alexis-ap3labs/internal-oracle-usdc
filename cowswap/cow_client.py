import requests
from datetime import datetime, timezone
import sys
from pathlib import Path
import os
from dotenv import load_dotenv
import json
from decimal import Decimal
import time
import logging
import random


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'  # Simplified format
)
logger = logging.getLogger(__name__)

"""
CoW Protocol (CoW Swap) API client.
Handles token price discovery and quote fetching for USDC conversions.
Used by balance managers to value non-USDC assets.
"""

# Add parent directory to PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))

from config.networks import NETWORK_TOKENS
from utils.retry import APIRetry

# Load environment variables
load_dotenv()

# Zero address for price quotes
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Rate limiting management
class RateLimiter:
    def __init__(self):
        self.last_request_time = 0
        self.min_delay = 1.5  # Minimum delay between requests in seconds (more conservative)
        self.backoff_delay = 0  # Current backoff delay
        self.max_backoff = 120  # Maximum backoff delay (2 minutes)
        self.consecutive_failures = 0
        self.circuit_breaker_threshold = 5  # Number of consecutive failures before circuit breaker
        self.circuit_breaker_timeout = 300  # 5 minutes circuit breaker timeout
        self.circuit_breaker_triggered = False
        self.circuit_breaker_time = 0
        
    def wait_if_needed(self):
        """Wait if we need to respect rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # Calculate total delay needed
        total_delay = self.min_delay + self.backoff_delay
        
        if time_since_last < total_delay:
            wait_time = total_delay - time_since_last
            logger.info(f"Rate limiting: waiting {wait_time:.1f}s before next request")
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def handle_rate_limit_error(self):
        """Handle rate limit error by increasing backoff"""
        if self.backoff_delay == 0:
            self.backoff_delay = 2  # Start with 2 seconds
        else:
            self.backoff_delay = min(self.backoff_delay * 2, self.max_backoff)
        
        # Add some randomness to avoid thundering herd
        jitter = random.uniform(0.1, 0.5)
        wait_time = self.backoff_delay + jitter
        
        logger.warning(f"Rate limit hit! Backing off for {wait_time:.1f}s")
        time.sleep(wait_time)
    
    def reset_backoff(self):
        """Reset backoff delay after successful request"""
        if self.backoff_delay > 0:
            logger.info("✓ Rate limit recovered, resetting backoff")
            self.backoff_delay = 0
    
    def adjust_delay_for_load(self, concurrent_requests: int = 1):
        """Adjust delay based on expected load"""
        if concurrent_requests > 10:
            self.min_delay = 3.0  # Increase delay for high load
        elif concurrent_requests > 5:
            self.min_delay = 2.0  # Moderate delay for medium load
        else:
            self.min_delay = 1.5  # Default delay for low load

# Global rate limiter instance
rate_limiter = RateLimiter()

def get_rate_limiter_status():
    """Get current rate limiter status for debugging"""
    return {
        "min_delay": rate_limiter.min_delay,
        "backoff_delay": rate_limiter.backoff_delay,
        "time_since_last_request": time.time() - rate_limiter.last_request_time,
        "is_backing_off": rate_limiter.backoff_delay > 0
    }

def batch_get_quotes(quote_requests: list, batch_size: int = 5, batch_delay: float = 10.0):
    """
    Process multiple quote requests in batches to avoid rate limiting.
    
    Args:
        quote_requests: List of dict with keys: network, sell_token, buy_token, amount, token_decimals, token_symbol
        batch_size: Number of requests to process in each batch
        batch_delay: Delay between batches in seconds
    
    Returns:
        List of quote results in the same order as input
    """
    results = []
    total_batches = (len(quote_requests) + batch_size - 1) // batch_size
    
    logger.info(f"Processing {len(quote_requests)} quotes in {total_batches} batches of {batch_size}")
    
    # Adjust rate limiter for batch processing
    rate_limiter.adjust_delay_for_load(len(quote_requests))
    
    for i in range(0, len(quote_requests), batch_size):
        batch = quote_requests[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} requests)")
        
        # Process batch
        for request in batch:
            result = get_quote(**request)
            results.append(result)
        
        # Wait between batches (except for the last batch)
        if i + batch_size < len(quote_requests):
            logger.info(f"Waiting {batch_delay}s before next batch...")
            time.sleep(batch_delay)
    
    logger.info(f"✓ Completed all {len(quote_requests)} quotes")
    return results

def reset_rate_limiter():
    """Reset rate limiter state - useful for testing or after long pauses"""
    global rate_limiter
    rate_limiter = RateLimiter()
    logger.info("Rate limiter reset")

def get_quote(network: str, sell_token: str, buy_token: str, amount: str, token_decimals: int = 18, token_symbol: str = "", max_retries: int = 3) -> dict:
    """
    Fetches price quote from CoW Protocol API for token conversion with fallback mechanism.
    
    Features automatic rate limiting and retry logic for 403/429 errors.
    
    Args:
        network: Network identifier ('ethereum' or 'base')
        sell_token: Address of token to sell
        buy_token: Address of token to buy (usually USDC)
        amount: Amount to sell in wei (as string)
        token_decimals: Decimals of the sell token (default 18)
        token_symbol: Symbol of the sell token (default "")
        max_retries: Maximum number of retries for stablecoin quotes (default 3)
    
    Returns:
        Dict containing:
        - quote: API response with buy amount, etc.
        - conversion_details: Information about the conversion method used
    
    Note:
        This function includes automatic rate limiting (1.5s minimum delay between requests)
        and exponential backoff for 403/429 errors. For multiple requests, consider using
        batch_get_quotes() to process them more efficiently.
    """
    api_network = "mainnet" if network == "ethereum" else network
    api_url = f"https://api.cow.fi/{api_network}/api/v1/quote"

    def make_request(params, max_retries=3):
        """Make request with rate limiting and 403 retry logic"""
        for attempt in range(max_retries):
            try:
                # Wait if needed for rate limiting
                rate_limiter.wait_if_needed()
                
                # Make the request
                response = APIRetry.post(api_url, json=params)
                
                if response.ok:
                    # Request successful, reset backoff
                    rate_limiter.reset_backoff()
                    return response.json()
                
                # Handle specific error codes
                if response.status_code == 403:
                    logger.warning(f"Rate limit (403) on attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:  # Don't wait on last attempt
                        rate_limiter.handle_rate_limit_error()
                        continue
                elif response.status_code == 429:
                    logger.warning(f"Too Many Requests (429) on attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:  # Don't wait on last attempt
                        rate_limiter.handle_rate_limit_error()
                        continue
                else:
                    logger.error(f"CoWSwap request failed: {response.status_code}")
                    return response.text
                    
            except Exception as e:
                logger.error(f"Request exception on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Simple delay for other errors
                    continue
        
        # All retries failed
        logger.error(f"All {max_retries} attempts failed")
        return "Request failed after all retries"

    base_params = {
        "sellToken": sell_token,
        "buyToken": buy_token,
        "from": ZERO_ADDRESS,
        "receiver": ZERO_ADDRESS,
        "validTo": int(datetime.now(timezone.utc).timestamp() + 3600),  # 1 hour validity
        "appData": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "partiallyFillable": False,
        "sellTokenBalance": "erc20",
        "buyTokenBalance": "erc20",
        "kind": "sell",
        "priceQuality": "fast"
    }

    # Direct 1:1 conversion for USDC
    if token_symbol == "USDC":
        logger.info("✓ Direct 1:1 conversion with USDC")
        return {
            "quote": {
                "quote": {
                    "buyAmount": amount,
                    "sellAmount": amount,
                    "feeAmount": "0"
                }
            },
            "conversion_details": {
                "source": "Direct",
                "price_impact": "0",
                "rate": "1",
                "fee_percentage": "0.0000%",
                "fallback": False,
                "note": "Direct 1:1 conversion"
            }
        }

    # Try direct quote first with original amount in wei
    status = get_rate_limiter_status()
    if status["is_backing_off"]:
        logger.info(f"Getting quote for {token_symbol}... (⏳ rate limit backoff: {status['backoff_delay']}s)")
    else:
        logger.info(f"Getting quote for {token_symbol}...")
    
    # Proceed with normal flow for all tokens
    params = {**base_params, "sellAmountBeforeFee": str(amount)}
    quote = make_request(params, max_retries=3)

    # If successful, return with direct quote details
    if isinstance(quote, dict) and 'quote' in quote:
        usdc_amount = int(quote['quote']['buyAmount'])
        # Calculate rate properly accounting for decimals
        # buyAmount is in USDC (6 decimals), sellAmount is in token decimals
        buy_amount_normalized = Decimal(quote['quote']['buyAmount']) / Decimal(10**6)  # USDC to regular units
        sell_amount_normalized = Decimal(quote['quote']['sellAmount']) / Decimal(10**token_decimals)  # Token to regular units
        rate = buy_amount_normalized / sell_amount_normalized
        
        logger.info(f"✓ {usdc_amount/1e6:.6f} USDC (Rate: {rate:.6f} USDC/token)")
        
        return {
            "quote": quote,
            "conversion_details": {
                "source": "CoWSwap",
                "price_impact": quote['quote'].get('priceImpact', '0'),
                "rate": str(rate),
                "fee_percentage": str(Decimal(quote['quote'].get('feeAmount', '0')) / Decimal(amount) * 100),
                "fallback": False,
                "note": "Direct CoWSwap quote"
            }
        }

    # If amount too small, try fallback with reference amount
    if isinstance(quote, str) and ("SellAmountDoesNotCoverFee" in quote or "NoLiquidity" in quote or "Request failed" in quote):
        logger.info("! Using fallback method...")
        
        # Calculate reference amount in wei (1000 tokens)
        reference_amount = str(1000 * 10**token_decimals)
        
        params = {**base_params, "sellAmountBeforeFee": reference_amount}
        fallback_quote = make_request(params, max_retries=3)

        if isinstance(fallback_quote, dict) and 'quote' in fallback_quote:
            # Calculate rate using reference quote with proper decimal handling
            sell_amount_normalized = Decimal(fallback_quote['quote']['sellAmount']) / Decimal(10**token_decimals)
            buy_amount_normalized = Decimal(fallback_quote['quote']['buyAmount']) / Decimal(10**6)  # USDC decimals
            
            # Calculate rate in normalized units
            rate = buy_amount_normalized / sell_amount_normalized
            
            # Apply rate to original amount (convert to normalized units first)
            original_amount_normalized = Decimal(amount) / Decimal(10**token_decimals)
            estimated_value_normalized = original_amount_normalized * rate
            estimated_value = int(estimated_value_normalized * Decimal(10**6))  # Convert back to USDC wei

            logger.info(f"✓ Fallback: {estimated_value/1e6:.6f} USDC (Rate: {rate:.6f} USDC/token)")

            return {
                "quote": {
                    "quote": {
                        "buyAmount": str(estimated_value),
                        "sellAmount": amount,
                        "feeAmount": "0"
                    }
                },
                "conversion_details": {
                    "source": "CoWSwap-Fallback",
                    "price_impact": "N/A",
                    "rate": f"{float(rate):.6f}",
                    "fee_percentage": "N/A",
                    "fallback": True,
                    "note": "Using reference amount of 1000 tokens for price discovery"
                }
            }

    # If all attempts fail
    logger.error(f"✗ Failed to get quote for {token_symbol}")
    return {
        "quote": None,
        "conversion_details": {
            "source": "Failed",
            "price_impact": "N/A",
            "rate": "0",
            "fee_percentage": "N/A",
            "fallback": True,
            "note": "All quote attempts failed"
        }
    }

if __name__ == "__main__":
    """
    Test script for CoW Protocol quote functionality.
    Tests both single quote and batch processing.
    """
    print("Testing CoW Protocol client with rate limiting...")
    
    # Test single quote
    print("\n1. Testing single quote:")
    amount = str(10000 * 10**18)
    
    result = get_quote(
        network="base",
        sell_token=NETWORK_TOKENS["base"]["USDS"]["address"],
        buy_token=NETWORK_TOKENS["base"]["USDC"]["address"],
        amount=amount,
        token_symbol="USDS"
    )
    
    if result["quote"] and 'quote' in result["quote"]:
        quote = result["quote"]["quote"]
        buy_amount = int(quote['buyAmount'])
        
        print(f"✓ Single quote successful:")
        print(f"  Input: 10000 USDS ({amount} wei)")
        print(f"  Output: {buy_amount/10**6:.6f} USDC ({buy_amount} wei)")
        print(f"  Conversion details: {json.dumps(result['conversion_details'], indent=2)}")
    else:
        print(f"✗ Single quote failed: {result}")
    
    # Test rate limiter status
    print("\n2. Rate limiter status:")
    status = get_rate_limiter_status()
    print(f"  Min delay: {status['min_delay']}s")
    print(f"  Backoff delay: {status['backoff_delay']}s")
    print(f"  Is backing off: {status['is_backing_off']}")
    
    # Test batch processing (small batch for demo)
    print("\n3. Testing batch processing:")
    requests = [
        {
            "network": "base",
            "sell_token": NETWORK_TOKENS["base"]["USDS"]["address"],
            "buy_token": NETWORK_TOKENS["base"]["USDC"]["address"],
            "amount": str(1000 * 10**18),
            "token_decimals": 18,
            "token_symbol": "USDS"
        },
        {
            "network": "base",
            "sell_token": NETWORK_TOKENS["base"]["USDS"]["address"],
            "buy_token": NETWORK_TOKENS["base"]["USDC"]["address"],
            "amount": str(2000 * 10**18),
            "token_decimals": 18,
            "token_symbol": "USDS"
        }
    ]
    
    batch_results = batch_get_quotes(requests, batch_size=2, batch_delay=2.0)
    print(f"✓ Batch processing completed: {len(batch_results)} results")
    
    print("\n4. Final rate limiter status:")
    final_status = get_rate_limiter_status()
    print(f"  Min delay: {final_status['min_delay']}s")
    print(f"  Backoff delay: {final_status['backoff_delay']}s")
    print(f"  Is backing off: {final_status['is_backing_off']}") 