import time
from functools import wraps
from typing import Callable, Any, Optional
import requests
from web3 import Web3
from web3.exceptions import ContractLogicError
from concurrent.futures import TimeoutError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RetryConfig:
    """Configuration for retry behavior"""
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 10.0,
        backoff_factor: float = 2.0
    ):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor

def with_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
    backoff_factor: float = 2.0
):
    """
    Decorator for adding retry logic with exponential backoff to functions.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Factor to multiply delay by after each retry
    """
    config = RetryConfig(max_retries, initial_delay, max_delay, backoff_factor)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = config.initial_delay
            
            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (TimeoutError, ContractLogicError, requests.exceptions.RequestException) as e:
                    last_exception = e
                    if attempt == config.max_retries:
                        logger.error(f"All {config.max_retries + 1} attempts failed. Last error: {str(e)}")
                        break
                        
                    logger.warning(f"Attempt {attempt + 1}/{config.max_retries + 1} failed: {str(e)}")
                    logger.info(f"Retrying in {delay:.2f} seconds...")
                    
                    time.sleep(delay)
                    delay = min(delay * config.backoff_factor, config.max_delay)
            
            raise last_exception
            
        return wrapper
    return decorator

class Web3Retry:
    """Utility class for Web3 retry operations"""
    
    @staticmethod
    @with_retry()
    def call_contract_function(contract_function, *args, **kwargs):
        """Call a contract function with retry logic"""
        return contract_function(*args, **kwargs)
    
    @staticmethod
    @with_retry()
    def get_balance(w3: Web3, address: str):
        """Get balance with retry logic"""
        return w3.eth.get_balance(address)

class APIRetry:
    """Utility class for API retry operations"""
    
    @staticmethod
    @with_retry(max_retries=5, initial_delay=2.0, max_delay=60.0)
    def get(url: str, params: Optional[dict] = None, **kwargs):
        """Make GET request with retry logic"""
        kwargs.setdefault('timeout', 60)  # 60 seconds timeout
        logger.info(f"Making GET request to {url}")
        response = requests.get(url, params=params, **kwargs)
        logger.info(f"GET request completed with status {response.status_code}")
        return response
    
    @staticmethod
    @with_retry(max_retries=5, initial_delay=2.0, max_delay=60.0)
    def post(url: str, json: Optional[dict] = None, **kwargs):
        """Make POST request with retry logic"""
        kwargs.setdefault('timeout', 60)  # 60 seconds timeout
        logger.info(f"Making POST request to {url}")
        if json:
            logger.debug(f"Request payload: {json}")
        response = requests.post(url, json=json, **kwargs)
        logger.info(f"POST request completed with status {response.status_code}")
        return response 