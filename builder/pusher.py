import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from typing import Dict, Any
import logging
from builder.aggregator import BalanceAggregator, build_overview

# Add parent directory to PYTHONPATH
root_path = str(Path(__file__).parent.parent)
sys.path.append(root_path)

# Load environment variables
env_path = Path(root_path) / '.env'
load_dotenv(env_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Debug: Check if .env file exists and variables are loaded
logger.info(f"Looking for .env file at: {env_path}")
logger.info(f".env file exists: {env_path.exists()}")
logger.info(f"MONGO_URI exists: {bool(os.getenv('MONGO_URI'))}")
logger.info(f"DATABASE_NAME exists: {bool(os.getenv('DATABASE_NAME'))}")
logger.info(f"COLLECTION_NAME exists: {bool(os.getenv('COLLECTION_NAME'))}")
logger.info(f"ADDRESSES exists: {bool(os.getenv('ADDRESSES'))}")

class BalancePusher:
    """
    Handles the storage of portfolio balances in MongoDB.
    Acts as a bridge between the BalanceAggregator and the database.
    """
    def __init__(self, database_name=None, collection_name=None):
        # Required MongoDB configuration from environment variables
        self.mongo_uri = os.getenv('MONGO_URI')
        self.database_name = database_name or os.getenv('DATABASE_NAME')
        self.collection_name = collection_name or os.getenv('COLLECTION_NAME')
        
        if not all([self.mongo_uri, self.database_name, self.collection_name]):
            raise ValueError("Missing required environment variables for MongoDB connection")
        
        # Initialize MongoDB connection
        self._init_mongo_connection()
        
        # Initialize aggregator
        self.aggregator = BalanceAggregator()

    def _init_mongo_connection(self) -> None:
        """Initialize MongoDB connection and verify access"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            # Test connection with timeout
            self.client.admin.command('ping')
            logger.info("MongoDB connection initialized successfully")
            logger.info(f"Database: {self.database_name}")
            logger.info(f"Collection: {self.collection_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {str(e)}")
            raise

    def _prepare_balance_data(self, raw_data: Dict[str, Any], address: str) -> Dict[str, Any]:
        """Prepare balance data for storage"""
        # Convert large numbers to strings
        data = self.convert_large_numbers_to_strings(raw_data)
        
        # Add metadata
        timestamp = datetime.now(timezone.utc)
        data.update({
            'address': address,
            'created_at': timestamp,
            'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
        })
        
        return data

    def _verify_insertion(self, doc_id: Any) -> bool:
        """Verify document was properly inserted"""
        try:
            inserted_doc = self.collection.find_one({"_id": doc_id})
            return bool(inserted_doc)
        except Exception as e:
            logger.error(f"Failed to verify document insertion: {str(e)}")
            return False

    def _format_balance_data(self, all_balances: Dict[str, Any], address: str, collection_start: datetime) -> Dict[str, Any]:
        """Format balance data for storage"""
        try:
            # Build overview
            overview = build_overview(address)
            
            # Prepare data for storage
            push_timestamp = datetime.now(timezone.utc)
            
            # Log the raw data for debugging
            logger.info("Raw overview data:")
            logger.info(f"Nav: {overview.get('nav', {})}")
            logger.info(f"Positions: {overview.get('positions', {})}")
            logger.info(f"Spot: {overview.get('spot', {})}")
            
            # Combine overview with the rest of the data
            formatted_data = {
                'timestamp': collection_start.strftime("%Y-%m-%d %H:%M:%S UTC"),
                'created_at': push_timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
                'address': address,
                'nav': overview.get('nav', {}),
                'positions': overview.get('positions', {}),
                'protocols': {
                    'equilibria': all_balances.get('equilibria', {}),
                    'pendle': all_balances.get('pendle', {}),
                    'sky': all_balances.get('sky', {}),
                    'convex': all_balances.get('convex', {})
                },
                'spot': overview.get('spot', {})
            }
            
            # Validate data structure
            self._validate_data_structure(formatted_data)
            
            # Convert large numbers to strings
            formatted_data = self.convert_large_numbers_to_strings(formatted_data)
            
            return formatted_data
            
        except Exception as e:
            logger.error(f"Error formatting balance data: {str(e)}")
            raise

    def _validate_data_structure(self, data: Dict[str, Any]) -> None:
        """Validate the data structure before pushing to MongoDB"""
        required_fields = ['timestamp', 'created_at', 'address', 'nav', 'positions', 'protocols', 'spot']
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate nav structure
        if not isinstance(data['nav'], dict):
            raise ValueError("nav must be a dictionary")
        
        # Validate positions
        if not isinstance(data['positions'], dict):
            raise ValueError("positions must be a dictionary")
        
        # Validate protocols
        if not isinstance(data['protocols'], dict):
            raise ValueError("protocols must be a dictionary")
        
        # Validate spot
        if not isinstance(data['spot'], dict):
            raise ValueError("spot must be a dictionary")
        
        # Log validation success
        logger.info("Data structure validation successful")

    def convert_large_numbers_to_strings(self, data: Dict) -> Dict:
        """Recursively converts large integers to strings"""
        try:
            if isinstance(data, dict):
                return {k: self.convert_large_numbers_to_strings(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [self.convert_large_numbers_to_strings(x) for x in data]
            elif isinstance(data, (int, float)) and (data > 2**53 or data < -2**53):
                return str(data)
            return data
        except Exception as e:
            logger.error(f"Error converting numbers to strings: {str(e)}")
            raise

    def _push_to_mongodb(self, formatted_data: Dict[str, Any]) -> None:
        """Push formatted data to MongoDB"""
        # Store data in MongoDB
        result = self.collection.insert_one(formatted_data)
        
        if not result.inserted_id:
            raise Exception("No document ID returned after insertion")
        
        logger.info(f"Document inserted with ID: {result.inserted_id}")
        
        # Verify insertion
        if self._verify_insertion(result.inserted_id):
            logger.info("Document verified in database")
        else:
            raise Exception("Document verification failed")
        
        # Print summary
        collection_duration = (datetime.now(timezone.utc) - datetime.strptime(formatted_data['timestamp'], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)).total_seconds()
        logger.info("="*80)
        logger.info("SUMMARY")
        logger.info("="*80)
        logger.info(f"Address: {formatted_data['address']}")
        
        # Get total value from the correct location in the data structure
        total_value = formatted_data.get('total_value_usdc', 0)
        logger.info(f"Total Value: {total_value} USDC")
        
        logger.info(f"Collection started at: {formatted_data['timestamp']}")
        logger.info(f"Pushed at: {formatted_data['created_at']}")
        logger.info(f"Collection duration: {collection_duration:.2f} seconds")
        logger.info(f"Database: {self.database_name}")
        logger.info(f"Collection: {self.collection_name}")
        logger.info(f"Document ID: {result.inserted_id}")
        logger.info("="*80)

    def push_balance_data(self, address: str) -> None:
        """
        Push balance data to MongoDB for a specific address.
        
        Args:
            address: The address to fetch and push balance data for
        """
        try:
            logger.info("="*80)
            logger.info(f"PUSHING BALANCE DATA FOR {address}")
            logger.info("="*80)
            
            # Log RPC URLs (masked for security)
            eth_rpc = os.getenv('ETHEREUM_RPC', '')
            base_rpc = os.getenv('BASE_RPC', '')
            logger.info(f"Ethereum RPC: {eth_rpc[:10]}...{eth_rpc[-10:] if eth_rpc else 'Not set'}")
            logger.info(f"Base RPC: {base_rpc[:10]}...{base_rpc[-10:] if base_rpc else 'Not set'}")
            
            # 1. Fetch portfolio data
            logger.info("1. Fetching portfolio data...")
            collection_start = datetime.now(timezone.utc)
            logger.info(f"Collection started at: {collection_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            
            # Get all balances using the aggregator
            all_balances = self.aggregator.get_balances(address)
            
            # Log detailed balance information
            logger.info("Detailed balance information:")
            for protocol, data in all_balances.items():
                if isinstance(data, dict):
                    logger.info(f"{protocol}:")
                    for key, value in data.items():
                        if isinstance(value, dict):
                            logger.info(f"  {key}:")
                            for subkey, subvalue in value.items():
                                logger.info(f"    {subkey}: {subvalue}")
                        else:
                            logger.info(f"  {key}: {value}")
                else:
                    logger.info(f"{protocol}: {data}")
            
            # 2. Format the data
            logger.info("2. Formatting data...")
            formatted_data = self._format_balance_data(all_balances, address, collection_start)
            
            # Log formatted data summary
            logger.info("Formatted data summary:")
            logger.info(f"NAV: {formatted_data.get('nav', {})}")
            logger.info(f"Total positions: {len(formatted_data.get('positions', {}))}")
            logger.info(f"Protocols: {list(formatted_data.get('protocols', {}).keys())}")
            
            # 3. Push to MongoDB
            logger.info("3. Pushing to MongoDB...")
            self._push_to_mongodb(formatted_data)
            
            logger.info("✓ Balance data pushed successfully")
            
        except Exception as e:
            logger.error(f"Error in push_balance_data: {str(e)}")
            raise
        finally:
            # Close MongoDB connection
            self.client.close()
            logger.info("MongoDB connection closed")

    def close(self):
        """Close MongoDB connection"""
        try:
            self.client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {str(e)}")

def main():
    """CLI entry point for testing balance pushing functionality."""
    import sys
    
    # Main configuration
    config = {
        'address': '0xc6835323372A4393B90bCc227c58e82D45CE4b7d',
        'database_name': 'detrade-core-usdc',
        'collection_name': 'oracle'
    }
    
    # Get address from command line argument if provided
    if len(sys.argv) > 1:
        config['address'] = sys.argv[1]
    
    # Process the configuration
    pusher = BalancePusher(
        database_name=config['database_name'],
        collection_name=config['collection_name']
    )
    try:
        pusher.push_balance_data(config['address'])
    finally:
        pusher.close()

if __name__ == "__main__":
    main()