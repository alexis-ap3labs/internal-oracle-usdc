from pymongo import MongoClient
from datetime import datetime
from pprint import pprint
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_mongodb():
    """Check MongoDB connection and display most recent document from both databases"""
    try:
        # Get MongoDB URI from .env
        mongo_uri = os.getenv('MONGO_URI')
        # Hardcoded configuration
        databases = ["detrade-core-usdc", "dev-detrade-core-usdc"]
        collection_name = "oracle"
        
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        
        # Test connection
        client.admin.command('ping')
        print("✓ Connection successful!")
        
        for db_name in databases:
            print(f"\n{'='*80}")
            print(f"Database: {db_name}")
            print(f"{'='*80}")
            
            # Get database and collection
            db = client[db_name]
            collection = db[collection_name]
            
            # Get most recent document based on timestamp
            print(f"\nFetching most recent document from {db_name}...")
            doc = collection.find_one(
                sort=[('timestamp', -1)]
            )
            
            if doc:
                print("\nMost recent document:")
                print("-"*40)
                print(f"ID: {doc['_id']}")
                print(f"Address: {doc['address']}")
                print(f"Timestamp: {doc['timestamp']}")
                print(f"Total Value: {doc['nav']['usdc']} USDC")
            else:
                print(f"No documents found in {db_name}.{collection_name}")
            
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    check_mongodb() 