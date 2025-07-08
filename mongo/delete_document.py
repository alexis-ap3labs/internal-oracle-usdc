from pymongo import MongoClient
import os
from dotenv import load_dotenv
from bson.objectid import ObjectId
import sys

# Load environment variables
load_dotenv()

def delete_documents(database_name: str, doc_ids: list) -> None:
    """Delete multiple documents from MongoDB by their _ids"""
    try:
        # Get MongoDB URI from .env
        mongo_uri = os.getenv('MONGO_URI')
        # Hardcoded collection name
        collection_name = "oracle"
        
        print("\nConnecting to MongoDB...")
        print(f"Database: {database_name}")
        print(f"Collection: {collection_name}\n")
        
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        
        # Test connection
        client.admin.command('ping')
        print("✓ Connection successful!")
        
        # Get database and collection
        db = client[database_name]
        collection = db[collection_name]
        
        documents_to_delete = []
        
        # Find and display all documents to delete
        print("\nDocuments to delete:")
        print("="*80)
        for doc_id in doc_ids:
            try:
                object_id = ObjectId(doc_id)
                doc = collection.find_one({"_id": object_id})
                if doc:
                    documents_to_delete.append(doc)
                    print(f"\nID: {doc_id}")
                    print(f"Address: {doc.get('address', 'N/A')}")
                    print(f"Created at: {doc.get('created_at', 'N/A')}")
                    if 'nav' in doc:
                        print(f"Total Value: {doc['nav']['usdc']} USDC")
                    print("-"*40)
                else:
                    print(f"\n❌ Document with ID {doc_id} not found")
            except:
                print(f"\n❌ Invalid document ID format: {doc_id}")
        
        print("="*80)
        
        if not documents_to_delete:
            print("\nNo valid documents found to delete")
            return
            
        # Confirm deletion
        confirm = input(f"\nAre you sure you want to delete these {len(documents_to_delete)} documents? (y/N): ")
        if confirm.lower() != 'y':
            print("Deletion cancelled")
            return
            
        # Delete documents
        object_ids = [doc["_id"] for doc in documents_to_delete]
        result = collection.delete_many({"_id": {"$in": object_ids}})
        
        if result.deleted_count > 0:
            print(f"\n✓ Successfully deleted {result.deleted_count} documents")
        else:
            print("\n❌ Failed to delete documents")
            
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    finally:
        client.close()
        print("\n✓ MongoDB connection closed")

def main():
    """CLI entry point for document deletion"""
    if len(sys.argv) < 3:
        print("Usage: python -m mongo.delete_document <database_name> <document_id1> [document_id2 ...]")
        print("Example: python -m mongo.delete_document detrade-core-usdc 6808a235759fa6a5b09dcc63 6808a235759fa6a5b09dcc64")
        sys.exit(1)
        
    database_name = sys.argv[1]
    doc_ids = sys.argv[2:]
    delete_documents(database_name, doc_ids)

if __name__ == "__main__":
    main() 