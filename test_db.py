from backend.core.parser import parser
from backend.core.database import db
import asyncio

TEST_FILE = "data/raw_txs/0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045_1.json"

def main():
    print("--- 🚀 Testing Database Ingestion ---")
    
    # 1. Parse
    print(f"Parsing {TEST_FILE}...")
    parsed_data = parser.parse_file(TEST_FILE)
    
    if not parsed_data:
        print("❌ No data parsed.")
        return

    # 2. Save
    print("Saving to Supabase...")
    db.save_batch(parsed_data)
    
    print("\n✅ DONE! Go check your Supabase Dashboard.")

if __name__ == "__main__":
    main()
