from backend.core.parser import parser
import json

# Use the file you generated in Week 1 (Vitalik's or yours)
TEST_FILE = "data/raw_txs/0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045_1.json"

def main():
    print(f"Parsing {TEST_FILE}...")
    
    # 1. Parse
    result = parser.parse_file(TEST_FILE)
    
    # 2. Inspect a random transaction
    if result:
        first_tx = list(result.values())[0]
        print("\n✅ Successfully Parsed!")
        print(f"Total Transactions: {len(result)}")
        print("\n--- Sample Transaction ---")
        print(json.dumps(first_tx, indent=2, default=str))
    else:
        print("❌ Parsing failed or empty file.")

if __name__ == "__main__":
    main()
