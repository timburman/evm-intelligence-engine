import asyncio
import os
from backend.core.etherscan import fetch_all_txs
from backend.core.token_registry import token_registry
from backend.core.coingecko import price_engine

# A known address with diverse assets (Vitalik's)
TEST_WALLET = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045" 

async def main():
    print("--- 🟢 STARTING SYSTEM DIAGNOSTICS ---")

    # 1. TEST ETHERSCAN (Data Ingestion)
    print("\n[1/3] Testing Etherscan Client...")
    try:
        data = await fetch_all_txs(TEST_WALLET, "1")
        tx_count = len(data.get("normal", []))
        print(f"✅ Success: Fetched {tx_count} normal transactions.")
        
        # Check if file exists
        if os.path.exists(f"backend/data/raw_txs/{TEST_WALLET}_1.json"):
            print("✅ Success: Local cache file created.")
        else:
            print("❌ Error: Cache file missing!")
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return

    # 2. TEST TOKEN REGISTRY (Spam Filter)
    print("\n[2/3] Testing Token Registry...")
    try:
        # Force a check on a known token (USDT)
        # Contract: 0xdac17f958d2ee523a2206206994597c13d831ec7
        usdt_addr = "0xdac17f958d2ee523a2206206994597c13d831ec7"
        
        # This should trigger the massive download if it's the first run
        cg_id = await token_registry.resolve_token("1", usdt_addr)
        
        if cg_id == "tether":
            print(f"✅ Success: Resolved USDT -> '{cg_id}'")
        else:
            print(f"❌ Error: Expected 'tether', got '{cg_id}'")
            
        # Test a fake address (Spam)
        fake_addr = "0x0000000000000000000000000000000000000000"
        spam_id = await token_registry.resolve_token("1", fake_addr)
        if spam_id is None:
            print("✅ Success: Identified Spam Token correctly.")
        else:
            print(f"❌ Error: Spam token resolved to '{spam_id}'")

    except Exception as e:
        print(f"❌ FAILED: {e}")
        return

    # 3. TEST PRICE ENGINE (Batching)
    print("\n[3/3] Testing Price Engine...")
    try:
        ids = ["bitcoin", "ethereum", "tether", "solana"]
        print(f"   Querying: {ids}")
        
        async for batch in price_engine.get_price_batches(ids):
            source = batch["source"]
            data = batch["data"]
            print(f"   Received batch from [{source}]: {data}")
            
        print("✅ Success: Price stream completed.")
    except Exception as e:
        print(f"❌ FAILED: {e}")

    print("\n--- 🏁 DIAGNOSTICS COMPLETE ---")

if __name__ == "__main__":
    asyncio.run(main())
