import asyncio
import time
import json
import os
import sys

from backend.core.etherscan import fetch_all_txs
from backend.core.parser import parser
from backend.core.database import db
from backend.core.pricer_engine import pricer_engine

# A moderately active Ethereum address (Ethereum Foundation Grant wallet or similar)
# You can override this via command line argument
DEFAULT_ADDRESS = "0xbD5446CB22814A1BD6F407B4bb702a4e2CA3D513"

async def run_pipeline(address: str, chain_id: str = "1"):
    print(f"--- 🚀 Starting Full Pipeline test for {address} ---")
    start_time = time.time()
    
    # 0. Clean the raw JSON cache for this address to force a fully fresh fetch
    filename = f"data/raw_txs/{address}_{chain_id}.json"
    if os.path.exists(filename):
        os.remove(filename)

    # 1. Fetch from Etherscan
    t1 = time.time()
    print("\n[1/4] Fetching all transactions from Etherscan...")
    raw_data = await fetch_all_txs(address, chain_id)
    fetch_time = time.time() - t1
    print(f"⏱️ Fetch took {fetch_time:.2f} seconds.")

    # 2. Parse Raw Data
    t2 = time.time()
    print("\n[2/4] Parsing raw data into DB format...")
    parsed_data = parser.parse_dict(raw_data)
    parse_time = time.time() - t2
    print(f"⏱️ Parsed {len(parsed_data)} unique transactions in {parse_time:.2f} seconds.")

    if not parsed_data:
        print("❌ No data parsed. Exiting.")
        return

    # 3. Save to Supabase (Database Layer)
    t3 = time.time()
    print("\n[3/4] Saving transactions to Supabase...")
    db.save_batch(parsed_data)
    save_time = time.time() - t3
    print(f"⏱️ Save to DB took {save_time:.2f} seconds.")

    # 4. Asynchronous Pricing
    t4 = time.time()
    print("\n[4/4] Running Pricer Engine until all transfers are priced...")
    loops = 0
    max_loops = 20 # To prevent infinite loop in case pricing fails persistently

    while loops < max_loops:
        # Check if there are any unpriced transfers strictly for this chain (using 1 record check)
        resp = db.supabase.table("token_transfers").select("id").is_("price_at_transaction", "null").limit(1).execute()
        
        if not resp.data:
            print("  ✅ No more unpriced transfers remaining.")
            break
            
        print(f"  👉 Starting pricing loop {loops + 1}...")
        await pricer_engine.update_token_transfers()
        loops += 1

    if loops == max_loops:
        print("⚠️ Hit safety max loop limit. Assuming partial failure on some rogue transfers.")

    pricing_time = time.time() - t4
    print(f"⏱️ Pricing engine complete in {pricing_time:.2f} seconds.")

    total_time = time.time() - start_time
    print("\n" + "="*50)
    print(f"🎉 FULL PIPELINE METRICS FOR {address}")
    print("="*50)
    print(f"🌐 Etherscan Fetch : {fetch_time:.2f} sec")
    print(f"🧠 Parsing Time    : {parse_time:.2f} sec")
    print(f"💾 Database Save   : {save_time:.2f} sec")
    print(f"💸 Pricing Engine  : {pricing_time:.2f} sec")
    print(f"➡️ TOTAL SYSTEM TIME: {total_time:.2f} sec")
    print("="*50)

if __name__ == "__main__":
    addr = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ADDRESS
    asyncio.run(run_pipeline(addr))
