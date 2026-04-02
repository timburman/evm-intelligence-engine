"""
Pipeline Orchestrator — The single entry point for the full sync flow.

Steps:
  1. Check 24h cooldown
  2. Get last fetched blocks from DB
  3. Fetch new txs from Etherscan (incremental)
  4. Parse raw data
  5. Save to DB
  6. Run pricer engine (with retry loop)
  7. Update wallet metadata
  8. Return result metrics
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

from backend.core.etherscan import fetch_all_txs
from backend.core.parser import parser
from backend.core.database import db
from backend.core.pricer_engine import pricer_engine


@dataclass
class PipelineResult:
    """Result metrics from a pipeline run."""
    address: str
    chain_id: str
    status: str = "pending"  # pending, skipped, completed, error
    message: str = ""

    # Timing
    fetch_time: float = 0.0
    parse_time: float = 0.0
    save_time: float = 0.0
    pricing_time: float = 0.0
    total_time: float = 0.0

    # Counts
    tx_count: int = 0
    transfer_count: int = 0
    priced_count: int = 0
    failed_count: int = 0
    pricing_loops: int = 0

    # Block tracking
    last_blocks: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "address": self.address,
            "chain_id": self.chain_id,
            "status": self.status,
            "message": self.message,
            "timing": {
                "fetch_seconds": round(self.fetch_time, 2),
                "parse_seconds": round(self.parse_time, 2),
                "save_seconds": round(self.save_time, 2),
                "pricing_seconds": round(self.pricing_time, 2),
                "total_seconds": round(self.total_time, 2),
            },
            "counts": {
                "transactions": self.tx_count,
                "transfers": self.transfer_count,
                "priced": self.priced_count,
                "failed": self.failed_count,
                "pricing_loops": self.pricing_loops,
            },
            "last_blocks": self.last_blocks,
        }


async def run_full_pipeline(
    address: str,
    chain_id: str = "1",
    force: bool = False,
) -> PipelineResult:
    """
    Runs the complete sync pipeline for a wallet address.
    
    Args:
        address: The wallet address to sync
        chain_id: The EVM chain ID (default: "1" for Ethereum)
        force: If True, bypasses the 24h cooldown
    
    Returns:
        PipelineResult with timing and count metrics
    """
    result = PipelineResult(address=address.lower(), chain_id=chain_id)
    start_time = time.time()

    try:
        # ── Step 1: Check 24h Cooldown ──
        if not force and not db.should_refetch(address):
            result.status = "skipped"
            result.message = "Wallet was synced within the last 24 hours. Use force=True to override."
            wallet = db.get_wallet_info(address.lower())
            if wallet:
                result.last_blocks = {
                    "normal": wallet.get("last_block_normal", 0),
                    "internal": wallet.get("last_block_internal", 0),
                    "erc20": wallet.get("last_block_erc20", 0),
                }
            result.total_time = time.time() - start_time
            print(f"[PIPELINE] ⏭️ Skipping {address[:8]}... — synced within 24h")
            return result

        # ── Step 2: Get Last Fetched Blocks ──
        last_blocks = db.get_last_blocks(address)
        print(f"[PIPELINE] 📊 Last blocks: normal={last_blocks['normal']}, internal={last_blocks['internal']}, erc20={last_blocks['erc20']}")

        # ── Step 3: Fetch from Etherscan (Incremental) ──
        t1 = time.time()
        print(f"\n[PIPELINE] 🌐 Fetching transactions for {address[:8]}... (chain {chain_id})")
        raw_data = await fetch_all_txs(address, chain_id)
        result.fetch_time = time.time() - t1

        # ── Step 4: Parse Raw Data ──
        t2 = time.time()
        print(f"[PIPELINE] 🧠 Parsing raw data...")
        parsed_data = parser.parse_dict(raw_data)
        result.parse_time = time.time() - t2
        result.tx_count = len(parsed_data)

        if not parsed_data:
            result.status = "completed"
            result.message = "No new transactions found."
            result.total_time = time.time() - start_time
            # Still update the fetch timestamp so we don't re-check for 24h
            db.update_wallet_fetch_metadata(address, last_blocks)
            return result

        # Count transfers
        for tx in parsed_data.values():
            result.transfer_count += len(tx.get("transfers", []))

        print(f"[PIPELINE] 📦 Parsed {result.tx_count} transactions with {result.transfer_count} transfers")

        # ── Step 5: Save to Supabase ──
        t3 = time.time()
        print(f"[PIPELINE] 💾 Saving to database...")
        db.save_batch(parsed_data)
        result.save_time = time.time() - t3

        # ── Step 6: Pricing Engine (Waterfall) ──
        t4 = time.time()
        print(f"[PIPELINE] 💸 Running pricing engine...")
        max_loops = 20

        while result.pricing_loops < max_loops:
            # Check for remaining unpriced transfers
            resp = (
                db.supabase.table("token_transfers")
                .select("id")
                .is_("price_at_transaction", "null")
                .limit(1)
                .execute()
            )

            if not resp.data:
                print("[PIPELINE] ✅ All transfers priced!")
                break

            print(f"[PIPELINE] 🔄 Pricing loop {result.pricing_loops + 1}...")
            await pricer_engine.update_token_transfers()
            result.pricing_loops += 1

        if result.pricing_loops == max_loops:
            print("[PIPELINE] ⚠️ Hit max pricing loops. Some transfers may be unfetchable.")

        result.pricing_time = time.time() - t4

        # ── Step 7: Count Results ──
        try:
            priced_resp = (
                db.supabase.table("token_transfers")
                .select("id", count="exact")
                .gt("price_at_transaction", 0)
                .execute()
            )
            result.priced_count = priced_resp.count or 0

            failed_resp = (
                db.supabase.table("token_transfers")
                .select("id", count="exact")
                .eq("price_at_transaction", -1)
                .execute()
            )
            result.failed_count = failed_resp.count or 0
        except Exception:
            pass

        # ── Step 8: Update Wallet Metadata ──
        new_blocks = {
            "normal": raw_data.get("metadata", {}).get("last_blocks", {}).get("normal", 0),
            "internal": raw_data.get("metadata", {}).get("last_blocks", {}).get("internal", 0),
            "erc20": raw_data.get("metadata", {}).get("last_blocks", {}).get("erc20", 0),
        }
        db.update_wallet_fetch_metadata(address, new_blocks)
        result.last_blocks = new_blocks

        result.status = "completed"
        result.message = f"Synced {result.tx_count} txs, priced {result.priced_count} transfers"
        result.total_time = time.time() - start_time

        print(f"\n{'='*50}")
        print(f"🎉 PIPELINE COMPLETE for {address[:8]}...")
        print(f"{'='*50}")
        print(f"🌐 Fetch    : {result.fetch_time:.2f}s")
        print(f"🧠 Parse    : {result.parse_time:.2f}s")
        print(f"💾 Save     : {result.save_time:.2f}s")
        print(f"💸 Price    : {result.pricing_time:.2f}s")
        print(f"➡️ TOTAL     : {result.total_time:.2f}s")
        print(f"{'='*50}")

        return result

    except Exception as e:
        result.status = "error"
        result.message = str(e)
        result.total_time = time.time() - start_time
        print(f"[PIPELINE] ❌ Error: {e}")
        return result
