"""
Full Pipeline Test Suite — Test-Driven Development

Tests:
  1. Full pipeline end-to-end (fetch → parse → store → price)
  2. 24h cooldown enforcement
  3. Pipeline idempotency (running twice produces same results)
  4. PnL calculation correctness
  5. Unfetchable price sentinel behavior
  6. Token filtering without CoinGecko
"""

import asyncio
import time
import json
import os
import sys

from backend.core.pipeline import run_full_pipeline, PipelineResult
from backend.core.database import db
from backend.core.pnl_calculator import pnl_calculator

# A moderately active Ethereum address
# You can override this via command line argument
DEFAULT_ADDRESS = "0x1Db3439a222C519ab44bb1144fC28167b4Fa6EE6"

PASS = "✅ PASS"
FAIL = "❌ FAIL"


def header(name: str):
    print(f"\n{'='*60}")
    print(f"  TEST: {name}")
    print(f"{'='*60}")


async def test_1_full_pipeline(address: str, chain_id: str = "1"):
    """
    Test 1: Full pipeline end-to-end.
    Verifies: fetch, parse, store, price all complete without errors.
    """
    header("Full Pipeline (End-to-End)")
    
    # Clean local JSON cache for fresh fetch
    filename = f"data/raw_txs/{address}_{chain_id}.json"
    if os.path.exists(filename):
        os.remove(filename)
        print(f"  🗑️ Cleaned local cache: {filename}")

    result = await run_full_pipeline(address, chain_id, force=True)

    # Assertions
    assert result.status == "completed", f"Pipeline status should be 'completed', got '{result.status}': {result.message}"
    assert result.tx_count > 0, f"Should have found transactions, got {result.tx_count}"
    assert result.transfer_count > 0, f"Should have found transfers, got {result.transfer_count}"
    assert result.total_time > 0, "Total time should be positive"

    print(f"\n  📊 Results:")
    print(f"     Transactions: {result.tx_count}")
    print(f"     Transfers:    {result.transfer_count}")
    print(f"     Priced:       {result.priced_count}")
    print(f"     Failed:       {result.failed_count}")
    print(f"     Loops:        {result.pricing_loops}")
    print(f"     Total Time:   {result.total_time:.2f}s")
    print(f"\n  {PASS} Full pipeline completed successfully!")
    
    return result


async def test_2_cooldown_enforcement(address: str, chain_id: str = "1"):
    """
    Test 2: 24h cooldown.
    After a successful sync, calling without force=True should be skipped.
    """
    header("24h Cooldown Enforcement")

    result = await run_full_pipeline(address, chain_id, force=False)

    assert result.status == "skipped", f"Expected 'skipped', got '{result.status}'"
    assert "24 hours" in result.message.lower() or "24h" in result.message.lower(), \
        f"Message should mention 24h: {result.message}"

    print(f"  ⏭️ Pipeline correctly skipped: {result.message}")
    print(f"\n  {PASS} 24h cooldown working!")


async def test_3_force_override(address: str, chain_id: str = "1"):
    """
    Test 3: Force override.
    With force=True, the pipeline should run even within 24h.
    """
    header("Force Override")

    result = await run_full_pipeline(address, chain_id, force=True)

    assert result.status == "completed", f"Expected 'completed' with force=True, got '{result.status}'"

    print(f"  🔄 Force override worked. Status: {result.status}")
    print(f"\n  {PASS} Force override working!")


async def test_4_idempotency(address: str, chain_id: str = "1"):
    """
    Test 4: Idempotency.
    Running the pipeline twice should produce the same results in the DB.
    """
    header("Pipeline Idempotency")

    # Get counts before second run
    tx_resp = (
        db.supabase.table("transactions")
        .select("tx_hash", count="exact")
        .eq("wallet_address", address.lower())
        .eq("chain_id", chain_id)
        .execute()
    )
    count_before = tx_resp.count or 0

    # Run pipeline again
    result = await run_full_pipeline(address, chain_id, force=True)

    # Get counts after
    tx_resp2 = (
        db.supabase.table("transactions")
        .select("tx_hash", count="exact")
        .eq("wallet_address", address.lower())
        .eq("chain_id", chain_id)
        .execute()
    )
    count_after = tx_resp2.count or 0

    assert count_before == count_after, \
        f"Transaction count changed! Before: {count_before}, After: {count_after}"

    print(f"  📊 Transactions before: {count_before}, after: {count_after}")
    print(f"\n  {PASS} Pipeline is idempotent!")


async def test_5_unfetchable_sentinel(address: str, chain_id: str = "1"):
    """
    Test 5: Unfetchable price sentinel.
    There should be NO null prices remaining — all should be either >0 (priced) or -1 (unfetchable).
    """
    header("Unfetchable Price Sentinel")

    # Check for null prices
    null_resp = (
        db.supabase.table("token_transfers")
        .select("id", count="exact")
        .is_("price_at_transaction", "null")
        .execute()
    )
    null_count = null_resp.count or 0

    # Check for -1 prices (unfetchable)
    failed_resp = (
        db.supabase.table("token_transfers")
        .select("id", count="exact")
        .eq("price_at_transaction", -1)
        .execute()
    )
    failed_count = failed_resp.count or 0

    # Check for successfully priced
    priced_resp = (
        db.supabase.table("token_transfers")
        .select("id", count="exact")
        .gt("price_at_transaction", 0)
        .execute()
    )
    priced_count = priced_resp.count or 0

    print(f"  📊 Priced:      {priced_count}")
    print(f"  📊 Unfetchable: {failed_count}")
    print(f"  📊 Still NULL:  {null_count}")

    assert null_count == 0, \
        f"Found {null_count} transfers still NULL — pricing loop should have resolved all"

    print(f"\n  {PASS} No NULL prices remaining!")


async def test_6_pnl_calculation(address: str, chain_id: str = "1"):
    """
    Test 6: PnL calculation.
    Verifies the PnL calculator returns valid results.
    """
    header("PnL Calculation (FIFO)")

    result = await pnl_calculator.calculate_pnl(address.lower(), chain_id)

    assert "tokens" in result, "Result should have 'tokens' key"
    assert "summary" in result, "Result should have 'summary' key"

    tokens = result["tokens"]
    summary = result["summary"]

    print(f"  📊 Tokens with activity: {len(tokens)}")
    print(f"  💰 Total Realized PnL:   ${summary['total_realized_pnl']:,.2f}")
    print(f"  📈 Total Unrealized PnL: ${summary['total_unrealized_pnl']:,.2f}")
    print(f"  🎯 Total PnL:            ${summary['total_pnl']:,.2f}")

    # Print top 5 tokens
    if tokens:
        print(f"\n  Top tokens by PnL impact:")
        for t in tokens[:5]:
            print(f"    {t['token_symbol']:>8}: "
                  f"Hold={t['current_holding']:.4f}  "
                  f"R.PnL=${t['realized_pnl']:>10,.2f}  "
                  f"U.PnL=${t['unrealized_pnl']:>10,.2f}")

    # Sanity checks
    for t in tokens:
        assert t["total_in"] >= 0, f"total_in should be >= 0 for {t['token_symbol']}"
        assert t["total_out"] >= 0, f"total_out should be >= 0 for {t['token_symbol']}"
        assert t["current_holding"] >= 0, f"current_holding should be >= 0 for {t['token_symbol']}"

    print(f"\n  {PASS} PnL calculation valid!")
    return result


async def test_7_wallet_metadata(address: str):
    """
    Test 7: Wallet metadata.
    Verifies last_fetched_at and last_block_* are persisted correctly.
    """
    header("Wallet Metadata Persistence")

    wallet = db.get_wallet_info(address.lower())

    assert wallet is not None, "Wallet should exist in DB"
    assert wallet.get("last_fetched_at") is not None, "last_fetched_at should be set"

    last_blocks = db.get_last_blocks(address.lower())
    assert last_blocks["normal"] > 0 or last_blocks["erc20"] > 0, \
        "At least one block number should be > 0"

    print(f"  📊 Last fetched: {wallet.get('last_fetched_at')}")
    print(f"  📊 Block normal:   {last_blocks['normal']}")
    print(f"  📊 Block internal: {last_blocks['internal']}")
    print(f"  📊 Block erc20:    {last_blocks['erc20']}")
    print(f"\n  {PASS} Wallet metadata persisted correctly!")


async def run_all_tests(address: str):
    """Run all tests sequentially."""
    print(f"\n{'#'*60}")
    print(f"  EVM Intelligence Engine — Full Test Suite")
    print(f"  Wallet: {address}")
    print(f"{'#'*60}")

    start = time.time()
    results = {}

    tests = [
        ("Full Pipeline", test_1_full_pipeline),
        ("24h Cooldown", test_2_cooldown_enforcement),
        ("Force Override", test_3_force_override),
        ("Idempotency", test_4_idempotency),
        ("Unfetchable Sentinel", test_5_unfetchable_sentinel),
        ("PnL Calculation", test_6_pnl_calculation),
        ("Wallet Metadata", test_7_wallet_metadata),
    ]

    passed = 0
    failed = 0

    for name, test_fn in tests:
        try:
            if test_fn == test_7_wallet_metadata:
                await test_fn(address)
            else:
                await test_fn(address)
            results[name] = True
            passed += 1
        except AssertionError as e:
            print(f"\n  {FAIL} {name}: {e}")
            results[name] = False
            failed += 1
        except Exception as e:
            print(f"\n  {FAIL} {name}: Unexpected error: {e}")
            results[name] = False
            failed += 1

    total_time = time.time() - start

    print(f"\n{'#'*60}")
    print(f"  TEST RESULTS")
    print(f"{'#'*60}")
    for name, passed_flag in results.items():
        status = PASS if passed_flag else FAIL
        print(f"  {status} {name}")
    print(f"{'#'*60}")
    print(f"  Total: {passed} passed, {failed} failed")
    print(f"  Time:  {total_time:.2f}s")
    print(f"{'#'*60}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    addr = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ADDRESS
    asyncio.run(run_all_tests(addr))
