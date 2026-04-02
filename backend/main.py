"""
EVM Portfolio Intelligence Engine — API Server

Endpoints:
  POST /wallet/sync          — Trigger full pipeline for a wallet
  GET  /wallet/{addr}/status — Sync status and metadata
  GET  /wallet/{addr}/transactions — Paginated transaction history
  GET  /wallet/{addr}/portfolio    — Current portfolio holdings
  GET  /wallet/{addr}/pnl          — Realized + Unrealized PnL
  GET  /wallet/{addr}/transfers    — Raw token transfers with pricing
  GET  /health                     — Health check
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from typing import Optional

from backend.core.database import db
from backend.core.pipeline import run_full_pipeline, PipelineResult
from backend.core.pnl_calculator import pnl_calculator
from backend.core.token_registry import token_registry

app = FastAPI(
    title="EVM Portfolio Intelligence Engine",
    description="Production-grade EVM portfolio indexer and PnL calculator",
    version="1.0.0",
)

# CORS — allow frontend to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory tracking of active sync jobs
_active_syncs: dict[str, PipelineResult] = {}


# ── Startup ──

@app.on_event("startup")
async def startup_event():
    """Runs once when the server starts."""
    print("[SYSTEM] 🚀 EVM Intelligence Engine starting...")

    # Optionally refresh CoinGecko list in background (non-blocking)
    if token_registry._is_cache_stale():
        print("[SYSTEM] CoinGecko cache stale — refreshing in background")
        asyncio.create_task(token_registry._refresh_registry_if_needed())
    else:
        print("[SYSTEM] CoinGecko cache is fresh")

    print("[SYSTEM] ✅ Engine ready")


# ── Health Check ──

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "engine": "EVM Portfolio Intelligence Engine",
        "version": "1.0.0",
    }


# ── Wallet Sync ──

@app.post("/wallet/sync")
async def sync_wallet(
    background_tasks: BackgroundTasks,
    address: str = Query(..., description="Wallet address to sync"),
    chain_id: str = Query("1", description="EVM chain ID"),
    force: bool = Query(False, description="Force sync even if within 24h cooldown"),
):
    """
    Triggers the full pipeline: Fetch → Parse → Store → Price.
    
    Respects a 24h cooldown per wallet unless force=True.
    The pipeline runs in the background — use /wallet/{address}/status to check progress.
    """
    address = address.lower()

    # Check if a sync is already running for this address
    active = _active_syncs.get(address)
    if active and active.status == "pending":
        return {
            "status": "already_running",
            "message": f"Sync is already in progress for {address[:8]}...",
        }

    # Quick 24h check before spawning background task
    if not force and not db.should_refetch(address):
        wallet = db.get_wallet_info(address)
        return {
            "status": "skipped",
            "message": "Wallet was synced within the last 24 hours. Use force=true to override.",
            "last_fetched_at": wallet.get("last_fetched_at") if wallet else None,
        }

    # Create a pending result
    result = PipelineResult(address=address, chain_id=chain_id, status="pending")
    _active_syncs[address] = result

    # Run pipeline in background
    async def _run_sync():
        try:
            pipeline_result = await run_full_pipeline(address, chain_id, force)
            _active_syncs[address] = pipeline_result
        except Exception as e:
            result.status = "error"
            result.message = str(e)

    asyncio.create_task(_run_sync())

    return {
        "status": "started",
        "message": f"Pipeline started for {address[:8]}... Check /wallet/{address}/status for progress.",
    }


# ── Wallet Status ──

@app.get("/wallet/{address}/status")
async def get_wallet_status(address: str, chain_id: str = "1"):
    """
    Returns the sync status and metadata for a wallet.
    """
    address = address.lower()

    # Check active sync
    active = _active_syncs.get(address)
    sync_status = active.to_dict() if active else None

    # Get wallet info from DB
    wallet = db.get_wallet_info(address)

    # Count transfers
    try:
        total_resp = (
            db.supabase.table("token_transfers")
            .select("id", count="exact")
            .in_("tx_hash", _get_wallet_tx_hashes(address, chain_id))
            .execute()
        )
        total_transfers = total_resp.count or 0

        priced_resp = (
            db.supabase.table("token_transfers")
            .select("id", count="exact")
            .in_("tx_hash", _get_wallet_tx_hashes(address, chain_id))
            .gt("price_at_transaction", 0)
            .execute()
        )
        priced_transfers = priced_resp.count or 0

        unpriced_resp = (
            db.supabase.table("token_transfers")
            .select("id", count="exact")
            .in_("tx_hash", _get_wallet_tx_hashes(address, chain_id))
            .is_("price_at_transaction", "null")
            .execute()
        )
        unpriced_transfers = unpriced_resp.count or 0
    except Exception:
        total_transfers = 0
        priced_transfers = 0
        unpriced_transfers = 0

    return {
        "wallet": address,
        "chain_id": chain_id,
        "exists": wallet is not None,
        "last_fetched_at": wallet.get("last_fetched_at") if wallet else None,
        "last_blocks": {
            "normal": wallet.get("last_block_normal", 0) if wallet else 0,
            "internal": wallet.get("last_block_internal", 0) if wallet else 0,
            "erc20": wallet.get("last_block_erc20", 0) if wallet else 0,
        },
        "transfers": {
            "total": total_transfers,
            "priced": priced_transfers,
            "unpriced": unpriced_transfers,
            "is_pricing_complete": unpriced_transfers == 0,
        },
        "active_sync": sync_status,
    }


# ── Transactions ──

@app.get("/wallet/{address}/transactions")
async def get_transactions(
    address: str,
    chain_id: str = "1",
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
):
    """
    Returns paginated transaction history with token transfers.
    """
    address = address.lower()
    offset = (page - 1) * limit

    try:
        # Get transactions
        tx_resp = (
            db.supabase.table("transactions")
            .select("*")
            .eq("wallet_address", address)
            .eq("chain_id", chain_id)
            .order("timestamp", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )

        transactions = tx_resp.data or []

        # Get transfers for these transactions
        if transactions:
            tx_hashes = [tx["tx_hash"] for tx in transactions]
            tf_resp = (
                db.supabase.table("token_transfers")
                .select("*")
                .in_("tx_hash", tx_hashes)
                .execute()
            )

            # Group transfers by tx_hash
            transfers_by_tx = {}
            for tf in (tf_resp.data or []):
                if tf["tx_hash"] not in transfers_by_tx:
                    transfers_by_tx[tf["tx_hash"]] = []
                transfers_by_tx[tf["tx_hash"]].append(tf)

            # Attach transfers to transactions
            for tx in transactions:
                tx["transfers"] = transfers_by_tx.get(tx["tx_hash"], [])

        # Get total count
        count_resp = (
            db.supabase.table("transactions")
            .select("tx_hash", count="exact")
            .eq("wallet_address", address)
            .eq("chain_id", chain_id)
            .execute()
        )

        return {
            "wallet": address,
            "chain_id": chain_id,
            "page": page,
            "limit": limit,
            "total": count_resp.count or 0,
            "transactions": transactions,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ── Portfolio ──

@app.get("/wallet/{address}/portfolio")
async def get_portfolio(address: str, chain_id: str = "1"):
    """
    Returns current portfolio — aggregated holdings with current values.
    Calculated from token_transfers using direction field.
    """
    address = address.lower()

    try:
        # Get all transaction hashes for this wallet
        tx_resp = (
            db.supabase.table("transactions")
            .select("tx_hash")
            .eq("wallet_address", address)
            .eq("chain_id", chain_id)
            .execute()
        )

        if not tx_resp.data:
            return {"wallet": address, "chain_id": chain_id, "holdings": [], "total_value_usd": 0.0}

        tx_hashes = [tx["tx_hash"] for tx in tx_resp.data]

        # Get all priced transfers
        all_transfers = []
        for i in range(0, len(tx_hashes), 200):
            chunk = tx_hashes[i : i + 200]
            tf_resp = (
                db.supabase.table("token_transfers")
                .select("token_address, token_symbol, amount_decimal, direction, price_at_transaction, value_usd")
                .in_("tx_hash", chunk)
                .gt("price_at_transaction", 0)
                .execute()
            )
            all_transfers.extend(tf_resp.data or [])

        # Aggregate by token
        holdings = {}
        for tf in all_transfers:
            addr = tf["token_address"]
            if addr not in holdings:
                holdings[addr] = {
                    "token_address": addr,
                    "token_symbol": tf["token_symbol"],
                    "balance": 0.0,
                    "total_value_usd": 0.0,
                }

            amount = float(tf.get("amount_decimal", 0))
            value = float(tf.get("value_usd", 0))

            if tf["direction"] == "IN":
                holdings[addr]["balance"] += amount
                holdings[addr]["total_value_usd"] += value
            elif tf["direction"] == "OUT":
                holdings[addr]["balance"] -= amount
                holdings[addr]["total_value_usd"] -= value

        # Filter out zero/negative balances and sort by value
        active_holdings = [
            h for h in holdings.values() if h["balance"] > 0.001
        ]
        active_holdings.sort(key=lambda x: x["total_value_usd"], reverse=True)

        total_value = sum(h["total_value_usd"] for h in active_holdings)

        return {
            "wallet": address,
            "chain_id": chain_id,
            "holdings": active_holdings,
            "total_value_usd": round(total_value, 2),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ── PnL ──

@app.get("/wallet/{address}/pnl")
async def get_pnl(address: str, chain_id: str = "1"):
    """
    Returns realized + unrealized PnL breakdown per token using FIFO method.
    """
    address = address.lower()

    try:
        result = await pnl_calculator.calculate_pnl(address, chain_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PnL calculation error: {str(e)}")


# ── Raw Transfers ──

@app.get("/wallet/{address}/transfers")
async def get_transfers(
    address: str,
    chain_id: str = "1",
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
    direction: Optional[str] = Query(None, description="Filter by direction: IN or OUT"),
    token: Optional[str] = Query(None, description="Filter by token address"),
):
    """
    Returns raw token transfers with pricing info.
    """
    address = address.lower()
    offset = (page - 1) * limit

    try:
        # Get transaction hashes for this wallet
        tx_resp = (
            db.supabase.table("transactions")
            .select("tx_hash, timestamp")
            .eq("wallet_address", address)
            .eq("chain_id", chain_id)
            .order("timestamp", desc=True)
            .execute()
        )

        if not tx_resp.data:
            return {"wallet": address, "chain_id": chain_id, "page": page, "limit": limit, "total": 0, "transfers": []}

        tx_map = {tx["tx_hash"]: tx["timestamp"] for tx in tx_resp.data}
        tx_hashes = list(tx_map.keys())

        # Build the query
        all_transfers = []
        for i in range(0, len(tx_hashes), 200):
            chunk = tx_hashes[i : i + 200]
            query = (
                db.supabase.table("token_transfers")
                .select("*")
                .in_("tx_hash", chunk)
            )

            if direction:
                query = query.eq("direction", direction.upper())
            if token:
                query = query.eq("token_address", token.lower())

            tf_resp = query.execute()
            for tf in (tf_resp.data or []):
                tf["timestamp"] = tx_map.get(tf["tx_hash"], "")
                all_transfers.append(tf)

        # Sort by timestamp
        all_transfers.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

        # Paginate
        total = len(all_transfers)
        paginated = all_transfers[offset : offset + limit]

        return {
            "wallet": address,
            "chain_id": chain_id,
            "page": page,
            "limit": limit,
            "total": total,
            "transfers": paginated,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ── Helpers ──

def _get_wallet_tx_hashes(address: str, chain_id: str) -> list[str]:
    """Helper to get all transaction hashes for a wallet."""
    try:
        resp = (
            db.supabase.table("transactions")
            .select("tx_hash")
            .eq("wallet_address", address.lower())
            .eq("chain_id", chain_id)
            .limit(10000)
            .execute()
        )
        return [tx["tx_hash"] for tx in (resp.data or [])]
    except Exception:
        return []
