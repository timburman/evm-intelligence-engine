from fastapi import FastAPI
from core.etherscan import fetch_all_txs
from core.token_registry import token_registry
from core.coingecko import price_engine
import asyncio

app = FastAPI(title="EVM Portfolio Intelligence Engine")


@app.on_event("startup")
async def startup_event():
    """
    Runs once when the server starts.
    """
    print("[SYSTEM] Starting Engine...")

    # Check if local CoinGecko list is > 24hr old
    if token_registry._is_cache_stale():
        print("[SYSTEM] Cache is stale. Triggering background update")

        # create_task ensures this runs without blocking the server startup
        asyncio.create_task(token_registry._refresh_registry_if_needed())
    else:
        print("[SYSTEM] Cache is fresh.")


@app.get("/analyze/{address}")
async def analyze_portfolio(address: str, chain_id: str = "1"):
    pass
