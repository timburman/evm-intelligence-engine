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
    """
    Week 1 Milestone Endpoint:
    Fetch Txs -> Filters Spam -> Streams Prices.
    """
    # 1. Fetch Transaction History(Etherscan)
    print(f"--- Analyzing {address} ---")
    raw_data = await fetch_all_txs(address, chain_id)

    # 2. Extract Unique Token Contracts
    touched_tokens = set(tx["contractAddress"] for tx in raw_data.get("erc20", []))
    print(f"[ANALYSIS] Found {len(touched_tokens)} unique tokens in history.")

    # 3. Resolve IDs and Filters Spam (TokenRegistry)
    # This maps '0xContract' -> 'bitcoin uisng O(1) lookup'
    valid_map = {}
    spam_count = 0

    for token_addr in touched_tokens:
        cg_id = await token_registry.resolve_token(chain_id, token_addr)
        if cg_id:
            valid_map[cg_id] = token_addr
        else:
            spam_count += 1

    # 4. Stream Prices (CoinGecko)
    # We consume the generator to build the response.
    portfolio_preview = []

    # Get the list of valid IDs to fetch
    ids_to_fetch = list(valid_map.keys())

    if ids_to_fetch:
        # stream_prices yields batches like: {"data": {"bitcoin": 65000}, "source": "cache"}
        async for batch in price_engine.get_price_batches(ids_to_fetch):
            source = batch["source"]
            prices = batch["data"]

            for cg_id, price_usd in prices.items():
                portfolio_preview.append(
                    {
                        "asset_id": cg_id,
                        "contract": valid_map.get(cg_id),
                        "price_usd": price_usd,
                        "source": source,
                    }
                )

    return {
        "wallet": address,
        "chain_id": chain_id,
        "stats": {
            "total_transactions": len(raw_data["normal"])
            + len(raw_data["internal"])
            + len(raw_data["erc20"]),
            "unique_token_seen": len(touched_tokens),
            "valid_assets_identified": len(valid_map),
            "spam_tokens_blocked": spam_count,
        },
        "portfolio": portfolio_preview,
        "next_step": "Week 2: Balance Calc & Database persistence",
    }
