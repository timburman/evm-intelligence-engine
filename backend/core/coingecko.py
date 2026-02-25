from typing import Any, AsyncGenerator
import httpx
import json
import os
import asyncio
import time


COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
RATE_LIMIT_DELAY = 2.5
PRICE_TTL = 600
CACHE_FILE = "data/prices/price_cache.json"


class CoinGeckoClient:
    """
    Handels price fetching with strict rate limiting and caching
    1. TTL Caching (10min)
    2. Async Streaming (Yields results)
    3. Stale Fallback (Old prices)
    """

    def __init__(self) -> None:
        # Structure: { "bitcoin": { "price": 60000, "timestamp": 23124134 } }
        self.price_cache: dict[str, dict] = {}
        self._load_cache_from_disk()

    def _load_cache_from_disk(self) -> None:
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r") as f:
                    self.price_cache = json.load(f)
                print(f"[Price] Loaded {len(self.price_cache)} prices from disk cache")
            except Exception as e:
                print(f"[PRICE] cache load failed: {e}")
                self.price_cache = {}

    def _save_cache_to_disk(self):
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(self.price_cache, f)

    async def get_price_batches(
        self, token_ids: list[str]
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Yields Batches of prices.
        Batch 1: Immediate Cached Prices (Fast)
        Batch 2...N: Fresh API Prices (Slower)
        """
        unique_ids = list(set(token_ids))
        if not unique_ids:
            return

        to_fetch = []
        cached_batch = {}
        now = time.time()

        # 1. Sort: Cache vs Needs-Fetch
        for tid in unique_ids:
            data = self.price_cache.get(tid)
            # Check if exists AND is fresh (TTL)
            if data and (now - data["ts"] < PRICE_TTL):
                cached_batch[tid] = data["price"]
            else:
                to_fetch.append(tid)

        # 2. Yield Cached Batch Immediately (0ms Latency)
        if cached_batch:
            # yield metadata so main.py knows source
            yield {"data": cached_batch, "source": "cache"}

        # 3. Fetch Missing in Chunks
        if not to_fetch:
            return

        print(f"[PRICE] Fetching {len(to_fetch)} fresh prices...")

        chunk_size = 50
        async with httpx.AsyncClient() as client:
            for i in range(0, len(to_fetch), chunk_size):
                chunk = to_fetch[i : i + chunk_size]
                chunk_str = ",".join(chunk)

                try:
                    resp = await client.get(
                        f"{COINGECKO_API_URL}/simple/price",
                        params={"ids": chunk_str, "vs_currencies": "usd"},
                    )

                    if resp.status_code == 200:
                        api_data = resp.json()

                        fresh_batch = {}
                        for tid in chunk:
                            if tid in api_data:
                                price = api_data[tid].get("usd", 0.0)
                                fresh_batch[tid] = price

                                # Update Internal Cache
                                self.price_cache[tid] = {
                                    "price": price,
                                    "ts": time.time(),
                                }
                            else:
                                fresh_batch[tid] = 0.0

                        # Save to Disk after every successful fetch (Safety)
                        self._save_cache_to_disk()

                        yield {"data": fresh_batch, "source": "api"}
                    elif resp.status_code == 429:
                        print("[WARN] Rate Limit! Using Stale Cache if available.")

                        # Emergency: Try to yield stale data for this chunk
                        stale_batch = {}
                        for tid in chunk:
                            if tid in self.price_cache:
                                stale_batch[tid] = self.price_cache[tid]["price"]
                        if stale_batch:
                            yield {"data": stale_batch, "source": "stale_cache"}

                        await asyncio.sleep(10)
                except Exception as e:
                    print(f"[ERROR] Batch failed: {e}")

                if i + chunk_size < len(to_fetch):
                    await asyncio.sleep(RATE_LIMIT_DELAY)


price_engine = CoinGeckoClient()
