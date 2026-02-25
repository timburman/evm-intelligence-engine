from typing import AsyncGenerator
import httpx
import json
import os
import asyncio
import time

from core.token_registry import COINGECKO_LIST_URL

COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
RATE_LIMIT_DELAY = 2.5
PRICE_TTL = 600


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

    async def stream_prices(self, token_ids: list[str]) -> AsyncGenerator[dict, None]:
        """
        Yields price objects one by one as they are processed.
        This allows the frontend to start rendering immediately
        """

        # 1. Deduplicate and Check Cache
        unique_ids = list(set(token_ids))
        to_fetch = []

        for tid in unique_ids:
            cached = self.price_cache.get(tid)
            now = time.time()

            # If valid cache exists, yield it!
            if cached and (now - cached["timestamp"] < PRICE_TTL):
                yield {"id": tid, "price": cached["price"], "source": "cache"}
            else:
                to_fetch.append(tid)

        if not to_fetch:
            return

        print(f"[PRICE] Fetching fresh prices for {len(to_fetch)} assets...")

        chunk_size = 50
        async with httpx.AsyncClient() as client:
            for i in range(0, len(to_fetch), chunk_size):
                chunk = to_fetch[i : i + chunk_size]
                chunk_str = ",".join(chunk)

                try:
                    resp = await client.get(
                        f"{COINGECKO_LIST_URL}/simple/price",
                        params={"ids": chunk_str, "vs_currencies": "usd"},
                    )
                    if resp.status_code == 200:
                        data = resp.json()

                        for tid in chunk:
                            if tid in data:
                                price = data[tid].get("usd", 0.0)

                                self.price_cache[tid] = {
                                    "price": price,
                                    "timestamp": time.time(),
                                }
                                yield {"id": tid, "price": price, "source": "api"}
                            else:
                                yield {"id": tid, "price": 0.0, "source": "error"}
                    elif resp.status_code == 429:
                        print("[WARN] Rate Limit! Serving stale data if available...")
                        # Fallback: Check if we have old cache for this chunk
                        for tid in chunk:
                            if tid in self.price_cache:
                                yield {
                                    "id": tid,
                                    "price": self.price_cache[tid]["price"],
                                    "source": "stale_cache",
                                }
                        await asyncio.sleep(10)
                except Exception as e:
                    print(f"[Error] Batch failed: {e}")

                if i + chunk_size < len(to_fetch):
                    await asyncio.sleep(RATE_LIMIT_DELAY)


price_engine = CoinGeckoClient()
