from os.path import exists
from typing import Optional
import httpx
import asyncio
import json
import os
from datetime import datetime, timezone

CG_HISTORY_URL = "https://api.coingecko.com/api/v3/coins/{id}/history"
LLAMA_HISTORY_URL = "https://coins.llama.fi/prices/historical/{timestamp}/{prefix}:{id}"
HISTORY_CACHE_FILE = "data/prices/historical_cache.json"


class HistoricalPricer:
    """
    The Time Macine lol.
    Waterfall Architecture: Local Cache -> CoinGecko -> DefiLlama
    """

    def __init__(self) -> None:
        # Structure {"ethereum": {"2024-05-28": 1850:55}}
        self.historical_cache = {}
        self._load_cache()

    def _load_cache(self):
        if os.path.exists(HISTORY_CACHE_FILE):
            try:
                with open(HISTORY_CACHE_FILE, "r") as f:
                    self.historical_cache = json.load(f)
            except Exception as e:
                print(f"[PRICER] Cache load failed: {e}")
                self.historical_cache = {}

    def _save_cache(self):
        os.makedirs(os.path.dirname(HISTORY_CACHE_FILE), exist_ok=True)
        with open(HISTORY_CACHE_FILE, "w") as f:
            json.dump(self.historical_cache, f)

    def _parse_dates(self, sql_timestamp: str) -> tuple:
        """
        Takes '2023-05-15 14:30:00' and creates the formats needed for APIs.
        Returns: (cache_key, cg_date, unix_timestamp)
        """
        dt = datetime.strptime(sql_timestamp, "%Y-%m-%d %H:%M%S").replace(
            tzinfo=timezone.utc
        )

        cache_key = dt.strftime("%Y-%m-%d")
        cg_date = dt.strftime("%d-%m-%Y")
        unix_ts = int(dt.timestamp())

        return cache_key, cg_date, unix_ts

    async def get_historical_price(self, coin_id: str, sql_timestamp: str) -> float:
        """
        The MC
        Tries Cache, then CoinGecko and then DefiLlama
        """
        if not coin_id:
            return 0.0

        cache_key, cg_date, unix_ts = self._parse_dates(sql_timestamp)

        # Step 1: Check Cache(0ms, 0Cost)
        if (
            coin_id in self.historical_cache
            and cache_key in self.historical_cache[coin_id]
        ):
            return self.historical_cache[coin_id][cache_key]

        print(f"[PRICER] Fetching {coin_id} for {cache_key}...")

        # Step 2: Try CoinGecko
        price = await self._fetch_coingecko(coin_id, cg_date)

        if price > 0:
            self._update_cache(coin_id, cache_key, price)
            return price

        # Step 3: CoinGecko Failed.. Fallback to DefiLlama
        print(f"[WARN] CoinGecko failed for {coin_id}. Moving to DefiLlama")
        price = await self._fetch_defillama(coin_id, unix_ts)

        if price > 0:
            self._update_cache(coin_id, cache_key, price)
            return price

        print(f"[ERROR] Both APIs failed to find {coin_id} on {cache_key}.")
        return 0.0
