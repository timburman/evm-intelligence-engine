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
        dt = datetime.strptime(sql_timestamp, '%Y-%m-%d %H:%M%S').replace(tzinfo=timezone.utc)

        cache_key = dt.strftime('%Y-%m-%d')
        cg_date = dt.strftime('%d-%m-%Y')
        unix_ts = int(dt.timestamp())

        return cache_key, cg_date, unix_ts
