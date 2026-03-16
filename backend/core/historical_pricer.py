import httpx
import json
import os
from datetime import datetime, timezone
from backend.core.database import db

LLAMA_HISTORY_URL = "https://coins.llama.fi/prices/historical/{timestamp}/{prefix}:{id}"
HISTORY_CACHE_FILE = "data/prices/historical_cache.json"

KNOWN_STABLECOINS = {
    "usd-coin",
    "tether",
    "dai",
    "frax",
    "binance-usd",
    "true-usd",
    "liquidity-usd",
}

MAJOR_COINS = {"ethereum", "polygon-ecosystem-token", "binancecoin"}


class HistoricalPricer:
    """
    The Time Macine lol. (Smart Router)
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
        clean_ts = sql_timestamp.replace("T", " ").split("+")[0].split(".")[0]

        dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )

        cache_key = dt.strftime("%Y-%m-%d")
        unix_ts = int(dt.timestamp())
        year = dt.year

        return cache_key, unix_ts, year

    async def get_historical_price(self, coin_id: str, sql_timestamp: str) -> float:
        """
        The MC
        Tries Cache, then db and then DefiLlama
        """
        if not coin_id:
            return 0.0

        if coin_id in KNOWN_STABLECOINS:
            return 1.0

        cache_key, unix_ts, year = self._parse_dates(sql_timestamp)

        # Layer 1: Check Cache(0ms, 0Cost)
        if (
            coin_id in self.historical_cache
            and cache_key in self.historical_cache[coin_id]
        ):
            val = self.historical_cache[coin_id][cache_key]
            if val == -1.0:
                return 0.0
            return val

        # Layer 1.7: The PRE-2020 Dead Zone (Random Tokens)
        if year < 2020 and coin_id not in MAJOR_COINS:
            print(f"[WARN] Skipping {coin_id} for {year} (Pre-Dex era).")
            self._update_cache(coin_id, cache_key, -1.0)
            return 0.0

        # Layer 1.5: The Pre-2020 Database Lookup (Major Coins Only)
        # Only Daily Candels Base
        if year < 2020 and coin_id in MAJOR_COINS:
            try:
                response = (
                    db.supabase.table("daily_prices")
                    .select("price_usd")
                    .eq("coin_id", coin_id)
                    .eq("date", cache_key)
                    .execute()
                )

                if response.data:
                    price = float(response.data[0]["price_usd"])
                    self._update_cache(coin_id, cache_key, price)
                    return price
            except Exception:
                pass

            self._update_cache(coin_id, cache_key, -1.0)
            return 0.0

        print(f"[PRICER] Resolving {coin_id} on {cache_key} (Exact Time: {unix_ts})")

        # Layer 2: The DefiLlama (Precision)
        price = await self._fetch_defillama(coin_id, unix_ts)
        if price > 0:
            self._update_cache(coin_id, cache_key, price)
            return price

        # Layer 3: DefiLlama Failed - Database Fallback
        # A daily candle is less accureate, but better thhan $0 lol
        if coin_id in MAJOR_COINS:
            print(f"[WARN] DefiLlama missed {coin_id}. Falling back")
            try:
                response = (
                    db.supabase.table("daily_prices")
                    .select("price_usd")
                    .eq("coin_id", coin_id)
                    .eq("date", cache_key)
                    .execute()
                )

                if response.data:
                    price = float(response.data[0]["price_usd"])
                    self._update_cache(coin_id, cache_key, price)
                    return price
            except Exception:
                pass

        print(f"[ERROR] All methods failed for {coin_id} on {cache_key}.")
        self._update_cache(coin_id, cache_key, -1.0)
        return 0.0

    async def _fetch_defillama(self, coin_id: str, unix_ts: int) -> float:
        """
        Hits the DefiLlama Historical API using the 'coingecko' prefix trick.
        """
        query_id = f"coingecko:{coin_id}"
        url = LLAMA_HISTORY_URL.format(
            timestamp=unix_ts, prefix="coingecko", id=coin_id
        )

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url)

                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("coins", {}).get(query_id, {}).get("price", 0.0)
            except Exception as e:
                print(f"[ERROR] DefiLlama connect error: {e}")

        return 0.0

    def _update_cache(self, coin_id: str, cache_key: str, price: float):
        """
        Saves successfully fetched data to disk
        """
        if coin_id not in self.historical_cache:
            self.historical_cache[coin_id] = {}
        self.historical_cache[coin_id][cache_key] = price
        self._save_cache()


historical_pricer = HistoricalPricer()
