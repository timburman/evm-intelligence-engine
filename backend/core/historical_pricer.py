import httpx
import json
import os
from datetime import datetime, timezone
from backend.core.database import db

LLAMA_HISTORY_URL = "https://coins.llama.fi/prices/historical/{timestamp}/{query_id}"
HISTORY_CACHE_FILE = "data/prices/historical_cache.json"

KNOWN_STABLECOINS = {
    "usd-coin",
    "tether",
    "dai",
    "frax",
    "binance-usd",
    "true-usd",
    "liquidity-usd",
    # Mainnet Contract Addresses:
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0x853d955acef822db058eb8505911ed77f175b99e",  # FRAX
    # Polygon USDC
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC.e on Polygon
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",  # USDC native on Polygon
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

    def _parse_dates(self, unix_ts: int) -> tuple:
        """
        Takes unix timestamp integer and creates the formats needed for APIs.
        Returns: (date_key, unix_ts_int, unix_ts_str, year)
        """
        dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)

        date_key = dt.strftime("%Y-%m-%d")
        unix_ts_int = int(unix_ts)
        unix_ts_str = str(unix_ts_int)
        year = dt.year

        return date_key, unix_ts_int, unix_ts_str, year

    async def get_historical_price(self, coin_id: str, unix_ts: int) -> float:
        """
        The MC
        Tries Cache, then db and then DefiLlama
        """
        if not coin_id:
            return 0.0

        if coin_id in KNOWN_STABLECOINS:
            return 1.0

        date_key, unix_ts_int, unix_ts_str, year = self._parse_dates(unix_ts)

        # Layer 1: Check Cache(0ms, 0Cost)
        if coin_id in self.historical_cache:
            # 1a. Exact Minute Match (DefiLlama Precision)
            if unix_ts_str in self.historical_cache[coin_id]:
                val = self.historical_cache[coin_id][unix_ts_str]
                if val == -1.0:
                    return 0.0
                return val

            # 1b. Daily Match (DB Backfill or Fast-Fail Negative Cache)
            if date_key in self.historical_cache[coin_id]:
                val = self.historical_cache[coin_id][date_key]
                if val == -1.0:
                    return 0.0
                return val

        # Layer 1.7: The PRE-2020 Dead Zone (Random Tokens)
        if year < 2020 and coin_id not in MAJOR_COINS:
            print(f"[WARN] Skipping {coin_id} for {year} (Pre-Dex era).")
            self._update_cache(coin_id, date_key, -1.0)
            return 0.0

        # Layer 1.5: The Pre-2020 Database Lookup (Major Coins Only)
        # Only Daily Candels Base
        if year < 2020 and coin_id in MAJOR_COINS:
            try:
                response = (
                    db.supabase.table("daily_prices")
                    .select("price_usd")
                    .eq("coin_id", coin_id)
                    .eq("date", date_key)
                    .execute()
                )

                if response.data:
                    price = float(response.data[0]["price_usd"])
                    self._update_cache(coin_id, date_key, price)
                    return price
            except Exception:
                pass

            self._update_cache(coin_id, date_key, -1.0)
            return 0.0

        # Layer 2: The DefiLlama (Precision)
        price = await self._fetch_defillama(coin_id, unix_ts_int)
        if price > 0:
            self._update_cache(coin_id, unix_ts_str, price)
            return price

        # Cache the miss so we don't re-query this exact timestamp
        self._update_cache(coin_id, unix_ts_str, -1.0)
        return 0.0

    async def get_database_daily_fallback(self, coin_id: str, unix_ts: int) -> float:
        """
        Layer 3 fallback called by the PricerEngine if both DefiLlama and GeckoTerminal miss an asset.
        """
        if coin_id not in MAJOR_COINS:
            return 0.0

        date_key, _, _, _ = self._parse_dates(unix_ts)
        print(f"[WARN] DefiLlama & GT missed {coin_id}. Falling back to DB daily candle...")
        try:
            response = (
                db.supabase.table("daily_prices")
                .select("price_usd")
                .eq("coin_id", coin_id)
                .eq("date", date_key)
                .execute()
            )

            if response.data:
                price = float(response.data[0]["price_usd"])
                self._update_cache(coin_id, date_key, price)
                return price
        except Exception:
            pass

        self._update_cache(coin_id, date_key, -1.0)
        return 0.0

    async def _fetch_defillama(self, coin_id: str, unix_ts: int) -> float:
        """
        Hits the DefiLlama Historical API using the 'coingecko' prefix trick.
        """
        if coin_id.startswith("0x"):
            query_id = f"ethereum:{coin_id.lower()}"
        else:
            query_id = f"coingecko:{coin_id}"
        url = LLAMA_HISTORY_URL.format(timestamp=unix_ts, query_id=query_id)

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url, timeout=10.0)

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
