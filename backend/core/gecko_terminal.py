import httpx
import asyncio
from typing import Optional, Dict, List


class GeckoTerminal:
    def __init__(self):
        self.base_url = "https://api.geckoterminal.com/api/v2"
        # Map EVM Chain IDs to GeckoTerminal Network Strings
        self.network_map = {
            "1": "eth",
            "137": "polygon_pos",
            "56": "bsc",
            "42161": "arbitrum",
            "10": "optimism",
            "8453": "base",
        }
        # In-memory caches to prevent spamming GT
        self.pool_cache: Dict[str, str] = {}
        self.candle_cache: Dict[str, List[Dict]] = {}

        # We need a custom User-Agent, GT blocks standard Python bots
        self.headers = {
            "Accept": "application/json;version=20230203",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/122.0.0.0",
        }

    async def get_top_pool(self, chain_id: str, token_address: str) -> Optional[str]:
        """Auto-discovers the most liquid DEX pool for a token."""
        network = self.network_map.get(chain_id)
        if not network:
            return None

        cache_key = f"{network}_{token_address.lower()}"
        if cache_key in self.pool_cache:
            return self.pool_cache[cache_key]

        url = f"{self.base_url}/networks/{network}/tokens/{token_address.lower()}/pools"

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url, headers=self.headers, timeout=15.0)
                if resp.status_code == 200:
                    data = resp.json().get("data", [])
                    if data:
                        # Grab the address of the #1 most liquid pool
                        pool_address = data[0]["attributes"]["address"]
                        self.pool_cache[cache_key] = pool_address
                        return pool_address
            except Exception as e:
                print(f"[GT] ❌ Failed to find pool for {token_address}: {e}")
        return None

    async def get_historical_candle(
        self, chain_id: str, pool_address: str, unix_ts: int
    ) -> float:
        """Fetches 5-minute candles and returns the closing price."""
        network = self.network_map.get(chain_id)
        if not network:
            return -1.0

        # We ask for candles BEFORE this timestamp. We add 300s (5 mins)
        # to ensure our exact transaction time is captured in the latest candle.
        target_ts = unix_ts + 300

        url = f"{self.base_url}/networks/{network}/pools/{pool_address}/ohlcv/minute"
        params = {
            "aggregate": 5,  # 5-minute candles
            "before_timestamp": target_ts,
            "limit": 10,  # We only need a few candles to find the closest one
        }

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    url, params=params, headers=self.headers, timeout=15.0
                )
                if resp.status_code == 200:
                    ohlcv_list = (
                        resp.json()
                        .get("data", {})
                        .get("attributes", {})
                        .get("ohlcv_list", [])
                    )
                    if ohlcv_list:
                        # GT returns: [timestamp, open, high, low, close, volume]
                        # We grab the most recent candle's close price (index 4)
                        closest_candle = ohlcv_list[0]
                        return float(closest_candle[4])
            except Exception as e:
                pass
        return -1.0


gt_client = GeckoTerminal()
