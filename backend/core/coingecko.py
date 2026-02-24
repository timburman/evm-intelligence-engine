import httpx
import json
import os
import asyncio
import time

COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
CACHE_DIR = "data/prices"
CACHE_FILE = f"{CACHE_DIR}/token_prices.json"
MAX_ADDRESSES_PER_CALL = 10
RATE_LIMIT_DELAY = 2.5


os.makedirs(CACHE_DIR, exist_ok=True)


class CoinGeckoClient:
    """
    Handels price fetching with strict rate limiting and caching
    Acts as the primary 'Spam Filter' by ignoring unpriced tokens.
    """

    def __init__(self) -> None:
        self.load_cache()

    def load_cache(self):
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                self.cache = json.load(f)

        else:
            self.cache = {}

    def _save_cache(self):
        with open(CACHE_FILE, "w") as f:
            json.dump(self.cache, f, indent=2)

    async def fetch_current_prices(
        self, chain_name: str, contract_addresses: list[str]
    ) -> dict[str, float]:
        """
        Fetches current USD prices for a list of contract addresses.
        Returns:
            Dictionary of {address_lowercase: price_usd}.
            Tokens not found by CoinGecko are EXCLUDED (Spam filtered).
        """
        addresses = [addr.lower() for addr in contract_addresses]

        missing_addresses = [addr for addr in addresses if addr not in self.cache]

        if missing_addresses:
            print(f"[Info] Fetching prices for {len(missing_addresses)} new tokens")
            await self._batch_fetch(chain_name, missing_addresses)

        results = {}
        spam_count = 0

        for addr in addresses:
            if addr in self.cache:
                results[addr] = self.cache[addr]
            else:
                spam_count += 1

        if spam_count > 0:
            print(f"[FILTER] Filtered out {spam_count} unpriced/spam tokens.")

        return results

    async def _batch_fetch(self, chain_name: str, addresses: list[str]):
        """
        Splits the list into chunks and queries API, respecting rate limits.
        """
        async with httpx.AsyncClient() as client:
            for i in range(0, len(addresses), MAX_ADDRESSES_PER_CALL):
                chunk = addresses[i : i + MAX_ADDRESSES_PER_CALL]
                chunk_str = ",".join(chunk)

                url = f"{COINGECKO_API_URL}/simple/token_price/{chain_name}"
                params = {"contract_addresses": chunk_str, "vs_currencies": "usd"}

                try:
                    resp = await client.get(url, params=params)

                    if resp.status_code == 200:
                        data = resp.json()

                        for addr, price_data in data.items():
                            if "usd" in price_data:
                                self.cache[addr.lower()] = price_data["usd"]
                    elif resp.status_code == 429:
                        print("[WARN] Rate limit hit! Backing off...")
                        time.sleep(10)
                    else:
                        print(f"[ERROR] CoinGecko status {resp.status_code}")
                except Exception as e:
                    print(f"[ERROR] Price fetch failed: {e}")

                self._save_cache()

                if i + MAX_ADDRESSES_PER_CALL < len(addresses):
                    await asyncio.sleep(RATE_LIMIT_DELAY)
