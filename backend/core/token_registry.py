import httpx
import os
import time
import json

# Constants
COINGECKO_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"
REGISTRY_FILE = "data/prices/coin_list.json"
MISSING_FILE = "data/prices/missing_tokens.json"

# Config
CACHE_DURATION = 86400
MISSING_TTL = 86400 * 3

class TokenRegistry:
    """
    Maintains a local 'Master List' of all valid CoinGecko tokens.
    Acts as the primary Spam Filter and ID Resolver.
    """

    def __init__(self) -> None:
        # Structure: { "chain_id": { "lowercase_address": "coingecko_id" } }
        self.lookup_map: dict[str, dict[str, str]] = {}
        self.missing_map: dict[str, float] = {}
        self.raw_data = []

        self.chain_map = {
            "1": "ethereum",
            "137": "polygon-pos",
            "42161": "arbitrum-one",
            "56": "binance-smart-chain",
        }

    async def initialize(self):
        """
        Call this at startup to load data.
        """
        await self._refresh_registry_if_needed()
        self._build_fast_lookup()

    async def resolve_token(self, chain_id: str, address: str) -> optional[str]:
        """
        Returns the CoinGecko API ID for a given address.
        If None -> It's Spam/Unknown
        """
        # 1. Check Memory Cache
        platform_id = self.chain_map.get(chain_id)
        if not platform_id:
            return None

        cg_id = self.lookup_map.get(platform_id, {}).get(address.lower())
        
        # 2. If missing. Try One refetch.
        if not cg_id:
            print(f"[REGISTRY] Token {address[:6]}... not found. Checking for updates")
            updated = await self._refresh_registry_if_needed(force=True)
            if updated:
                self._build_fast_lookup()
                cg_id = self.lookup_map.get(platform_id, {}).get(address.lower())

        return cg_id

    async def _refresh_registry_if_needed(self, force=False) -> bool:
        """
        Fetches the massive JSON list from CoinGecko if cache is old.
        Returns True if a new fetch happened.
        """
        is_exist = os.path.exists(REGISTRY_FILE)
        is_old = is_exist and (time.time() - os.path.getmtime(REGISTRY_FILE) > )

    def _build_fast_lookup(self):
        pass
