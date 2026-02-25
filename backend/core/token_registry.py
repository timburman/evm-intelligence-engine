from typing import Optional
import httpx
import os
import time
import json

# Constants
COINGECKO_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"
REGISTRY_FILE = "data/prices/coin_list.json"
MISSING_FILE = "data/prices/missing_tokens.json"

# Config
CACHE_DURATION = 86400  # 1 Day
MISSING_TTL = 86400 * 3  # 3 Days
REFRESH_COOLDOWN = 3600  # 1 Hour


class TokenRegistry:
    """
    Maintains a local 'Master List' of all valid CoinGecko tokens.
    Acts as the primary Spam Filter and ID Resolver.
    """

    def __init__(self) -> None:
        # Structure: { "chain_id": { "lowercase_address": "coingecko_id" } }
        self.lookup_map: dict[str, dict[str, str]] = {}

        # The Blacklist: {"0x213....": timestamp_checked}
        self.missing_map: dict[str, float] = {}
        self.raw_data = []

        self.last_refresh_ts = 0

        self.chain_map = {
            "1": "ethereum",
            "137": "polygon-pos",
            "42161": "arbitrum-one",
            "56": "binance-smart-chain",
        }

        # Initialize
        self._load_missing_cache()
        self.initialize_local()

    def initialize_local(self) -> None:
        """
        Loads the big list from disk if it exists.
        """
        if os.path.exists(REGISTRY_FILE):
            self.last_refresh_ts = os.path.getmtime(REGISTRY_FILE)

            # Check if file is stale immediately on startup
            if self._is_cache_stale():
                print(
                    "[STARTUP] Local cache is old. Will fetch new one on first request."
                )
            else:
                with open(REGISTRY_FILE, "r") as f:
                    data = json.load(f)
                    self._build_fast_lookup(data)

    def _load_missing_cache(self) -> None:
        if os.path.exists(MISSING_FILE):
            with open(MISSING_FILE, "r") as f:
                self.missing_map = json.load(f)

    def _save_missing_cache(self) -> None:
        with open(MISSING_FILE, "w") as f:
            json.dump(self.missing_map, f)

    def _is_cache_stale(self) -> bool:
        """
        Returns True if the local JSON is older than 1 day.
        """
        if not os.path.exists(REGISTRY_FILE):
            return True
        return (time.time() - os.path.getmtime(REGISTRY_FILE)) > CACHE_DURATION

    async def resolve_token(self, chain_id: str, address: str) -> Optional[str]:
        """
        Returns the CoinGecko API ID for a given address.
        Flow: Refresh Master List (if old) -> Check Master List -> Check Blacklist -> Hard Retry.
        If None -> It's Spam/Unknown
        """
        # 1. Check Memory Cache
        platform = self.chain_map.get(chain_id)
        if not platform:
            return None

        addr_lower = address.lower()

        # STEP 0: Auto-Update Master list
        # If the cache is > 24h old, we update it Now.
        # This ensures 'lookup_map' has the latest tokens before we search.
        if self._is_cache_stale():
            await self._refresh_registry_if_needed()

        # STEP 1: CHECK VALID LIST (Fast O(1) Lookup)
        # If Token X was listed yesterday, it's here now. We return it immediately
        cg_id = self.lookup_map.get(platform, {}).get(addr_lower)
        if cg_id:
            # If it was in the blacklist, remove it (it's valid now!)
            if addr_lower in self.missing_map:
                del self.missing_map[addr_lower]
                self._save_missing_cache()
            return cg_id

        # STEP 2: CHECK BLACKLIST (Smart Filtering)
        # Only reached if token is Not in the master list
        last_checked = self.missing_map.get(addr_lower)
        if last_checked:
            if (time.time() - last_checked) < MISSING_TTL:
                # It's spam, and we checked recently. IGNORE.
                return None
            else:
                # TTL Expired! Remove from flacklist and give it a chance below
                del self.missing_map[addr_lower]

        # STEP 3: HARD REFRESH (The Last Resort)
        # Maybe it was listed 5 minutes ago? We force a refresh.
        if (time.time() - self.last_refresh_ts) > REFRESH_COOLDOWN:
            print(
                f"[REGISTRY] Unknown token {addr_lower[:6]}... Checking remote updates..."
            )
            updated = await self._refresh_registry_if_needed(force=True)

            if updated:
                # Check one last time
                cg_id = self.lookup_map.get(platform, {}).get(addr_lower)
                if cg_id:
                    return cg_id
        else:
            print(
                f"[REGISTRY] Token {addr_lower[:6]} not found. Skipping refresh (Cooldown Active)."
            )

        # STEP 4: BLACKLIST IT
        # CoinGecko doesn't know it. It's Spam. See you in 3 days spam.
        print(
            f"[REGISTRY] Token {addr_lower[:6]} is likely SPAM. Blacklisting for 3 days."
        )
        self.missing_map[addr_lower] = time.time()
        self._save_missing_cache()

        return None

    async def _refresh_registry_if_needed(self, force=False) -> bool:
        """
        Fetches the massive JSON list from CoinGecko if cache is old.
        Returns True if a new fetch happened.
        """
        time_since_last_update = time.time() - self.last_refresh_ts
        if force and time_since_last_update < REFRESH_COOLDOWN:
            print(
                f"[REGISTRY] Refresh skipped. Last update was {int(time_since_last_update / 60)}m ago"
            )
            return False

        if self._is_cache_stale() or force:
            print("[REGISTRY] Downloading CoinGecko coin list")
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.get(
                        COINGECKO_LIST_URL, params={"include_platform": True}
                    )
                    if resp.status_code == 200:
                        data = resp.json()

                        os.makedirs(os.path.dirname(REGISTRY_FILE), exist_ok=True)
                        with open(REGISTRY_FILE, "w") as f:
                            json.dump(data, f)

                        self._build_fast_lookup(data)

                        # Update timestamp
                        self.last_refresh_ts = time.time()
                        return True
                    else:
                        print(
                            f"[ERROR] CoinGecko List Fetch Failed: {resp.status_code}"
                        )
                except Exception as e:
                    print(f"[ERROR] Registry update failed: {e}")
        return False

    def _build_fast_lookup(self, raw_data) -> None:
        """
        Converts raw list to O(1) Lookup.
        """
        print(f"[REGISTRY] Indexing {len(raw_data)} tokens...")
        self.lookup_map = {}
        for coin in raw_data:
            platforms = coin.get("platforms", {})
            coin_id = coin.get("id")
            for platform, address in platforms.items():
                if address:
                    if platform not in self.lookup_map:
                        self.lookup_map[platform] = {}
                    self.lookup_map[platform][address.lower()] = coin_id


token_registry = TokenRegistry()
