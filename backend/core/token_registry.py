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
MIN_POOL_LIQUIDITY_USD = 50000  # $50K min liquidity for GeckoTerminal validation

# ---- Layer 0: Hardcoded Known Tokens (Instant resolve, zero API calls) ----
# Maps: (chain_id, lowercase_address) -> coin_id
KNOWN_TOKENS = {
    # Ethereum Mainnet
    ("1", "NATIVE"): "ethereum",
    ("1", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"): "ethereum",  # WETH
    ("1", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): "usd-coin",  # USDC
    ("1", "0xdac17f958d2ee523a2206206994597c13d831ec7"): "tether",  # USDT
    ("1", "0x6b175474e89094c44da98b954eedeac495271d0f"): "dai",  # DAI
    ("1", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"): "wrapped-bitcoin",  # WBTC
    ("1", "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"): "uniswap",  # UNI
    ("1", "0x514910771af9ca656af840dff83e8264ecf986ca"): "chainlink",  # LINK
    ("1", "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"): "aave",  # AAVE
    ("1", "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2"): "maker",  # MKR
    ("1", "0xd533a949740bb3306d119cc777fa900ba034cd52"): "curve-dao-token",  # CRV
    ("1", "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f"): "havven",  # SNX
    ("1", "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce"): "shiba-inu",  # SHIB
    ("1", "0x6982508145454ce325ddbe47a25d4ec3d2311933"): "pepe",  # PEPE
    ("1", "0x853d955acef822db058eb8505911ed77f175b99e"): "frax",  # FRAX
    ("1", "0x5a98fcbea516cf06857215779fd812ca3bef1b32"): "lido-dao",  # LDO
    ("1", "0xae78736cd615f374d3085123a210448e74fc6393"): "rocket-pool-eth",  # rETH
    ("1", "0xbe9895146f7af43049ca1c1ae358b0541ea49704"): "coinbase-wrapped-staked-eth",  # cbETH
    ("1", "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"): "wrapped-steth",  # wstETH
    # Polygon
    ("137", "NATIVE"): "polygon-ecosystem-token",
    ("137", "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"): "wmatic",  # WMATIC
    ("137", "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"): "usd-coin",  # USDC.e
    ("137", "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"): "usd-coin",  # USDC native
    # BSC
    ("56", "NATIVE"): "binancecoin",
    ("56", "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"): "binancecoin",  # WBNB
    # Arbitrum
    ("42161", "NATIVE"): "ethereum",
    ("42161", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"): "ethereum",  # WETH
    ("42161", "0x912ce59144191c1204e64559fe8253a0e49e6548"): "arbitrum",  # ARB
}

# Chain ID to DeFiLlama/CoinGecko platform name
CHAIN_PLATFORM_MAP = {
    "1": "ethereum",
    "137": "polygon-pos",
    "42161": "arbitrum-one",
    "56": "binance-smart-chain",
}


class TokenRegistry:
    """
    Maintains a token validation system with minimal CoinGecko dependency.
    
    Validation layers:
      Layer 0: Hardcoded known tokens (instant, zero API)
      Layer 1: CoinGecko list (optional enrichment, loaded from disk)
      Layer 2: GeckoTerminal $50K liquidity gate (honeypot-safe)
    
    CoinGecko is NOT a hard dependency. The system works if CG is down.
    """

    def __init__(self) -> None:
        # Structure: { "platform_name": { "lowercase_address": "coingecko_id" } }
        self.lookup_map: dict[str, dict[str, str]] = {}

        # The Blacklist: {"0x213....": timestamp_checked}
        self.missing_map: dict[str, float] = {}
        self.raw_data = []

        self.last_refresh_ts = 0

        self.chain_map = CHAIN_PLATFORM_MAP

        # GeckoTerminal network names (different from CoinGecko platform names)
        self.gt_network_map = {
            "1": "eth",
            "137": "polygon_pos",
            "56": "bsc",
            "42161": "arbitrum",
        }

        # Initialize
        self._load_missing_cache()
        self.initialize_local()

    def initialize_local(self) -> None:
        """
        Loads the big CoinGecko list from disk if it exists. Optional enrichment.
        """
        if os.path.exists(REGISTRY_FILE):
            self.last_refresh_ts = os.path.getmtime(REGISTRY_FILE)

            if self._is_cache_stale():
                print(
                    "[STARTUP] Local CoinGecko cache is old. Will try to refresh on first request."
                )
            else:
                try:
                    with open(REGISTRY_FILE, "r") as f:
                        data = json.load(f)
                        self._build_fast_lookup(data)
                except Exception as e:
                    print(f"[REGISTRY] Failed to load CoinGecko cache: {e}")
        else:
            print("[STARTUP] No CoinGecko cache file found. Running without it.")

    def _load_missing_cache(self) -> None:
        if os.path.exists(MISSING_FILE):
            try:
                with open(MISSING_FILE, "r") as f:
                    self.missing_map = json.load(f)
            except Exception:
                self.missing_map = {}

    def _save_missing_cache(self) -> None:
        os.makedirs(os.path.dirname(MISSING_FILE), exist_ok=True)
        with open(MISSING_FILE, "w") as f:
            json.dump(self.missing_map, f)

    def _is_cache_stale(self) -> bool:
        """
        Returns True if the local CoinGecko JSON is older than 1 day.
        """
        if not os.path.exists(REGISTRY_FILE):
            return True
        return (time.time() - os.path.getmtime(REGISTRY_FILE)) > CACHE_DURATION

    async def resolve_token(self, chain_id: str, address: str) -> Optional[str]:
        """
        Returns a coin_id for pricing (CoinGecko ID or raw address).
        
        Validation Flow:
          Layer 0: Known tokens -> instant return
          Layer 1: CoinGecko list -> O(1) lookup
          Layer 2: GeckoTerminal $50K liquidity -> honeypot-safe filter
          Blacklist: 3-day TTL for tokens that fail all layers
        
        Returns None if token is spam/unknown.
        Returns a coin_id string usable by the pricer (either CG ID or contract address).
        """
        addr_lower = address.lower() if address != "NATIVE" else "NATIVE"

        # ── LAYER 0: Known Tokens (Instant, 0ms) ──
        known_id = KNOWN_TOKENS.get((chain_id, addr_lower))
        if known_id:
            return known_id

        # ── Check platform exists ──
        platform = self.chain_map.get(chain_id)
        if not platform:
            return None

        # ── LAYER 1: CoinGecko List (Optional Enrichment) ──
        # If we have a fresh CoinGecko list, check it first
        if self._is_cache_stale() and self.lookup_map:
            # Cache is stale but we have data — try background refresh
            # Don't block on it
            pass
        elif self._is_cache_stale() and not self.lookup_map:
            # No CoinGecko data at all — try to fetch, but don't make it critical
            await self._refresh_registry_if_needed()

        cg_id = self.lookup_map.get(platform, {}).get(addr_lower)
        if cg_id:
            # Found in CoinGecko list — definitely valid
            if addr_lower in self.missing_map:
                del self.missing_map[addr_lower]
                self._save_missing_cache()
            return cg_id

        # ── CHECK BLACKLIST (Smart Filtering) ──
        last_checked = self.missing_map.get(addr_lower)
        if last_checked:
            if (time.time() - last_checked) < MISSING_TTL:
                # Checked recently, still spam
                return None
            else:
                # TTL expired, give it another chance
                del self.missing_map[addr_lower]

        # ── LAYER 2: GeckoTerminal Liquidity Gate ($50K minimum) ──
        # This is the REAL spam filter — $50K+ liquidity is hard to fake
        gt_valid = await self._check_geckoterminal_liquidity(chain_id, addr_lower)
        if gt_valid:
            print(f"[REGISTRY] ✅ Token {addr_lower[:10]}... passed GT liquidity gate")
            # Return the raw address — DeFiLlama can price it as "ethereum:{address}"
            return addr_lower

        # ── LAYER 1.5: CoinGecko Hard Refresh (Last Resort) ──
        # Maybe it was listed recently? Try one refresh if cooldown allows
        if (time.time() - self.last_refresh_ts) > REFRESH_COOLDOWN:
            print(
                f"[REGISTRY] Unknown token {addr_lower[:10]}... Trying CoinGecko refresh..."
            )
            updated = await self._refresh_registry_if_needed(force=True)
            if updated:
                cg_id = self.lookup_map.get(platform, {}).get(addr_lower)
                if cg_id:
                    return cg_id

        # ── BLACKLIST IT ──
        print(
            f"[REGISTRY] ❌ Token {addr_lower[:10]}... failed all checks. Blacklisting for 3 days."
        )
        self.missing_map[addr_lower] = time.time()
        self._save_missing_cache()

        return None

    async def _check_geckoterminal_liquidity(self, chain_id: str, token_address: str) -> bool:
        """
        Checks GeckoTerminal for a pool with ≥$50K liquidity.
        Returns True if the token has sufficient liquidity (likely real project).
        """
        network = self.gt_network_map.get(chain_id)
        if not network:
            return False

        url = f"https://api.geckoterminal.com/api/v2/networks/{network}/tokens/{token_address}/pools"
        headers = {
            "Accept": "application/json;version=20230203",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/122.0.0.0",
        }

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url, headers=headers, timeout=10.0)
                if resp.status_code == 200:
                    pools = resp.json().get("data", [])
                    if pools:
                        # Check the top pool's reserve_in_usd
                        top_pool = pools[0]
                        reserve_usd = float(
                            top_pool.get("attributes", {}).get("reserve_in_usd", "0") or "0"
                        )
                        if reserve_usd >= MIN_POOL_LIQUIDITY_USD:
                            return True
                        else:
                            print(
                                f"[REGISTRY] Token {token_address[:10]}... top pool has ${reserve_usd:,.0f} liquidity (need ${MIN_POOL_LIQUIDITY_USD:,.0f}+)"
                            )
            except Exception as e:
                print(f"[REGISTRY] GT liquidity check failed for {token_address[:10]}...: {e}")

        return False

    async def _refresh_registry_if_needed(self, force=False) -> bool:
        """
        Fetches the CoinGecko coin list if cache is old. Optional enrichment.
        Returns True if a new fetch happened.
        """
        time_since_last_update = time.time() - self.last_refresh_ts
        if force and time_since_last_update < REFRESH_COOLDOWN:
            return False

        if self._is_cache_stale() or force:
            print("[REGISTRY] Downloading CoinGecko coin list (optional enrichment)...")
            async with httpx.AsyncClient() as client:
                try:
                    resp = await client.get(
                        COINGECKO_LIST_URL, params={"include_platform": True}, timeout=30.0
                    )
                    if resp.status_code == 200:
                        data = resp.json()

                        os.makedirs(os.path.dirname(REGISTRY_FILE), exist_ok=True)
                        with open(REGISTRY_FILE, "w") as f:
                            json.dump(data, f)

                        self._build_fast_lookup(data)

                        self.last_refresh_ts = time.time()
                        return True
                    else:
                        print(
                            f"[REGISTRY] CoinGecko fetch failed: {resp.status_code} (non-critical)"
                        )
                except Exception as e:
                    print(f"[REGISTRY] CoinGecko update failed (non-critical): {e}")
        return False

    def _build_fast_lookup(self, raw_data) -> None:
        """
        Converts raw CoinGecko list to O(1) Lookup.
        """
        print(f"[REGISTRY] Indexing {len(raw_data)} tokens from CoinGecko...")
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
