This is the perfect moment to snapshot the architecture. You’ve built the heavy-duty backend of the EVM-Intelligence Engine, navigating past enterprise paywalls and API rate limits. 

Here is the comprehensive "State of the Union" handoff document. You can copy-paste this exactly into your next environment to instantly align the context.

---

# 🚀 EVM-Intelligence Engine: Production Handoff Document

## 📌 Project Overview
The goal is to build a production-grade EVM portfolio indexer and PnL calculator. Unlike basic web3 apps, this engine accurately prices historical transactions (including gas costs and ERC-20 transfers) to the exact minute they occurred, scaling efficiently without spending thousands on enterprise API tiers. 

## 🏗️ What We Have Built & How

### 1. The Database Foundation (Supabase)
* **Tables:** `transactions`, `token_transfers`, and `daily_prices`.
* **Setup:** Tracks raw on-chain data and leaves a `price_at_transaction` column `NULL` so our asynchronous background workers can index them.

### 2. The God-Tier Time Machine (`historical_pricer.py`)
* **The Problem:** CoinGecko slammed us with a 365-day historical paywall, and no single free API can handle both 2015-era Ethereum and 2024-era meme coins.
* **The Solution (Multi-Layer Routing):**
    * **Layer 0 (Bypass):** Hardcoded stablecoin registry instantly returns `$1.00`.
    * **Layer 1 (The Yahoo Finance Backfill):** We completely bypassed crypto APIs for the "Dark Ages" by backfilling 9 years of major assets (ETH, BNB, MATIC) from Yahoo Finance directly into Supabase. 
    * **Layer 2 (DeFiLlama Exact-Time):** For modern tokens, we query DeFiLlama using exact UNIX timestamps for minute-precision accuracy.
    * **Dual-Key Caching:** We built a local JSON cache. Successful DefiLlama hits are cached by exact `unix_timestamp`. Failures (scam tokens) are cached by `YYYY-MM-DD` to instantly "fast-fail" them for the rest of the day, saving massive API limits.

### 3. The Bouncer (`token_registry.py`)
* **The Problem:** 80% of ERC-20 transfers are spam/malicious airdrops. Hitting pricing APIs for them causes instant rate limits.
* **The Solution:** A robust token registry acting as a shield.
    * Maps contract addresses to CoinGecko IDs.
    * Uses a **Smart Blacklist** with a 3-day Time-To-Live (TTL) for unknown tokens.
    * Implements a 1-hour refresh cooldown to prevent panic-looping the API.
    * Tokens failing this check are skipped in the DB (left `NULL`), saving the API compute.

### 4. The Async Pricing Engine (`pricer_engine.py`)
* **The Problem:** Sequential API calls take 10+ minutes per 1,000 transactions and trigger timeouts.
* **The Solution (The Waterfall Architecture):**
    * Uses `asyncio.gather` and Semaphores to batch process concurrent requests.
    * **Gas Cost Logic Fix:** Explicitly ignores 'IN' transactions so we don't subtract gas the user didn't actually pay.
    * **DeFiLlama -> GeckoTerminal Fallback:** If DeFiLlama misses a token or times out, the engine gracefully falls back to GeckoTerminal, auto-discovers the most liquid DEX pool for that token, and pulls the nearest 5-minute OHLCV candle close price.

### 5. Frontend UI Strategy: "Progressive PnL"
* To survive live demos without 10-second loader screens, the frontend will be split:
    * **Fast Lane (0.1s):** Instantly renders the user's major assets (ETH, USDC) by querying the pre-filled local Supabase DB.
    * **Slow Lane (Background):** A backend worker slowly crunches the obscure ERC-20 tokens via the Waterfall Architecture, dynamically updating the frontend as prices resolve.