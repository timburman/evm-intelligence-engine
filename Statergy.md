

# Full Strategy: Price Resolution for Non-Swap Transactions

---

## The Problem Statement

A non-swap transaction happened — a plain ETH transfer, an ERC-20 transfer, an NFT mint, a contract call with value, a bridge deposit, whatever. There's no swap event to extract price from. We need the USD value at that exact moment.

---

## The Resolution Pipeline

Think of this as a **waterfall**. Each layer is faster and cheaper than the next fallback. We go down only on miss.

---

### Layer 0: Known Stablecoins — Instant, No Lookup

Before doing anything, check: **is the token a stablecoin?**

- USDC, USDT, DAI, FRAX, LUSD, BUSD, TUSD, etc.
- Price is $1. Done. No computation needed.
- Maintain a hardcoded/configurable set of known stablecoin addresses per chain
- This eliminates a surprisingly large percentage of lookups

---

### Layer 1: Local Price Database — Microseconds

Check your own database first.

**Schema concept:**
```
token_prices (
    chain_id,
    token_address,
    block_number,
    timestamp,
    price_usd,
    source,          -- "uniswap_v3", "chainlink", "defillama", etc.
    PRIMARY KEY (chain_id, token_address, block_number)
)
```

**Lookup logic:**
- Exact block match? Return it.
- No exact match? Find the **nearest block** within a ±50 block window (~10 minutes on Ethereum). If the price exists nearby, interpolate or just use the closest one. Token prices don't usually move meaningfully in 10 minutes unless it's a major event.
- Miss? Move to Layer 2.

**Why this works:** Over time, this database fills up. Every price you resolve through any downstream layer gets written back here. After weeks of operation, your hit rate on this layer approaches 95%+.

---

### Layer 2: Wrapped Native Token Price Derivation — Instant Math

Many ERC-20 tokens have a **deterministic price relationship** to another token you already know:

- **WETH** = ETH (always 1:1, it's just wrapped)
- **wstETH** = stETH × exchange rate (readable on-chain from the contract itself)
- **rETH** = ETH × exchange rate (from RocketPool contract)
- **cUSDC, aUSDC** = USDC × exchange rate (from Compound/Aave contract)
- **LP tokens** = derivable from pool reserves and total supply

So if you know ETH's price, you automatically know WETH, stETH, wstETH, rETH, cbETH prices with one on-chain read of the exchange rate at that block.

**Maintain a registry** of these derivative relationships:
```
WETH → ETH × 1.0
wstETH → read wstETH.stEthPerToken() at block, multiply by ETH price
rETH → read rETH.getExchangeRate() at block, multiply by ETH price
aUSDC → read aToken exchange rate, multiply by USDC price ($1)
```

This covers a **huge** chunk of DeFi tokens with zero external calls.

---

### Layer 3: On-Chain DEX Pool Reads — One RPC Call

This is your primary price discovery for any post-2019 token.

**Step 3a: Find the right pool**

You need to know which pool to query. Build a **pool registry**:

- For Uniswap V2-style: call the Factory contract's `getPair(tokenA, tokenB)` — this is a one-time lookup per token pair, cache it forever
- For Uniswap V3: use the Factory's `getPool(tokenA, tokenB, fee)` — try fee tiers 500, 3000, 10000
- Prefer pools paired against WETH or stablecoins (USDC, USDT, DAI) since you already know those prices
- Prefer pools with highest liquidity (check reserves/TVL once, cache the preference)

**Pool priority per token:**
1. Token/USDC pool (direct USD price, no multiplication needed)
2. Token/USDT pool
3. Token/WETH pool (multiply by ETH price)
4. Token/DAI pool

**Step 3b: Read price at historical block**

For **Uniswap V2** style pools:
- Call `getReserves()` at the target block number
- Price = reserve of quote token / reserve of base token
- Adjust for decimals

For **Uniswap V3** pools:
- Call `slot0()` at the target block number
- Extract `sqrtPriceX96`
- Price = (sqrtPriceX96 / 2^96)² adjusted for decimals
- Alternatively, V3 has a built-in **TWAP oracle** via `observe()` — you can get time-weighted average price over any historical window, which smooths out manipulation

For **Chainlink oracles** (ETH, BTC, LINK, and ~50 major tokens):
- Call `latestRoundData()` at the target block
- Returns the oracle-reported price at that block
- More reliable than DEX reads for major assets (aggregated from many sources, manipulation-resistant)

**Step 3c: Multi-hop pricing**

What if Token X doesn't have a pool against USDC or WETH, but it has a pool against Token Y, which has a pool against WETH?

- Token X → Token Y (from their pool)
- Token Y → WETH (from their pool)
- WETH → USD (from Chainlink or WETH/USDC pool)
- Multiply through

Limit to **2 hops max**. More than that and the price becomes unreliable.

**Important:** All of this is just RPC calls. No API keys. No rate limits beyond your RPC provider. And for historical blocks, these never change, so **cache every result permanently in Layer 1**.

---

### Layer 4: DefiLlama Historical Timestamp — Background Async

For when on-chain reads fail:
- Token launched before DEXes existed
- Token has no on-chain liquidity pool
- Token is on a chain where you haven't indexed pools yet

**DefiLlama endpoint:** `GET https://coins.llama.fi/prices/historical/{timestamp}/{coins}`

- Takes a Unix timestamp
- Takes multiple tokens in one call (comma-separated)
- Returns closest known price
- Free, no key needed
- Rate limit: ~dozens per minute

**Critical: This is NOT in the hot path.** 

Flow:
1. Transaction comes in, can't resolve price through Layers 0-3
2. Mark it as `price_pending`
3. Add to a background queue
4. Background worker picks it up, respects rate limits, calls DefiLlama
5. Stores result in Layer 1 database
6. Updates the transaction record with USD value

The user might see "price resolving..." for 2-5 seconds on rare tokens. That's acceptable.

---

### Layer 5: CoinGecko Historical — Last Resort

If DefiLlama doesn't have it:

**Endpoint:** `GET /api/v3/coins/{id}/market_chart/range?vs_currency=usd&from={unix}&to={unix}`

- Problem: CoinGecko uses its own token IDs, not contract addresses (for some tokens)
- You need a mapping table: `contract_address → coingecko_id`
- Hourly granularity for ranges < 90 days
- Daily granularity for ranges > 90 days

Same async background pattern as Layer 4. Queue it, fetch it, cache it forever.

---

### Layer 6: Price Unknown — Accept Reality

Some tokens genuinely don't have a discoverable price:
- Internal protocol tokens that were never traded
- Test tokens
- Tokens with zero liquidity
- Tokens that only existed for a few blocks (rug pulls pre-listing)

Mark these as `price_unavailable` with a reason. Don't loop forever trying to find something that doesn't exist.

---

## The Background Sync Engine

Running independently of transaction processing:

### Job 1: Major Asset History Backfill (Run Once)
- Pull full daily price history for ETH, BTC, and top 50 tokens from DefiLlama
- Chunked into manageable date ranges
- Store everything in Layer 1 database
- Takes a few hours with rate limiting. Do it once at setup.

### Job 2: Active Token Watchlist Sync (Runs Every 30-60 Seconds)
- Maintain a list of tokens your system has seen recently
- Pull current prices for all of them in bulk
- DefiLlama's `/prices/current/{coins}` accepts many tokens in one call
- Store in Layer 1 with current block/timestamp

### Job 3: New Token Discovery Backfill (Event-Driven)
- When a new token appears for the first time, trigger a background job
- Pull its last 90 days of price history
- Find its best DEX pool and cache the pool address
- After this completes, all future lookups for this token hit Layer 1 or Layer 3

### Job 4: Pending Price Resolution (Continuous)
- Pick up transactions marked `price_pending`
- Try Layer 4, then Layer 5
- Update transaction records
- Batch where possible to minimize API calls

---

## Special Considerations

### Multi-Chain
- Pool addresses differ per chain
- Chainlink feed addresses differ per chain
- DefiLlama handles this with `{chain}:{address}` format
- Your pool registry needs to be per-chain

### ETH (Native Token, Not ERC-20)
- ETH itself doesn't have a contract address
- Use WETH price (identical) or Chainlink ETH/USD feed
- DefiLlama uses `ethereum:0x0000000000000000000000000000000000000000` or `coingecko:ethereum`

### Token Decimals Matter
- USDC has 6 decimals, WETH has 18
- If you get the decimal conversion wrong, your price is off by orders of magnitude
- Fetch and cache decimals per token (one-time `decimals()` call per token, cache forever)

### Price Manipulation / Sanity Checks
- A single-block DEX pool read can be manipulated (flash loans)
- For high-value transactions, consider reading price across **multiple pools** and taking the median
- Or use Uniswap V3 TWAP (`observe()`) which is manipulation-resistant
- Compare against Chainlink if available — if DEX price differs by more than 5%, flag it

---

## Summary: The Full Waterfall

```
Transaction comes in (non-swap)
         │
         ▼
   Is it a stablecoin? ──── YES ──→ Price = $1.00 ✓
         │ NO
         ▼
   Check local price DB ─── HIT ──→ Return cached price ✓
         │ MISS
         ▼
   Derivative token? ────── YES ──→ Compute from base token price ✓
   (wstETH, rETH, etc.)
         │ NO
         ▼
   Has DEX pool? ────────── YES ──→ Read pool at historical block ✓
   (Uni V2/V3, Chainlink)          Cache result in DB
         │ NO / FAIL
         ▼
   Queue for background ──────────→ DefiLlama historical fetch
   Mark as "pending"                Cache result in DB
         │ FAIL                     Update transaction
         ▼
   CoinGecko fallback ───────────→ Same pattern
         │ FAIL
         ▼
   Mark as "price unavailable"
```

**Hot path (Layers 0-3):** Synchronous, sub-100ms, no external API dependency
**Cold path (Layers 4-5):** Async, seconds to minutes, rate-limited, but results cached permanently

Over time, the cold path gets hit less and less as your local database grows. After a few weeks of operation, you're essentially running a self-sufficient price oracle.
