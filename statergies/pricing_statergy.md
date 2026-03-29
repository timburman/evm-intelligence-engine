Got it — here is the same strategy, clean and structured without emojis.

---

# FULL WATERFALL ARCHITECTURE (DEX PRICE AT TIMESTAMP)

## Goal

Get price at a Unix timestamp for:

* many transactions
* many tokens
* fast, scalable, mostly free

---

# CORE PRINCIPLES

1. Never query per transaction
2. Always batch by (chain, pool)
3. Always fetch time ranges, not points
4. Cache everything
5. APIs first, RPC last

---

# SYSTEM OVERVIEW

```
INPUT → Normalize → Group → Batch Fetch → Cache → Resolve → Output
```

---

# STEP 1: PARSE AND NORMALIZE TRANSACTIONS

For each transaction extract:

```
{
  chain,
  pool_address,
  token0,
  token1,
  timestamp
}
```

---

## Pool detection (critical)

You must identify:

```
(chain, dex, pool_address)
```

Not just token pair.

Reason:

* Same pair exists in multiple pools
* Different fee tiers (Uniswap V3)
* Different DEXs

---

# STEP 2: GROUP TRANSACTIONS

Structure:

```
Map<
  pool_id,
  [timestamps]
>
```

Example:

```
pool_A → [t1, t2, t3]
pool_B → [t4, t5]
pool_C → [t6]
```

---

# STEP 3: COMPUTE TIME RANGES PER POOL

For each pool:

```
min_ts = min(timestamps)
max_ts = max(timestamps)
```

---

## Chunk the range

Use:

```
1 day chunks (recommended)
```

Example:

```
Jan 1 → Jan 30
→ split into 30 chunks
```

---

# STEP 4: WATERFALL DATA FETCHING

For each (pool + time chunk):

---

## Layer 1 — DeFiLlama

* Query using timestamp

If success:

```
confidence = HIGH
source = defillama
```

---

## Layer 2 — GeckoTerminal

* Fetch 1-minute OHLC data

Store:

```
pool_id → [
  {timestamp, open, high, low, close}
]
```

---

### Candle handling (important)

Do not use average.

Use:

```
if near start → open
if near end   → close
else          → interpolate
```

Interpolation:

```
delta = (tx_ts - candle_start) / 60
price = open + (close - open) * delta
```

---

## Layer 3 — RPC fallback

Only if:

* API fails
* Data missing

Steps:

1. Convert timestamp → nearest block
2. Call:

```
getReserves()
```

3. Compute:

```
price = reserve1 / reserve0
```

Mark:

```
confidence = MEDIUM
```

---

# OPTIONAL (ACCURACY BOOST)

If needed:

* Fetch nearest swap log
* Compute execution price
* Override reserve price

---

# STEP 5: CACHE DESIGN

Structure:

```
cache = {
  pool_id: {
    candles: [...],
    last_updated: timestamp
  }
}
```

---

## Key format

```
pool_id = chain + dex + pool_address
```

---

## Cache rules

* Do not refetch existing ranges
* Merge new chunks into cache
* Persist to database or disk

---

# STEP 6: PRICE RESOLUTION

For each transaction:

---

## Locate candle

Binary search:

```
find candle where:
candle.timestamp <= tx_timestamp < next_candle
```

---

## Compute price

```
delta = (tx_ts - candle_start) / 60
price = open + (close - open) * delta
```

---

## Return

```
{
  price,
  source,
  confidence
}
```

---

# STEP 7: PERFORMANCE OPTIMIZATION

## Batch everything

* per pool
* per time range

---

## Parallelism

* 3–5 concurrent API calls

---

## Preload strategy

Before resolving transactions:

```
fetch all required ranges first
```

Then resolve locally.

---

## Multi-user optimization

Use global cache:

```
pool → shared across users
```

---

# ACCURACY TIERS

| Source        | Accuracy  | Speed     |
| ------------- | --------- | --------- |
| DeFiLlama     | High      | Very high |
| GeckoTerminal | Medium    | Very high |
| RPC reserves  | High      | Low       |
| Swap logs     | Very high | Very low  |

---

# FINAL WATERFALL

```
1. Try DeFiLlama
2. Else use GeckoTerminal candle
3. Else use RPC reserves
4. Optional: refine with swap logs
```

---

# EDGE CASES

* Low liquidity pools
* Missing candles
* Multiple pools per pair
* Stablecoin pairs
* Tokens with unusual mechanics

---

# FINAL INSIGHT

System should scale with:

```
number of pools × time ranges
```

Not:

```
number of transactions
```


