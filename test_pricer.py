import asyncio
from backend.core.historical_pricer import historical_pricer

async def main():
    print("--- 🧪 Testing Precision Router Time Machine ---")

    # Our test suite: (coin_id, timestamp, expected_behavior)
    tests = [
        # 1. Stablecoin (Should bypass everything and return $1.0)
        ("usd-coin", "2023-01-01 12:00:00", "Layer 0 (Stablecoin Bypass)"),
        
        # 2. Pre-2020 Major Coin (Should hit your Yahoo Finance Supabase DB)
        ("ethereum", "2018-12-15 08:30:00", "Layer 1.5 (DB Backfill)"),
        
        # 3. Pre-2020 Random Token (Should instantly fast-fail, no API call)
        ("pepe", "2018-12-15 08:30:00", "Layer 1.7 (Fast-Fail Pre-DEX era)"),
        
        # 4. Post-2020 Major Coin (Should hit DefiLlama for exact minute precision)
        ("ethereum", "2023-05-15 14:30:00", "Layer 2 (DefiLlama Exact Time)"),
        
        # 5. Post-2020 Random Token (Should hit DefiLlama)
        ("uniswap", "2021-09-01 10:00:00", "Layer 2 (DefiLlama Exact Time)")
    ]

    for coin, ts, expected in tests:
        print(f"\n▶️ Querying: {coin} at {ts}")
        print(f"   Expecting: {expected}")
        price = await historical_pricer.get_historical_price(coin, ts)
        print(f"   ✅ Result: ${price}")

    print("\n--- 🏁 Test Suite Complete ---")

if __name__ == "__main__":
    asyncio.run(main())
