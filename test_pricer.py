import asyncio
from backend.core.historical_pricer import historical_pricer

async def main():
    print("--- 🌊 Testing Waterfall Pricer ---")
    timestamp = "2021-05-01 12:00:00"
    
    # Let's purposefully sabotage CoinGecko by giving it a fake date format it hates, 
    # but that our parser won't crash on, OR we can just let it run normally. 
    # To see it work, just run a valid query first.
    
    price = await historical_pricer.get_historical_price("ethereum", timestamp)
    print(f"✅ Final Output: ${price}")

if __name__ == "__main__":
    asyncio.run(main())
