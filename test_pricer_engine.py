# Now we can safely import our deeply nested modules
from backend.core.pricer_engine import pricer_engine
import asyncio


async def main():
    print("--- 🚀 Starting Pricing Engine Test ---")

    # Run the token transfer pricer (with the token_registry shield attached)
    await pricer_engine.update_token_transfers()

    print("--- 🏁 Finished ---")


if __name__ == "__main__":
    asyncio.run(main())
