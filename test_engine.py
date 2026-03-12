import asyncio
from backend.core.pricer_engine import pricer_engine

async def main():
    print("--- 🚀 Starting Pricing Engine ---")
    await pricer_engine.update_gas_costs()
    print("--- 🏁 Finished ---")

if __name__ == "__main__":
    asyncio.run(main())
