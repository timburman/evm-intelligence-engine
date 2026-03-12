import httpx
import asyncio
from datetime import datetime, timezone
from core.database import db
import yfinance as yf

# Map our internal coin IDs to Yahoo Finance tickers
TICKER_MAP = {
    "ethereum": "ETH-USD",
    "polygon-ecosystem-token": "MATIC-USD",  
    "binancecoin": "BNB-USD",
}


async def backfill_coin(coin_id: str):
    print(f"[{coin_id}] Fetching entire price history from CoinGecko...")

    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": "max", "interval": "daily"}

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params, timeout=30.0)
            if resp.status_code != 200:
                print(f"[coin_id] Failed to fetch. Status: {resp.status_code}")
                return

            data = resp.json()
            prices = data.get("prices", [])

            if not prices:
                print(f"[{coin_id}] No price data returned.")
                return

            print(
                f"[{coin_id}] Downloaded {len(prices)} days of data. Preparing DB insert..."
            )

            db_rows = []
            for item in prices:
                # CoinGecko returns timestamp in milliseconds
                timestamp_ms = item[0]
                price_usd = item[1]

                # Convert to YYYY-MM-DD
                dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                date_str = dt.strftime("%Y-%m-%d")

                db_rows.append(
                    {
                        "coin_id": coin_id,
                        "date": date_str,
                        "price_usd": round(price_usd, 6),
                        "source": "coingecko_backfill",
                    }
                )

            # Batch Upsert to Supabase in chunks of 1000
            print(f"[{coin_id}] Saving to Supabase...")
            for i in range(0, len(db_rows), 1000):
                chunk = db_rows[i : i + 1000]
                db.supabase.table("daily_prices").upsert(chunk).execute()

            print(f"[{coin_id}] Successfuly backfilled {len(db_rows)} days of history.")

        except Exception as e:
            print(f"[{coin_id}] Error: {e}")


async def main():
    print("-- Backfilling --")
    for coin in MAJOR_COINS:
        await backfill_coin(coin)

        print("Sleep 15s for rate limits....")

        await asyncio.sleep(15)

    print("-- Backfilling completed --")


if __name__ == "__main__":
    asyncio.run(main())
