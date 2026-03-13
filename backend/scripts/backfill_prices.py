import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.database import db
import yfinance as yf

# Map our internal coin IDs to Yahoo Finance tickers
TICKER_MAP = {
    "ethereum": "ETH-USD",
    "polygon-ecosystem-token": "MATIC-USD",
    "binancecoin": "BNB-USD",
}


def backfill_coin(coin_id: str, ticker: str):
    print(f"[{coin_id}] Fetching entire price history from Yahoo Finance ({ticker})...")

    try:
        # Fetch max history from Yahoo
        asset = yf.Ticker(ticker)
        hist = asset.history(period="max")

        if hist.empty:
            print(f"[{coin_id}] No data found for {ticker}")
            return

        print(
            f"[{coin_id}] Downloaded {len(hist)} days of data. Preparing DB insertion"
        )

        db_rows = []
        # yfinance returns a Pandas DataFrame where the Index is the Date
        for date, row in hist.iterrows():
            # Date is a pandas Timestamp, convert to YYY-MM-DD
            date_str = date.strftime("%Y-%m-%d")
            price_usd = float(row["Close"])

            db_rows.append(
                {
                    "coin_id": coin_id,
                    "date": date_str,
                    "price_usd": round(price_usd, 6),
                    "source": "yfinance_backfill",
                }
            )

        # Batch Upsert to Supabase
        print(f"[{coin_id}] Saving to Supabase...")
        for i in range(0, len(db_rows), 1000):
            chunk = db_rows[i : i + 1000]
            db.supabase.table("daily_prices").upsert(chunk).execute()

        print(
            f"[{coin_id}] Successfully backfilled {len(db_rows)} days of history!"
        )
    except Exception as e:
        print(f"[{coin_id}] Error: {e}")


def main():
    print("-- Backfill Init --")
    for coin_id, ticker in TICKER_MAP.items():
        backfill_coin(coin_id, ticker)

    print("-- Backfill Completed")


if __name__ == "__main__":
    main()
