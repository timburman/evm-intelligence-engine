from typing import Any, cast
from backend.core.historical_pricer import historical_pricer
from backend.core.database import db


class PricerEngine:
    """
    Scans the database for unpriced data, calculates USD values,
    and writes them back.
    """

    def __init__(self) -> None:
        # Maps chain_id to the native token's CoinGecko ID
        self.chain_coin_map = {
            "1": "ethereum",
            "137": "polygon-ecosystem-token",
            "42161": "ethereum",
            "56": "binance-smart-chain",
        }

    async def update_gas_costs(self):
        """
        Finds Transactions with 0 gas cost and calculates their historical USD value.
        """
        print("[ENGINE] Scanning for unpriced gas costs")

        try:
            # 1. Fetch up to 1000 upriced transactions
            # We select only the columns we need to save memory
            response = (
                db.supabase.table("transactions")
                .select("tx_hash, chain_id, timestamp, gas_used, gas_price")
                .eq("gas_cost_usd", 0)
                .limit(1000)
                .execute()
            )

            txs = cast(list[dict[str, Any]], response.data)

            if not txs:
                print("[ENGINE] All gas costs are up to date!")
                return

            print(f"[ENGINE] Pricing {len(txs)} transactions...")
            updates = []

            # 2. Process each transaction
            for tx in txs:
                coin_id = self.chain_coin_map.get(tx["chain_id"], "ethereum")

                # Use historical_pricer
                price = await historical_pricer.get_historical_price(
                    coin_id, tx["timestamp"]
                )

                if price > 0:
                    # Math: (gas_used * gas_price) gives us Wei. Divide by 10^18 to get eth.. yay!
                    gas_used = int(tx["gas_used"])
                    gas_price = int(tx["gas_price"])

                    gas_native = (gas_used * gas_price) / (10**18)
                    gas_used = round(gas_native * price, 2)

                    # Prepare the row for updating..
                    # Upsert requires the Primary Keys (tx_hash, chain_id) to know which row to update.
                    updates.append(
                        {
                            "tx_hash": tx["tx_hash"],
                            "chain_id": tx["chain_id"],
                            "gas_cost_usd": gas_used,
                        }
                    )

            # 3. batch update supabase
            if updates:
                print(
                    f"[ENGINE] Saving {len(updates)} calculated gas costs to database"
                )
                # We chunk it into 500s just to be safe
                for i in range(0, len(updates), 500):
                    chunk = updates[i : i + 500]
                    db.supabase.table("transactions").upsert(chunk).execute()

                print("[ENGINE] Gas pricing batch complete!")

        except Exception as e:
            print(f"[ENGINE] Error updating gas costs: {e}")


pricer_engine = PricerEngine()
