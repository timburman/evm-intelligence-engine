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

    async def update_token_transfers(self):
        """
        Finds unpriced token transfers and calculates their historical USD value.
        """
        print("[ENGINE] Scanning for unpriced token transfers...")

        try:
            # 1. Fetch unpriced transfers
            response = (
                db.supabase.table("token_transfers")
                .select("id, tx_hash, token_address, amount_decimal")
                .is_("price_at_transaction", "null")
                .limit(1000)
                .execute()
            )

            transfers = cast(list[dict[str, Any]], response.data)

            if not transfers:
                print("[ENGINE] All token transfers are priced!")
                return

            print(f"[ENGINE] Pricing {len(transfers)} token transfers...")

            # 2. Grab the timestamp from transactions
            tx_hashes = list(set([t["tx_hash"] for t in transfers]))
            tx_resp = (
                db.supabase.table("transactions")
                .select("tx_hash, timestamp, chain_id")
                .in_("tx_hash", tx_hashes)
                .execute()
            )

            tx_map = {tx["tx_hash"]: tx for tx in tx_resp.data}
            updates = []

            # 3. Process each transfer
            for t in transfers:
                tx_data = tx_map.get(t["tx_hash"])
                if not tx_data:
                    continue

                # Determine what to ask the TM(Time Machine)
                if t["token_address"] == "NATIVE":
                    coin_id = self.chain_coin_map.get(
                        str(tx_data["chain_id"]), "ethereum"
                    )
                else:
                    coin_id = str(t["token_address"])

                price = await historical_pricer.get_historical_price(
                    coin_id, str(tx_data["timestamp"])
                )
                if price > 0:
                    amount = float(t["amount_decimal"])
                    value_usd = round(amount * price, 2)

                    updates.append(
                        {
                            "id": t["id"],
                            "price_at_transaction": price,
                            "value": value_usd,
                        }
                    )

            # 4. Batch Update Supabase
            if updates:
                print(f"[ENGINE] Saving {len(updates)} calculated token value to DB")
                for i in range(0, len(updates), 500):
                    chunk = updates[i : i + 500]
                    db.supabase.table("token_transfers").upsert(chunk).execute()

                print("[ENGINE] Token transfer pricing batch compelte!")
        except Exception as e:
            print(f"[ENGINE] Error updating transfer prices: {e}")


pricer_engine = PricerEngine()
