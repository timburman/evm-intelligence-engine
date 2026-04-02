import asyncio
from typing import List, Dict, Any, cast
from backend.core.database import db
from backend.core.historical_pricer import historical_pricer
from backend.core.token_registry import token_registry
from backend.core.gecko_terminal import gt_client


class PricerEngine:
    def __init__(self):
        self.chain_coin_map = {
            "1": "ethereum",
            "137": "polygon-ecosystem-token",
            "42161": "ethereum",
            "56": "binancecoin",
        }

    async def update_token_transfers(self):
        print("\n[ENGINE] 🔍 Scanning for unpriced token transfers...")

        try:
            response = (
                db.supabase.table("token_transfers")
                .select("id, tx_hash, token_address, amount_decimal, direction")
                .is_("price_at_transaction", "null")
                .limit(1000)
                .execute()
            )

            transfers = cast(List[Dict[str, Any]], response.data)
            if not transfers:
                print("[ENGINE] ✅ All token transfers are priced!")
                return

            print(f"[ENGINE] ⚡ Processing {len(transfers)} token transfers...")

            # 1. Grab timestamps in chunks
            tx_hashes = list(set([t["tx_hash"] for t in transfers]))
            tx_map = {}
            for i in range(0, len(tx_hashes), 50):
                chunk = tx_hashes[i : i + 50]
                tx_resp = (
                    db.supabase.table("transactions")
                    .select("tx_hash, timestamp, chain_id")
                    .in_("tx_hash", chunk)
                    .execute()
                )
                for tx in tx_resp.data:
                    tx_map[tx["tx_hash"]] = tx

            unique_requests = set()
            transfer_context = []

            # 2. Resolve Tokens
            from datetime import datetime, timezone

            for t in transfers:
                tx_data = tx_map.get(t["tx_hash"])
                if not tx_data:
                    continue

                chain_id = str(tx_data["chain_id"])
                token_addr = str(t["token_address"])
                
                try:
                    # Clean the ISO timestamp and convert to integer Unix timestamp
                    clean_ts = str(tx_data["timestamp"]).replace("T", " ").split("+")[0].split(".")[0]
                    dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    timestamp = int(dt.timestamp())
                except Exception as e:
                    print(f"[ENGINE] ❌ Failed to parse timestamp {tx_data['timestamp']}: {e}")
                    continue

                # Round to nearest 5 minutes (300 seconds) for API compression
                rounded_timestamp = timestamp - (timestamp % 300)

                if token_addr == "NATIVE":
                    coin_id = self.chain_coin_map.get(chain_id, "ethereum")
                else:
                    coin_id = await token_registry.resolve_token(chain_id, token_addr)

                if not coin_id:
                    # Token failed registry validation (spam/unknown) — mark as unfetchable
                    transfer_context.append((t, None, rounded_timestamp))
                    continue

                unique_requests.add((chain_id, token_addr, coin_id, rounded_timestamp))
                transfer_context.append((t, coin_id, rounded_timestamp))

            print(
                f"[ENGINE] 🧠 Compressed to {len(unique_requests)} unique price queries."
            )

            # 3. The Waterfall Fetcher (DefiLlama -> GeckoTerminal)
            sem = asyncio.Semaphore(
                10
            )  # Bumped up to 10 since we handle timeouts better
            price_map = {}

            async def waterfall_fetch(chain_id, token_addr, coin, ts):
                async with sem:
                    # 💧 LAYER 1: DefiLlama (Fast, but strict)
                    price = await historical_pricer.get_historical_price(coin, ts)

                    # 💧 LAYER 2: GeckoTerminal OHLCV Fallback
                    if price <= 0 and token_addr != "NATIVE":
                        print(
                            f"[GT] 🔄 DefiLlama missed {coin}. Falling back to GeckoTerminal..."
                        )
                        pool_addr = await gt_client.get_top_pool(chain_id, token_addr)
                        if pool_addr:
                            price = await gt_client.get_historical_candle(
                                chain_id, pool_addr, ts
                            )

                    # 💧 LAYER 3: Database Daily Candle Fallback
                    if price <= 0:
                        price = await historical_pricer.get_database_daily_fallback(coin, ts)

                    return (coin, ts, price)

            # Fire the waterfall
            tasks = [
                waterfall_fetch(c_id, t_addr, coin, ts)
                for c_id, t_addr, coin, ts in unique_requests
            ]
            results = await asyncio.gather(*tasks)

            for coin, ts, price in results:
                price_map[(coin, ts)] = price

            # 4. Map Prices Back to Transfers
            updates_success = []
            updates_failed = []
            for t, coin_id, ts in transfer_context:
                if coin_id is None:
                    # Spam token — mark as unfetchable
                    updates_failed.append({
                        "id": t["id"],
                        "price_at_transaction": -1.0,
                        "value_usd": 0.0,
                    })
                    continue

                price = price_map.get((coin_id, ts), -1.0)

                if price >= 0:
                    amount = float(t["amount_decimal"])
                    updates_success.append(
                        {
                            "id": t["id"],
                            "price_at_transaction": price,
                            "value_usd": round(amount * price, 2),
                        }
                    )
                else:
                    # All layers failed — mark as unfetchable so we don't retry forever
                    updates_failed.append({
                        "id": t["id"],
                        "price_at_transaction": -1.0,
                        "value_usd": 0.0,
                    })

            # 5. Batch Update Supabase
            all_updates = updates_success + updates_failed
            if all_updates:
                print(
                    f"[ENGINE] 💾 Saving {len(updates_success)} priced + {len(updates_failed)} unfetchable to DB..."
                )
                for i in range(0, len(all_updates), 500):
                    db.supabase.table("token_transfers").upsert(
                        all_updates[i : i + 500]
                    ).execute()
                print("[ENGINE] ✅ Token transfer pricing batch complete!")

        except Exception as e:
            print(f"[ENGINE] ❌ Error updating transfer prices: {e}")


pricer_engine = PricerEngine()
