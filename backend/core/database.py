import os
from typing import Any, Optional
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()


class DatabaseClient:
    def __init__(self) -> None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError("Supabase URL/KEY missing in .env")

        self.supabase: Client = create_client(url, key)

    def save_batch(self, parsed_txs: dict[str, Any]):
        """
        Takes the Parsed output and inserts it into 3 tables
        1. Tokens
        2. Transactions
        3. Token Transfers
        """
        if not parsed_txs:
            return

        print(f"[DB] Preparing to save {len(parsed_txs)} transactions...")

        # Step 0: Upsert Wallet
        # We must ensure the user exists before we add their histroy
        first_tx = next(iter(parsed_txs.values()))
        wallet_address = first_tx["wallet_address"]

        try:
            self.supabase.table("wallets").upsert(
                {"address": wallet_address, "label": "Imported Wallet"},
                on_conflict="address",
            ).execute()
            print(f"[DB] Wallet {wallet_address[:6]}... synced.")
        except Exception as e:
            print(f"[DB] Failed to create wallet: {e}")
            return

        # We need to convert the nested dictionary into flat lists for SQL

        tx_rows = []
        transfer_rows = []
        tokens_seen = {}  # mapping (address -> token_info)

        # We need a clean list of hashes
        tx_hashes = []

        for tx_hash, tx in parsed_txs.items():
            tx_hashes.append(tx_hash)

            tx_rows.append(
                {
                    "tx_hash": tx["tx_hash"],
                    "chain_id": tx["chain_id"],
                    "wallet_address": tx["wallet_address"],
                    "block_number": tx["block_number"],
                    "timestamp": tx["timestamp"],
                    "from_address": tx["from_address"],
                    "to_address": tx["to_address"],
                    "gas_used": tx["gas_used"],
                    "gas_price": tx["gas_price"],
                    "gas_cost_usd": 0,  # We'll calculate this in Week-3 Target
                    "category": "uncategorized",
                }
            )

            # Prepare Transfer Rows and Collect Tokens
            for transfer in tx["transfers"]:
                # Add to transfers list
                transfer_rows.append(
                    {
                        "tx_hash": tx_hash,
                        "chain_id": tx["chain_id"],
                        "token_address": transfer["token_address"],
                        "token_symbol": transfer["token_symbol"],
                        "amount_raw": transfer["amount_raw"],
                        "amount_decimal": transfer["amount_decimal"],
                        "direction": transfer["direction"],
                    }
                )

                if transfer["token_address"] == "NATIVE":
                    tokens_seen["NATIVE"] = {
                        "contract_address": "NATIVE",
                        "chain_id": tx["chain_id"],
                        "symbol": "ETH",  # Hardcore for Mainnet (Future: it would be dynamic)
                        "name": "Native Ether",
                        "decimals": 18,
                    }

                else:
                    tokens_seen[transfer["token_address"]] = {
                        "contract_address": transfer["token_address"],
                        "chain_id": tx["chain_id"],
                        "symbol": transfer["token_symbol"],
                        "name": transfer["token_symbol"],
                        "decimals": 18,
                    }

        # -- Batch Execution --
        try:
            # Step A: Upsert Tokens (Must exist before transfers reference them)
            if tokens_seen:
                token_data = list(tokens_seen.values())
                self.supabase.table("tokens").upsert(
                    token_data, on_conflict="contract_address"
                ).execute()
                print(f"[DB] Upserted {len(token_data)} tokens.")

            # Step B: Upsert Transactions
            # We use chunks of 1000 to avoid request size limits
            self._batch_upsert("transactions", tx_rows)

            # Step C: Upsert Transfers
            if transfer_rows:
                # 1. Delete existing transfers for these transactions
                # This prevents duplicates if run the script twice.
                # We do this in chunks to avoid URL length limits
                chunk_size = 200
                for i in range(0, len(tx_hashes), chunk_size):
                    batch_hashes = tx_hashes[i : i + chunk_size]
                    self.supabase.table("token_transfers").delete().in_(
                        "tx_hash", batch_hashes
                    ).execute()
                self._batch_upsert("token_transfers", transfer_rows)

            print(f"[DB] Batch Complete for {wallet_address}")

        except Exception as e:
            print(f"[DB] Database Error: {e}")

    def _batch_upsert(self, table: str, data: list[dict]):
        """
        Helper to break big lists into chunks of 1000.
        """
        if not data:
            return
        chunk_size = 1000
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            self.supabase.table(table).upsert(chunk).execute()
        print(f"[DB] Upserted {len(data)} rows to '{table}'")

    # ---- Wallet Metadata (24h Cooldown & Incremental Sync) ----

    def get_wallet_info(self, address: str) -> Optional[dict]:
        """
        Returns wallet record including last_fetched_at and last block numbers.
        """
        try:
            resp = (
                self.supabase.table("wallets")
                .select("*")
                .eq("address", address.lower())
                .execute()
            )
            if resp.data:
                return resp.data[0]
        except Exception as e:
            print(f"[DB] Failed to get wallet info: {e}")
        return None

    def should_refetch(self, address: str) -> bool:
        """
        Returns True if the wallet has never been fetched or was fetched >24h ago.
        """
        from datetime import datetime, timezone, timedelta

        wallet = self.get_wallet_info(address.lower())
        if not wallet:
            return True

        last_fetched = wallet.get("last_fetched_at")
        if not last_fetched:
            return True

        try:
            # Parse ISO timestamp from Supabase
            clean_ts = str(last_fetched).replace("T", " ").split("+")[0].split(".")[0]
            dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - dt) > timedelta(hours=24)
        except Exception:
            return True

    def get_last_blocks(self, address: str) -> dict:
        """
        Returns the last fetched block numbers per category.
        """
        wallet = self.get_wallet_info(address.lower())
        if not wallet:
            return {"normal": 0, "internal": 0, "erc20": 0}

        return {
            "normal": wallet.get("last_block_normal") or 0,
            "internal": wallet.get("last_block_internal") or 0,
            "erc20": wallet.get("last_block_erc20") or 0,
        }

    def update_wallet_fetch_metadata(self, address: str, blocks: dict):
        """
        Updates the wallet's last_fetched_at timestamp and block numbers after a successful sync.
        """
        from datetime import datetime, timezone

        try:
            update_data = {
                "address": address.lower(),
                "last_fetched_at": datetime.now(timezone.utc).isoformat(),
            }

            # Only update block numbers that are greater than 0 (i.e., new data was found)
            if blocks.get("normal", 0) > 0:
                update_data["last_block_normal"] = blocks["normal"]
            if blocks.get("internal", 0) > 0:
                update_data["last_block_internal"] = blocks["internal"]
            if blocks.get("erc20", 0) > 0:
                update_data["last_block_erc20"] = blocks["erc20"]

            self.supabase.table("wallets").upsert(
                update_data, on_conflict="address"
            ).execute()
            print(f"[DB] Wallet metadata updated for {address[:8]}...")
        except Exception as e:
            print(f"[DB] Failed to update wallet metadata: {e}")


db = DatabaseClient()
