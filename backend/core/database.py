import os
from typing import Any
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

        # We need to convert the nested dictionary into flat lists for SQL

        tx_rows = []
        transfer_rows = []
        tokens_seen = {}  # mapping (address -> token_info)

        for tx_hash, tx in parsed_txs.items():
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

                if transfer["token_address"] != "NATIVE":
                    tokens_seen[transfer["token_address"]] = {
                        "contract_address": transfer["token_address"],
                        "chain_id": tx["chain_id"],
                        "symbol": transfer["token_symbol"],
                        # Leaving name/decimal null for now
                        # We'd fetch these from RPC or Registry
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
            # Note: We delete old transfers for these TXs first to avoid duplication bugs
            # (Simple for MVP)
            # For now, just append. In production, we'll handle ID Conflicts
            self.supabase.table("token_transfers").insert(transfer_rows).execute()
            print(f"[DB] Inserted {len(transfer_rows)} transfers.")

        except Exception as e:
            print(f"[DB] Database Error: {e}")

    def _batch_upsert(self, table: str, data: list[dict]):
        """
        Helper to break big lists into chunks of 1000.
        """
        chunk_size = 1000
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            self.supabase.table(table).upsert(chunk).execute()
        print(f"[DB] Upserted {len(data)} rows to '{table}'")


db = DatabaseClient()
