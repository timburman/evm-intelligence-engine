import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Any


class TransactionParser:
    """
    The Brains. Converts raw Etherscan logs into SQL-ready rows.
    """

    def parse_file(self, file_path: str) -> dict[str, Any]:
        """
        Reads a raw JSON file and returns a structured object
        ready for database insertion.
        """
        if not os.path.exists(file_path):
            print(f"[PATH] File not found: {file_path}")
            return {}

        with open(file_path, "r") as f:
            raw_data = json.load(f)

        chain_id = raw_data.get("metadata", {}).get("chain_id", "1")
        wallet_address = raw_data.get("metadata", {}).get("address", "").lower()

        # Map tx_has -> Transaction Object
        parsed_txs = {}

        # 1. Process Normal Transactions (The Parent Rows)
        for tx in raw_data.get("normal", []):
            tx_hash = tx.get("hash")

            parsed_txs[tx_hash] = {
                "tx_hash": tx_hash,
                "chain_id": chain_id,
                "wallet_address": wallet_address,
                "block_number": int(tx.get("blockNumber", 0)),
                "timestamp": self._parse_timestamp(tx.get("timeStamp")),
                "from_address": tx.get("from", "").lower(),
                "to_address": tx.get("to", "").lower(),
                "gas_used": int(tx.get("gasUsed", 0)),
                "gas_price": int(tx.get("gasPrice", 0)),
                # Calculate Gas Gost in Native Token (ETH/POL/BNB)
                "gas_cost_native": self._calculate_gas_cost(tx),
                "transfers": [],
            }

            # Add the primary ETH transfer if Value > 0
            eth_value = int(tx.get("value", 0))
            if eth_value > 0:
                direction = (
                    "OUT" if tx.get("from", "").lower() == wallet_address else "IN"
                )
                parsed_txs[tx_hash]["transfers"].append(
                    {
                        "token_address": "NATIVE",
                        "token_symbol": "ETH",  # Hadcoded for Now, Need to be dynamic later
                        "amount_raw": eth_value,
                        "amount_decimal": self._to_decimal(eth_value, 18),
                        "direction": direction,
                    }
                )

        # 2. PROCESS Internal Transactions (Smart Contract Eth Sends)
        # These often don't have a unique hash, they belong to a parent Normal Tx.
        for tx in raw_data.get("internal", []):
            parent_hash = tx.get("hash")

            # If we missed the parent, skip for now
            if parent_hash not in parsed_txs:
                continue

            eth_value = int(tx.get("value", 0))
            if eth_value > 0:
                # Did I recieve this interal transfer?
                is_receiver = tx.get("to", "").lower() == wallet_address
                is_sender = tx.get("from", "").lower() == wallet_address

                if is_receiver or is_sender:
                    direction = "IN" if is_receiver else "OUT"
                    parsed_txs[parent_hash]["transfers"].append(
                        {
                            "token_address": "NATIVE",
                            "token_symbol": "ETH",
                            "amount_raw": eth_value,
                            "amount_decimal": self._to_decimal(eth_value, 18),
                            "direction": direction,
                            "type": "internal_transfer",
                        }
                    )

        for tx in raw_data.get("erc20", []):
            parent_hash = tx.get("hash")

            if parent_hash not in parsed_txs:
                # In an Indexer, we would create a "Stub" transaction here.
                # For Week 2, we skip orphans.
                continue

            token_value = int(tx.get("value", 0))
            decimals = int(tx.get("tokenDecimal", 0))

            direction = "OUT" if tx.get("from", "").lower() == wallet_address else "IN"

            parsed_txs[parent_hash]["transfers"].append(
                {
                    "token_address": tx.get("contractAddress", "").lower(),
                    "token_symbol": tx.get("tokenSymbol", "UNKNOWN"),
                    "amount_raw": token_value,
                    "amount_decimal": self._to_decimal(token_value, decimals),
                    "direction": direction,
                }
            )

        return parsed_txs

    # Helper Functions

    def _to_decimal(self, raw_value: int, decimals: int) -> float:
        """
        Converts raw integer (Wei) to float
        """
        if raw_value == 0:
            return 0.0
        return float(Decimal(raw_value) / Decimal(10**decimals))

    def _parse_timestamp(self, ts: str) -> str:
        """
        Converts Unix string to SQL Timestamp.
        """
        return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")

    def _calculate_gas_cost(self, tx: dict) -> float:
        """
        Calculates Gas Used * Gas Price in ETH.
        """
        used = int(tx.get("gasUsed", 0))
        price = int(tx.get("gasPrice", 0))
        return self._to_decimal(used * price, 18)


parser = TransactionParser()
