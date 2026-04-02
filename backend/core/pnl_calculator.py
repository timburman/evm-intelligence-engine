"""
PnL Calculator — Realized & Unrealized Profit/Loss using FIFO.

Uses the `direction` field from token_transfers:
  - "IN"  → Acquisition (buy) — add to FIFO cost basis queue
  - "OUT" → Disposal (sell) — pop from FIFO queue, compute gain/loss

Unrealized PnL uses DeFiLlama's /prices/current/ endpoint for live prices.
"""

import httpx
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

from backend.core.database import db


DEFILLAMA_CURRENT_URL = "https://coins.llama.fi/prices/current/{query_ids}"

# Map chain_id to DeFiLlama chain prefix
CHAIN_PREFIX_MAP = {
    "1": "ethereum",
    "137": "polygon",
    "56": "bsc",
    "42161": "arbitrum",
}


@dataclass
class TokenPnL:
    """PnL breakdown for a single token."""
    token_address: str = ""
    token_symbol: str = "UNKNOWN"
    chain_id: str = "1"

    # Quantities
    total_in: float = 0.0       # Total amount received (bought/received)
    total_out: float = 0.0      # Total amount sent (sold/sent)
    current_holding: float = 0.0  # in - out

    # USD Values
    total_cost_basis: float = 0.0    # Total USD spent acquiring
    total_proceeds: float = 0.0      # Total USD received selling

    # PnL
    realized_pnl: float = 0.0       # proceeds - cost of sold lots (FIFO)
    unrealized_pnl: float = 0.0     # (current_price - avg_cost) × remaining

    # Prices
    avg_buy_price: float = 0.0      # cost_basis / total_in
    current_price: float = 0.0      # Live price from DeFiLlama

    # FIFO queue for remaining cost lots
    cost_lots: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "token_address": self.token_address,
            "token_symbol": self.token_symbol,
            "chain_id": self.chain_id,
            "total_in": round(self.total_in, 6),
            "total_out": round(self.total_out, 6),
            "current_holding": round(self.current_holding, 6),
            "total_cost_basis": round(self.total_cost_basis, 2),
            "total_proceeds": round(self.total_proceeds, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "avg_buy_price": round(self.avg_buy_price, 6),
            "current_price": round(self.current_price, 6),
        }


class PnLCalculator:
    """
    Calculates realized and unrealized PnL from token_transfers data.
    Uses FIFO (First-In-First-Out) cost basis method.
    """

    async def calculate_pnl(self, wallet_address: str, chain_id: str = "1") -> dict:
        """
        Main entry point. Fetches all priced transfers for a wallet,
        computes FIFO PnL per token, and returns the breakdown.
        """
        wallet_address = wallet_address.lower()

        # 1. Fetch all priced transfers for this wallet (join with transactions for ordering)
        transfers = self._fetch_transfers(wallet_address, chain_id)
        if not transfers:
            return {
                "wallet": wallet_address,
                "chain_id": chain_id,
                "tokens": [],
                "summary": {
                    "total_realized_pnl": 0.0,
                    "total_unrealized_pnl": 0.0,
                    "total_pnl": 0.0,
                },
            }

        # 2. Group transfers by token
        token_groups = self._group_by_token(transfers)

        # 3. Calculate FIFO PnL per token
        pnl_results = {}
        for token_key, token_transfers in token_groups.items():
            pnl = self._calculate_fifo_pnl(token_key, token_transfers, chain_id)
            pnl_results[token_key] = pnl

        # 4. Fetch current prices for unrealized PnL
        await self._fetch_current_prices(pnl_results, chain_id)

        # 5. Calculate unrealized PnL
        for token_key, pnl in pnl_results.items():
            if pnl.current_holding > 0 and pnl.current_price > 0:
                # Remaining cost basis from FIFO lots
                remaining_cost = sum(qty * price for qty, price in pnl.cost_lots)
                current_value = pnl.current_holding * pnl.current_price
                pnl.unrealized_pnl = current_value - remaining_cost

        # 6. Build response
        total_realized = sum(p.realized_pnl for p in pnl_results.values())
        total_unrealized = sum(p.unrealized_pnl for p in pnl_results.values())

        return {
            "wallet": wallet_address,
            "chain_id": chain_id,
            "tokens": [p.to_dict() for p in sorted(
                pnl_results.values(),
                key=lambda x: abs(x.realized_pnl) + abs(x.unrealized_pnl),
                reverse=True,
            )],
            "summary": {
                "total_realized_pnl": round(total_realized, 2),
                "total_unrealized_pnl": round(total_unrealized, 2),
                "total_pnl": round(total_realized + total_unrealized, 2),
            },
        }

    def _fetch_transfers(self, wallet_address: str, chain_id: str) -> list[dict]:
        """
        Fetches all priced token transfers for a wallet, ordered by timestamp.
        Only includes transfers with valid prices (price > 0).
        """
        try:
            # Get all transaction hashes for this wallet
            tx_resp = (
                db.supabase.table("transactions")
                .select("tx_hash, timestamp")
                .eq("wallet_address", wallet_address)
                .eq("chain_id", chain_id)
                .order("timestamp", desc=False)
                .execute()
            )

            if not tx_resp.data:
                return []

            tx_map = {tx["tx_hash"]: tx["timestamp"] for tx in tx_resp.data}
            tx_hashes = list(tx_map.keys())

            # Fetch transfers in chunks
            all_transfers = []
            for i in range(0, len(tx_hashes), 200):
                chunk = tx_hashes[i : i + 200]
                tf_resp = (
                    db.supabase.table("token_transfers")
                    .select("id, tx_hash, token_address, token_symbol, amount_decimal, direction, price_at_transaction, value_usd")
                    .in_("tx_hash", chunk)
                    .gt("price_at_transaction", 0)  # Only priced transfers
                    .execute()
                )
                for tf in tf_resp.data:
                    tf["timestamp"] = tx_map.get(tf["tx_hash"], "")
                    all_transfers.append(tf)

            # Sort by timestamp for correct FIFO ordering
            all_transfers.sort(key=lambda x: x["timestamp"])
            return all_transfers

        except Exception as e:
            print(f"[PNL] Error fetching transfers: {e}")
            return []

    def _group_by_token(self, transfers: list[dict]) -> dict[str, list[dict]]:
        """Groups transfers by token_address."""
        groups = defaultdict(list)
        for tf in transfers:
            groups[tf["token_address"]].append(tf)
        return dict(groups)

    def _calculate_fifo_pnl(self, token_address: str, transfers: list[dict], chain_id: str) -> TokenPnL:
        """
        Calculates realized PnL using FIFO for a single token.
        Uses the `direction` field to determine buys vs sells.
        """
        pnl = TokenPnL(
            token_address=token_address,
            token_symbol=transfers[0].get("token_symbol", "UNKNOWN") if transfers else "UNKNOWN",
            chain_id=chain_id,
        )

        # FIFO queue: deque of (quantity, price_per_unit)
        fifo_queue: deque[tuple[float, float]] = deque()

        for tf in transfers:
            amount = float(tf.get("amount_decimal", 0))
            price = float(tf.get("price_at_transaction", 0))
            direction = tf.get("direction", "").upper()

            if amount <= 0 or price <= 0:
                continue

            if direction == "IN":
                # ── BUY / RECEIVE ──
                pnl.total_in += amount
                pnl.total_cost_basis += amount * price
                fifo_queue.append((amount, price))

            elif direction == "OUT":
                # ── SELL / SEND ──
                pnl.total_out += amount
                proceeds = amount * price
                pnl.total_proceeds += proceeds

                # FIFO: dequeue cost lots to match this sell amount
                remaining_sell = amount
                cost_of_sold = 0.0

                while remaining_sell > 0 and fifo_queue:
                    lot_qty, lot_price = fifo_queue[0]

                    if lot_qty <= remaining_sell:
                        # Consume entire lot
                        cost_of_sold += lot_qty * lot_price
                        remaining_sell -= lot_qty
                        fifo_queue.popleft()
                    else:
                        # Partial lot consumption
                        cost_of_sold += remaining_sell * lot_price
                        fifo_queue[0] = (lot_qty - remaining_sell, lot_price)
                        remaining_sell = 0

                # Realized PnL for this sale = proceeds - cost basis (FIFO)
                pnl.realized_pnl += proceeds - cost_of_sold

        # Final calculations
        pnl.current_holding = pnl.total_in - pnl.total_out
        if pnl.current_holding < 0:
            pnl.current_holding = 0  # Shouldn't happen, but safety
        if pnl.total_in > 0:
            pnl.avg_buy_price = pnl.total_cost_basis / pnl.total_in

        # Store remaining FIFO lots for unrealized PnL calculation
        pnl.cost_lots = list(fifo_queue)

        return pnl

    async def _fetch_current_prices(self, pnl_results: dict[str, TokenPnL], chain_id: str):
        """
        Fetches current prices from DeFiLlama for all tokens with remaining holdings.
        Uses the /prices/current/ endpoint (free, no key needed).
        """
        chain_prefix = CHAIN_PREFIX_MAP.get(chain_id, "ethereum")

        # Build query for tokens with holdings
        tokens_to_price = []
        for token_addr, pnl in pnl_results.items():
            if pnl.current_holding > 0:
                if token_addr == "NATIVE":
                    # Use coingecko prefix for native tokens
                    from backend.core.pricer_engine import pricer_engine
                    coin_id = pricer_engine.chain_coin_map.get(chain_id, "ethereum")
                    tokens_to_price.append(("coingecko:" + coin_id, token_addr))
                elif token_addr.startswith("0x"):
                    tokens_to_price.append((f"{chain_prefix}:{token_addr}", token_addr))

        if not tokens_to_price:
            return

        # DeFiLlama accepts comma-separated query IDs
        query_ids = ",".join(q[0] for q in tokens_to_price)
        url = DEFILLAMA_CURRENT_URL.format(query_ids=query_ids)

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(url, timeout=15.0)
                if resp.status_code == 200:
                    data = resp.json().get("coins", {})
                    for query_id, token_addr in tokens_to_price:
                        price = data.get(query_id, {}).get("price", 0.0)
                        if price > 0 and token_addr in pnl_results:
                            pnl_results[token_addr].current_price = price
            except Exception as e:
                print(f"[PNL] Error fetching current prices: {e}")


pnl_calculator = PnLCalculator()
