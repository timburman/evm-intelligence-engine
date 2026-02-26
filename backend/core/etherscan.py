import httpx
import json
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()
ETHERSCAN_KEY = os.getenv("ETHERSCAN_API_KEY")
BASE_URL = "https://api.etherscan.io/v2/api"

os.makedirs("data/raw_txs", exist_ok=True)


async def fetch_all_txs(address: str, chain_id: str = "1") -> dict:
    """
    Fetches the full wallet history using Etherscan API,
    Chain_id default "1", i.e Ethereum
    """
    filename = f"data/raw_txs/{address}_{chain_id}.json"

    if os.path.exists(filename):
        print(f"Loading {address} from cache")
        with open(filename, "r") as f:
            cache = json.load(f)
    else:
        print(f"New wallet detected: {address}")
        cache = {
            "metadata": {
                "chain_id": chain_id,
                "address": address,
                "last_blocks": {"normal": 0, "internal": 0, "erc20": 0},
            },
            "normal": [],
            "internal": [],
            "erc20": [],
        }

    async with httpx.AsyncClient() as client:
        # Sync Normal transactions
        last_normal = cache["metadata"]["last_blocks"]["normal"]
        new_normal, last_block_n = await _sync_category(
            client, "txlist", address, chain_id, last_normal
        )
        if new_normal:
            cache["normal"].extend(new_normal)
            cache["metadata"]["last_blocks"]["normal"] = last_block_n

        # Sync Internal Transactions
        last_internal = cache["metadata"]["last_blocks"]["internal"]
        new_internal, last_block_i = await _sync_category(
            client, "txlistinternal", address, chain_id, last_internal
        )
        if new_internal:
            cache["internal"].extend(new_internal)
            cache["metadata"]["last_blocks"]["internal"] = last_block_i

        # Sync ERC20 Transactions
        last_erc20 = cache["metadata"]["last_blocks"]["erc20"]
        new_erc20, last_block_e = await _sync_category(
            client, "tokentx", address, chain_id, last_erc20
        )
        if new_erc20:
            cache["erc20"].extend(new_erc20)
            cache["metadata"]["last_blocks"]["erc20"] = last_block_e

    with open(filename, "w") as f:
        json.dump(cache, f, indent=2)

    summary = f"Synced: +{len(new_normal)} Normal, +{len(new_internal)} Internal, +{len(new_erc20)} ERC20"
    print(f"[Success] {summary}")

    return cache


async def _sync_category(
    client: httpx.AsyncClient, action: str, address: str, chain_id: str, last_block: int
) -> tuple[list[dict], int]:
    """
    Fetches only new starting from last_block + 1.
    Returns: tupple(List of new txns, new highest block number)
    """
    start_block = int(last_block) + 1 if int(last_block) != 0 else int(last_block)

    params = {
        "chainid": chain_id,
        "module": "account",
        "action": action,
        "address": address,
        "startblock": start_block,
        "endblock": 9999999999,
        "sort": "asc",
        "apikey": ETHERSCAN_KEY,
    }

    try:
        resp = await client.get(BASE_URL, params=params)
        data = resp.json()

        if data["status"] == "1":
            new_txs = data["result"]
            if new_txs:
                new_max_block = max(int(tx["blockNumber"]) for tx in new_txs)
                return new_txs, new_max_block
            return [], last_block
        elif data["message"] == "No transactions found":
            return [], last_block
        else:
            error_reason = data.get("result", "Unknown Error")
            print(f"[API ERROR] {action}: {data['message']} -> {error_reason}")

            if "API Key" in str(error_reason):
                print("[CRITICAL]: Etherscan API KEY is missing or invalid")
            return [], last_block
    except Exception as e:
        print(f"[ERROR] Connection failed in {action}: {e}")
        return [], last_block
