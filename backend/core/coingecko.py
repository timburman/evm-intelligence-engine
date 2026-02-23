import httpx
import json
import os
import asyncio
import time

COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
CACHE_DIR = "data/prices"
CACHE_FILE = f"{CACHE_DIR}/token_prices.json"
MAX_ADDRESSES_PER_CALL = 10
RATE_LIMIT_DELAY = 2.5


os.makedirs(CACHE_DIR, exist_ok=True)
