"""
Database Migration Script — Run this once to add new columns to the wallets table.

Usage: python -m backend.scripts.migrate_wallets
Or:    cd backend && python scripts/migrate_wallets.py
"""

import os
import sys

# Add backend to path if running directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import db


def check_and_guide():
    """
    Checks if the migration columns exist and guides the user if not.
    """
    print("[MIGRATION] Checking wallets table schema...")

    try:
        resp = db.supabase.table("wallets").select("*").limit(1).execute()
        if resp.data:
            cols = list(resp.data[0].keys())
            print(f"[MIGRATION] Current columns: {cols}")
        else:
            # Insert a dummy row to check schema
            print("[MIGRATION] Wallets table is empty, checking via insert test...")
    except Exception as e:
        print(f"[MIGRATION] Error: {e}")

    # Check for new columns
    needed_cols = ["last_fetched_at", "last_block_normal", "last_block_internal", "last_block_erc20"]
    missing = []

    for col in needed_cols:
        try:
            db.supabase.table("wallets").select(col).limit(1).execute()
            print(f"  ✅ Column '{col}' exists")
        except Exception:
            print(f"  ❌ Column '{col}' is MISSING")
            missing.append(col)

    if not missing:
        print("\n[MIGRATION] ✅ All columns exist! No migration needed.")
        return True

    print(f"\n[MIGRATION] ⚠️ Missing {len(missing)} column(s).")
    print("[MIGRATION] Please run the following SQL in your Supabase Dashboard SQL Editor:")
    print(f"[MIGRATION] Dashboard URL: https://supabase.com/dashboard/project/atozxukgfxkghdztkkzo/sql/new")
    print()
    print("--- COPY BELOW ---")
    print()
    print("ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_fetched_at TIMESTAMPTZ;")
    print("ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_block_normal BIGINT DEFAULT 0;")
    print("ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_block_internal BIGINT DEFAULT 0;")
    print("ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_block_erc20 BIGINT DEFAULT 0;")
    print()
    print("--- END COPY ---")
    print()
    print("[MIGRATION] After running the SQL, re-run this script to verify.")
    return False


if __name__ == "__main__":
    result = check_and_guide()
    sys.exit(0 if result else 1)
