"""Read-only signed Backpack authentication probe; no RFQ acceptance or withdrawal."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from backpack_stocks.backpack import Backpack, BackpackAuth
from backpack_stocks.environment import load_environment
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy


async def main():
    load_environment(".env", ["BACKPACK_ED25519_SEED_BASE64", "BACKPACK_API_KEY", *PROXY_ENV_NAMES])
    auth = BackpackAuth(os.environ["BACKPACK_ED25519_SEED_BASE64"])
    if os.environ.get("BACKPACK_API_KEY") and auth.public_key != os.environ["BACKPACK_API_KEY"]:
        raise ValueError("Backpack public/private key mismatch")
    bp = Backpack(auth, proxy=resolve_proxy("BACKPACK"))
    try:
        account, balances = await asyncio.gather(bp.account(), bp.balances())
        report = {
            "observed_at": datetime.now(UTC).isoformat(),
            "authenticated": True,
            "account_flags": {
                k: account.get(k) for k in ("autoLend", "autoBorrowSettlements", "liquidating")
            },
            "available": {
                k: balances.get(k, {}).get("available", "0") for k in ("USDC", "USDT", "USD", "WEN.US")
            },
            "financial_mutations": 0,
        }
        target = Path("state/backpack-auth.json")
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(report, f, indent=2)
        print(json.dumps(report, indent=2))
    finally:
        await bp.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as error:
        raise SystemExit(f"Backpack read-only auth check failed ({type(error).__name__})") from None
