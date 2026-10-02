"""Inspect FLWS funding and optionally submit one bounded, non-borrowing purchase."""

import argparse
import asyncio
import json
import os
import secrets
from pathlib import Path

import httpx

from backpack_stocks.backpack import Backpack, BackpackAuth
from backpack_stocks.environment import load_environment
from backpack_stocks.proxy import resolve_proxy
from backpack_stocks.transport import Transport, UnknownOutcome

ROOT = Path(__file__).resolve().parents[1]


async def main(execute: bool):
    load_environment(
        ROOT / ".env",
        ["BACKPACK_ED25519_SEED_BASE64", "OUTBOUND_PROXY_URL", "BACKPACK_PROXY_URL"],
    )
    rejected = {}

    async def capture(response):
        if response.status_code >= 400:
            await response.aread()
            rejected["http_status"] = response.status_code
            try:
                payload = response.json()
            except ValueError:
                return
            if isinstance(payload, dict):
                for key in ("code", "message"):
                    if isinstance(payload.get(key), (str, int)):
                        rejected[key] = str(payload[key])[:300]

    client = httpx.AsyncClient(
        base_url="https://api.backpack.exchange",
        timeout=20,
        trust_env=False,
        follow_redirects=False,
        proxy=resolve_proxy("BACKPACK"),
        event_hooks={"response": [capture]},
    )
    bp = Backpack(
        auth=BackpackAuth(os.environ["BACKPACK_ED25519_SEED_BASE64"]),
        transport=Transport("https://api.backpack.exchange", client=client, interval=0.15),
    )
    try:
        balances = await bp.balances()
        print(
            json.dumps(
                {
                    "balances": {
                        symbol: {
                            key: balances.get(symbol, {}).get(key, "0")
                            for key in ("available", "locked", "lent")
                        }
                        for symbol in ("USD", "USDC", "USDT", "FLWS.US")
                    }
                }
            ),
            flush=True,
        )
        if not execute:
            return
        journal = ROOT / "state/manual-flws-purchase.json"
        if journal.exists():
            raise ValueError("Purchase attempt already recorded; reconcile it before any new submission")
        client_id = secrets.randbits(32)
        params = {
            "symbol": "FLWS.US_USDC_RFQ",
            "side": "Bid",
            "quantity": "1",
            "clientId": client_id,
            "executionMode": "Immediate",
            "price": "3",
            "autoBorrow": False,
            "autoLend": False,
            "autoLendRedeem": False,
            "autoBorrowRepay": False,
        }
        record = {"params": params, "state": "submitting"}

        def save():
            with journal.open("w") as out:
                os.chmod(journal, 0o600)
                json.dump(record, out, indent=2)

        save()
        try:
            result = await bp.private("POST", "/api/v1/rfq", "rfqSubmit", **params)
        except Exception as error:
            record.update(
                state="unknown" if isinstance(error, UnknownOutcome) else "rejected",
                error_type=type(error).__name__,
                rejection=dict(rejected),
            )
            save()
            print(json.dumps({"purchase": record}), flush=True)
            return
        record.update(state="submitted", rfq=result)
        save()
        print(json.dumps({"purchase": record}), flush=True)
        rfq_id = result["rfqId"]
        for _ in range(25):
            fills = await bp.rfq_fills(rfq_id)
            actual = [f for f in fills if f.get("rfqId") == rfq_id]
            if actual:
                record.update(state="filled", fills=actual)
                save()
                print(json.dumps({"purchase": record}), flush=True)
                print(json.dumps({"FLWS.US": (await bp.balances()).get("FLWS.US", {})}), flush=True)
                return
            history = await bp.private(
                "GET", "/wapi/v1/history/rfq", "rfqHistoryQueryAll", symbol=params["symbol"], limit=100
            )
            found = [r for r in history if r.get("rfqId") == rfq_id]
            if found:
                record["history"] = found
                save()
                if found[0].get("status") in {"Cancelled", "Canceled", "Expired", "Rejected"}:
                    print(json.dumps({"purchase": record}), flush=True)
                    return
            await asyncio.sleep(1)
        print(
            json.dumps({"purchase": record, "note": "No confirmed fill yet; do not repeat purchase"}),
            flush=True,
        )
    except Exception as error:
        print(json.dumps({"error_type": type(error).__name__, "rejection": rejected}), flush=True)
    finally:
        await bp.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    asyncio.run(main(parser.parse_args().execute))
