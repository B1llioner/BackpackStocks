"""Compare public exact-in FLWS quotes; never connects/signs/submits a wallet."""

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

from backpack_stocks.cli import load_settings
from backpack_stocks.environment import load_environment
from backpack_stocks.jupiter import Jupiter
from backpack_stocks.models import USDC_MINT
from backpack_stocks.monitor import SpreadMonitor
from backpack_stocks.proxy import PROXY_ENV_NAMES, resolve_proxy
from backpack_stocks.runtime import public_clients
from backpack_stocks.transport import RemoteError


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--taker", help="Optional public wallet address for a read-only frontend fee check")
    args = parser.parse_args()
    load_environment(".env", PROXY_ENV_NAMES)
    settings = load_settings("examples/telegram-direct-paper-1k.toml")
    settings.backpack_private_reads = False
    settings.monitor.quote_backend = "execution"
    bp, dex, rpc = public_clients(settings)
    jupiter = Jupiter(proxy=resolve_proxy("JUPITER"))
    monitor = SpreadMonitor(settings, bp, dex, rpc)
    records = {"observed_at": datetime.now(UTC).isoformat(), "quotes": [], "mutations": 0}
    try:
        report = await monitor.refresh_report("FLWS.US")
        print(monitor.text("FLWS.US", snapshot=report), flush=True)
        records["monitor"] = report
        mint = monitor.assets["FLWS.US"].mint
        forward = next(
            r for r in report["rows"] if r["asset"] == "FLWS.US" and r["direction"] == "backpack_to_jupiter"
        )
        cases = [
            (mint, USDC_MINT, 9_800_000),
            (mint, USDC_MINT, forward["input_raw"]),
            (USDC_MINT, mint, 1_000_000_000),
        ]
        for source, target, quantity in cases:
            quote = await jupiter.quote_pair(source, target, quantity, priority=10)
            selected = {
                k: quote.get(k)
                for k in (
                    "inputMint",
                    "outputMint",
                    "inAmount",
                    "outAmount",
                    "otherAmountThreshold",
                    "slippageBps",
                    "priceImpact",
                    "priceImpactPct",
                    "router",
                    "mode",
                    "feeBps",
                    "feeMint",
                    "platformFee",
                    "routePlan",
                    "_request_started_at",
                )
            }
            records["quotes"].append(selected)
            print(json.dumps(selected), flush=True)
        direct = await dex.quote_pair(mint, USDC_MINT, 9_800_000, priority=10)
        records["direct_small"] = direct
        print(
            "direct_small",
            json.dumps({k: direct.get(k) for k in ("outAmount", "routePlan", "contextSlot", "_observed_at")}),
            flush=True,
        )
        frontend = await jupiter._get(
            "https://ultra-api.jup.ag/order",
            {
                "inputMint": mint,
                "outputMint": USDC_MINT,
                "amount": "9800000",
                "clientPlatform": "jupiter.web",
            },
        )
        records["frontend_small"] = {
            k: v for k, v in frontend.items() if k not in {"transaction", "requestId", "taker"}
        }
        print("frontend_small", json.dumps(records["frontend_small"]), flush=True)
        if args.taker:
            wallet_quote = await jupiter._get(
                "https://ultra-api.jup.ag/order",
                {
                    "inputMint": mint,
                    "outputMint": USDC_MINT,
                    "amount": "9800000",
                    "clientPlatform": "jupiter.web",
                    "taker": args.taker,
                },
            )
            records["frontend_wallet"] = {
                k: wallet_quote.get(k)
                for k in (
                    "inAmount",
                    "outAmount",
                    "otherAmountThreshold",
                    "priceImpact",
                    "feeBps",
                    "signatureFeeLamports",
                    "prioritizationFeeLamports",
                    "rentFeeLamports",
                    "gasless",
                    "errorCode",
                )
            }
            records["frontend_wallet"]["network_output_difference_raw"] = int(frontend["outAmount"]) - int(
                wallet_quote["outAmount"]
            )
            print("frontend_wallet", json.dumps(records["frontend_wallet"]), flush=True)
            records["wallet_large"] = []
            for source, target, quantity in cases[1:]:
                wallet_quote = await jupiter._get(
                    "/order",
                    {"inputMint": source, "outputMint": target, "amount": str(quantity), "taker": args.taker},
                )
                item = {
                    k: wallet_quote.get(k)
                    for k in (
                        "inAmount",
                        "outAmount",
                        "otherAmountThreshold",
                        "slippageBps",
                        "feeBps",
                        "platformFee",
                        "gasless",
                        "errorCode",
                        "errorMessage",
                        "signatureFeeLamports",
                        "prioritizationFeeLamports",
                        "rentFeeLamports",
                    )
                }
                records["wallet_large"].append(item)
                print("wallet_large", json.dumps(item), flush=True)
        Path("research/fresh-reports/flws-jupiter.json").write_text(json.dumps(records, indent=2))
    except RemoteError as error:
        records["read_unavailable"] = str(error)
        Path("research/fresh-reports/flws-jupiter-unavailable.json").write_text(json.dumps(records, indent=2))
        print("read_unavailable", str(error), flush=True)
    finally:
        await monitor.close()
        for client in (jupiter, bp, dex, rpc):
            await client.close()


if __name__ == "__main__":
    asyncio.run(main())
