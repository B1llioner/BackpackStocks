"""Export public pool IDs and stock coverage; no network or private environment."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

from backpack_stocks.models import USDC_MINT


def export(path: str):
    catalog = json.loads(Path(path).read_text())
    pools = {p["address"]: p for p in catalog["pools"]}
    rows = []
    for asset in sorted(catalog["assets"], key=lambda a: a["symbol"]):
        links = {address for symbol, address, _ in catalog["links"] if symbol == asset["symbol"]}
        direct = [
            pools[address]
            for address in links
            if asset["mint"] in {pools[address]["mintA"], pools[address]["mintB"]}
        ]
        counts = Counter(p["kind"] for p in direct)
        rows.append(
            {
                "symbol": asset["symbol"],
                "mint": asset["mint"],
                "raydium_clmm": counts["clmm"],
                "raydium_cpmm": counts["cpmm"],
                "direct_usdc": sum(USDC_MINT in {p["mintA"], p["mintB"]} for p in direct),
                "local_quote_pools": sum(
                    p.get("snapshot", {}).get("state", {}).get("quoteSupported", False) for p in direct
                ),
                "other_direct_venues": ",".join(sorted({p["venue"] for p in direct if p["kind"] == "other"})),
                "jupiter_quote": any(
                    q["symbol"] == asset["symbol"] and q["source"] == "jupiter" for q in catalog["quotes"]
                ),
                "mint_gate": catalog["tokens"][asset["mint"]]["reason"],
            }
        )
    target = Path("research/pool-pricing")
    with (target / "coverage.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    inventory = []
    for p in sorted(pools.values(), key=lambda p: p["address"]):
        state = p.get("snapshot", {}).get("state", {})
        inventory.append(
            {
                "address": p["address"],
                "venue": p["venue"],
                "kind": p["kind"],
                "program": p.get("program"),
                "mint_a": p["mintA"],
                "mint_b": p["mintB"],
                "verified_pool_layout": p["verified"],
                "local_quote_supported": state.get("quoteSupported", False),
                "slot": p.get("snapshot", {}).get("slot"),
                "observed_at": p.get("snapshot", {}).get("observed_at"),
                "reason": state.get("quoteReason", p.get("reason")),
                "linked_stocks": ",".join(
                    sorted({symbol for symbol, address, _ in catalog["links"] if address == p["address"]})
                ),
            }
        )
    with (target / "inventory.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(inventory[0]))
        w.writeheader()
        w.writerows(inventory)
    table = [
        "| Акция | CLMM | CPMM | Прямые пары с USDC | Локальные CPMM | Другие прямые venues из маршрута |",
        "|---|---:|---:|---:|---:|---|",
    ]
    table += [
        f"| {r['symbol']} | {r['raydium_clmm']} | {r['raydium_cpmm']} | {r['direct_usdc']} | {r['local_quote_pools']} | {r['other_direct_venues'] or '—'} |"
        for r in rows
    ]
    (target / "coverage.md").write_text("\n".join(table) + "\n")
    print(f"Exported {len(rows)} equities and {len(inventory)} public addresses")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", nargs="?", default="research/pool-pricing/catalog-native.json")
    export(parser.parse_args().path)
