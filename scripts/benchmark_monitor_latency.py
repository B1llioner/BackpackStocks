"""Offline benchmark: synthetic RFQ/DEX inputs, real monitor and SQLite command path.

No credentials, RPC calls, Telegram messages, purchases or swaps.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import tempfile
import time
from decimal import ROUND_UP, Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from backpack_stocks.engine import Engine
from backpack_stocks.feed import StockFeed
from backpack_stocks.models import STOCK_MINT, MonitorPolicy, Observation, Policy, Settings
from backpack_stocks.monitor import FORWARD, REVERSE, Asset, SpreadMonitor
from backpack_stocks.stock_money import StockValueCalculator
from backpack_stocks.store import Store
from backpack_stocks.telegram import TelegramController


def summary(samples):
    samples = sorted(samples)
    return {"p50_ms": statistics.median(samples), "p95_ms": samples[int(len(samples) * 0.95)]}


def measure(call, rounds=200):
    for _ in range(10):
        call()
    samples = []
    for _ in range(rounds):
        started = time.perf_counter_ns()
        call()
        samples.append((time.perf_counter_ns() - started) / 1e6)
    return summary(samples)


def legacy_values(jobs):
    result = []
    for quantity, decimals, price, fees, buy in jobs:
        value = (
            Decimal(quantity)
            / 10**decimals
            * Decimal(price)
            * (1 + Decimal(fees if buy else -fees) / 10000)
            * 10**6
        )
        result.append(int(value.to_integral_value(rounding=ROUND_UP)) if buy else int(value))
    return result


async def main():
    now = [time.time()]

    def clock():
        return now[0]

    settings = Settings(
        source="public",
        policy=Policy(withdrawal_semantics="deduct"),
        monitor=MonitorPolicy(rfq_validation=False, jupiter_flow=False, max_quote_age_s=15),
    )
    feed = StockFeed()
    feed.subscribe = AsyncMock()
    bp = SimpleNamespace(auth=None, stock_feed=feed, http=SimpleNamespace(status=lambda: {}))
    jup = SimpleNamespace(interval=2.1, quote_pair=AsyncMock(), status=lambda: {})
    rpc = SimpleNamespace(http=SimpleNamespace(status=lambda: {}))
    mon = SpreadMonitor(settings, bp, jup, rpc, clock=clock)
    for i in range(68):
        symbol = f"S{i:02d}.US"
        mon.assets[symbol] = Asset(
            symbol,
            STOCK_MINT,
            6,
            Decimal("0.06"),
            Decimal("0.12"),
            Decimal("0.06"),
            True,
            ({"name": "REGULAR", "stepSize": "0.00001", "minQuantity": "0.01", "maxQuantity": "50000"},),
        )
        feed.latest[symbol[:-3]] = Observation(
            ask="9.123456", bid="9.123456", timestamp=clock(), session="REGULAR", source="synthetic benchmark"
        )
        mon.chain[symbol] = "verified_unit_mapping"
    mon.metadata_at = clock()
    jup.quote_pair.return_value = {"outAmount": "108000000", "priceImpact": "0.1"}
    await mon.quote(("S00.US", FORWARD))
    jup.quote_pair.return_value = {"outAmount": "12000000", "priceImpact": "0.1"}
    await mon.quote(("S00.US", REVERSE))
    base = dict(mon.results)
    for asset in mon.assets.values():
        for direction in (FORWARD, REVERSE):
            mon.results[(asset.symbol, direction)] = {
                **base[("S00.US", direction)],
                "asset": asset.symbol,
                "backpack_quote_confirmed": True,
                "backpack_rfq_valid_until": clock() + 15,
            }
        mon.rfq.latest[(asset.symbol, "Bid")] = mon.rfq.result(
            asset.symbol, "Bid", "1", "REGULAR", "available", "rfq_quote_available", "9.123456"
        )
    settings.monitor.rfq_validation = True
    jup.quote_pair.reset_mock()
    jobs = [
        (
            row["acquired_raw"] if direction == FORWARD else row["sellable_raw"],
            6,
            row["backpack_price"],
            settings.policy.backpack_fee_bps,
            direction == FORWARD,
        )
        for (_, direction), row in mon.results.items()
    ]
    python, rust = StockValueCalculator("python"), StockValueCalculator("rust")
    assert legacy_values(jobs) == python.batch(jobs) == rust.batch(jobs)
    report = {
        "observed_at": clock(),
        "inputs": "synthetic RFQ and DEX prices; no external calls",
        "assets": 68,
        "directions": len(jobs),
        "includes_validation_and_ffi": True,
    }
    for size in (136, 68000):
        batch = (jobs * (size // len(jobs) + 1))[:size]
        report[f"stock_values_{size}"] = {
            "legacy_decimal": measure(lambda rows=batch: legacy_values(rows), rounds=20),
            "python_integer": measure(lambda rows=batch: python.batch(rows), rounds=20),
            "rust_integer": measure(lambda rows=batch: rust.batch(rows), rounds=20),
        }
    for name, calculator in (("python", python), ("rust", rust)):
        mon.stock_values = calculator
        assert mon.report()["fresh_calculations"] == 136
        report[f"full_validated_report_{name}"] = measure(mon.report)
    with tempfile.TemporaryDirectory() as directory:
        settings.database = str(Path(directory) / "benchmark.sqlite3")
        store = Store(settings.database, settings)
        try:
            engine = Engine(settings, store, SimpleNamespace(), SimpleNamespace(), clock=clock)
            engine.monitor = mon
            bot = TelegramController(engine, SimpleNamespace(), 1, clock=clock)
            bot.menu_registered = bot.queue_replies = True
            samples = {"spreads": [], "status": []}
            identity = 0
            for _ in range(200):
                for command in samples:
                    identity += 1
                    update = {
                        "update_id": identity,
                        "message": {
                            "date": int(clock()),
                            "from": {"id": 1, "is_bot": False},
                            "chat": {"id": 1, "type": "private"},
                            "text": "/" + command,
                        },
                    }
                    started = time.perf_counter_ns()
                    await bot.handle(update)
                    samples[command].append((time.perf_counter_ns() - started) / 1e6)
                    while not bot.outbox.empty():
                        bot.outbox.get_nowait()
                        bot.outbox.task_done()
            report["telegram_command_preparation"] = {key: summary(value) for key, value in samples.items()}
        finally:
            store.close()
    jup.quote_pair.assert_not_called()
    now[0] += 16
    assert not mon.report()["rows"]
    report["expired_inputs_hidden"] = True
    report["telegram_network_delivery_measured"] = False
    target = Path("research/fresh-reports/latency-benchmark.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
