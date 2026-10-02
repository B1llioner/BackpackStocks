"""Read only sanitized runtime evidence; never reads keys, signed transactions or logs."""

import json
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    state = root / "state"
    runtime = json.loads((state / "telegram-runtime.json").read_text())
    from backpack_stocks.cli import load_settings

    settings = load_settings(str(root / runtime["config"]))
    monitor = json.loads((state / "direct-monitor.json").read_text())
    connection = sqlite3.connect(f"file:{root / settings.database}?mode=ro", uri=True)
    try:
        report = {
            "observed_at": datetime.now(UTC).isoformat(),
            "runtime": runtime,
            "snapshot_age_s": round(time.time() - monitor["observed_at"], 3),
            "monitor": {
                k: monitor.get(k)
                for k in (
                    "dex_backend",
                    "universe",
                    "unit_verified",
                    "fresh_quotes",
                    "candidates",
                    "completed_quotes",
                    "notional_usdc",
                    "blocked",
                    "pending",
                    "errors",
                    "latency_p95_ms",
                    "ws_connected",
                    "ws_reconnects",
                    "rate_limits",
                )
            },
            "fresh_spreads": [
                {
                    k: row.get(k)
                    for k in (
                        "asset",
                        "direction",
                        "profit_raw",
                        "edge_bps",
                        "indicative",
                        "trading_supported",
                    )
                }
                for row in monitor["rows"]
                if row["fresh"]
            ],
            "cycles": connection.execute("SELECT count(*) FROM cycles").fetchone()[0],
            "stopped": connection.execute("SELECT value FROM meta WHERE key='stopped'").fetchone()[0],
            "telegram_delivered": connection.execute(
                "SELECT count(*) FROM events WHERE kind='telegram_reply_sent'"
            ).fetchone()[0],
        }
    finally:
        connection.close()
    destination = root / "research/direct-raydium/paper-runtime.json"
    destination.write_text(json.dumps(report, indent=2))
    print(json.dumps({k: v for k, v in report.items() if k not in {"fresh_spreads", "monitor"}}, indent=2))
    print(json.dumps({k: v for k, v in report["monitor"].items() if k != "rate_limits"}, indent=2))


if __name__ == "__main__":
    main()
