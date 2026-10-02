"""Timed read-only soak observer for the running paper process and structured logs."""

import argparse
import json
import os
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--interval", type=float, default=30)
    args = parser.parse_args()
    if not 0 < args.minutes <= 180 or not 1 <= args.interval <= 60:
        raise ValueError("Invalid bounded observation duration")
    start = time.monotonic()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    destination = ROOT / "research/soak-1k" / stamp
    destination.mkdir(parents=True, exist_ok=True)
    log_offset = 0
    log_inode = None
    samples = 0
    captured_errors = []
    previous_pid = None
    pointer = ROOT / "state/soak-watch.json"
    pointer.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "started_at": datetime.now(UTC).isoformat(),
                "duration_s": args.minutes * 60,
                "output": str(destination),
            },
            indent=2,
        )
    )
    os.chmod(pointer, 0o600)
    while True:
        elapsed = time.monotonic() - start
        runtime = json.loads((ROOT / "state/telegram-runtime.json").read_text())
        from backpack_stocks.cli import load_settings

        settings = load_settings(str(ROOT / runtime["config"]))
        try:
            os.kill(runtime["pid"], 0)
            alive = True
        except ProcessLookupError:
            alive = False
        monitor_path = ROOT / "state/direct-monitor.json"
        monitor = json.loads(monitor_path.read_text()) if monitor_path.exists() else {}
        new_errors = []
        if settings.log_file and (ROOT / settings.log_file).exists():
            log = ROOT / settings.log_file
            if log.stat().st_ino != log_inode:
                log_inode, log_offset = log.stat().st_ino, 0
            with log.open() as stream:
                stream.seek(log_offset)
                for line in stream:
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if row.get("level") == "ERROR":
                        new_errors.append(row)
                log_offset = stream.tell()
        captured_errors.extend(new_errors)
        database = ROOT / settings.database
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        try:
            events = dict(connection.execute("SELECT kind,count(*) FROM events GROUP BY kind"))
            cycles = connection.execute("SELECT count(*) FROM cycles").fetchone()[0]
        finally:
            connection.close()
        samples += 1
        sample = {
            "time": datetime.now(UTC).isoformat(),
            "elapsed_s": round(elapsed, 2),
            "pid": runtime["pid"],
            "pid_changed": previous_pid is not None and previous_pid != runtime["pid"],
            "alive": alive,
            "mode": settings.mode,
            "size_usdc": settings.monitor.notional_usdc,
            "snapshot_age_s": round(time.time() - monitor.get("observed_at", 0), 1),
            "universe": monitor.get("universe"),
            "quotes": monitor.get("completed_quotes"),
            "candidates": monitor.get("candidates"),
            "monitor_errors": monitor.get("errors"),
            "blocked": monitor.get("blocked"),
            "ws_connected": monitor.get("ws_connected"),
            "cycles": cycles,
            "alerts": events.get("spread_alert", 0),
            "delivered": events.get("telegram_reply_sent", 0),
            "new_errors": new_errors,
        }
        previous_pid = runtime["pid"]
        with (destination / "samples.jsonl").open("a") as stream:
            stream.write(json.dumps(sample) + "\n")
        print(json.dumps(sample), flush=True)
        if elapsed >= args.minutes * 60:
            summary = {
                "started_at": stamp,
                "duration_s": elapsed,
                "samples": samples,
                "error_events": len(captured_errors),
                "last_sample": sample,
            }
            (destination / "summary.json").write_text(json.dumps(summary, indent=2))
            print(
                json.dumps(
                    {
                        "complete": True,
                        "output": str(destination),
                        "duration_s": elapsed,
                        "error_events": len(captured_errors),
                    }
                ),
                flush=True,
            )
            return
        time.sleep(min(args.interval, max(0, args.minutes * 60 - elapsed)))


if __name__ == "__main__":
    main()
