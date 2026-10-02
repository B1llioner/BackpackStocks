"""Restart only this project's paper Telegram process; credentials stay in local .env."""

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state"
PID = STATE / "telegram.pid"


def private_write(path: Path, content: str):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(content)


def main():
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    args = parser.parse_args()
    active = STATE / "telegram-runtime.json"
    previous_config = json.loads(active.read_text()).get("config") if active.exists() else None
    config = str(
        (ROOT / (args.config or previous_config or "examples/telegram-direct-paper.toml"))
        .resolve()
        .relative_to(ROOT)
    )
    from backpack_stocks.cli import load_settings

    if load_settings(str(ROOT / config)).mode != "paper":
        raise ValueError("Restart script only runs paper")
    if PID.exists():
        previous = int(PID.read_text().strip())
        result = subprocess.run(["ps", "-p", str(previous), "-o", "command="], capture_output=True, text=True)
        command = result.stdout.strip()
        if result.returncode == 0 and command:
            if str(ROOT / ".venv/bin/backpack-stocks") not in command or " telegram" not in command:
                raise RuntimeError("Saved PID belongs to a different process; refusing to stop it")
            os.kill(previous, signal.SIGTERM)
            for _ in range(100):
                result = subprocess.run(
                    ["ps", "-p", str(previous), "-o", "stat="], capture_output=True, text=True
                )
                if result.returncode != 0 or result.stdout.strip().startswith("Z"):
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError("Previous bot did not stop; no competing poller started")
    fd = os.open(STATE / "telegram.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "ab", buffering=0) as log:
        log.write(b"\n--- direct paper restart ---\n")
        process = subprocess.Popen(
            [str(ROOT / ".venv/bin/backpack-stocks"), "--config", config, "telegram"],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
    private_write(PID, str(process.pid) + "\n")
    private_write(
        STATE / "telegram-runtime.json",
        json.dumps(
            {"pid": process.pid, "mode": "paper", "config": config, "started_at": time.time()}, indent=2
        ),
    )
    time.sleep(2)
    if process.poll() is not None:
        raise RuntimeError("Paper process exited at startup; inspect local log")
    print(json.dumps({"pid": process.pid, "mode": "paper", "config": config}))


if __name__ == "__main__":
    main()
