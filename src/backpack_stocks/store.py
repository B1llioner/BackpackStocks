from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .models import Cycle, Phase, Settings


class Store:
    """Exclusive process-lifetime writer lock; intent is committed before external submission."""

    def __init__(self, path: str, settings: Settings):
        target = Path(path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._lock = open(str(target) + ".lock", "a+")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock.close()
            raise RuntimeError("Database already has an active execution owner") from None
        self.epoch = uuid.uuid4().hex
        self.db = sqlite3.connect(target, isolation_level=None)
        os.chmod(target, 0o600)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS cycles(id TEXT PRIMARY KEY, phase TEXT NOT NULL, body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY, time REAL NOT NULL,
            cycle_id TEXT, kind TEXT NOT NULL, body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS intents(id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL,
            action TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, result TEXT,
            UNIQUE(cycle_id,action));
          CREATE TABLE IF NOT EXISTS ledger(location TEXT NOT NULL, asset TEXT NOT NULL,
            bucket TEXT NOT NULL, amount INTEGER NOT NULL CHECK(amount >= 0),
            PRIMARY KEY(location,asset,bucket));
          CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY, result TEXT NOT NULL);
        """)
        previous = self.get_meta("settings")
        encoded = settings.model_dump_json()
        if previous:
            stored = Settings.model_validate_json(previous)
            if stored.mode == settings.mode == "paper":
                # Paper scheduling may be tuned without abandoning its journal.
                # Prices, amounts, fees, TTLs and every live setting stay pinned.
                stored.monitor.concurrency = settings.monitor.concurrency
                stored.monitor.exploration_s = settings.monitor.exploration_s
                stored.monitor.quote_refresh_s = settings.monitor.quote_refresh_s
            if stored.model_dump_json() != encoded:
                self.close()
                raise RuntimeError("Config differs from the database; use a new database for changed policy")
        self.set_meta("settings", encoded)
        self.set_meta("owner_epoch", self.epoch)
        if not self.get_meta("initialized"):
            with self.atomic():
                self.set_meta("initialized", "1")
                if settings.mode == "paper":
                    from .models import raw

                    self.credit("backpack", "USDC", "available", raw(settings.paper_initial_usdc))

    def close(self) -> None:
        self.db.close()
        fcntl.flock(self._lock, fcntl.LOCK_UN)
        self._lock.close()

    @contextmanager
    def atomic(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def get_meta(self, key: str) -> str | None:
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r[0] if r else None

    def set_meta(self, key: str, value: str) -> None:
        self.db.execute(
            "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value)
        )

    def event(self, cycle_id: str | None, kind: str, body: dict) -> None:
        self.db.execute(
            "INSERT INTO events(time,cycle_id,kind,body) VALUES(?,?,?,?)",
            (time.time(), cycle_id, kind, json.dumps(body)),
        )

    def save(self, cycle: Cycle) -> None:
        self.db.execute(
            "INSERT INTO cycles VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET "
            "phase=excluded.phase,body=excluded.body",
            (cycle.id, cycle.phase.value, cycle.model_dump_json()),
        )

    def active(self) -> Cycle | None:
        row = self.db.execute(
            "SELECT body FROM cycles WHERE phase NOT IN (?,?) ORDER BY rowid DESC LIMIT 1",
            (Phase.COMPLETE.value, Phase.CANCELLED.value),
        ).fetchone()
        return Cycle.model_validate_json(row[0]) if row else None

    def cycles(self) -> list[dict]:
        return [json.loads(r[0]) for r in self.db.execute("SELECT body FROM cycles ORDER BY rowid DESC")]

    def balance(self, location: str, asset: str, bucket: str = "available") -> int:
        r = self.db.execute(
            "SELECT amount FROM ledger WHERE location=? AND asset=? AND bucket=?", (location, asset, bucket)
        ).fetchone()
        return r[0] if r else 0

    def credit(self, location: str, asset: str, bucket: str, amount: int) -> None:
        result = self.db.execute(
            "UPDATE ledger SET amount=amount+? WHERE location=? AND asset=? AND bucket=?",
            (amount, location, asset, bucket),
        )
        if result.rowcount == 0:
            self.db.execute("INSERT INTO ledger VALUES(?,?,?,?)", (location, asset, bucket, amount))

    def move(self, asset: str, amount: int, source: tuple[str, str], dest: tuple[str, str]) -> None:
        if amount < 0 or self.balance(source[0], asset, source[1]) < amount:
            raise ValueError("Insufficient tracked funds")
        self.credit(source[0], asset, source[1], -amount)
        self.credit(dest[0], asset, dest[1], amount)

    def ledger(self) -> list[dict]:
        return [
            dict(zip(["location", "asset", "bucket", "raw"], r, strict=True))
            for r in self.db.execute("SELECT location,asset,bucket,amount FROM ledger")
        ]

    def intent(self, cycle: Cycle, action: str, payload: dict) -> dict:
        existing = self.find_intent(cycle.id, action)
        if existing:
            return existing
        identity = f"{cycle.id}:{action}"
        with self.atomic():
            self.db.execute(
                "INSERT INTO intents VALUES(?,?,?,?,?,NULL)",
                (identity, cycle.id, action, "PREPARED", json.dumps(payload)),
            )
            self.event(cycle.id, "intent", {"id": identity, "action": action, "epoch": self.epoch})
        return self.require_intent(cycle.id, action)

    def require_intent(self, cycle_id: str, action: str) -> dict:
        result = self.find_intent(cycle_id, action)
        if result is None:
            raise ValueError("Required durable action intent is missing")
        return result

    def find_intent(self, cycle_id: str, action: str) -> dict | None:
        r = self.db.execute(
            "SELECT id,status,payload,result FROM intents WHERE cycle_id=? AND action=?", (cycle_id, action)
        ).fetchone()
        return (
            {
                "id": r[0],
                "status": r[1],
                "payload": json.loads(r[2]),
                "result": json.loads(r[3]) if r[3] else None,
            }
            if r
            else None
        )

    def intent_status(self, identity: str, status: str, result: dict | None = None) -> None:
        self.db.execute(
            "UPDATE intents SET status=?,result=? WHERE id=?",
            (status, json.dumps(result) if result else None, identity),
        )

    def idempotent(self, key: str) -> dict | None:
        r = self.db.execute("SELECT result FROM requests WHERE key=?", (key,)).fetchone()
        return json.loads(r[0]) if r else None

    def remember_request(self, key: str, result: dict) -> None:
        self.db.execute("INSERT INTO requests VALUES(?,?)", (key, json.dumps(result)))

    def events(self, limit: int = 100) -> list[dict]:
        return [
            {"seq": r[0], "time": r[1], "cycle_id": r[2], "kind": r[3], "body": json.loads(r[4])}
            for r in self.db.execute("SELECT * FROM events ORDER BY seq DESC LIMIT ?", (limit,))
        ]
