"""Public finalized wallet activity, with durable pagination and a notification outbox."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, TextIO
from zoneinfo import ZoneInfo

from solders.pubkey import Pubkey
from solders.signature import Signature

from .models import TOKEN, TOKEN_2022, USDC_MINT
from .observability import error_code, event
from .solana import SolanaRPC
from .wallet_stream import WalletStream

if TYPE_CHECKING:
    from .monitor import SpreadMonitor

TITAN = "T1TANpTeScyeqVzzgNViGDNrkQ6qHz9KrSBS4aNXvGT"
BACKPACK_WITHDRAW = "FewRXXTzV2WwigvTQNsVanhEmCM9wzfNvoT1divNtbVH"
BACKPACK_SENDER = "43DbAvKxhXh1oSxkJSqGosNw3HpBnmsWiak6tB5wpecN"
POLL_S = 20


def public_address(value: str) -> str:
    if not isinstance(value, str) or not 32 <= len(value) <= 44:
        raise ValueError("Invalid public wallet address")
    return str(Pubkey.from_string(value))


def signature_id(value: str) -> str:
    if not isinstance(value, str) or not 80 <= len(value) <= 88:
        raise ValueError("Invalid transaction signature")
    return str(Signature.from_string(value))


def decode_activity(tx: dict, wallet: str, signature: str, stocks: dict[str, str]) -> dict:
    """Use integer balances belonging to this owner, including created/closed ATAs."""
    signature_id(signature)
    if tx["transaction"]["signatures"][0] != signature:
        raise ValueError("Transaction signature mismatch")
    meta = tx["meta"]
    if not isinstance(meta, dict):
        raise ValueError("Missing transaction metadata")
    block_time = tx.get("blockTime")
    if type(block_time) is not int or block_time < 0:
        raise ValueError("Missing transaction time")
    deltas: dict[str, int] = defaultdict(int)
    precision: dict[str, int] = {}
    owners: dict[int, tuple[str, str]] = {}
    for row in [*meta.get("preTokenBalances", []), *meta.get("postTokenBalances", [])]:
        index = row["accountIndex"]
        owner = row.get("owner")
        if owner:
            value = owner, row["mint"]
            if index in owners and owners[index] != value:
                raise ValueError("Token account owner/mint changed")
            owners[index] = value
    for sign, rows in ((-1, meta.get("preTokenBalances", [])), (1, meta.get("postTokenBalances", []))):
        for row in rows:
            mint = row["mint"]
            owner = row.get("owner") or owners.get(row["accountIndex"], (None, None))[0]
            if owner != wallet:
                continue
            program = row.get("programId")
            if program is not None and program not in {TOKEN, TOKEN_2022}:
                raise ValueError("Unexpected token program")
            amount = row["uiTokenAmount"]
            decimals = amount["decimals"]
            if type(decimals) is not int or not 0 <= decimals <= 18:
                raise ValueError("Invalid token decimals")
            if mint in precision and precision[mint] != decimals:
                raise ValueError("Token precision changed")
            precision[mint] = decimals
            units = int(amount["amount"])
            if not 0 <= units < 2**64:
                raise ValueError("Invalid token amount")
            deltas[mint] += sign * units
    if USDC_MINT in precision and precision[USDC_MINT] != 6:
        raise ValueError("Invalid USDC precision")
    instructions = tx["transaction"]["message"]["instructions"]
    instructions = [
        *instructions,
        *(i for g in meta.get("innerInstructions") or [] for i in g["instructions"]),
    ]
    programs = sorted({i["programId"] for i in instructions if "programId" in i})
    keys = tx["transaction"]["message"]["accountKeys"]
    payer = keys[0]["pubkey"] if isinstance(keys[0], dict) else keys[0]
    fee = meta["fee"]
    if type(fee) is not int or fee < 0:
        raise ValueError("Invalid transaction fee")
    tips = sum(
        i.get("parsed", {}).get("info", {}).get("lamports", 0)
        for i in instructions
        if i.get("programId") == "11111111111111111111111111111111"
        and i.get("parsed", {}).get("type") == "transfer"
        and i["parsed"]["info"].get("source") == wallet
    )
    legs: list[dict] = [
        {"mint": mint, "asset": stocks.get(mint), "raw": units, "decimals": precision[mint]}
        for mint, units in deltas.items()
        if units and mint != USDC_MINT
    ]
    usdc = deltas.get(USDC_MINT, 0)
    failed = meta.get("err") is not None
    known = [leg for leg in legs if leg["asset"]]
    kind = "transfer"
    if failed:
        kind = "failed"
        legs, usdc = [], 0  # Failed instructions cannot produce a successful trade.
    elif len(legs) == len(known) == 1 and known[0]["raw"] * usdc < 0:
        # A paired balance change alone does not prove a swap; verify the router.
        kind = "sale" if known[0]["raw"] < 0 else "purchase"
        if TITAN not in programs:
            kind = "paired_transfer"
    elif (
        known
        and BACKPACK_WITHDRAW in programs
        and all(leg["raw"] > 0 for leg in known)
        and any(
            i.get("programId") == TOKEN_2022
            and i.get("parsed", {}).get("type") == "transferChecked"
            and i["parsed"]["info"].get("authority") == BACKPACK_SENDER
            and i["parsed"]["info"].get("mint") in stocks
            for i in instructions
        )
    ):
        kind = "backpack_receipt"
    return {
        "signature": signature,
        "wallet": wallet,
        "slot": tx["slot"],
        "block_time": block_time,
        "version": tx.get("version"),
        "kind": kind,
        "legs": legs,
        "usdc_raw": usdc,
        "fee_lamports": fee if payer == wallet else 0,
        "sol_transfer_lamports": tips,
        "router": "Titan Atlas" if TITAN in programs else None,
        "acquisition_cost_known": False,
    }


class WalletJournal:
    def __init__(self, path: str | Path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.path, isolation_level=None)
        os.chmod(self.path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS wallets(address TEXT PRIMARY KEY, added REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS accounts(address TEXT PRIMARY KEY, wallet TEXT NOT NULL,
                head TEXT, crawl_before TEXT, target_head TEXT, checked REAL NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS transactions(wallet TEXT NOT NULL, signature TEXT NOT NULL,
                historical INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                retry_at REAL NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0,
                body TEXT, notified INTEGER NOT NULL DEFAULT 0, block_time INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(wallet,signature));
            CREATE TABLE IF NOT EXISTS quotes(asset TEXT NOT NULL,direction TEXT NOT NULL,
                observed REAL NOT NULL,body TEXT NOT NULL,PRIMARY KEY(asset,direction,observed));
            CREATE INDEX IF NOT EXISTS quotes_time ON quotes(observed);
            CREATE TABLE IF NOT EXISTS size_requests(asset TEXT PRIMARY KEY,requested REAL NOT NULL);
        """)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            columns = {r[1] for r in self.db.execute("PRAGMA table_info(transactions)")}
            if "block_time" not in columns:
                self.db.execute("ALTER TABLE transactions ADD COLUMN block_time INTEGER NOT NULL DEFAULT 0")
            self.db.execute(
                "UPDATE transactions SET block_time=json_extract(body,'$.block_time') "
                "WHERE block_time=0 AND body IS NOT NULL"
            )
            self.db.execute("CREATE INDEX IF NOT EXISTS activity_time ON transactions(block_time)")
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            self.db.close()
            raise

    def close(self) -> None:
        self.db.close()

    def add(self, address: str, now: float | None = None) -> None:
        address = public_address(address)
        if not self.db.execute("SELECT 1 FROM wallets WHERE address=?", (address,)).fetchone():
            if self.db.execute("SELECT COUNT(*) FROM wallets").fetchone()[0] >= 4:
                raise ValueError("At most four watched wallets")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("INSERT OR IGNORE INTO wallets VALUES(?,?)", (address, now or time.time()))
            self.db.execute("INSERT OR IGNORE INTO accounts(address,wallet) VALUES(?,?)", (address, address))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def activities(self, *, symbol: str | None = None, limit: int = 6) -> list[dict]:
        rows = self.db.execute(
            "SELECT body FROM transactions WHERE body IS NOT NULL ORDER BY block_time DESC,rowid DESC LIMIT 200"
        ).fetchall()
        records = [json.loads(row[0]) for row in rows]
        records = [r for r in records if any(leg["asset"] for leg in r["legs"])]
        if symbol:
            records = [r for r in records if any(leg["asset"] == symbol for leg in r["legs"])]
        return sorted(records, key=lambda r: (r["block_time"], r["slot"]), reverse=True)[:limit]

    def request_size_search(self, symbol: str) -> None:
        from .stock_cli import stock_symbol

        symbol = stock_symbol(symbol)
        self.db.execute(
            "INSERT INTO size_requests VALUES(?,?) "
            "ON CONFLICT(asset) DO UPDATE SET requested=excluded.requested",
            (symbol, time.time()),
        )

    def notifications(self) -> list[dict]:
        rows = self.db.execute(
            "SELECT body FROM transactions WHERE historical=0 AND notified=0 AND state='ready' "
            "ORDER BY rowid LIMIT 10"
        ).fetchall()
        return [json.loads(row[0]) for row in rows]

    def ack(self, record: dict) -> None:
        self.db.execute(
            "UPDATE transactions SET notified=1 WHERE wallet=? AND signature=?",
            (record["wallet"], record["signature"]),
        )


class WalletWatcher:
    def __init__(
        self, journal: WalletJournal, rpc: SolanaRPC, monitor: SpreadMonitor | None, *, clock=time.time
    ):
        self.journal, self.rpc, self.monitor, self.clock = journal, rpc, monitor, clock
        self.task: asyncio.Task | None = None
        self.stream_task: asyncio.Task | None = None
        self.decoder_task: asyncio.Task | None = None
        self.decode_lock = asyncio.Lock()
        self.decode_ready = asyncio.Event()
        self.stream = (
            WalletStream(rpc.url, rpc.proxy, self.queue_signature) if isinstance(rpc, SolanaRPC) else None
        )
        self.lock: TextIO | None = None
        self.last_poll = 0.0
        self.last_error: str | None = None
        self.last_discovery: dict[str, float] = {}
        self.last_cleanup = 0.0
        self.initial_focus: dict[str, set[str]] = defaultdict(set)

    def queue_signature(self, wallet: str, signature: str) -> None:
        signature = signature_id(signature)
        if not self.journal.db.execute("SELECT 1 FROM wallets WHERE address=?", (wallet,)).fetchone():
            return
        inserted = self.journal.db.execute(
            "INSERT OR IGNORE INTO transactions(wallet,signature,historical) VALUES(?,?,0)",
            (wallet, signature),
        ).rowcount
        if inserted:
            self.decode_ready.set()

    def update_stream(self) -> None:
        if self.stream:
            self.stream.update(
                {
                    r["address"]: r["wallet"]
                    for r in self.journal.db.execute("SELECT address,wallet FROM accounts")
                }
            )

    def focus_recent_history(self) -> None:
        if self.monitor is None:
            return
        # Inspect the latest two traded stocks once at startup, without replaying
        # the entire historical catalogue or notifying about old transactions.
        for record in self.journal.activities(limit=20):
            wallet = record["wallet"]
            if len(self.initial_focus[wallet]) >= 2 or record["kind"] not in {"sale", "purchase"}:
                continue
            if not 0 <= self.clock() - record["block_time"] <= 86400:
                continue
            for leg in record["legs"]:
                symbol = leg["asset"]
                if symbol and symbol not in self.initial_focus[wallet]:
                    if self.monitor.request_refresh(symbol):
                        self.initial_focus[wallet].add(symbol)

    def apply_size_requests(self) -> None:
        if self.monitor is None:
            return
        requests = self.journal.db.execute(
            "SELECT asset FROM size_requests ORDER BY requested LIMIT 4"
        ).fetchall()
        for request in requests:
            if self.monitor.request_optimization(request["asset"]):
                self.journal.db.execute("DELETE FROM size_requests WHERE asset=?", (request["asset"],))

    def capture_quote(self, row: dict) -> None:
        if not self.journal.db.execute("SELECT 1 FROM wallets LIMIT 1").fetchone():
            return
        fields = {
            key: row[key]
            for key in (
                "asset",
                "direction",
                "observed_at",
                "dex_observed_at",
                "backpack_timestamp",
                "backpack_price",
                "backpack_rfq_valid_until",
                "backpack_valid_until",
                "backpack_source",
                "backpack_market",
                "backpack_fee_bps",
                "book_levels",
                "book_fee_raw",
                "cost_raw",
                "profit_raw",
                "delivered_raw",
                "quoted_output_raw",
                "minimum_output_raw",
                "search_budget_usdc",
                "quote_backend",
                "quote_profile",
                "router",
                "route_markets",
                "stock_decimals",
                "withdrawal_fee_raw",
                "gas_raw",
                "deposit_gas_raw",
                "backpack_deposit_required",
                "price_impact_pct",
                "backpack_session",
            )
            if key in row
        }
        self.journal.db.execute(
            "INSERT OR REPLACE INTO quotes VALUES(?,?,?,?)",
            (row["asset"], row["direction"], row["observed_at"], json.dumps(fields)),
        )
        if self.clock() - self.last_cleanup >= 3600:
            self.journal.db.execute("DELETE FROM quotes WHERE observed<?", (self.clock() - 7 * 86400,))
            self.last_cleanup = self.clock()

    def stocks(self) -> dict[str, str]:
        return {a.mint: a.symbol for a in self.monitor.assets.values()} if self.monitor else {}

    async def discover_accounts(self, wallet: str) -> None:
        self.last_discovery[wallet] = self.clock()
        for program in (TOKEN, TOKEN_2022):
            result = await self.rpc.call(
                "getTokenAccountsByOwner",
                [wallet, {"programId": program}, {"encoding": "jsonParsed", "commitment": "finalized"}],
                priority=20,
            )
            for row in result["value"]:
                info = row["account"]["data"]["parsed"]["info"]
                if info["owner"] != wallet or info["mint"] not in {*self.stocks(), USDC_MINT}:
                    continue
                address = public_address(row["pubkey"])
                self.journal.db.execute(
                    "INSERT OR IGNORE INTO accounts(address,wallet) VALUES(?,?)", (address, wallet)
                )
        self.last_discovery[wallet] = self.clock()

    async def poll_account(self, account: sqlite3.Row) -> None:
        config: dict = {"limit": 20 if account["head"] is None else 50, "commitment": "finalized"}
        if account["crawl_before"]:
            config["before"] = account["crawl_before"]
        rows = await self.rpc.call("getSignaturesForAddress", [account["address"], config], priority=20)
        if not isinstance(rows, list) or len(rows) > config["limit"]:
            raise ValueError("Invalid signature page")
        signatures = [signature_id(row["signature"]) for row in rows]
        head = account["head"]
        stop = signatures.index(head) if head in signatures else len(signatures)
        target = account["target_head"] or (signatures[0] if signatures else None)
        completed = head is None or stop < len(signatures) or len(signatures) < config["limit"]
        db = self.journal.db
        db.execute("BEGIN IMMEDIATE")
        try:
            added = db.execute("SELECT added FROM wallets WHERE address=?", (account["wallet"],)).fetchone()[
                0
            ]
            for index, signature in enumerate(signatures[:stop]):
                block_time = rows[index].get("blockTime")
                historical = head is None and (type(block_time) is not int or block_time < added)
                db.execute(
                    "INSERT OR IGNORE INTO transactions(wallet,signature,historical) VALUES(?,?,?)",
                    (account["wallet"], signature, int(historical)),
                )
            db.execute(
                "UPDATE accounts SET head=?,crawl_before=?,target_head=?,checked=? WHERE address=?",
                (
                    target if completed else head,
                    None if completed else signatures[-1],
                    None if completed else target,
                    self.clock(),
                    account["address"],
                ),
            )
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def correlation(self, record: dict) -> dict:
        if self.monitor is None:
            return {"reason": "monitor_unavailable"}
        from .monitor import FORWARD, REVERSE

        stocks = [leg for leg in record["legs"] if leg["asset"]]
        if len(stocks) != 1:
            return {"reason": "no_single_stock"}
        symbol = stocks[0]["asset"]
        direction = REVERSE if record["kind"] == "purchase" else FORWARD
        stored = self.journal.db.execute(
            "SELECT body FROM quotes WHERE asset=? AND direction=? AND observed<=? ORDER BY observed DESC LIMIT 1",
            (symbol, direction, record["block_time"]),
        ).fetchone()
        row = json.loads(stored[0]) if stored else self.monitor.results.get((symbol, direction))
        context = {"reason": "no_contemporaneous_quote", "checked_at": self.clock()}
        if row:
            offset = row["dex_observed_at"] - record["block_time"]
            context.update(
                quote_time=row["dex_observed_at"],
                offset_s=offset,
                monitor_cost_raw=row["cost_raw"],
                monitor_profit_raw=row["profit_raw"],
                monitor_quantity_raw=row["delivered_raw"],
            )
            if (
                -self.monitor.settings.monitor.max_quote_age_s <= offset <= 0
                and row["observed_at"] <= record["block_time"]
                and row["backpack_timestamp"] <= record["block_time"]
                and (row.get("backpack_valid_until") or row.get("backpack_rfq_valid_until") or 0)
                > record["block_time"]
            ):
                context["reason"] = "recent_monitor_quote"
        # A wallet observation prioritizes a new calculation, never an automatic trade.
        context["refresh_requested"] = self.monitor.request_refresh(symbol)
        context.update(self.monitor.budget_info(symbol))
        return context

    async def poll_once(self) -> None:
        wallets = self.journal.db.execute("SELECT address FROM wallets ORDER BY added").fetchall()
        for item in wallets:
            wallet = item["address"]
            if self.clock() - self.last_discovery.get(wallet, 0) >= 300:
                try:
                    await self.discover_accounts(wallet)
                except Exception as error:
                    self.last_error = error_code(error)
                    event("wallet_discovery_error", component="wallet_watch", code=self.last_error)
            owner = self.journal.db.execute("SELECT * FROM accounts WHERE address=?", (wallet,)).fetchone()
            tokens = self.journal.db.execute(
                "SELECT * FROM accounts WHERE wallet=? AND address<>? ORDER BY checked LIMIT 2",
                (wallet, wallet),
            ).fetchall()
            for account in [owner, *tokens]:
                try:
                    await self.poll_account(account)
                except Exception as error:
                    self.last_error = error_code(error)
                    event("wallet_poll_error", component="wallet_watch", code=self.last_error)
        self.update_stream()
        async with self.decode_lock:
            await self.decode_pending()
        self.last_poll = self.clock()

    async def decode_pending(self) -> None:
        pending = self.journal.db.execute(
            "SELECT * FROM transactions WHERE state='pending' AND retry_at<=? "
            "ORDER BY historical,rowid LIMIT 4",
            (self.clock(),),
        ).fetchall()
        for item in pending:
            try:
                tx = await self.rpc.transaction(item["signature"], priority=20)
                if tx is None:
                    raise ValueError("Transaction not yet available")
                record = decode_activity(tx, item["wallet"], item["signature"], self.stocks())
                record["correlation"] = (
                    self.correlation(record) if not item["historical"] else {"reason": "historical_import"}
                )
                relevant = any(leg["asset"] for leg in record["legs"])
                self.journal.db.execute(
                    "UPDATE transactions SET body=?,state='ready',notified=?,block_time=? "
                    "WHERE wallet=? AND signature=?",
                    (
                        json.dumps(record),
                        int(not relevant),
                        record["block_time"],
                        item["wallet"],
                        item["signature"],
                    ),
                )
                event(
                    "wallet_transaction",
                    component="wallet_watch",
                    signature=record["signature"],
                    state=record["kind"],
                    quantity=record["usdc_raw"],
                    elapsed_s=max(0, self.clock() - record["block_time"]),
                )
            except Exception as error:
                self.last_error = error_code(error)
                delay = min(300, POLL_S * 2 ** min(item["failures"], 4))
                self.journal.db.execute(
                    "UPDATE transactions SET retry_at=?,failures=failures+1 WHERE wallet=? AND signature=?",
                    (self.clock() + delay, item["wallet"], item["signature"]),
                )
                event("wallet_decode_error", component="wallet_watch", code=self.last_error)

    async def decode_loop(self) -> None:
        while True:
            self.decode_ready.clear()
            try:
                if self.monitor is None or self.monitor.assets:
                    async with self.decode_lock, asyncio.timeout(30):
                        await self.decode_pending()
            except Exception as error:
                self.last_error = error_code(error)
                event("wallet_decode_loop_error", component="wallet_watch", code=self.last_error)
            try:
                await asyncio.wait_for(self.decode_ready.wait(), 1)
            except TimeoutError:
                pass

    def start(self) -> None:
        self.lock = open(str(self.journal.path) + ".poll.lock", "a+")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            return  # Another runtime owns polling; this runtime can still read/register.
        self.task = asyncio.create_task(self.loop())
        self.decoder_task = asyncio.create_task(self.decode_loop())
        self.update_stream()
        if self.stream:
            self.stream_task = asyncio.create_task(self.stream.run())

    async def loop(self) -> None:
        while True:
            started = self.clock()
            try:
                if self.monitor is None or self.monitor.assets:
                    self.apply_size_requests()
                    self.focus_recent_history()
                    async with asyncio.timeout(45):
                        await self.poll_once()
            except Exception as error:
                self.last_error = error_code(error)
                event("wallet_watch_error", component="wallet_watch", code=self.last_error)
            await asyncio.sleep(max(1, POLL_S - (self.clock() - started)))

    async def close(self) -> None:
        tasks = [t for t in (self.task, self.stream_task, self.decoder_task) if t]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.lock:
            fcntl.flock(self.lock, fcntl.LOCK_UN)
            self.lock.close()
        self.journal.close()

    def text(self, symbol: str | None = None) -> str:
        wallets = self.journal.db.execute("SELECT address FROM wallets ORDER BY added").fetchall()
        if not wallets:
            return "Нет отслеживаемых адресов. /watch <адрес Solana>"
        checked = self.journal.db.execute("SELECT MAX(checked) FROM accounts").fetchone()[0]
        accounts = self.journal.db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] - len(wallets)
        cadence = max(POLL_S, ((accounts + 1) // 2) * POLL_S)
        stamp = local_time(checked) if checked else "ожидает первого запроса"
        stream_active = self.stream and self.stream.connected and self.stream.subscribed
        lines = [f"Транзакции · finalized · проверка {stamp} МСК"]
        if stream_active:
            lines.append(
                f"Поток транзакций: {self.stream.subscribed} подписок; HTTP-страховка кошелька каждые {POLL_S}с."
            )
        else:
            lines.append(f"Поток недоступен; HTTP кошелька каждые {POLL_S}с, token accounts ~{cadence}с.")
        if self.last_error:
            lines.append(f"Последняя ошибка: {self.last_error}; повтор включён.")
        for row in wallets:
            lines.append(f"https://solscan.io/account/{row['address']}")
        for record in self.journal.activities(symbol=symbol):
            # Historical correlation stays intact; current refresh/budget status
            # is rendered truthfully instead of repeating a stale queue promise.
            context = dict(record.get("correlation", {}))
            legs = [leg for leg in record["legs"] if leg["asset"]]
            if self.monitor and len(legs) == 1 and context.get("reason") == "no_contemporaneous_quote":
                context.update(self.monitor.budget_info(legs[0]["asset"]))
                context["refresh_requested"] = self.monitor.request_refresh(legs[0]["asset"])
                record["correlation"] = context
            lines.append(activity_text(record))
        return "\n\n".join(lines)


def local_time(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, ZoneInfo("Europe/Moscow")).strftime("%H:%M:%S")


def activity_text(record: dict) -> str:
    labels = {
        "sale": "продажа",
        "purchase": "покупка",
        "backpack_receipt": "приход из Backpack",
        "transfer": "перевод",
        "paired_transfer": "парные переводы",
        "failed": "ошибка транзакции",
    }
    stamp = datetime.fromtimestamp(record["block_time"], ZoneInfo("Europe/Moscow")).strftime(
        "%d.%m.%Y %H:%M:%S"
    )
    lines = [f"{stamp} МСК · {labels[record['kind']]}"]
    for leg in record["legs"]:
        if leg["asset"]:
            quantity = Decimal(abs(leg["raw"])) / 10 ** leg["decimals"]
            lines.append(f"{leg['asset']}: {'−' if leg['raw'] < 0 else '+'}{quantity.normalize():f} шт")
            if len(record["legs"]) == 1 and record["kind"] in {"sale", "purchase"}:
                value = Decimal(abs(record["usdc_raw"])) / 10**6
                lines.append(
                    f"{value.normalize():f} USDC · {value / quantity:.6f} USDC/шт · {record['router']}"
                )
    if record["kind"] == "sale":
        lines.append("Цена покупки в Backpack неизвестна; прибыль не подтверждена.")
    if record["fee_lamports"] or record["sol_transfer_lamports"]:
        lines.append(
            f"Сетевая комиссия {Decimal(record['fee_lamports']) / 10**9:f} SOL; "
            f"исходящие переводы {Decimal(record['sol_transfer_lamports']) / 10**9:f} SOL."
        )
    reason = record.get("correlation", {}).get("reason")
    if reason == "no_contemporaneous_quote":
        context = record["correlation"]
        if context.get("within_budget") is False:
            lines.append(
                f"Нет котировки на момент сделки. Сейчас минимум {context['minimum_buy_quantity']} шт ≈ {Decimal(context['minimum_cost_usdc']):.2f} USDC > лимита {context['budget_usdc']}; кол исключён по объёму."
            )
        elif context.get("refresh_requested"):
            lines.append("Нет котировки на момент сделки; новый расчёт поставлен в очередь.")
        else:
            lines.append("Нет котировки на момент сделки; новый расчёт пока недоступен.")
    elif reason == "recent_monitor_quote":
        comparison = record["correlation"]
        lines.append(
            f"Наш расчёт: объём {Decimal(comparison['monitor_cost_raw']) / 10**6:f} USDC; "
            f"net {Decimal(comparison['monitor_profit_raw']) / 10**6:f} USDC."
        )
    lines.append(f"https://solscan.io/tx/{record['signature']}")
    return "\n".join(lines)
