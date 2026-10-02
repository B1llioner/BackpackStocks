"""Manual stock RFQs: frontend estimates, non-binding quotes, bounded purchases.

No browser session secrets are exported. An operator supplies their own Backpack
API seed through the existing environment loader. Only buy(..., live=True) sends
an Immediate RFQ; quotes always use AwaitAccept and never accept a quote.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
import secrets
import time
from decimal import Decimal
from pathlib import Path

from .backpack import Backpack, BackpackAuth
from .models import Settings, dec
from .proxy import resolve_proxy
from .runtime import required_env
from .sessions import active_session, validate_quantity
from .transport import RemoteError, UnknownOutcome


class StockCommandError(ValueError):
    """A fixed, operator-readable message containing no remote payload or secrets."""


def stock_symbol(value: str) -> str:
    symbol = value.upper().removesuffix(".US")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*(?:\.[A-Z0-9]+)?", symbol):
        raise StockCommandError("Use a US stock ticker, for example PTN, FLWS or BRK.B")
    return symbol + ".US"


def positive(value: str) -> Decimal:
    if len(value) > 50 or not re.fullmatch(r"\d+(?:\.\d+)?", value):
        raise StockCommandError("Use a positive decimal without grouping or exponent")
    number = dec(value)
    if number <= 0:
        raise StockCommandError("Positive quantity/price required")
    return number


async def estimate(backpack: Backpack, symbol: str, quantity: str) -> dict:
    ticker = symbol.removesuffix(".US")
    snapshots = await backpack.public("/wapi/v1/stocks/snapshots", tickers=ticker)
    snapshot = next((row for row in snapshots if row.get("ticker") == ticker), None)
    if snapshot is None or snapshot.get("fmv") is None:
        raise StockCommandError("Backpack frontend FMV estimate unavailable")
    price = dec(str(snapshot["fmv"]))
    if price <= 0:
        raise StockCommandError("Invalid Backpack FMV estimate")
    return {
        "symbol": symbol,
        "quantity": quantity,
        "estimated_price": str(price),
        "estimated_cost_usdc": str(positive(quantity) * price),
        "source": "Backpack frontend snapshots.fmv; estimate only, no RFQ price",
        "confirmed": False,
    }


async def validate_session(backpack: Backpack, symbol: str, quantity: str) -> None:
    securities, sessions, holidays = await asyncio.gather(
        backpack.securities(), backpack.sessions(), backpack.holidays()
    )
    security = next((row for row in securities if row.get("asset") == symbol), None)
    if security is None:
        raise StockCommandError("Stock is not available for Backpack RFQs")
    session = active_session(sessions, holidays)
    if session is None:
        raise StockCommandError("No RFQ trading session is open")
    try:
        validate_quantity(security, session, quantity)
    except ValueError as error:
        raise StockCommandError(str(error)) from None


def matching_rfq(rfq: dict, *, symbol: str, quantity: str, client_id: int, mode: str) -> None:
    if (
        not isinstance(rfq.get("rfqId"), str)
        or not rfq["rfqId"].isdigit()
        or rfq.get("symbol") != symbol
        or rfq.get("side") != "Bid"
        or rfq.get("clientId") != client_id
        or dec(rfq.get("quantity", "0")) != dec(quantity)
        or rfq.get("executionMode") != mode
    ):
        raise StockCommandError("Backpack RFQ response does not match the request")


async def quote(backpack: Backpack, symbol: str, quantity: str, timeout: float) -> dict:
    result = await estimate(backpack, symbol, quantity)
    await validate_session(backpack, symbol, quantity)
    market = symbol + "_USDC_RFQ"
    client_id = secrets.randbits(32)
    rfq = await backpack.submit_rfq(quantity, client_id, symbol=market)
    matching_rfq(rfq, symbol=market, quantity=quantity, client_id=client_id, mode="AwaitAccept")
    rfq_id = rfq["rfqId"]
    result.update(rfq_id=rfq_id, quote_poll_count=0, observed_quote_count=0)
    deadline = time.monotonic() + timeout
    try:
        # Inspection never accepts a quote. Reserving 1.5 seconds for acceptance
        # here would discard the entire acceptance window of a 2.5-second stock
        # RFQ and miss quotes submitted at the end of its first second.
        while time.monotonic() < deadline and time.time() * 1000 < rfq["expiryTime"]:
            records = await backpack.rfqs(rfq_id, symbol=market)
            result["quote_poll_count"] += 1
            if time.time() * 1000 >= rfq["expiryTime"]:
                break
            matching = [row for row in records if row["rfq"]["rfqId"] == rfq_id]
            if matching:
                matching_rfq(
                    matching[0]["rfq"],
                    symbol=market,
                    quantity=quantity,
                    client_id=client_id,
                    mode="AwaitAccept",
                )
                candidates = [
                    q
                    for q in matching[0]["quotes"]
                    if q.get("rfqId") == rfq_id and q.get("status") == "New" and dec(q["askPrice"]) > 0
                ]
                result["observed_quote_count"] = max(result["observed_quote_count"], len(candidates))
                if candidates:
                    best = min(candidates, key=lambda q: dec(q["askPrice"]))
                    result.update(
                        {
                            "confirmed": True,
                            "rfq_price": best["askPrice"],
                            "rfq_cost_usdc": str(positive(quantity) * dec(best["askPrice"])),
                            "source": "Backpack AwaitAccept RFQ askPrice; cancelled after inspection",
                        }
                    )
                    return result
            await asyncio.sleep(min(0.5, max(0, deadline - time.monotonic())))
        result.update(reason="no_rfq_candidate", buy_available=False)
        return result
    finally:
        # Cancel only our non-binding request. Never accept, refresh or cancel a user order.
        try:
            await backpack.cancel_rfq(rfq_id)
        except RemoteError:
            # Expired requests cannot always be cancelled; they still cannot execute.
            result["cleanup"] = "cancellation_unconfirmed; AwaitAccept cannot buy without acceptance"


class OrderJournal:
    """One lock and one durable, private file per operator-chosen request ID."""

    def __init__(self, folder: Path, request_id: str):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", request_id):
            raise StockCommandError("request-id must be 1–64 letters, digits, underscores or hyphens")
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = folder / (request_id + ".json")
        self.lock = os.open(folder / (request_id + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self.lock)
            raise

    def read(self) -> dict | None:
        if not self.path.exists():
            return None
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd) as handle:
            return json.load(handle)

    def save(self, record: dict) -> None:
        temporary = self.path.with_suffix(".tmp")
        fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(record, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
        directory = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def close(self) -> None:
        os.close(self.lock)


async def reconcile(backpack: Backpack, record: dict, journal: OrderJournal) -> dict:
    market = record["params"]["symbol"]
    if not record.get("rfq_id"):
        # A missing response must be reconciled by the original clientId, never resubmitted.
        rows = await backpack.rfqs(symbol=market)
        rows = [row["rfq"] for row in rows] + await backpack.rfq_history(symbol=market)
        matching = [r for r in rows if r.get("clientId") == record["params"]["clientId"]]
        if not matching:
            return record
        rfq = matching[0]
        matching_rfq(
            rfq,
            symbol=market,
            quantity=record["params"]["quantity"],
            client_id=record["params"]["clientId"],
            mode="Immediate",
        )
        record["rfq_id"] = rfq["rfqId"]
    fills = await backpack.rfq_fills(record["rfq_id"])
    actual = [f for f in fills if f.get("rfqId") == record["rfq_id"]]
    if actual:
        quantity = sum((dec(f["fillQuantity"]) for f in actual), Decimal(0))
        cost = sum((dec(f["fillQuoteQuantity"]) for f in actual), Decimal(0))
        if quantity <= 0 or cost < 0:
            raise StockCommandError("Invalid Backpack fill amounts")
        maximum_cost = quantity * dec(record["params"]["price"]) * (1 + dec(record["fee_bps"]) / 10000)
        if quantity > dec(record["params"]["quantity"]) or cost > maximum_cost:
            raise StockCommandError(
                "Backpack fills exceed the quantity/price limit; reconcile the recorded order"
            )
        record.update(
            state="FILLED" if quantity == dec(record["params"]["quantity"]) else "PARTIAL",
            filled_quantity=str(quantity),
            cost_usdc=str(cost),
            average_price=str(cost / quantity),
        )
        journal.save(record)
        if record["state"] == "FILLED":
            return record
    rows = await backpack.rfq_history(symbol=market, rfq_id=record["rfq_id"])
    found = [r for r in rows if r.get("rfqId") == record["rfq_id"]]
    if found:
        status = found[0].get("status")
        if status in {"Expired", "Cancelled", "Canceled", "Rejected", "Filled"}:
            # A terminal status without its fill receipt must not be reported as a purchase.
            record["state"] = (
                "PARTIAL" if actual else "AWAITING_FILL_RECEIPT" if status == "Filled" else status.upper()
            )
            record["exchange_status"] = status
    journal.save(record)
    return record


async def buy(
    backpack: Backpack,
    settings: Settings,
    *,
    symbol: str,
    quantity: str,
    max_price: str,
    live: bool,
    request_id: str,
    timeout: float,
    folder: Path,
) -> dict:
    quantity_value, limit = positive(quantity), positive(max_price)
    maximum_cost = quantity_value * limit * (1 + Decimal(settings.policy.backpack_fee_bps) / 10000)
    params: dict = {
        "symbol": symbol + "_USDC_RFQ",
        "side": "Bid",
        "quantity": quantity,
        "executionMode": "Immediate",
        "price": max_price,
        "autoBorrow": False,
        "autoLend": False,
        "autoLendRedeem": False,
        "autoBorrowRepay": False,
    }
    if not live:
        if maximum_cost > dec(settings.policy.max_cost_usdc):
            raise StockCommandError("Maximum purchase cost exceeds policy.max_cost_usdc")
        return {
            "state": "DRY_RUN",
            "request_id": request_id,
            "maximum_cost_usdc": str(maximum_cost),
            "params": params,
        }
    journal = OrderJournal(folder, request_id)
    try:
        if backpack.auth is None:
            raise StockCommandError("Authenticated Backpack client required")
        record = journal.read()
        if record is not None:
            if {k: v for k, v in record["params"].items() if k != "clientId"} != params:
                raise StockCommandError("request-id already belongs to a different purchase")
            if record["account"] != backpack.auth.public_key:
                raise StockCommandError("request-id belongs to another Backpack account")
            if record["state"] in {"SUBMITTING", "UNKNOWN", "SUBMITTED", "PARTIAL", "AWAITING_FILL_RECEIPT"}:
                return await reconcile(backpack, record, journal)
            return record
        if maximum_cost > dec(settings.policy.max_cost_usdc):
            raise StockCommandError("Maximum purchase cost exceeds policy.max_cost_usdc")
        await validate_session(backpack, symbol, quantity)
        balances, account = await asyncio.gather(backpack.balances(), backpack.account())
        if account.get("liquidating") is not False:
            raise StockCommandError("Account is liquidating or its liquidation status is unknown")
        # autoBorrowSettlements controls general collateral reconciliation, not
        # this RFQ. Its autoBorrow=False plus available USDC prevents borrowing
        # for the purchase without changing unrelated account settings.
        if dec(balances.get("USDC", {}).get("available", "0")) < maximum_cost:
            raise StockCommandError(
                "Insufficient available USDC for the maximum purchase cost; USD/USDT are separate assets"
            )
        params["clientId"] = secrets.randbits(32)
        record = {
            "request_id": request_id,
            "params": params,
            "state": "SUBMITTING",
            "account": backpack.auth.public_key,
            "fee_bps": str(settings.policy.backpack_fee_bps),
        }
        journal.save(record)
        try:
            response = await backpack.private("POST", "/api/v1/rfq", "rfqSubmit", **params)
            matching_rfq(
                response,
                symbol=params["symbol"],
                quantity=quantity,
                client_id=params["clientId"],
                mode="Immediate",
            )
            record.update(state="SUBMITTED", rfq_id=response["rfqId"])
        except BaseException as error:
            record["state"] = (
                "REJECTED"
                if isinstance(error, RemoteError) and not isinstance(error, UnknownOutcome)
                else "UNKNOWN"
            )
            journal.save(record)
            raise
        journal.save(record)
        deadline = time.monotonic() + timeout
        while True:
            record = await reconcile(backpack, record, journal)
            if (
                record["state"] not in {"SUBMITTED", "PARTIAL", "AWAITING_FILL_RECEIPT"}
                or time.monotonic() >= deadline
            ):
                return record
            await asyncio.sleep(min(1, max(0, deadline - time.monotonic())))
    finally:
        journal.close()


async def run_stock_command(args, settings: Settings) -> dict:
    symbol = stock_symbol(args.symbol)
    quantity = str(positive(args.quantity))
    auth = None
    if args.command == "stock-quote" and args.rfq or args.command == "stock-buy" and args.live:
        try:
            auth = BackpackAuth(required_env(settings.backpack_seed_env))
        except ValueError:
            raise StockCommandError(
                "Set backpack_seed_env's variable in the local .env to a base64 32-byte Backpack API seed"
            ) from None
    backpack = Backpack(auth, interval=settings.backpack_interval_s, proxy=resolve_proxy("BACKPACK"))
    try:
        if args.command == "stock-quote":
            if args.rfq:
                return await quote(backpack, symbol, quantity, args.timeout)
            return await estimate(backpack, symbol, quantity)
        result = await buy(
            backpack,
            settings,
            symbol=symbol,
            quantity=quantity,
            max_price=str(positive(args.max_price)),
            live=args.live,
            request_id=args.request_id,
            timeout=args.timeout,
            folder=Path(args.journal_dir),
        )
        return {k: v for k, v in result.items() if k != "account"}
    finally:
        await backpack.close()
