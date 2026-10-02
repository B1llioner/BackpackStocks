"""Explicitly owned native calculator with a reference implementation fallback."""

from __future__ import annotations

import importlib
from collections.abc import Sequence

from .pool_math import clmm_interval_amounts, cpmm_exact_in

FIELDS = (
    "input_raw",
    "received_raw",
    "trade_fee_raw",
    "creator_fee_raw",
    "input_transfer_fee_raw",
    "output_transfer_fee_raw",
    "pool_output_raw",
    "output_raw",
)
MAX_BATCH = 100_000


class PoolCalculator:
    def __init__(
        self,
        *,
        backend: str = "auto",
        threads: int = 4,
        parallel_threshold: int = 65536,
        interval_parallel_threshold: int = 4096,
    ):
        if backend not in {"auto", "python", "rust"} or type(threads) is not int or not 1 <= threads <= 16:
            raise ValueError("Invalid pool calculator backend/workers")
        if type(parallel_threshold) is not int or not 1 <= parallel_threshold <= MAX_BATCH:
            raise ValueError("Invalid parallel threshold")
        if type(interval_parallel_threshold) is not int or not 1 <= interval_parallel_threshold <= MAX_BATCH:
            raise ValueError("Invalid interval parallel threshold")
        self.native = None
        self.threshold = parallel_threshold
        self.interval_threshold = interval_parallel_threshold
        if backend != "python":
            try:
                module = importlib.import_module("_pool_math")
            except ModuleNotFoundError as error:
                if error.name != "_pool_math" or backend == "rust":
                    raise
            else:
                if module.MODEL != "cp-swap-59fb845a-exact-in":
                    raise ValueError("Native pool math model mismatch")
                self.native = module.PoolMath(threads)
        self.backend = "rust" if self.native is not None else "python"

    @staticmethod
    def _validate(rows: Sequence[tuple]) -> None:
        if len(rows) > MAX_BATCH:
            raise ValueError("Pool batch exceeds 100000 rows")
        for row in rows:
            if len(row) != 9 or type(row[5]) is not bool:
                raise ValueError("Invalid CPMM row shape")
            for index, value in enumerate(row):
                if index == 5:
                    continue
                bound = 2**32 if index in {3, 4} else 2**16 if index == 7 else 2**64
                if type(value) is not int or not 0 <= value < bound:
                    raise ValueError("Invalid unsigned amount/rate")

    def cpmm_batch(self, rows: Sequence[tuple], *, parallel: bool | None = None) -> list[dict]:
        self._validate(rows)
        if parallel is not None and type(parallel) is not bool:
            raise ValueError("Parallel override must be boolean")
        use_parallel = len(rows) >= self.threshold if parallel is None else parallel
        if self.native is not None:
            result = self.native.cpmm_batch(list(rows), use_parallel)
            return [
                {"error": error} if error else dict(zip(FIELDS, values, strict=True))
                for values, error in result
            ]
        output = []
        q: dict
        for amount, rin, rout, trade, creator, creator_input, transfer_in, transfer_bps, transfer_max in rows:
            try:
                q = cpmm_exact_in(
                    amount,
                    rin,
                    rout,
                    trade,
                    creator_fee_rate=creator,
                    creator_on_input=creator_input,
                    input_transfer_fee=transfer_in,
                    output_transfer_bps=transfer_bps,
                    output_transfer_maximum=transfer_max,
                )
            except ValueError:
                q = {"error": "invalid_or_unquotable_input"}
            output.append(q)
        return output

    def clmm_intervals(
        self, rows: Sequence[tuple[int, int, int]], *, parallel: bool | None = None
    ) -> list[dict]:
        if len(rows) > MAX_BATCH or any(
            len(row) != 3 or any(type(x) is not int or not 0 <= x < 2**128 for x in row) for row in rows
        ):
            raise ValueError("Invalid/bounded CLMM interval inputs")
        if parallel is not None and type(parallel) is not bool:
            raise ValueError("Parallel override must be boolean")
        use_parallel = len(rows) >= self.interval_threshold if parallel is None else parallel
        if self.native is not None:
            results = self.native.clmm_intervals(list(rows), use_parallel)
            output = []
            for (_, start, end), (values, error) in zip(rows, results, strict=True):
                keys = ("input_a", "output_b") if end < start else ("input_b", "output_a")
                output.append({"error": error} if error else dict(zip(keys, values, strict=True)))
            return output
        output = []
        q: dict
        for row in rows:
            try:
                q = clmm_interval_amounts(*row)
                if max(q.values()) >= 2**128:
                    raise ValueError("CLMM interval output overflow")
            except ValueError:
                q = {"error": "invalid_or_unquotable_interval"}
            output.append(q)
        return output


class PreparedCpmmPools:
    """Immutable frame: reserves/fees parsed once; compact amounts passed on each quote.

    Frames are scoped to a coherent snapshot. The caller owns slot/TTL checks and
    must replace this object when reserves or issuer capabilities change.
    """

    def __init__(self, calculator: PoolCalculator, states: Sequence[dict], *, parallel_threshold: int = 4096):
        if len(states) > MAX_BATCH:
            raise ValueError("Too many prepared pools")
        if type(parallel_threshold) is not int or not 1 <= parallel_threshold <= MAX_BATCH:
            raise ValueError("Invalid prepared parallel threshold")
        self.calculator = calculator
        self.threshold = parallel_threshold
        self.pool_count = len(states)
        self.book = None
        rows = []
        for s in states:
            if s.get("kind") != "cpmm" or not s.get("quoteSupported"):
                raise ValueError("Unsupported pool cannot enter prepared frame")
            a, b = (
                s.get("transferFeeA", {"bps": 0, "maximum": 0}),
                s.get("transferFeeB", {"bps": 0, "maximum": 0}),
            )
            creator = s["fees"]["creatorFeeRate"] if s["enableCreatorFee"] else 0
            row = (
                int(s["reserveA"]),
                int(s["reserveB"]),
                s["fees"]["tradeFeeRate"],
                creator,
                s["feeOn"],
                a["bps"],
                a["maximum"],
                b["bps"],
                b["maximum"],
            )
            if (
                any(type(x) is not int or x < 0 for x in row)
                or min(row[:2]) <= 0
                or row[4] not in {0, 1, 2}
                or row[2] + row[3] >= 1_000_000
                or max(row[5], row[7]) > 10000
                or max(row[0], row[1], row[6], row[8]) >= 2**64
            ):
                raise ValueError("Invalid prepared pool frame")
            rows.append(row)
        self._rows = tuple(rows)
        if calculator.native is not None:
            module = importlib.import_module("_pool_math")
            self.book = module.PoolBook(calculator.native, rows)

    def quote_batch(
        self, jobs: Sequence[tuple[int, bool, int, int]], *, parallel: bool | None = None
    ) -> list[tuple[int, int, int]]:
        if len(jobs) > MAX_BATCH:
            raise ValueError("Prepared quote batch exceeds 100000 rows")
        if parallel is not None and type(parallel) is not bool:
            raise ValueError("Parallel override must be boolean")
        # Every externally supplied amount is checked; cached reserves are not reparsed.
        for index, direction, amount, slip in jobs:
            if (
                type(index) is not int
                or not 0 <= index < self.pool_count
                or type(direction) is not bool
                or type(amount) is not int
                or not 0 <= amount < 2**64
                or type(slip) is not int
                or not 0 <= slip <= 500
            ):
                raise ValueError("Invalid prepared quote job")
        use_parallel = len(jobs) >= self.threshold if parallel is None else parallel
        if self.book is not None:
            return self.book.quote_batch(list(jobs), use_parallel)
        output = []
        for index, direction, amount, slip in jobs:
            a, b, trade, creator, fee_on, a_bps, a_max, b_bps, b_max = self._rows[index]
            rin, rout, in_bps, in_max, out_bps, out_max = (
                (a, b, a_bps, a_max, b_bps, b_max) if direction else (b, a, b_bps, b_max, a_bps, a_max)
            )
            try:
                q = cpmm_exact_in(
                    amount,
                    rin,
                    rout,
                    trade,
                    creator_fee_rate=creator,
                    creator_on_input=fee_on == 0 or fee_on == (1 if direction else 2),
                    input_transfer_fee=min(in_max, (amount * in_bps + 9999) // 10000),
                    output_transfer_bps=out_bps,
                    output_transfer_maximum=out_max,
                )
            except ValueError as error:
                code = (
                    3
                    if str(error) == "Fees consume input"
                    else 4
                    if str(error) == "Amount rounds to zero output"
                    else 1
                )
                output.append((0, 0, code))
            else:
                output.append((q["output_raw"], q["output_raw"] * (10000 - slip) // 10000, 0))
        return output

    def quote_grid(
        self,
        seeds: Sequence[tuple[int, bool, int]],
        multipliers: Sequence[int],
        *,
        slippage_bps: int = 50,
        parallel: bool | None = None,
    ) -> list[tuple[int, int, int, int]]:
        count = len(seeds) * len(multipliers)
        if count > MAX_BATCH or len(seeds) > MAX_BATCH or len(multipliers) > MAX_BATCH:
            raise ValueError("Quote grid exceeds 100000 jobs")
        if type(slippage_bps) is not int or not 0 <= slippage_bps <= 500:
            raise ValueError("Invalid grid slippage")
        if any(type(m) is not int or not 1 <= m < 2**32 for m in multipliers):
            raise ValueError("Invalid integer size multiplier")
        for index, direction, amount in seeds:
            if (
                type(index) is not int
                or not 0 <= index < self.pool_count
                or type(direction) is not bool
                or type(amount) is not int
                or not 0 <= amount < 2**64
            ):
                raise ValueError("Invalid grid pool seed")
        if parallel is not None and type(parallel) is not bool:
            raise ValueError("Parallel override must be boolean")
        use_parallel = count >= self.threshold if parallel is None else parallel
        if self.book is not None:
            return self.book.quote_grid(list(seeds), list(multipliers), slippage_bps, use_parallel)
        output = []
        for index, direction, amount in seeds:
            for multiplier in multipliers:
                scaled = amount * multiplier
                if scaled >= 2**64:
                    output.append((0, 0, 0, 5))
                    continue
                out, minimum, error = self.quote_batch([(index, direction, scaled, slippage_bps)])[0]
                output.append((0, 0, 0, error) if error else (scaled, out, minimum, 0))
        return output
