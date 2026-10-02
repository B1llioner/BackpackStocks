"""Exact stock RFQ valuation, with an optional bounded Rust batch kernel."""

from __future__ import annotations

import importlib
import re
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation

ValueRow = tuple[int, int, str, int, bool]
MAX_BATCH = 100_000


class StockValueCalculator:
    def __init__(self, backend: str = "auto"):
        if backend not in {"auto", "rust", "python"}:
            raise ValueError("Invalid stock valuation backend")
        self.native = None
        if backend != "python":
            try:
                module = importlib.import_module("_pool_math")
            except ModuleNotFoundError as error:
                if error.name != "_pool_math" or backend == "rust":
                    raise
            else:
                if getattr(module, "STOCK_MONEY_MODEL", None) == "stock-usdc-integer-v1":
                    self.native = module.stock_values
                elif backend == "rust":
                    raise ValueError("Native stock valuation model mismatch; rebuild pool-math")
        self.backend = "rust" if self.native else "python"

    @staticmethod
    def reference(row: ValueRow) -> int | None:
        quantity, decimals, text, fees, buy = row
        try:
            if re.fullmatch(r"\+?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", text) is None:
                return None
            price = Decimal(text)
            if not price.is_finite() or price <= 0 or not -255 <= price.adjusted() <= 255:
                return None
            numerator, denominator = price.as_integer_ratio()
            numerator *= quantity * (10000 + fees if buy else 10000 - fees) * 10**6
            denominator *= 10**decimals * 10000
            floor, remainder = divmod(numerator, denominator)
            value = floor + int(buy and remainder != 0)
            return value if 0 <= value < 2**64 else None
        except (InvalidOperation, ValueError, OverflowError):
            return None

    def batch(self, rows: Sequence[ValueRow]) -> list[int | None]:
        if len(rows) > MAX_BATCH:
            raise ValueError("Stock valuation batch exceeds bound")
        for quantity, decimals, text, fees, buy in rows:
            if (
                type(quantity) is not int
                or not 0 < quantity < 2**64
                or type(decimals) is not int
                or not 0 <= decimals <= 255
                or type(text) is not str
                or not 0 < len(text) <= 128
                or type(fees) is not int
                or not 0 <= fees <= 10000
                or type(buy) is not bool
            ):
                raise ValueError("Invalid stock valuation job")
        if self.native is None:
            return [self.reference(row) for row in rows]
        results = self.native(list(rows))
        # Only precision/overflow limitations use the exact Python integer path.
        # Invalid prices fail their own row without masking other valid prices.
        return [
            value if code == 0 else self.reference(row) if code == 5 else None
            for row, (value, code) in zip(rows, results, strict=True)
        ]

    def value(self, quantity: int, decimals: int, price: str, fee_bps: int, *, buy: bool) -> int:
        result = self.batch([(quantity, decimals, price, fee_bps, buy)])[0]
        if result is None:
            raise ValueError("Invalid or overflowing stock valuation")
        return result
