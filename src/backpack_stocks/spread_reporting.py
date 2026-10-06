"""Compact spreads from the monitor's strictly fresh numerical price pairs."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from fractions import Fraction
from math import isfinite
from zoneinfo import ZoneInfo

FORWARD = "backpack_to_jupiter"
REVERSE = "jupiter_to_backpack"


def dex_price(pair: dict) -> Decimal:
    if pair.get("quoted_output_raw", 0) > 0:
        if pair["direction"] == FORWARD:
            delivered = (
                Decimal(pair["delivered_raw"]) / 10 ** pair["stock_decimals"]
                if "delivered_raw" in pair
                else Decimal(pair["sell_quantity"])
            )
            if delivered > 0:
                return Decimal(pair["quoted_output_raw"]) / 1_000_000 / delivered
        else:
            return Decimal(pair["cost_raw"]) / 1_000_000 / (
                Decimal(pair["quoted_output_raw"]) / 10 ** pair["stock_decimals"]
            )
    return Decimal(pair["dex_price"])


def signed_percent(value: Decimal) -> str:
    if value and abs(value) < Decimal("0.005"):
        for precision in (4, 6):
            if value.quantize(Decimal(1).scaleb(-precision)):
                return f"{value:+.{precision}f}%"
        return f"{value:+.2E}%"
    return f"{value:+.2f}%"


def best_spreads(prices: list[dict], *, limit: int = 5) -> list[dict]:
    best: dict[tuple[str, str], dict] = {}
    for pair in prices:
        try:
            if (
                pair.get("fresh") is False
                or pair.get("backpack_source") not in {"rfq", "indicative", "spot"}
                or pair["direction"] not in {FORWARD, REVERSE}
                or type(pair["cost_raw"]) is not int
                or pair["cost_raw"] <= 0
                or type(pair["profit_raw"]) is not int
                or not isinstance(pair["asset"], str)
                or not pair["asset"]
                or not isfinite(pair["updated_at"])
                or pair["updated_at"] < 0
            ):
                continue
            bp, dex = Decimal(pair["backpack_price"]), dex_price(pair)
            if not all(value.is_finite() and value > 0 for value in (bp, dex)):
                continue
            datetime.fromtimestamp(pair["updated_at"], ZoneInfo("Europe/Moscow"))
            rank = Fraction(pair["profit_raw"], pair["cost_raw"]), pair["updated_at"]
            key = pair["asset"], pair["direction"]
            previous = best.get(key)
            if previous is None or rank > (
                Fraction(previous["profit_raw"], previous["cost_raw"]), previous["updated_at"]
            ):
                best[key] = pair
        except (KeyError, TypeError, ValueError, ArithmeticError, OSError):
            continue
    return sorted(
        best.values(),
        key=lambda pair: (
            -Fraction(pair["profit_raw"], pair["cost_raw"]),
            pair["asset"],
            pair["direction"],
        ),
    )[: min(5, max(0, limit))]


def format_spreads(prices: list[dict]) -> list[str]:
    pairs = best_spreads(prices)
    if not pairs:
        return []
    lines = ["Свежие спреды · net после расходов и slippage"]
    for pair in pairs:
        forward = pair["direction"] == FORWARD
        dex_label = "Raydium" if pair.get("quote_backend") == "raydium" else "Jup"
        bp_label = "BP стакан" if pair["backpack_source"] == "spot" else "BP"
        direction = f"{bp_label} → {dex_label}" if forward else f"{dex_label} → {bp_label}"
        bp, dex = Decimal(pair["backpack_price"]), dex_price(pair)
        spread = (dex / bp - 1 if forward else bp / dex - 1) * 100
        net = Decimal(pair["profit_raw"]) * 100 / pair["cost_raw"]
        profit = Decimal(pair["profit_raw"]) / 1_000_000
        signed = f"{profit:+.2f}" if abs(profit) >= Decimal("0.01") or not profit else f"{profit:+.6f}"
        cost = format(Decimal(pair["cost_raw"]) / 1_000_000, ".6f").rstrip("0").rstrip(".")
        displayed_dex = format(dex, ".12f").rstrip("0").rstrip(".")
        timestamp = datetime.fromtimestamp(pair["updated_at"], ZoneInfo("Europe/Moscow"))
        lines.extend(
            [
                "",
                f"{pair['asset']} · {direction} · {timestamp:%H:%M:%S} МСК",
                f"Спред цены: {signed_percent(spread)}; net: {signed_percent(net)} ({signed} USDC)",
                f"Объём: {cost} USDC · {bp_label} {pair['backpack_price']} / {dex_label} {displayed_dex} USDC/шт",
            ]
        )
    if any(pair["backpack_source"] == "indicative" for pair in pairs):
        lines.append("\nПубличная BP: для входа нужна своя RFQ на выбранный объём.")
    if any(pair["direction"] == REVERSE for pair in pairs):
        lines.append("Продажа на BP после зачисления; цена за время перевода может измениться.")
    return lines


def compact_coverage(snapshot: dict) -> list[str]:
    coverage = snapshot.get("forward_coverage", {})
    if not isinstance(coverage, dict) or not coverage.get("enabled"):
        return []
    eligible, fresh = (
        coverage.get("eligible"),
        coverage.get("pricedfresh"),
    )
    if type(eligible) is not int or type(fresh) is not int or not 0 <= fresh <= eligible:
        return ["BP → DEX: сведения о покрытии пока недоступны."]
    checked, refused, waitingbank = (
        coverage.get("checked"),
        coverage.get("refused"),
        coverage.get("waitingbank"),
    )
    prefix = f"BP → DEX: свежие пары {fresh}/{eligible}"
    if type(checked) is not int or type(refused) is not int or type(waitingbank) is not int:
        return [prefix + "; причины ожидания: /monitor."]
    if not (0 <= refused <= checked <= eligible and 0 <= waitingbank <= checked - refused - fresh):
        return ["BP → DEX: сведения о покрытии пока недоступны."]
    return [
        prefix + f"; BP без свежей цены {eligible - checked + refused}; ждут DEX {waitingbank}."
    ]
