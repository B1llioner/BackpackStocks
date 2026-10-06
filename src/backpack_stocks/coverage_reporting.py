"""Render forward coverage separately from profitable volume search."""

from __future__ import annotations

from math import isfinite


def _freshness(venue: str, value: object) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and value > 0:
        return f"{venue} до {value:g}с"
    return f"{venue}: срок не указан"


def format_forward_coverage(snapshot: dict) -> list[str]:
    """Consume the monitor's classified coverage, without treating history as fresh."""
    coverage = snapshot.get("forward_coverage")
    if not isinstance(coverage, dict) or not coverage.get("enabled"):
        return []
    required = (
        "universe",
        "eligible",
        "checked",
        "pricedfresh",
        "refused",
        "waiting",
        "unverified",
        "first_pass_completed",
    )
    if any(type(coverage.get(key)) is not int or coverage[key] < 0 for key in required):
        return ["BP → DEX: сведения о проходе пока недоступны."]
    universe, eligible = coverage["universe"], coverage["eligible"]
    priced, refused, waiting, unverified = (
        coverage["pricedfresh"],
        coverage["refused"],
        coverage["waiting"],
        coverage["unverified"],
    )
    known_blocks = coverage.get("known_blocks", 0)
    if (
        universe != eligible + unverified
        or universe != priced + refused + waiting + unverified
        or type(known_blocks) is not int
        or not 0 <= known_blocks <= waiting
        or priced + refused > coverage["checked"]
        or coverage["checked"] > eligible
        or known_blocks > coverage["first_pass_completed"]
        or coverage["first_pass_completed"] > eligible
    ):
        return ["BP → DEX: сведения о проходе пока недоступны."]
    awaiting = waiting - known_blocks
    for key in ("pending", "waitingbank", "currentroute_missing", "unattempted"):
        if key in coverage and (type(coverage[key]) is not int or not 0 <= coverage[key] <= eligible):
            return ["BP → DEX: сведения о проходе пока недоступны."]
    if (
        coverage.get("waitingbank", 0) > awaiting
        or coverage.get("currentroute_missing", 0) > coverage.get("waitingbank", 0)
        or coverage.get("unattempted", 0) > eligible - coverage["first_pass_completed"]
    ):
        return ["BP → DEX: сведения о проходе пока недоступны."]
    completed = coverage["first_pass_completed"]
    done = bool(coverage.get("done_universe") and eligible > 0 and completed == eligible)
    lines = [
        f"BP → DEX · первичный проход {completed}/{eligible} допустимых"
        + (" · завершён" if done else " · продолжается"),
        "Минимальная законная партия; результат относится только к этому объёму.",
        f"Сейчас из {universe}: свежие пары BP+DEX {priced}; отказ BP {refused}; "
        f"ожидают расчёта {awaiting}; закрыта сессия / несовместима партия {known_blocks}; "
        f"неподтверждены {unverified}.",
        f"Действующая проверка BP: {coverage['checked']}/{eligible}; отказ не даёт численной пары.",
    ]
    pending, waitingbank, missing = (
        coverage.get("pending"),
        coverage.get("waitingbank"),
        coverage.get("currentroute_missing"),
    )
    if all(type(value) is int and value >= 0 for value in (pending, waitingbank, missing)):
        lines.append(
            f"В работе {pending}; ожидают свежих данных DEX {waitingbank}; "
            f"нет маршрута по актуальным данным {missing}."
        )
    if "unattempted" in coverage:
        lines.append(f"Без попытки или установленной причины: {coverage['unattempted']}/{eligible}.")
    lines.append(
        "Свежесть цены: "
        + _freshness("BP", snapshot.get("backpack_price_ttl_s"))
        + "; "
        + _freshness("DEX", snapshot.get("dex_bank_ttl_s"))
        + "."
    )
    return lines
