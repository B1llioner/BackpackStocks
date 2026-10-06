from copy import deepcopy

import pytest

from backpack_stocks.coverage_reporting import format_forward_coverage


def report(**updates):
    coverage = {
        "enabled": True,
        "universe": 5,
        "eligible": 4,
        "checked": 3,
        "pricedfresh": 1,
        "refused": 1,
        "pending": 1,
        "waiting": 2,
        "waitingbank": 1,
        "currentroute_missing": 0,
        "unverified": 1,
        "first_pass_completed": 3,
        "done_universe": False,
        "minimum_probe": True,
        "profitable_search_separate": True,
        "rows": [],
    }
    coverage.update(updates)
    return {
        "backpack_price_ttl_s": 15,
        "dex_bank_ttl_s": 8,
        "forward_coverage": coverage,
    }


def test_report_keeps_negative_fresh_pairs_separate_from_refusals_and_pass_progress():
    snapshot = report(
        rows=[
            {"asset": "NEG.US", "state": "fresh_priced", "quantity": "1", "profit_raw": -1000000},
            {"asset": "REFUSED.US", "state": "refused", "reason": "rfq_insufficient_balance"},
        ]
    )
    before = deepcopy(snapshot)
    text = "\n".join(format_forward_coverage(snapshot))
    assert "первичный проход 3/4 допустимых · продолжается" in text
    assert "Минимальная законная партия; результат относится только к этому объёму" in text
    assert "из 5: свежие пары BP+DEX 1; отказ BP 1; ожидают расчёта 2" in text
    assert "закрыта сессия / несовместима партия 0; неподтверждены 1" in text
    assert "отказ не даёт численной пары" in text
    assert "BP до 15с; DEX до 8с" in text
    assert "прибыли нет" not in text
    assert snapshot == before


def test_finished_first_pass_does_not_claim_old_numerical_results_are_fresh():
    text = "\n".join(
        format_forward_coverage(report(first_pass_completed=4, done_universe=True, pricedfresh=0, waiting=3))
    )
    assert "первичный проход 4/4 допустимых · завершён" in text
    assert "свежие пары BP+DEX 0" in text
    assert "ожидают расчёта 3" in text


def test_known_metadata_blocks_are_separate_from_waiting_and_from_api_refusals():
    snapshot = report(
        known_blocks=2,
        unattempted=0,
        first_pass_completed=4,
        done_universe=True,
        pricedfresh=0,
        waiting=3,
        rows=[
            {"asset": "CLOSED.US", "state": "skipped_current_session"},
            {"asset": "LOT.US", "state": "unsupported_quantity"},
            {"asset": "OLD.US", "state": "waiting_bank", "pair": {"fresh": False}},
        ],
    )
    text = "\n".join(format_forward_coverage(snapshot))
    assert "первичный проход 4/4 допустимых · завершён" in text
    assert "свежие пары BP+DEX 0; отказ BP 1; ожидают расчёта 1" in text
    assert "закрыта сессия / несовместима партия 2; неподтверждены 1" in text
    assert "Без попытки или установленной причины: 0/4" in text


def test_unattempted_is_not_a_checked_outcome_and_keeps_the_pass_incomplete():
    text = "\n".join(format_forward_coverage(report(unattempted=1)))
    assert "первичный проход 3/4 допустимых · продолжается" in text
    assert "Без попытки или установленной причины: 1/4" in text


def test_legacy_coverage_without_known_blocks_still_has_a_complete_partition():
    snapshot = report()
    assert "known_blocks" not in snapshot["forward_coverage"]
    text = "\n".join(format_forward_coverage(snapshot))
    assert "ожидают расчёта 2; закрыта сессия / несовместима партия 0; неподтверждены 1" in text
    assert "Без попытки" not in text


def test_stale_route_failure_remains_waiting_for_dex_not_a_current_missing_route():
    snapshot = report(
        rows=[{"asset": "STALE.US", "state": "waiting_bank", "reason": "stale_pool_quote", "pair": None}]
    )
    text = "\n".join(format_forward_coverage(snapshot))
    assert "ожидают свежих данных DEX 1" in text
    assert "нет маршрута по актуальным данным 0" in text
    assert "свежие пары BP+DEX 1" in text


def test_actual_snapshot_ttls_are_used_and_missing_limits_are_not_invented():
    snapshot = report()
    snapshot.update(backpack_price_ttl_s=6.5, dex_bank_ttl_s=3)
    assert "BP до 6.5с; DEX до 3с" in "\n".join(format_forward_coverage(snapshot))
    snapshot.pop("backpack_price_ttl_s")
    snapshot.pop("dex_bank_ttl_s")
    text = "\n".join(format_forward_coverage(snapshot))
    assert "BP: срок не указан; DEX: срок не указан" in text
    assert "15с" not in text and "8с" not in text


@pytest.mark.parametrize("snapshot", [{}, {"forward_coverage": {"enabled": False}}])
def test_older_or_disabled_snapshots_do_not_show_forward_coverage(snapshot):
    assert format_forward_coverage(snapshot) == []


def test_inconsistent_partition_cannot_be_presented_as_full_coverage():
    text = "\n".join(
        format_forward_coverage(report(pricedfresh=4, first_pass_completed=4, done_universe=True))
    )
    assert "сведения о проходе пока недоступны" in text
    assert "завершён" not in text


@pytest.mark.parametrize(
    "updates",
    [
        {"known_blocks": -1},
        {"known_blocks": True},
        {"known_blocks": 3},
        {"known_blocks": 2, "first_pass_completed": 1},
        {"known_blocks": 2, "waitingbank": 1},
        {"checked": 1},
        {"unattempted": 2},
        {"unattempted": -1},
        {"pending": 5},
        {"currentroute_missing": 2},
    ],
)
def test_invalid_counts_cannot_inflate_coverage_or_its_pending_groups(updates):
    text = "\n".join(format_forward_coverage(report(**updates)))
    assert "сведения о проходе пока недоступны" in text
    assert "завершён" not in text and "свежие пары" not in text


def test_empty_universe_does_not_claim_a_completed_pass():
    text = "\n".join(
        format_forward_coverage(
            report(
                universe=0,
                eligible=0,
                checked=0,
                pricedfresh=0,
                refused=0,
                waiting=0,
                unverified=0,
                first_pass_completed=0,
                done_universe=True,
                pending=0,
                waitingbank=0,
            )
        )
    )
    assert "завершён" not in text
