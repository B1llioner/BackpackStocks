from decimal import Decimal

import pytest
from test_rfq_validation import monitored

from backpack_stocks.models import raw
from backpack_stocks.monitor import FORWARD, REVERSE
from backpack_stocks.spread_reporting import best_spreads, compact_coverage, format_spreads


def pair(symbol="WEN.US", **changes):
    row = {
        "asset": symbol,
        "direction": FORWARD,
        "backpack_source": "rfq",
        "backpack_price": "10",
        "dex_price": "9.9",
        "quote_backend": "raydium",
        "cost_raw": raw("100"),
        "profit_raw": -raw("2"),
        "quoted_output_raw": raw("99"),
        "sell_quantity": "10",
        "stock_decimals": 6,
        "updated_at": 10000,
    }
    row.update(changes)
    return row


@pytest.mark.parametrize("profit", [-raw("2"), 0])
def test_digest_omits_negative_and_zero_net_even_with_a_positive_price_spread(profit):
    assert format_spreads([pair(profit_raw=profit, quoted_output_raw=raw("102"))]) == []


def test_digest_shows_positive_net_with_actual_cost_and_both_prices():
    text = "\n".join(format_spreads([pair(profit_raw=raw("2"), quoted_output_raw=raw("103"))]))
    assert "WEN.US · BP → Raydium · 05:46:40 МСК" in text
    assert "Спред цены: +3.00%; net: +2.00% (+2.00 USDC)" in text
    assert "Объём: 100 USDC · BP 10 / Raydium 10.3 USDC/шт" in text
    assert "RFQ на выбранный объём" not in text


def test_best_volume_is_selected_by_exact_net_ratio_and_distinct_asset_direction():
    high_absolute = pair(cost_raw=raw("1000"), profit_raw=raw("10"))
    high_percentage = pair(cost_raw=raw("100"), profit_raw=raw("2"))
    reverse = pair(direction=REVERSE, profit_raw=raw("3"))
    prices = [high_absolute, high_percentage, reverse]
    prices.extend(pair(f"S{i:02d}.US", profit_raw=-raw(str(i))) for i in range(72))
    prices.extend(pair(f"P{i:02d}.US", profit_raw=raw(Decimal(i) / 10)) for i in range(1, 8))
    best = best_spreads(prices)
    assert len(best) == 5
    assert best[0] is reverse and best[1] is high_percentage
    assert high_absolute not in best
    assert len({(row["asset"], row["direction"]) for row in best}) == 5
    assert all(row["profit_raw"] > 0 for row in best)
    assert len(best_spreads(prices, limit=20)) == 5


def test_sort_does_not_round_nearly_equal_net_percentages_before_selecting_volume():
    first = pair(cost_raw=10**30, profit_raw=10**28)
    greater = pair(cost_raw=10**30, profit_raw=10**28 + 1)
    assert best_spreads([first, greater]) == [greater]


@pytest.mark.parametrize("source", ["indicative", "spot"])
def test_positive_public_price_requires_own_rfq_but_actual_book_fill_does_not(source):
    text = "\n".join(format_spreads([pair(backpack_source=source, profit_raw=raw("2"))]))
    assert "net: +2.00% (+2.00 USDC)" in text
    assert ("нужна своя RFQ на выбранный объём" in text) is (source == "indicative")
    assert "Положительных расчётов" not in text and "подтверждена" not in text
    assert ("BP стакан → Raydium" in text) is (source == "spot")


def test_reverse_price_spread_uses_buy_venue_denominator_and_retains_transfer_price_risk():
    text = "\n".join(
        format_spreads([pair(direction=REVERSE, quoted_output_raw=raw("11"), profit_raw=raw("2"))])
    )
    assert "Raydium → BP" in text
    assert "Спред цены: +10.00%; net: +2.00% (+2.00 USDC)" in text
    assert "Продажа на BP после зачисления; цена за время перевода может измениться." in text


def test_price_spread_uses_raw_dex_math_even_when_six_decimal_price_hides_the_difference():
    row = pair(
        backpack_price="0.00005",
        dex_price="0.000050",
        quoted_output_raw=10001,
        sell_quantity="200",
        cost_raw=10000,
        profit_raw=1,
    )
    text = "\n".join(format_spreads([row]))
    assert "Спред цены: +0.01%" in text
    assert "Raydium 0.000050005 USDC/шт" in text


@pytest.mark.parametrize(
    "changes",
    [
        {"fresh": False},
        {"backpack_source": "model"},
        {"backpack_source": "pool_spot"},
        {"cost_raw": 0},
        {"backpack_price": "NaN"},
        {"quoted_output_raw": 0, "dex_price": "-1"},
        {"updated_at": float("inf")},
        {"updated_at": 1e100},
    ],
)
def test_unusable_or_estimated_inputs_cannot_enter_a_fresh_digest(changes):
    assert format_spreads([pair(**{"profit_raw": raw("2"), **changes})]) == []


def test_empty_prices_do_not_pull_an_old_value_or_a_positive_call_from_history():
    mon, _ = monitored()
    snapshot = {
        "prices": [],
        "rows": [pair(profit_raw=raw("10"), fresh=True, signal_eligible=True)],
        "price_history": [pair(profit_raw=raw("10"))],
        "price_catalog": [pair(profit_raw=raw("10"))],
    }
    text = mon.text(snapshot=snapshot)
    assert "Свежих полных котировок пока нет" in text
    assert "Спред цены:" not in text and "net:" not in text and "WEN.US" not in text


def test_selected_negative_pair_is_not_mislabelled_as_a_missing_fresh_quote():
    mon, _ = monitored()
    text = mon.text("WEN.US", snapshot={"prices": [pair()]})
    assert "нет положительного net" in text
    assert "свежей котировки пока нет" not in text
    assert "Спред цены:" not in text and "net:" not in text


def test_digest_puts_values_before_compact_coverage_and_limits_seventy_two_tickers():
    mon, _ = monitored()
    snapshot = {
        "prices": [
            pair(f"S{i:02d}.US", profit_raw=raw(Decimal(72 - i) / 100), quoted_output_raw=raw("102"))
            for i in range(72)
        ],
        "forward_coverage": {
            "enabled": True,
            "universe": 73,
            "eligible": 72,
            "pricedfresh": 39,
            "checked": 70,
            "refused": 0,
            "waitingbank": 31,
            "first_pass_completed": 72,
        },
    }
    text = mon.text(snapshot=snapshot)
    assert text.count("Спред цены:") == 5 and "S71.US" not in text
    assert text.index("Спред цены:") < text.index("свежие пары 39/72")
    assert "BP без свежей цены 2; ждут DEX 31" in text
    assert "первичный проход" not in text and "минимальная законная партия" not in text
    assert "Подробнее: /monitor · Все объёмы: /prices" in text
    assert "Проверена сторона" not in text and "Без попытки" not in text
    assert len(text) < 2000


def test_symbol_and_report_keys_filter_fresh_spreads_without_reintroducing_coverage():
    mon, _ = monitored()
    snapshot = {
        "prices": [pair(), pair(direction=REVERSE, profit_raw=raw("2")), pair("AMD.US")],
        "report_keys": [("WEN.US", REVERSE)],
    }
    text = mon.text("WEN.US", snapshot=snapshot)
    assert "Raydium → BP" in text and "BP → Raydium" not in text and "AMD.US" not in text
    assert "/prices WEN.US" in text


def test_compact_coverage_cannot_inflate_the_number_of_current_pairs():
    lines = compact_coverage(
        {"forward_coverage": {"enabled": True, "eligible": 72, "pricedfresh": 73, "first_pass_completed": 72}}
    )
    assert lines == ["BP → DEX: сведения о покрытии пока недоступны."]


def test_nonzero_small_spreads_do_not_round_to_a_misleading_zero_percent():
    row = pair(backpack_price="1", quoted_output_raw=1000001, sell_quantity="1", profit_raw=1)
    text = "\n".join(format_spreads([row]))
    assert "Спред цены: +0.0001%; net: +0.000001% (+0.000001 USDC)" in text
