import importlib
import random

import pytest

from backpack_stocks.stock_money import MAX_BATCH, StockValueCalculator


@pytest.fixture
def rust():
    module = pytest.importorskip("_pool_math")
    if not hasattr(module, "STOCK_MONEY_MODEL"):
        pytest.skip("Rebuild the stock valuation extension")
    return StockValueCalculator("rust")


def test_native_parity_for_amount_precision_fees_rounding_and_overflow(rust):
    rng = random.Random(680112)
    jobs = [
        (
            rng.randrange(1, 2**64),
            rng.randrange(19),
            f"{rng.randrange(1, 100000)}.{rng.randrange(10**8):08d}",
            rng.randrange(10001),
            bool(rng.randrange(2)),
        )
        for _ in range(4000)
    ]
    jobs += [(112_000_000, 6, "8.903772", 0, True), (1, 6, "1.000001", 1, True), (1, 6, "1.000001", 1, False)]
    for price in [
        "1e-7",
        "+1E2",
        ".1",
        "1.",
        "1e-100",
        "1.0000000000000000000000000000000000000001",
        "1e100",
        "NaN",
        "inf",
        "-1",
        "0",
        "1.2.3",
        " 1",
        "1_000",
    ]:
        jobs.extend((1_000_000, 6, price, 0, buy) for buy in (True, False))
    assert rust.batch(jobs) == StockValueCalculator("python").batch(jobs)
    assert rust.value(112_000_000, 6, "8.903772", 0, buy=True) == 997_222_464
    assert rust.value(17_910_000, 6, "6.1", 0, buy=False) == 109_251_000


def test_native_invalid_price_is_local_to_its_row(rust):
    assert rust.batch([(1_000_000, 6, "NaN", 0, True), (1_000_000, 6, "6.29", 0, True)]) == [None, 6_290_000]
    with pytest.raises(ValueError, match="overflowing"):
        rust.value(1_000_000, 6, "-1", 0, buy=True)


@pytest.mark.parametrize(
    "job",
    [
        (True, 6, "1", 0, True),
        (1, -1, "1", 0, True),
        (1, 256, "1", 0, True),
        (1, 6, "1", 10001, True),
        (1, 6, "1", 0, 1),
        (1, 6, "1" * 129, 0, True),
    ],
)
def test_jobs_are_bounded_and_strictly_typed(job):
    with pytest.raises(ValueError, match="job"):
        StockValueCalculator("python").batch([job])


def test_batch_size_is_bounded(rust):
    with pytest.raises(ValueError, match="bound"):
        rust.batch([(1, 6, "1", 0, True)] * (MAX_BATCH + 1))


def test_optional_native_backend_never_masks_a_broken_import(monkeypatch):
    def missing(name):
        raise ModuleNotFoundError("synthetic", name="unrelated_dependency")

    monkeypatch.setattr(importlib, "import_module", missing)
    with pytest.raises(ModuleNotFoundError):
        StockValueCalculator()
