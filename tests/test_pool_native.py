import random
from concurrent.futures import ThreadPoolExecutor

import pytest

from backpack_stocks.pool_compute import MAX_BATCH, PoolCalculator, PreparedCpmmPools
from backpack_stocks.pool_math import Q64


def test_python_reference_fallback_and_shape_validation():
    calc = PoolCalculator(backend="python")
    assert calc.backend == "python"
    for rows in ([()], [(True, 1, 1, 0, 0, False, 0, 0, 0)], [(2**64, 1, 1, 0, 0, False, 0, 0, 0)]):
        with pytest.raises(ValueError):
            calc.cpmm_batch(rows)
    with pytest.raises(ValueError):
        calc.cpmm_batch([(1, 1, 1, 0, 0, False, 0, 0, 0)] * (MAX_BATCH + 1))


def test_each_kernel_uses_its_own_measured_parallel_threshold():
    class Dispatch:
        def cpmm_batch(self, rows, parallel):
            assert parallel is (len(rows) >= 3)
            return [(None, "probe") for _ in rows]

        def clmm_intervals(self, rows, parallel):
            assert parallel is (len(rows) >= 2)
            return [(None, "probe") for _ in rows]

    calc = PoolCalculator(backend="python", parallel_threshold=3, interval_parallel_threshold=2)
    calc.native = Dispatch()
    row = (101, 10000, 20000, 3000, 10000, True, 0, 0, 0)
    for size in range(1, 5):
        assert len(calc.cpmm_batch([row] * size)) == size
        assert len(calc.clmm_intervals([(100, Q64, Q64 * 2)] * size)) == size


@pytest.fixture
def native():
    pytest.importorskip("_pool_math", reason="Build optional release Rust extension to run native parity")
    return PoolCalculator(backend="rust", threads=4)


def test_seeded_cpmm_native_integer_parity_and_rayon_order(native):
    rand = random.Random(20261002)
    rows = []
    for _ in range(5000):
        amount, rin, rout = [rand.randrange(100000, 2**64) for _ in range(3)]
        rows.append(
            (
                amount,
                rin,
                rout,
                rand.randrange(300000),
                rand.randrange(300000),
                bool(rand.randrange(2)),
                rand.randrange(amount // 2),
                rand.randrange(1000),
                rand.randrange(2**50),
            )
        )
    expected = PoolCalculator(backend="python").cpmm_batch(rows)
    assert expected == native.cpmm_batch(rows, parallel=False) == native.cpmm_batch(rows, parallel=True)


def test_clmm_u128_big_integer_parity_and_overflow(native):
    rand = random.Random(17)
    rows = [
        (rand.randrange(1, 2**80), rand.randrange(Q64 // 2, Q64 * 2), rand.randrange(Q64 // 2, Q64 * 2))
        for _ in range(2048)
    ]
    expected = PoolCalculator(backend="python").clmm_intervals(rows)
    assert (
        expected == native.clmm_intervals(rows, parallel=False) == native.clmm_intervals(rows, parallel=True)
    )
    for row in [(0, Q64, Q64 * 2), (1, 0, Q64), (1, Q64, Q64), (2**128 - 1, 1, 2)]:
        assert native.clmm_intervals([row], parallel=True)[0].get("error")


def test_concurrent_callers_have_no_shared_balance_or_order_state(native):
    rows = [(101, 10000, 20000, 3000, 10000, True, 0, 0, 0)] * 2048
    expected = native.cpmm_batch(rows, parallel=False)
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: native.cpmm_batch(rows, parallel=True), range(20)))
    assert all(r == expected for r in results)
    rows[0] = (0, 10000, 20000, 3000, 10000, True, 0, 0, 0)
    result = native.cpmm_batch(rows, parallel=True)
    assert result[0]["error"] and all(r == expected[i] for i, r in enumerate(result[1:], 1))


def test_prepared_frame_direction_units_immutability_and_rayon(native):
    state = {
        "kind": "cpmm",
        "quoteSupported": True,
        "reserveA": "10000",
        "reserveB": "20000",
        "fees": {"tradeFeeRate": 3000, "creatorFeeRate": 10000},
        "feeOn": 1,
        "enableCreatorFee": True,
        "transferFeeA": {"bps": 300, "maximum": 5},
        "transferFeeB": {"bps": 200, "maximum": 4},
    }
    reference = PreparedCpmmPools(PoolCalculator(backend="python"), [state])
    prepared = PreparedCpmmPools(native, [state])
    jobs = [(0, bool(i % 2), 101 + i, 50) for i in range(10000)]
    expected = reference.quote_batch(jobs)
    assert expected == prepared.quote_batch(jobs, parallel=False) == prepared.quote_batch(jobs, parallel=True)
    state["reserveA"] = "1"
    state["fees"]["tradeFeeRate"] = 999999
    assert expected == reference.quote_batch(jobs) == prepared.quote_batch(jobs)
    for job in [(1, True, 101, 50), (0, True, 2**64, 50), (0, True, True, 50), (0, True, 101, 501)]:
        with pytest.raises(ValueError):
            prepared.quote_batch([job])
    with pytest.raises(ValueError):
        PreparedCpmmPools(native, [{**state, "quoteSupported": False}])


def test_native_grid_matches_expanded_batch_and_checks_bounds(native):
    state = {
        "kind": "cpmm",
        "quoteSupported": True,
        "reserveA": "1000000",
        "reserveB": "2000000",
        "fees": {"tradeFeeRate": 3000, "creatorFeeRate": 10000},
        "feeOn": 2,
        "enableCreatorFee": True,
    }
    prepared = PreparedCpmmPools(native, [state])
    seeds, sizes = [(0, True, 1000), (0, False, 1100)], list(range(1, 21))
    jobs = [(i, d, amount * m, 50) for i, d, amount in seeds for m in sizes]
    expected = [
        (job[2], out, minimum, error)
        for job, (out, minimum, error) in zip(jobs, prepared.quote_batch(jobs), strict=True)
    ]
    assert (
        expected
        == prepared.quote_grid(seeds, sizes, parallel=False)
        == prepared.quote_grid(seeds, sizes, parallel=True)
    )
    assert prepared.quote_grid([(0, True, 2**64 - 1)], [1, 2])[1] == (0, 0, 0, 5)
    for bad in ([0], [-1], [True], [2**32]):
        with pytest.raises(ValueError):
            prepared.quote_grid(seeds, bad)
    with pytest.raises(ValueError):
        prepared.quote_grid(seeds * 5000, list(range(1, 21)))


def test_reference_and_native_partial_error_codes_match(native):
    state = {
        "kind": "cpmm",
        "quoteSupported": True,
        "reserveA": "10000",
        "reserveB": "1",
        "fees": {"tradeFeeRate": 3000, "creatorFeeRate": 10000},
        "feeOn": 0,
        "enableCreatorFee": True,
    }
    reference = PreparedCpmmPools(PoolCalculator(backend="python"), [state])
    prepared = PreparedCpmmPools(native, [state])
    jobs = [(0, True, 0, 50), (0, True, 1, 50), (0, True, 101, 50)]
    assert reference.quote_batch(jobs) == prepared.quote_batch(jobs) == [(0, 0, 1), (0, 0, 3), (0, 0, 4)]


async def test_rpc_snapshot_failure_keeps_partial_scan_and_blocks_quotes():
    import json
    from pathlib import Path
    from types import SimpleNamespace

    from backpack_stocks.pool_catalog import refresh_catalog
    from backpack_stocks.pool_state import CPMM, decode_pool
    from backpack_stocks.transport import RemoteError

    root = Path(__file__).resolve().parents[1]
    f = json.loads((root / "tests/fixtures/pool-pricing/rpc-fixtures.json").read_text())
    address = next(a for a in f["pools"] if f["accounts"][a]["owner"] == CPMM)
    state = decode_pool(f["accounts"][address])
    catalog = json.loads((root / "tests/fixtures/pool-pricing/catalog-refreshed.json").read_text())
    asset = next(a for a in catalog["assets"] if a["mint"] in {state["mintA"], state["mintB"]})
    pool = next(p for p in catalog["pools"] if p["address"] == address)
    catalog.update(
        assets=[asset],
        pools=[pool],
        links=[(asset["symbol"], address, "raydium_index")],
        quotes=[],
        errors=[],
    )

    class RPC:
        http = SimpleNamespace(status=lambda: {})

        async def accounts(self, refs, *, parsed=False, priority=0):
            if parsed:
                return {"context": {"slot": f["mint_slot"]}, "value": [f["mints"].get(r) for r in refs]}
            if len(refs) > 1:
                raise RemoteError("synthetic read failure")
            return {"context": {"slot": f["raw_slot"]}, "value": [f["accounts"].get(r) for r in refs]}

    partial = await refresh_catalog(catalog, RPC())
    assert partial["summary"]["snapshots"] == 0 and partial["quotes"] == []
    assert partial["errors"][0]["stage"] == "snapshot_rpc"
    assert "snapshot" not in partial["pools"][0]
