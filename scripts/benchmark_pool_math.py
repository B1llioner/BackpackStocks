"""Offline end-to-end math benchmarks, including Python validation and FFI."""

from __future__ import annotations

import json
import platform
import statistics
import time
from pathlib import Path

from backpack_stocks.pool_catalog import cpmm_request
from backpack_stocks.pool_compute import PoolCalculator, PreparedCpmmPools


def measure(call, rounds=9):
    call()
    samples = []
    for _ in range(rounds):
        started = time.perf_counter_ns()
        call()
        samples.append((time.perf_counter_ns() - started) / 1e6)
    return {"median_ms": statistics.median(samples), "min_ms": min(samples), "max_ms": max(samples)}


def main():
    catalog = json.loads(Path("research/pool-pricing/catalog-refreshed.json").read_text())
    stocks = {a["mint"]: a for a in catalog["assets"]}
    cpmm, intervals, states, jobs = [], [], [], []
    for p in catalog["pools"]:
        s = p.get("snapshot", {}).get("state", {})
        if s.get("quoteSupported"):
            mint = next(m for m in (s["mintA"], s["mintB"]) if m in stocks)
            cpmm.append(cpmm_request(s, mint, 10 ** stocks[mint]["decimals"])[0])
            jobs.append((len(states), mint == s["mintA"], 10 ** stocks[mint]["decimals"], 50))
            states.append(s)
        elif s.get("kind") == "clmm" and int(s["liquidity"]) > 0:
            start = int(s["sqrtPriceX64"])
            intervals.append((int(s["liquidity"]), start, start * 10001 // 10000))
    python = PoolCalculator(backend="python")
    rust = PoolCalculator(backend="rust", threads=4)
    records = []
    for kernel, base, sizes in (
        ("cpmm_batch", cpmm, (20, len(cpmm), len(cpmm) * 20, 100000)),
        ("clmm_intervals", intervals, (20, len(intervals), len(intervals) * 20)),
    ):
        for size in sizes:
            rows = (base * ((size + len(base) - 1) // len(base)))[:size]
            reference = getattr(python, kernel)(rows)
            seq = getattr(rust, kernel)(rows, parallel=False)
            par = getattr(rust, kernel)(rows, parallel=True)
            for a, b, c in zip(reference, seq, par, strict=True):
                assert (a == b == c) or (a.get("error") and b.get("error") and c.get("error")), (
                    "Parity failed"
                )
            item = {
                "kernel": kernel,
                "rows": size,
                "python": measure(lambda k=kernel, r=rows: getattr(python, k)(r)),
                "rust_seq": measure(lambda k=kernel, r=rows: getattr(rust, k)(r, parallel=False)),
                "rust_rayon": measure(lambda k=kernel, r=rows: getattr(rust, k)(r, parallel=True)),
            }
            records.append(item)
            print(json.dumps(item), flush=True)
    prepared_python, prepared_rust = PreparedCpmmPools(python, states), PreparedCpmmPools(rust, states)
    for size in (20, len(jobs), len(jobs) * 20, 100000):
        batch = (jobs * ((size + len(jobs) - 1) // len(jobs)))[:size]
        assert (
            prepared_python.quote_batch(batch)
            == prepared_rust.quote_batch(batch, parallel=False)
            == prepared_rust.quote_batch(batch, parallel=True)
        )
        item = {
            "kernel": "prepared_cpmm",
            "rows": size,
            "python": measure(lambda b=batch: prepared_python.quote_batch(b)),
            "rust_seq": measure(lambda b=batch: prepared_rust.quote_batch(b, parallel=False)),
            "rust_rayon": measure(lambda b=batch: prepared_rust.quote_batch(b, parallel=True)),
        }
        records.append(item)
        print(json.dumps(item), flush=True)
    seeds = [(i, direction, amount) for i, direction, amount, _ in jobs]
    sizes = list(range(1, 21))
    expanded = [
        (i, direction, amount * multiplier, 50) for i, direction, amount in seeds for multiplier in sizes
    ]
    expected = [
        (job[2], out, minimum, error)
        for job, (out, minimum, error) in zip(expanded, prepared_python.quote_batch(expanded), strict=True)
    ]
    assert (
        expected
        == prepared_rust.quote_grid(seeds, sizes, parallel=False)
        == prepared_rust.quote_grid(seeds, sizes, parallel=True)
    )
    item = {
        "kernel": "rust_generated_grid",
        "rows": len(expanded),
        "pools": len(seeds),
        "sizes": len(sizes),
        "python_expanded_batch": measure(lambda: prepared_python.quote_batch(expanded)),
        "rust_expanded_batch": measure(lambda: prepared_rust.quote_batch(expanded, parallel=True)),
        "rust_grid_seq": measure(lambda: prepared_rust.quote_grid(seeds, sizes, parallel=False)),
        "rust_grid_rayon": measure(lambda: prepared_rust.quote_grid(seeds, sizes, parallel=True)),
    }
    records.append(item)
    print(json.dumps(item), flush=True)
    report = {
        "observed_at": time.time(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "rust_workers": 4,
        "rounds": 9,
        "release": True,
        "includes_validation_and_ffi": True,
        "clmm_scope": "fee-free interval primitive; not full tick traversal or a swap quote",
        "records": records,
    }
    Path("research/pool-pricing/rust-benchmark.json").write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
