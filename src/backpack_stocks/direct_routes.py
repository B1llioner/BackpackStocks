"""Bounded exact-in pool routes. Every leg uses fresh, validated on-chain state."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING

from .models import USDC_MINT
from .observability import MARKET_BLOCKS, error_code, event
from .quote_batch import QuoteAccountBatch

if TYPE_CHECKING:
    from .direct import DirectRaydium

WSOL = "So11111111111111111111111111111111111111112"


def combine(legs: list[dict], *, sequential: bool) -> dict:
    first, last = legs[0], legs[-1]
    if sequential:
        for previous, current in zip(legs, legs[1:], strict=False):
            if previous["outputMint"] != current["inputMint"] or int(previous["outAmount"]) != int(
                current["inAmount"]
            ):
                raise ValueError("Direct route intermediate amount mismatch")
        amount, output = int(first["inAmount"]), int(last["outAmount"])
        minimum = int(last["otherAmountThreshold"])
    else:
        if len({(q["inputMint"], q["outputMint"]) for q in legs}) != 1:
            raise ValueError("Direct split pair mismatch")
        seen: set[str] = set()
        for q in legs:
            pools = {item["swapInfo"]["ammKey"] for item in q["routePlan"]}
            if pools & seen:
                raise ValueError("Direct split reuses mutable pool state")
            seen.update(pools)
        amount = sum(int(q["inAmount"]) for q in legs)
        output = sum(int(q["outAmount"]) for q in legs)
        minimum = sum(int(q["otherAmountThreshold"]) for q in legs)
    efficiency = Decimal(1)
    for q in legs:
        efficiency *= max(Decimal(0), 1 - Decimal(q["priceImpact"]) / 100)
    # For splits the worst leg is conservative; for hops fees compound.
    impact = (1 - efficiency) * 100 if sequential else max(Decimal(q["priceImpact"]) for q in legs)
    route = []
    for q in legs:
        for item in q["routePlan"]:
            route.append(
                {
                    **item,
                    "bps": item.get("bps", 10000)
                    if sequential or item["swapInfo"]["inputMint"] != first["inputMint"]
                    else item.get("bps", 10000) * int(q["inAmount"]) // amount,
                }
            )
    return {
        "inputMint": first["inputMint"],
        "outputMint": last["outputMint"],
        "inAmount": str(amount),
        "outAmount": str(output),
        "otherAmountThreshold": str(minimum),
        "priceImpact": str(impact),
        "swapMode": "ExactIn",
        "router": "Direct Raydium",
        "source": "fresh local Raydium exact-in",
        "quote_source": "direct_pools",
        "contextSlot": max(q["contextSlot"] for q in legs),
        "_observed_at": min(q["_observed_at"] for q in legs),
        "routePlan": route,
        # Composed quotes are monitoring-only until a guarded atomic builder exists.
        "direct_execution_supported": False,
        "_direct_route": legs,
    }


async def family_quote(
    router: DirectRaydium,
    pools: list[str],
    source: str,
    target: str,
    amount: int,
    priority: int,
    rpc: QuoteAccountBatch,
    partials: dict[int, list[dict]] | None = None,
) -> tuple[list[dict], list[str]]:
    pools = list(dict.fromkeys(pools))
    jobs = [(pool, amount) for pool in pools]
    if partials is not None and amount >= 4:
        amounts = {amount * n // 4 for n in (1, 2, 3)}
        amounts |= {amount - units for units in amounts}
        jobs.extend((pool, units) for pool in pools for units in sorted(amounts))
    splits = []
    if router.settings.direct.split_pools and len(pools) >= 2 and amount >= 4:
        for numerator in (1, 2, 3):
            left = amount * numerator // 4
            pair = [(pools[0], left), (pools[1], amount - left)]
            jobs.extend(pair)
            splits.append(pair)
    jobs = list(dict.fromkeys(jobs))
    values = await asyncio.gather(
        *(
            router.pool_quote(pool, source, target, units, priority=priority, _rpc=rpc)
            for pool, units in jobs
        ),
        return_exceptions=True,
    )
    good, codes = {}, []
    now = time.time()
    for job, value in zip(jobs, values, strict=True):
        if isinstance(value, BaseException):
            code = error_code(value)
            codes.append(code)
            router.failures += 1
            event(
                "pool_quote_blocked" if code in MARKET_BLOCKS else "pool_quote_error",
                level="INFO" if code in MARKET_BLOCKS else "ERROR",
                pool=job[0],
                code=code,
                error_type=type(value).__name__,
            )
        elif 0 <= now - value["_observed_at"] <= router.settings.policy.quote_max_age_s:
            good[job] = value
            if partials is not None:
                partials.setdefault(job[1], []).append(value)
        else:
            codes.append("stale_pool_quote")
    result = [good[(pool, amount)] for pool in pools if (pool, amount) in good]
    result.extend(
        combine([good[job] for job in pair], sequential=False)
        for pair in splits
        if all(job in good for job in pair)
    )
    return result, codes


async def composed_quote(
    router: DirectRaydium, source: str, target: str, amount: int, priority: int, rpc: QuoteAccountBatch
) -> dict:
    from .direct import QuoteUnavailable

    started = time.time()
    stock = target if source == USDC_MINT else source
    direct = router.candidates.get(stock, [])
    sol = router.sol_candidates.get(stock, [])
    bridges = await router.bridge_pools() if sol else []
    direct_parts: dict[int, list[dict]] = {}
    first_parts: dict[int, list[dict]] = {}
    mixed = router.settings.direct.split_pools and bool(direct and sol and bridges) and source == stock
    first_pools, first_source, first_target = (
        (sol, source, WSOL) if source == stock else (bridges, USDC_MINT, WSOL)
    )
    (candidates, codes), (first, first_codes) = await asyncio.gather(
        family_quote(router, direct, source, target, amount, priority, rpc, direct_parts if mixed else None),
        family_quote(
            router,
            first_pools,
            first_source,
            first_target,
            amount,
            priority,
            rpc,
            first_parts if mixed else None,
        ),
    )
    codes.extend(first_codes)
    if first:
        # Exact-in output is monotone: the largest verified intermediate credit
        # feeds the best next pool. Never value SOL as if it were already USDC.
        best_first = max(first, key=lambda q: int(q["outAmount"]))
        second_pools = bridges if source == stock else sol
        second, errors = await family_quote(
            router, second_pools, WSOL, target, int(best_first["outAmount"]), priority, rpc
        )
        codes.extend(errors)
        candidates.extend(combine([best_first, q], sequential=True) for q in second)
    if mixed:

        async def mixed_path(units: int) -> list[dict]:
            choices = first_parts.get(units, [])
            direct_choices = direct_parts.get(amount - units, [])
            if not choices or not direct_choices:
                return []
            first_leg = max(choices, key=lambda q: int(q["outAmount"]))
            direct_leg = max(direct_choices, key=lambda q: int(q["otherAmountThreshold"]))
            next_legs, errors = await family_quote(
                router, bridges, WSOL, target, int(first_leg["outAmount"]), priority, rpc
            )
            codes.extend(errors)
            return [
                combine([direct_leg, combine([first_leg, leg], sequential=True)], sequential=False)
                for leg in next_legs
            ]

        paths = await asyncio.gather(*(mixed_path(amount * n // 4) for n in (1, 2, 3)))
        candidates.extend(q for path in paths for q in path)
    candidates = [
        q
        for q in candidates
        if 0 <= time.time() - q["_observed_at"] <= router.settings.policy.quote_max_age_s
    ]
    if not candidates:
        raise QuoteUnavailable(codes)
    best = max(candidates, key=lambda q: int(q["otherAmountThreshold"]))
    router.quote_count += 1
    best.update(_request_started_at=started, candidate_routes=len(candidates), quote_source="direct_pools")
    return best
