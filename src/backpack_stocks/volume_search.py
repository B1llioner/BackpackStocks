"""Choose probe sizes; old samples guide requests, never supply an execution price."""

from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal


class VolumeSearch:
    fractions = (Decimal(1), Decimal("0.5"), Decimal("0.25"), Decimal("0.75"), Decimal("0.125"))

    def __init__(self):
        self.samples: dict[tuple[str, str], deque[dict]] = defaultdict(lambda: deque(maxlen=12))
        self.turns: dict[tuple[str, str], int] = defaultdict(int)
        self.refusals: dict[tuple[str, str], tuple[Decimal, float]] = {}
        self.largest: dict[tuple[str, str], Decimal] = {}

    def refuse(self, key: tuple[str, str], budget: Decimal, now: float) -> None:
        self.refusals[key] = budget, now

    def next(
        self,
        key: tuple[str, str],
        cap: Decimal,
        now: float,
        *,
        auto_size: bool = False,
        minimum: Decimal = Decimal(0),
        maximum: Decimal | None = None,
    ) -> Decimal:
        turn = self.turns[key]
        self.turns[key] += 1
        recent = [s for s in self.samples[key] if 0 <= now - s["observed_at"] <= 180]
        if auto_size:
            # The seed is a starting point, never a spend limit. Expand beyond
            # successful sizes; provider refusals and session limits bound probes.
            largest = max(
                [cap, self.largest.get(key, cap), *(Decimal(s["search_budget_usdc"]) for s in recent)]
            )
            ceiling = maximum or largest * 2
            refusal = self.refusals.get(key)
            if refusal and 0 <= now - refusal[1] < 60:
                accepted = self.largest.get(key, Decimal(0))
                reduced = (accepted + refusal[0]) / 2 if accepted < refusal[0] else refusal[0] / 2
                ceiling = min(ceiling, reduced)
            if turn == 0:
                chosen = cap
            elif turn % 3 == 1:
                chosen = largest * 2
            elif turn % 3 == 2:
                feasible = [s for s in recent if s["size_feasible"]]
                chosen = (
                    Decimal(max(feasible, key=lambda s: s["profit_raw"])["search_budget_usdc"])
                    if feasible
                    else cap / 2
                )
            else:
                chosen = largest * self.fractions[(turn // 3) % len(self.fractions)]
            return min(maximum or ceiling, max(minimum, min(ceiling, chosen)))
        # Complete the coarse grid and keep exploring it after prices change.
        if turn < len(self.fractions) or turn % 3 == 0:
            return cap * self.fractions[turn % len(self.fractions)]
        usable = [s for s in recent if s["profit_raw"] > 0 and s["size_feasible"]]
        if not usable:
            return cap * self.fractions[turn % len(self.fractions)]
        best = max(usable, key=lambda s: s["profit_raw"])
        budget = Decimal(best["search_budget_usdc"])
        # Alternate refreshing the winner and testing a neighbouring size.
        if turn % 3 == 1:
            return min(cap, budget)
        step = cap / 16
        tested = {Decimal(s["search_budget_usdc"]) for s in recent}
        neighbours = [max(cap / 32, budget - step), min(cap, budget + step)]
        return min(neighbours, key=lambda size: (size in tested, abs(size - budget)))

    def record(self, key: tuple[str, str], row: dict) -> None:
        self.samples[key].append(dict(row))
        self.largest[key] = max(self.largest.get(key, Decimal(0)), Decimal(row["search_budget_usdc"]))
