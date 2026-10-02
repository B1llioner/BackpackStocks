"""Choose probe sizes; old samples guide requests, never supply an execution price."""

from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal


class VolumeSearch:
    fractions = (Decimal(1), Decimal("0.5"), Decimal("0.25"), Decimal("0.75"), Decimal("0.125"))

    def __init__(self):
        self.samples: dict[tuple[str, str], deque[dict]] = defaultdict(lambda: deque(maxlen=12))
        self.turns: dict[tuple[str, str], int] = defaultdict(int)

    def next(self, key: tuple[str, str], cap: Decimal, now: float) -> Decimal:
        turn = self.turns[key]
        self.turns[key] += 1
        # Complete the coarse grid and keep exploring it after prices change.
        if turn < len(self.fractions) or turn % 3 == 0:
            return cap * self.fractions[turn % len(self.fractions)]
        recent = [s for s in self.samples[key] if 0 <= now - s["observed_at"] <= 180]
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
