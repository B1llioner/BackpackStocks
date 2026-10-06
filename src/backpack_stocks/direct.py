"""Direct single-pool Raydium quotes and locally constructed instructions."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import struct
import time
from collections import Counter
from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal
from functools import partial
from pathlib import Path

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey

from .clmm_worker import ClmmCalculationError, ClmmWorkerPool
from .models import ATA, STOCK_MINT, TOKEN, TOKEN_2022, USDC_MINT, Settings, dec
from .observability import MARKET_BLOCKS, QuoteUnavailable, error_code, event
from .pool_bank_feed import PoolBankFeed
from .pool_catalog import CLOCK, SYSVAR, mint_capabilities, transfer_fee_profile
from .pool_compute import PoolCalculator, PreparedCpmmPools
from .pool_math import spot_b_per_a
from .pool_state import CPMM, decode_fee_config, decode_pool
from .proxy import resolve_proxy
from .quote_batch import QuoteAccountBatch
from .raydium import CLMM, Raydium
from .solana import SolanaRPC, associated_address
from .swap_guard import COMPUTE, SYSTEM

MEMO = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
WSOL = "So11111111111111111111111111111111111111112"


class LocalQuoteNotReady(QuoteUnavailable):
    code = "pool_bank_not_ready"

    def __init__(self):
        super().__init__([self.code])
        self.code = "pool_bank_not_ready"


def pda(program: str, *seeds: bytes) -> str:
    return str(Pubkey.find_program_address(list(seeds), Pubkey.from_string(program))[0])


def account_bytes(account: dict | None, program: str, kind: str, size: int) -> bytes:
    if not account or account["owner"] != program or account.get("executable"):
        raise ValueError("Direct account owner/executable mismatch")
    data = base64.b64decode(account["data"][0], validate=True)
    if len(data) != size or data[:8] != hashlib.sha256(("account:" + kind).encode()).digest()[:8]:
        raise ValueError("Direct account layout/discriminator mismatch")
    return data


def vault_info(account: dict, mint: str, authority: str, program: str) -> int:
    data = account.get("data", {}).get("parsed", {})
    info = data.get("info", {})
    if (
        account.get("owner") != program
        or data.get("type") != "account"
        or info.get("mint") != mint
        or info.get("owner") != authority
        or info.get("state") != "initialized"
        or info.get("delegate")
        or info.get("closeAuthority")
    ):
        raise ValueError("Direct vault ownership/control mismatch")
    amount = int(info["tokenAmount"]["amount"])
    if not 0 <= amount < 2**64:
        raise ValueError("Invalid vault amount")
    return amount


def checked_clock(account: dict | None) -> tuple[int, int]:
    if not account or account["owner"] != SYSVAR:
        raise ValueError("Invalid Clock owner")
    data = account["data"]
    if isinstance(data, dict):
        parsed = data.get("parsed", {})
        if data.get("program") != "sysvar" or parsed.get("type") != "clock":
            raise ValueError("Invalid parsed Clock")
        return int(parsed["info"]["epoch"]), int(parsed["info"]["unixTimestamp"])
    raw = base64.b64decode(data[0], validate=True)
    if len(raw) != 40:
        raise ValueError("Invalid Clock")
    return struct.unpack_from("<Q", raw, 16)[0], struct.unpack_from("<q", raw, 32)[0]


def validate_auxiliary(pool: str, program: str, state: dict, accounts: dict, arrays: list[str]) -> None:
    observation = state["observation"]
    # CLMM swap_v2 constrains this key to PoolState.observation_key. Legacy
    # pools have a non-PDA oracle; ownership, layout and embedded pool_id still
    # have to match. CPMM's initialized oracle uses the derived address.
    if program == CPMM and observation != pda(program, b"observation", bytes(Pubkey.from_string(pool))):
        raise ValueError("Observation PDA mismatch")
    raw = account_bytes(accounts[observation], program, "ObservationState", 4483 if program == CLMM else 4075)
    offset = 19 if program == CLMM else 11
    if str(Pubkey.from_bytes(raw[offset : offset + 32])) != pool:
        raise ValueError("Observation belongs to another pool")
    for address in arrays:
        data = account_bytes(accounts[address], CLMM, "TickArrayState", 10240)
        start = struct.unpack_from("<i", data, 40)[0]
        expected = pda(CLMM, b"tick_array", bytes(Pubkey.from_string(pool)), struct.pack(">i", start))
        if address != expected or str(Pubkey.from_bytes(data[8:40])) != pool:
            raise ValueError("Tick array PDA/pool mismatch")
        if start % (state["tickSpacing"] * 60) or not -443636 - state["tickSpacing"] * 60 <= start <= 443636:
            raise ValueError("Invalid tick array start")
        for index in range(60):
            pos = 44 + index * 168
            gross = int.from_bytes(data[pos + 20 : pos + 36], "little")
            orders = int.from_bytes(data[pos + 124 : pos + 140], "little")
            if gross or orders:
                if struct.unpack_from("<i", data, pos)[0] != start + index * state["tickSpacing"]:
                    raise ValueError("Initialized tick index mismatch")


class DirectRaydium:
    """No Jupiter HTTP/API. Quotes cover a bounded subset of catalogued direct pools."""

    def __init__(self, settings: Settings, rpc: SolanaRPC):
        self.settings, self.rpc = settings, rpc
        self.worker = ClmmWorkerPool(settings.direct.clmm_worker)
        self.calculator = PoolCalculator()
        catalog_path = Path(settings.direct.catalog)
        if (
            not catalog_path.exists()
            and settings.direct.catalog == "research/pool-pricing/catalog-native.json"
        ):
            # Clean installs use public pool addresses, never historical prices.
            # Every bank, reserve and mint is still validated on-chain below.
            catalog_path = Path(__file__).with_name("direct_pool_catalog.json")
        catalog = json.loads(catalog_path.read_text())
        self.observed_pools: set[str] = set()
        if settings.direct.watched_catalog and Path(settings.direct.watched_catalog).exists():
            watched = json.loads(Path(settings.direct.watched_catalog).read_text())
            self.observed_pools = {row["address"] for row in watched["pools"]}
            catalog["pools"] = [*catalog["pools"], *watched["pools"]]
            catalog["assets"] = [*catalog["assets"], *watched.get("assets", [])]
        self.assets = {a["mint"]: a for a in catalog["assets"]}
        self.by_symbol = {a["symbol"]: a for a in self.assets.values()}
        self.candidates: dict[str, list[str]] = {}
        self.sol_candidates: dict[str, list[str]] = {}
        grouped: dict[str, list[tuple[int, str]]] = {}
        sol_grouped: dict[str, list[tuple[int, str]]] = {}
        for pool in catalog["pools"]:
            pair = {pool.get("mintA"), pool.get("mintB")}
            mint = next((m for m in pair if m in self.assets), None)
            state = pool.get("snapshot", {}).get("state", {})
            if mint and pair & {USDC_MINT, WSOL} and state.get("kind") in {"cpmm", "clmm"}:
                # Historical liquidity is only discovery priority, never a quote.
                weight = int(state.get("liquidity", state.get("reserveB", "0")))
                if pool.get("watched"):
                    weight += 2**256
                target = grouped if USDC_MINT in pair else sol_grouped
                target.setdefault(mint, []).append((weight, pool["address"]))
        for mint, rows in grouped.items():
            self.candidates[mint] = list(dict.fromkeys(a for _, a in sorted(rows, reverse=True)))[
                : settings.direct.max_pools
            ]
        for mint, rows in sol_grouped.items():
            self.sol_candidates[mint] = list(dict.fromkeys(a for _, a in sorted(rows, reverse=True)))[
                : settings.direct.max_pools
            ]
        selected_addresses = {
            a
            for group in (self.candidates, self.sol_candidates)
            for addresses in group.values()
            for a in addresses
        }
        self.pool_catalog = {p["address"]: p for p in catalog["pools"] if p["address"] in selected_addresses}
        self.semaphore = asyncio.Semaphore(settings.direct.concurrency)
        self.quote_count = 0
        self.failures = 0
        self.cached_prices: dict[str, tuple[float, str]] = {}
        self.screen_status: dict = {}
        self.gas_pools: list[str] = []
        self.gas_discovery_lock = asyncio.Lock()
        self.discovery_lock = asyncio.Lock()
        self.discovery_checked: dict[str, float] = {}
        self.bank_feed: PoolBankFeed | None = None
        self.array_hints: dict[tuple[str, str], int] = {}

    def update_assets(self, assets: list[dict]) -> None:
        for row in assets:
            Pubkey.from_string(row["mint"])
            self.assets[row["mint"]] = row
            self.by_symbol[row["symbol"]] = row

    async def ensure_candidates(self, mint: str) -> None:
        if self.candidates.get(mint) and (not self.settings.direct.via_sol or self.sol_candidates.get(mint)):
            return
        # A completed discovery cooldown must not queue behind another stock's
        # slow discovery request. Keep the guarded recheck inside the lock too.
        if time.time() - self.discovery_checked.get(mint, 0) < 300:
            return
        async with self.discovery_lock:
            if time.time() - self.discovery_checked.get(mint, 0) < 300:
                return
            self.discovery_checked[mint] = time.time()
            ray = Raydium(self.rpc, proxy=resolve_proxy("RAYDIUM"))
            try:
                for quote_mint, group in [
                    (USDC_MINT, self.candidates),
                    *([(WSOL, self.sol_candidates)] if self.settings.direct.via_sol else []),
                ]:
                    if len(group.get(mint, [])) >= self.settings.direct.max_pools:
                        continue
                    found = await ray.pools(mint, quote_mint)
                    rows = [p for p in found if p["programId"] in {CLMM, CPMM}][
                        : self.settings.direct.max_pools
                    ]
                    if not rows:
                        continue
                    response = await self.rpc.accounts([p["id"] for p in rows])
                    for p, account in zip(rows, response["value"], strict=True):
                        if not account or account["owner"] != p["programId"]:
                            continue
                        state = decode_pool(account)
                        if {state["mintA"], state["mintB"]} != {mint, quote_mint}:
                            continue
                        address = p["id"]
                        self.pool_catalog[address] = {
                            "address": address,
                            "program": account["owner"],
                            "mintA": state["mintA"],
                            "mintB": state["mintB"],
                            "snapshot": {"state": state},
                        }
                        group[mint] = list(dict.fromkeys([*group.get(mint, []), address]))[
                            : self.settings.direct.max_pools
                        ]
            finally:
                await ray.close()

    async def register_pool_hints(self, hints: list[dict], assets: list[dict]) -> int:
        """Validate observed pool ownership/pair before adding bounded discovery refs."""
        if not self.settings.direct.watched_catalog:
            return 0
        references = {
            row["pool"]: row["program"]
            for row in hints
            if row.get("program") in {CLMM, CPMM} and row.get("pool") not in self.observed_pools
        }
        references = dict(list(references.items())[:24])
        if not references:
            return 0
        for address in references:
            Pubkey.from_string(address)
        response = await self.rpc.accounts(list(references), priority=5)
        supported = {row["mint"]: row for row in assets}
        added = []
        for address, account in zip(references, response["value"], strict=True):
            if not account or account["owner"] != references[address]:
                continue
            state = decode_pool(account)
            pair = {state["mintA"], state["mintB"]}
            stock = next((mint for mint in pair if mint in supported), None)
            if stock is None or len(pair & {USDC_MINT, WSOL}) != 1:
                continue
            if state["decimalsA" if state["mintA"] == stock else "decimalsB"] != supported[stock]["decimals"]:
                continue
            self.assets[stock] = supported[stock]
            self.by_symbol[supported[stock]["symbol"]] = supported[stock]
            row = {
                "address": address,
                "program": account["owner"],
                "mintA": state["mintA"],
                "mintB": state["mintB"],
                "watched": True,
                "snapshot": {"state": state},
            }
            self.pool_catalog[address] = row
            group = self.candidates if USDC_MINT in pair else self.sol_candidates
            group[stock] = list(dict.fromkeys([address, *group.get(stock, [])]))[
                : self.settings.direct.max_pools
            ]
            added.append(row)
        if added:
            path = Path(self.settings.direct.watched_catalog)
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            watched = json.loads(path.read_text()) if path.exists() else {"pools": [], "assets": []}
            merged = {row["address"]: row for row in [*watched["pools"], *added]}
            if len(merged) > 2048:
                raise ValueError("Watched pool discovery exceeds bound")
            watched_assets = {row["mint"]: row for row in [*watched.get("assets", []), *supported.values()]}
            temporary = path.with_suffix(".tmp")
            temporary.write_text(
                json.dumps({"pools": list(merged.values()), "assets": list(watched_assets.values())})
            )
            os.chmod(temporary, 0o600)
            temporary.replace(path)
            self.observed_pools.update(row["address"] for row in added)
            event("wallet_pools_registered", quotes=len(added), component="direct_pools")
        return len(added)

    @property
    def interval(self) -> float:
        return self.settings.rpc_interval_s * 4

    def status(self) -> dict:
        return {
            **self.rpc.http.status(),
            "backend": "direct_raydium",
            "quotes": self.quote_count,
            "failed_pools": self.failures,
            "cpmm_backend": self.calculator.backend,
            "clmm_workers": self.worker.status(),
            "rpc": self.rpc.http.status(),
            "screen": self.screen_status,
            "via_sol": self.settings.direct.via_sol,
            "split_pools": self.settings.direct.split_pools,
            "sol_assets": len(self.sol_candidates),
            "bank_feed": self.bank_feed.status() if self.bank_feed else None,
        }

    async def close(self) -> None:
        await self.worker.close()

    async def prices(self, mints: list[str]) -> dict:
        """Broad point-price screening from fresh RPC banks, independent of exact swaps.

        Spot prices never become an amount quote or execution input. Each pool and
        its CPMM vaults share one bank; Clock proves bank freshness. Chunk size <=100.
        """
        selected = [
            (mint, address) for mint in dict.fromkeys(mints) for address in self.candidates.get(mint, [])
        ]
        seed_refs, seed_entries = [CLOCK], []
        if self.settings.direct.via_sol and self.sol_candidates:
            bridges = await self.bridge_pools()
            if bridges:
                bridge = bridges[0]
                response = await self.rpc.accounts([bridge], priority=10)
                account = response["value"][0]
                state = decode_pool(account)
                if {state["mintA"], state["mintB"]} != {WSOL, USDC_MINT}:
                    raise ValueError("Direct pool pair/program mismatch")
                self.pool_catalog[bridge] = {"program": account["owner"], "snapshot": {"state": state}}
                seed_refs += [
                    bridge,
                    *([state["vaultA"], state["vaultB"]] if state["kind"] == "cpmm" else []),
                ]
                seed_entries = [(WSOL, bridge)]
                selected += [
                    (mint, address)
                    for mint in dict.fromkeys(mints)
                    for address in self.sol_candidates.get(mint, [])
                ]
        chunks: list[tuple[list[str], list[tuple[str, str]]]] = []
        refs = list(seed_refs)
        entries: list[tuple[str, str]] = list(seed_entries)
        for mint, address in selected:
            state = self.pool_catalog[address]["snapshot"]["state"]
            group = [address, *([state["vaultA"], state["vaultB"]] if state["kind"] == "cpmm" else [])]
            extra = [a for a in group if a not in refs]
            if len(refs) + len(extra) > 100:
                chunks.append((refs, entries))
                refs, entries = list(seed_refs), list(seed_entries)
                extra = group
            refs.extend(extra)
            entries.append((mint, address))
        if entries:
            chunks.append((refs, entries))
        prices: dict[str, list[dict]] = {}
        rejected: Counter = Counter()
        for addresses, entries in chunks:
            # Point-price screening selects background exploration. It must
            # not compete with complete banks for current exact minimum quotes.
            response = await self.rpc.accounts(addresses, parsed=True, priority=10)
            accounts = dict(zip(addresses, response["value"], strict=True))
            _, timestamp = checked_clock(accounts[CLOCK])
            observed = time.time()
            if abs(observed - timestamp) > 30:
                raise ValueError("RPC finalized bank is too old")
            sol_price = None
            for mint, address in entries:
                try:
                    account = accounts[address]
                    program = self.pool_catalog[address]["program"]
                    data = account_bytes(account, program, "PoolState", 1544 if program == CLMM else 637)
                    state = decode_pool(account)
                    pair = {state["mintA"], state["mintB"]}
                    quote_mint = USDC_MINT if USDC_MINT in pair else WSOL
                    expected = {mint: 9 if mint == WSOL else 6, quote_mint: 9 if quote_mint == WSOL else 6}
                    if pair != {mint, quote_mint} or any(
                        state["decimals" + side] != expected[state["mint" + side]] for side in ("A", "B")
                    ):
                        raise ValueError("Direct pool pair/program mismatch")
                    if state["status"] & (16 if program == CLMM else 4):
                        raise ValueError("Pool swaps disabled")
                    opened = struct.unpack_from("<Q", data, 1080)[0] if program == CLMM else state["openTime"]
                    if timestamp <= opened:
                        raise ValueError("Pool not open")
                    if program == CLMM:
                        if int(state["liquidity"]) <= 0:
                            raise ValueError("No bounded directional CLMM ticks")
                        spot = dec(state["priceBperA"])
                    else:
                        original = self.pool_catalog[address]["snapshot"]["state"]
                        if any(state["vault" + side] != original["vault" + side] for side in ("A", "B")):
                            raise ValueError("Pool routing changed during refresh")
                        reserves = []
                        for side in ("A", "B"):
                            reserve = vault_info(
                                accounts[state["vault" + side]],
                                state["mint" + side],
                                pda(CPMM, b"vault_and_lp_mint_auth_seed"),
                                state["tokenProgram" + side],
                            ) - sum(int(state[k + side]) for k in ("protocolFees", "fundFees", "creatorFees"))
                            if reserve <= 0:
                                raise ValueError("Empty effective reserves")
                            reserves.append(reserve)
                        spot = dec(
                            spot_b_per_a(reserves[0], reserves[1], state["decimalsA"], state["decimalsB"])
                        )
                    if spot <= 0:
                        raise ValueError("Nonpositive pool spot")
                    normalized = spot if state["mintA"] == mint else 1 / spot
                    if mint == WSOL:
                        sol_price = normalized
                    elif quote_mint == WSOL:
                        if sol_price is None:
                            raise ValueError("SOL bridge spot unavailable")
                        normalized *= sol_price
                    prices.setdefault(mint, []).append(
                        {
                            "price": normalized,
                            "pool": address,
                            "observed_at": observed,
                            "slot": response["context"]["slot"],
                        }
                    )
                except (ValueError, KeyError, TypeError) as error:
                    rejected[error_code(error)] += 1
        result = {USDC_MINT: {"usdPrice": "1", "_observed_at": time.time()}}
        for mint, rows in prices.items():
            sell, buy = max(rows, key=lambda r: r["price"]), min(rows, key=lambda r: r["price"])
            result[mint] = {
                "usdPrice": str(rows[0]["price"]),
                "sellSpotPrice": str(sell["price"]),
                "buySpotPrice": str(buy["price"]),
                "sellPool": sell["pool"],
                "buyPool": buy["pool"],
                "_observed_at": min(r["observed_at"] for r in rows),
                "priceSource": "finalized Raydium pool spot; depth/fees not applied",
            }
        self.screen_status = {
            "assets": len(prices),
            "pools": sum(map(len, prices.values())),
            "rpc_batches": len(chunks),
            "rejected": dict(rejected),
        }
        return result

    async def quote(self, amount: int, *, reverse: bool = False) -> dict:
        a, b = (USDC_MINT, STOCK_MINT) if reverse else (STOCK_MINT, USDC_MINT)
        return await self.quote_pair(a, b, amount)

    async def quote_v1(self, amount: int, slippage_bps: int) -> dict:
        if slippage_bps != self.settings.policy.slippage_bps:
            raise ValueError("Direct slippage differs from configured policy")
        return await self.quote(amount)

    @asynccontextmanager
    async def batch_quotes(self, *, continuous: bool = False):
        batch: QuoteAccountBatch
        if continuous:
            self.bank_feed = PoolBankFeed(self.rpc, self.settings.policy.quote_max_age_s)
            batch = self.bank_feed
        else:
            batch = QuoteAccountBatch(self.rpc)
        try:
            yield partial(self.quote_pair, _rpc=batch)
        finally:
            if continuous:
                self.bank_feed = None
            await batch.close()

    async def cached_plan_quote(
        self, source: str, target: str, amount: int, *, plan: dict, priority: int = 10
    ) -> dict:
        """Reprice the proven route tree locally with exact intermediate credits."""
        from .direct_routes import combine

        feed = self.bank_feed
        if feed is None or plan["inputMint"] != source or plan["outputMint"] != target:
            raise LocalQuoteNotReady()

        async def reprice(node, units):
            legs = node.get("_direct_route")
            if not legs:
                bank = feed.peek((node["pool"], node["inputMint"], node["outputMint"], priority))
                if bank is None:
                    raise LocalQuoteNotReady()
                return await self.quote_bank(node["pool"], node["inputMint"], node["outputMint"], units, bank)
            split = all(
                (leg["inputMint"], leg["outputMint"]) == (node["inputMint"], node["outputMint"])
                for leg in legs
            )
            values: list[dict] = []
            remaining = units
            for index, leg in enumerate(legs):
                if split:
                    credit = (
                        remaining
                        if index == len(legs) - 1
                        else units * int(leg["inAmount"]) // int(node["inAmount"])
                    )
                    remaining -= credit
                else:
                    credit = units if not values else int(values[-1]["outAmount"])
                values.append(await reprice(leg, credit))
            return combine(values, sequential=not split)

        result = await reprice(plan, amount)
        result.update(_local_only=True, direct_execution_supported=False, _request_started_at=time.time())
        feed.promote(result, priority)
        feed.local_quotes += 1
        return result

    @staticmethod
    def bank_versions(plan: dict) -> list[list]:
        legs = plan.get("_direct_route")
        if legs:
            return [version for leg in legs for version in DirectRaydium.bank_versions(leg)]
        if "pool" not in plan:
            return []
        return [
            [plan["pool"], plan["inputMint"], plan["outputMint"], plan["contextSlot"], plan["_observed_at"]]
        ]

    def bank_versions_are_current(self, versions: list[list], priority: int = 10) -> bool:
        feed = self.bank_feed
        if feed is None:
            return True
        if not versions:
            return False
        for pool, source, target, slot, observed in versions:
            bank = feed.peek((pool, source, target, priority))
            if not bank or observed != bank["observed"] or slot != bank["response"]["context"]["slot"]:
                return False
        return True

    async def quote_pair(
        self,
        source: str,
        target: str,
        amount: int,
        *,
        priority: int = 0,
        _rpc: QuoteAccountBatch | None = None,
    ) -> dict:
        if type(amount) is not int or not 0 < amount < 2**63 or source == target:
            raise ValueError("Invalid exact-in amount/pair")
        Pubkey.from_string(source)
        Pubkey.from_string(target)
        stock = target if source == USDC_MINT else source
        if USDC_MINT not in {source, target} or stock not in {*self.assets, WSOL}:
            raise ValueError("Direct router supports catalogued stock/USDC pairs only")
        if self.settings.direct.via_sol and stock != WSOL:
            from .direct_routes import composed_quote

            await self.ensure_candidates(stock)

            if _rpc is None:
                async with self.semaphore, self.batch_quotes() as batch_quote:
                    # batch_quotes exposes its account reader through the bound partial.
                    return await composed_quote(
                        self, source, target, amount, priority, batch_quote.keywords["_rpc"]
                    )
            scope = QuoteAccountBatch(_rpc)
            try:
                composed = await composed_quote(self, source, target, amount, priority, scope)
                if isinstance(_rpc, PoolBankFeed):
                    _rpc.promote(composed, priority)
                return composed
            finally:
                await scope.close()
        pools = self.candidates.get(stock, [])
        if stock == WSOL:
            pools = await self.bridge_pools()
        started = time.time()
        results = await asyncio.gather(
            *(self._limited_quote(address, source, target, amount, priority, _rpc) for address in pools),
            return_exceptions=True,
        )
        valid = [
            q
            for q in results
            if isinstance(q, dict)
            and 0 <= time.time() - q["_observed_at"] <= self.settings.policy.quote_max_age_s
        ]
        self.failures += len(results) - len(valid)
        codes = []
        for address, result in zip(pools, results, strict=True):
            if isinstance(result, BaseException):
                code = error_code(result)
                codes.append(code)
                event(
                    "pool_quote_blocked" if code in MARKET_BLOCKS else "pool_quote_error",
                    level="INFO" if code in MARKET_BLOCKS else "ERROR",
                    pool=address,
                    code=code,
                    error_type=type(result).__name__,
                )
            elif result not in valid:
                codes.append("stale_pool_quote")
        if not valid:
            raise QuoteUnavailable(codes)
        best = max(valid, key=lambda q: int(q["outAmount"]))
        self.quote_count += 1
        best.update(_request_started_at=started, candidate_pools=len(pools), successful_pools=len(valid))
        return best

    async def bridge_pools(self) -> list[str]:
        async with self.gas_discovery_lock:
            if not self.gas_pools:
                ray = Raydium(self.rpc, proxy=resolve_proxy("RAYDIUM"))
                try:
                    found = await ray.pools(WSOL, USDC_MINT)
                finally:
                    await ray.close()
                self.gas_pools = [p["id"] for p in found if p["programId"] in {CLMM, CPMM}][:2]
            return list(self.gas_pools)

    async def _limited_quote(
        self,
        address: str,
        source: str,
        target: str,
        amount: int,
        priority: int,
        rpc: QuoteAccountBatch | None = None,
    ) -> dict:
        if rpc is not None:
            return await self.pool_quote(address, source, target, amount, priority=priority, _rpc=rpc)
        async with self.semaphore:
            return await self.pool_quote(address, source, target, amount, priority=priority)

    async def _pool_bank(
        self,
        pool: str,
        source: str,
        target: str,
        amount: int,
        *,
        priority: int = 0,
        _rpc: QuoteAccountBatch | None = None,
        _max_arrays: int | None = None,
        _reuse_bank: bool = True,
    ) -> dict:
        if type(amount) is not int or not 0 < amount < 2**63 or source == target:
            raise ValueError("Invalid exact-in amount/pair")
        rpc = _rpc if _rpc is not None else self.rpc
        array_limit = _max_arrays or (
            self.array_hints.get((pool, source), self.settings.direct.max_tick_arrays)
            if isinstance(_rpc, PoolBankFeed)
            else self.settings.direct.max_tick_arrays
        )
        bitmap = pda(CLMM, b"pool_tick_array_bitmap_extension", bytes(Pubkey.from_string(pool)))
        hint = self.pool_catalog.get(pool, {})
        min_slot = None
        bitmap_response = None
        previous = (
            _rpc.ready.get((pool, source, target, priority))
            if isinstance(_rpc, PoolBankFeed) and _reuse_bank
            else None
        )
        reuse = bool(previous and previous["program"] == CLMM and previous["array_limit"] == array_limit)
        if reuse:
            assert previous is not None
            # Known addresses only: every account and the directional bitmap
            # are fetched and validated again in one fresh bank below.
            initial_state = previous["state"]
            program = CLMM
            min_slot = previous["response"]["context"]["slot"]
        elif _rpc is not None and hint.get("program") == CPMM:
            # Catalogue state supplies addresses only. The complete fresh bank
            # below verifies the pool, routing, mints, vaults, fees and Clock.
            initial_state = {
                **hint["snapshot"]["state"],
                "observation": hint["snapshot"]["state"].get("observation")
                or pda(CPMM, b"observation", bytes(Pubkey.from_string(pool))),
            }
            program = CPMM
        else:
            refs = [pool, bitmap] if _rpc is not None and hint.get("program") == CLMM else [pool]
            initial = await rpc.accounts(refs, priority=priority)
            initial_state = decode_pool(initial["value"][0])
            program = initial["value"][0]["owner"]
            min_slot = initial["context"]["slot"]
            if len(refs) == 2:
                bitmap_response = initial
        if program not in {CLMM, CPMM} or {source, target} != {
            initial_state["mintA"],
            initial_state["mintB"],
        }:
            raise ValueError("Direct pool pair/program mismatch")
        arrays: list[str] = list(previous["arrays"]) if reuse and previous is not None else []
        if program == CLMM and not reuse:
            response = bitmap_response or await rpc.accounts(
                [pool, bitmap], min_slot=min_slot, priority=priority
            )
            min_slot = max(min_slot or 0, response["context"]["slot"])
            account_bytes(response["value"][1], CLMM, "TickArrayBitmapExtension", 1832)
            params = self._worker_params(pool, response["value"][0], response["value"][1], source)
            params["maxArrays"] = array_limit
            arrays = (await self.worker.call({**params, "op": "addresses"}))["addresses"]
            if not arrays or len(arrays) > self.settings.direct.max_tick_arrays:
                raise ValueError("No bounded directional CLMM ticks")
        refs = list(
            dict.fromkeys(
                [
                    pool,
                    initial_state["config"],
                    initial_state["vaultA"],
                    initial_state["vaultB"],
                    initial_state["mintA"],
                    initial_state["mintB"],
                    initial_state["observation"],
                    CLOCK,
                    *([bitmap] if program == CLMM else []),
                    *arrays,
                ]
            )
        )
        # jsonParsed keeps unsupported program data as base64; mint/vault parsing is RPC-native.
        response = await rpc.accounts(refs, parsed=True, min_slot=min_slot, priority=priority)
        observed = time.time()
        acquired_monotonic = time.monotonic()
        accounts = dict(zip(refs, response["value"], strict=True))
        account_bytes(accounts[pool], program, "PoolState", 1544 if program == CLMM else 637)
        state = decode_pool(accounts[pool])
        for key in ("config", "vaultA", "vaultB", "mintA", "mintB", "observation"):
            if state[key] != initial_state[key]:
                raise ValueError("Pool routing changed during refresh")
        if state["status"] & (16 if program == CLMM else 4):
            raise ValueError("Pool swaps disabled")
        epoch, timestamp = checked_clock(accounts[CLOCK])
        # Clock is protocol time for epoch/fees/opening, not the wall-clock
        # acquisition time of this complete atomic RPC account bank.
        if abs(time.time() - timestamp) > 30:
            raise ValueError("RPC finalized bank is too old")
        pool_raw = base64.b64decode(accounts[pool]["data"][0], validate=True)
        opened = struct.unpack_from("<Q", pool_raw, 1080)[0] if program == CLMM else state["openTime"]
        if timestamp <= opened:
            raise ValueError("Pool not open")
        state["fees"] = decode_fee_config(accounts[state["config"]], state["kind"])
        if program == CLMM:
            config_raw = account_bytes(accounts[state["config"]], CLMM, "AmmConfig", 117)
            if (
                struct.unpack_from("<H", config_raw, 51)[0] != state["tickSpacing"]
                or not state["tickSpacing"]
            ):
                raise ValueError("CLMM config/pool tick spacing mismatch")
            fees = state["fees"]
            if fees["tradeFeeRate"] >= 1_000_000 or fees["protocolFeeRate"] + fees["fundFeeRate"] > 1_000_000:
                raise ValueError("Invalid CLMM fees")
        for side in ("A", "B"):
            cap = mint_capabilities(accounts[state["mint" + side]])
            if not cap["quote_safe"] or cap["decimals"] != state["decimals" + side]:
                raise ValueError("Unsafe mint capability/precision")
            if program == CPMM and state["tokenProgram" + side] != cap["program"]:
                raise ValueError("Pool mint token program mismatch")
            state["tokenProgram" + side] = cap["program"]
            state["transferFee" + side] = (
                transfer_fee_profile(cap["transfer_fee_config"], epoch)
                if cap.get("transfer_fee_config")
                else {"bps": 0, "maximum": 0}
            )
            authority = pool if program == CLMM else pda(CPMM, b"vault_and_lp_mint_auth_seed")
            vault = vault_info(
                accounts[state["vault" + side]], state["mint" + side], authority, cap["program"]
            )
            state["vaultAmount" + side] = vault
            if program == CPMM:
                reserve = vault - sum(
                    int(state[k + side]) for k in ("protocolFees", "fundFees", "creatorFees")
                )
                if reserve <= 0:
                    raise ValueError("Empty effective reserves")
                state["reserve" + side] = str(reserve)
        if program == CLMM:
            account_bytes(accounts[bitmap], CLMM, "TickArrayBitmapExtension", 1832)
            params = self._worker_params(pool, accounts[pool], accounts[bitmap], source)
            params["maxArrays"] = array_limit
            current_arrays = (await self.worker.call({**params, "op": "addresses"}))["addresses"]
            if current_arrays != arrays:
                if not reuse:
                    raise ValueError("Directional tick bitmap changed during refresh")
                # Price movement changed the required tick accounts. Discover
                # and fetch the entire new bank before it can become usable.
                return await self._pool_bank(
                    pool,
                    source,
                    target,
                    amount,
                    priority=priority,
                    _rpc=_rpc,
                    _max_arrays=_max_arrays,
                    _reuse_bank=False,
                )
        validate_auxiliary(pool, program, state, accounts, arrays)
        return {
            "state": state,
            "program": program,
            "accounts": accounts,
            "arrays": arrays,
            "bitmap": bitmap,
            "timestamp": timestamp,
            "observed": observed,
            "_acquired_monotonic": acquired_monotonic,
            "response": response,
            "array_limit": array_limit,
            "arrays_verified": program == CLMM,
        }

    async def pool_quote(
        self,
        pool: str,
        source: str,
        target: str,
        amount: int,
        *,
        priority: int = 0,
        _rpc: QuoteAccountBatch | None = None,
    ) -> dict:
        if type(amount) is not int or not 0 < amount < 2**63 or source == target:
            raise ValueError("Invalid exact-in amount/pair")
        banks = _rpc
        if (
            _rpc is not None
            and isinstance(_rpc.rpc, QuoteAccountBatch)
            and (isinstance(_rpc.rpc, PoolBankFeed) or {source, target} == {WSOL, USDC_MINT})
        ):
            # The monitor shares all amount-independent banks; ordinary quote
            # scopes share only SOL/USDC bridge banks, preserving original age.
            banks = _rpc.rpc
        fetch = partial(self._pool_bank, pool, source, target, amount, priority=priority, _rpc=banks)
        bank = (
            await banks.pool_snapshot(
                (pool, source, target, priority), fetch, self.settings.policy.quote_max_age_s
            )
            if banks is not None and (self.settings.direct.via_sol or isinstance(banks, PoolBankFeed))
            else await fetch()
        )
        try:
            return await self.quote_bank(pool, source, target, amount, bank)
        except ClmmCalculationError as error:
            if (
                error.code != "insufficient_tick_coverage"
                or bank.get("array_limit", 8) >= self.settings.direct.max_tick_arrays
            ):
                raise
            # A larger amount may need more ticks than the fast refresh bank.
            # Fetch full coverage; never return a partially filled amount.
            bank = await self._pool_bank(
                pool,
                source,
                target,
                amount,
                priority=priority,
                _rpc=banks,
                _max_arrays=self.settings.direct.max_tick_arrays,
            )
            return await self.quote_bank(pool, source, target, amount, bank)

    async def quote_bank(self, pool: str, source: str, target: str, amount: int, bank: dict) -> dict:
        """Reuse exact math only within this validated bank, keeping its original age."""
        state = bank["state"]
        if (
            type(amount) is not int
            or not 0 < amount < 2**63
            or source == target
            or {source, target} != {state["mintA"], state["mintB"]}
        ):
            raise ValueError("Invalid local bank amount/pair")
        # The feed still selects the current complete bank and guards its head.
        # Cached math cannot make an expired/future acquisition usable again.
        if not 0 <= time.time() - bank["observed"] < self.settings.policy.quote_max_age_s:
            raise LocalQuoteNotReady()
        memo = bank.get("_exact_quote_math")
        if memo is None or memo["bank_id"] != id(bank):
            memo = {"bank_id": id(bank), "locks": {}, "quotes": {}}
            bank["_exact_quote_math"] = memo
        slippage = self.settings.policy.slippage_bps
        array_limit = bank.get("array_limit", self.settings.direct.max_tick_arrays)
        key = (
            pool,
            source,
            target,
            amount,
            slippage,
            array_limit,
            bank["response"]["context"]["slot"],
            bank["observed"],
        )
        quotes = memo["quotes"]
        if key in quotes:
            return deepcopy(quotes[key])
        locks = memo["locks"]
        entry = locks.setdefault(key, {"lock": asyncio.Lock(), "users": 0})
        entry["users"] += 1
        try:
            # Different exact amounts can use independent SDK processes. Equal
            # inputs share one calculation, including callers waiting for it.
            async with entry["lock"]:
                if not 0 <= time.time() - bank["observed"] < self.settings.policy.quote_max_age_s:
                    raise LocalQuoteNotReady()
                if key not in quotes:
                    result = await self._quote_bank_math(
                        pool, source, target, amount, bank, slippage=slippage, array_limit=array_limit
                    )
                    if len(quotes) >= 64:
                        quotes.pop(next(iter(quotes)))
                    quotes[key] = deepcopy(result)
                # _direct contains the bank's state. Even the first caller
                # receives a copy so it cannot poison the bank or cached math.
                return deepcopy(quotes[key])
        finally:
            entry["users"] -= 1
            if not entry["users"]:
                locks.pop(key)

    async def _quote_bank_math(
        self,
        pool: str,
        source: str,
        target: str,
        amount: int,
        bank: dict,
        *,
        slippage: int,
        array_limit: int,
    ) -> dict:
        state, program, accounts = bank["state"], bank["program"], bank["accounts"]
        arrays, bitmap = list(bank["arrays"]), bank["bitmap"]
        timestamp, observed, response = bank["timestamp"], bank["observed"], bank["response"]
        direction = source == state["mintA"]
        if program == CPMM:
            state["quoteSupported"] = True
            frame = PreparedCpmmPools(self.calculator, [state])
            output, _, error = frame.quote_batch([(0, direction, amount, slippage)])[0]
            if error:
                raise ValueError("Unquotable CPMM amount")
            spot = dec(
                spot_b_per_a(
                    int(state["reserveA"]), int(state["reserveB"]), state["decimalsA"], state["decimalsB"]
                )
            )
        else:
            params = self._worker_params(pool, accounts[pool], accounts[bitmap], source)
            params["maxArrays"] = array_limit
            if not bank.get("arrays_verified"):
                expected = (await self.worker.call({**params, "op": "addresses"}))["addresses"]
                if expected != arrays:
                    raise ValueError("Directional tick bitmap changed during refresh")
            incoming = state["transferFeeA" if direction else "transferFeeB"]
            outgoing = state["transferFeeB" if direction else "transferFeeA"]
            net = amount - min(incoming["maximum"], (amount * incoming["bps"] + 9999) // 10000)
            result = await self.worker.call(
                {
                    **params,
                    "op": "quote",
                    "amount": str(net),
                    "timestamp": timestamp,
                    "configData": accounts[state["config"]]["data"][0],
                    "arrays": [{"address": a, "data": accounts[a]["data"][0]} for a in arrays],
                }
            )
            used_arrays = result["accounts"]
            if not used_arrays or used_arrays != arrays[: len(used_arrays)]:
                raise ValueError("CLMM worker returned invalid swap account coverage")
            # All arrays were validated for the quote; only consumed arrays belong
            # in the transaction, where unnecessary keys can exceed the packet cap.
            arrays = used_arrays
            self.array_hints[(pool, source)] = min(
                self.settings.direct.max_tick_arrays, max(2, len(arrays) + 1)
            )
            gross = int(result["output"])
            output = gross - min(outgoing["maximum"], (gross * outgoing["bps"] + 9999) // 10000)
            if not 4295048017 < int(result["sqrtPrice"]) < 79226673521066979257578248090:
                raise ValueError("CLMM exhausted price limit")
            spot = dec(state["priceBperA"])
        if not 0 < output < 2**63:
            raise ValueError("Output outside durable amount bounds")
        input_decimals, output_decimals = (
            (state["decimalsA"], state["decimalsB"])
            if direction
            else (state["decimalsB"], state["decimalsA"])
        )
        ideal = (
            Decimal(amount)
            * (spot if direction else 1 / spot)
            * Decimal(10) ** (output_decimals - input_decimals)
        )
        impact = max(Decimal(0), (1 - Decimal(output) / ideal) * 100)
        stock = source if source != USDC_MINT else target
        if USDC_MINT in {source, target}:
            self.cached_prices[stock] = (observed, str(spot if state["mintB"] == USDC_MINT else 1 / spot))
        return {
            "inputMint": source,
            "outputMint": target,
            "inAmount": str(amount),
            "outAmount": str(output),
            "otherAmountThreshold": str(output * (10000 - slippage) // 10000),
            "priceImpact": str(impact),
            "swapMode": "ExactIn",
            "router": "Direct Raydium",
            "source": "fresh local Raydium exact-in",
            "pool": pool,
            "kind": state["kind"],
            "direct_execution_supported": True,
            "routePlan": [
                {
                    "percent": 100,
                    "swapInfo": {
                        "ammKey": pool,
                        "label": "Raydium " + state["kind"].upper(),
                        "inputMint": source,
                        "outputMint": target,
                    },
                }
            ],
            "contextSlot": response["context"]["slot"],
            "_observed_at": observed,
            "_direct": {
                "state": state,
                "program": program,
                "arrays": arrays,
                "bitmap": bitmap if program == CLMM else None,
            },
        }

    def _worker_params(self, pool: str, account: dict, bitmap: dict, source: str) -> dict:
        state = decode_pool(account)
        return {
            "pool": pool,
            "program": CLMM,
            "poolData": account["data"][0],
            "bitmapData": bitmap["data"][0],
            "zeroForOne": source == state["mintA"],
            "maxArrays": self.settings.direct.max_tick_arrays,
        }


def direct_instructions(q: dict, wallet: str, minimum: int, settings: Settings) -> list[Instruction]:
    amount = int(q["inAmount"])
    if (
        not 0 < amount < 2**63
        or not 0 < int(q["otherAmountThreshold"]) <= minimum <= int(q["outAmount"]) < 2**63
    ):
        raise ValueError("Direct encoded minimum/input outside policy")
    ctx = q["_direct"]
    state = ctx["state"]
    program = ctx["program"]
    if program not in {CLMM, CPMM} or {q["inputMint"], q["outputMint"]} != {state["mintA"], state["mintB"]}:
        raise ValueError("Direct swap pair/program mismatch")
    direction = q["inputMint"] == state["mintA"]
    a, b = ("A", "B") if direction else ("B", "A")
    source = associated_address(wallet, state["mint" + a], state["tokenProgram" + a])
    target = associated_address(wallet, state["mint" + b], state["tokenProgram" + b])

    def instruction(program_id: str, data: bytes, rows: list[tuple[str, bool, bool]]) -> Instruction:
        return Instruction(
            Pubkey.from_string(program_id),
            data,
            [AccountMeta(Pubkey.from_string(k), s, w) for k, s, w in rows],
        )

    d = settings.direct
    if (
        5000 + (d.compute_units * d.priority_micro_lamports + 999999) // 1000000
        > settings.policy.max_network_lamports
    ):
        raise ValueError("Direct priority fee exceeds policy")
    result = [
        instruction(COMPUTE, b"\x02" + struct.pack("<I", d.compute_units), []),
        instruction(COMPUTE, b"\x03" + struct.pack("<Q", d.priority_micro_lamports), []),
        instruction(
            ATA,
            b"\x01",
            [
                (wallet, True, True),
                (target, False, True),
                (wallet, False, False),
                (state["mint" + b], False, False),
                (SYSTEM, False, False),
                (state["tokenProgram" + b], False, False),
            ],
        ),
    ]
    shared = [(wallet, True, False)]
    if program == CPMM:
        shared += [(pda(CPMM, b"vault_and_lp_mint_auth_seed"), False, False)]
    shared += [
        (state["config"], False, False),
        (q["pool"], False, True),
        (source, False, True),
        (target, False, True),
        (state["vault" + a], False, True),
        (state["vault" + b], False, True),
    ]
    if program == CPMM:
        shared += [
            (state["tokenProgram" + a], False, False),
            (state["tokenProgram" + b], False, False),
            (state["mint" + a], False, False),
            (state["mint" + b], False, False),
            (state["observation"], False, True),
        ]
        data = hashlib.sha256(b"global:swap_base_input").digest()[:8] + struct.pack("<QQ", amount, minimum)
    else:
        if not 1 <= len(ctx["arrays"]) <= settings.direct.max_tick_arrays:
            raise ValueError("Unbounded CLMM remaining accounts")
        shared += [
            (state["observation"], False, True),
            (TOKEN, False, False),
            (TOKEN_2022, False, False),
            (MEMO, False, False),
            (state["mint" + a], False, False),
            (state["mint" + b], False, False),
            (ctx["bitmap"], False, False),
            *[(v, False, True) for v in ctx["arrays"]],
        ]
        data = (
            hashlib.sha256(b"global:swap_v2").digest()[:8]
            + struct.pack("<QQ", amount, minimum)
            + bytes(16)
            + b"\x01"
        )
    result.append(instruction(program, data, shared))
    return result
