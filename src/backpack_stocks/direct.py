"""Direct single-pool Raydium quotes and locally constructed instructions."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import struct
import time
from collections import Counter
from contextlib import asynccontextmanager
from decimal import Decimal
from functools import partial
from pathlib import Path

from solders.instruction import AccountMeta, Instruction
from solders.pubkey import Pubkey

from .clmm_worker import ClmmWorker
from .models import ATA, STOCK_MINT, TOKEN, TOKEN_2022, USDC_MINT, Settings, dec
from .observability import MARKET_BLOCKS, QuoteUnavailable, error_code, event
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
    if observation != pda(program, b"observation", bytes(Pubkey.from_string(pool))):
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
        self.worker = ClmmWorker(settings.direct.clmm_worker)
        self.calculator = PoolCalculator()
        catalog = json.loads(Path(settings.direct.catalog).read_text())
        self.assets = {a["mint"]: a for a in catalog["assets"]}
        self.by_symbol = {a["symbol"]: a for a in self.assets.values()}
        self.candidates: dict[str, list[str]] = {}
        grouped: dict[str, list[tuple[int, str]]] = {}
        for pool in catalog["pools"]:
            pair = {pool.get("mintA"), pool.get("mintB")}
            mint = next((m for m in pair if m in self.assets), None)
            state = pool.get("snapshot", {}).get("state", {})
            if mint and USDC_MINT in pair and state.get("kind") in {"cpmm", "clmm"}:
                # Historical liquidity is only discovery priority, never a quote.
                weight = int(state.get("liquidity", state.get("reserveB", "0")))
                grouped.setdefault(mint, []).append((weight, pool["address"]))
        for mint, rows in grouped.items():
            self.candidates[mint] = [a for _, a in sorted(rows, reverse=True)[: settings.direct.max_pools]]
        selected_addresses = {a for addresses in self.candidates.values() for a in addresses}
        self.pool_catalog = {p["address"]: p for p in catalog["pools"] if p["address"] in selected_addresses}
        self.semaphore = asyncio.Semaphore(settings.direct.concurrency)
        self.quote_count = 0
        self.failures = 0
        self.cached_prices: dict[str, tuple[float, str]] = {}
        self.screen_status: dict = {}
        self.gas_pools: list[str] = []

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
            "rpc": self.rpc.http.status(),
            "screen": self.screen_status,
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
        chunks: list[tuple[list[str], list[tuple[str, str]]]] = []
        refs = [CLOCK]
        entries: list[tuple[str, str]] = []
        for mint, address in selected:
            state = self.pool_catalog[address]["snapshot"]["state"]
            group = [address, *([state["vaultA"], state["vaultB"]] if state["kind"] == "cpmm" else [])]
            extra = [a for a in group if a not in refs]
            if len(refs) + len(extra) > 100:
                chunks.append((refs, entries))
                refs, entries = [CLOCK], []
                extra = group
            refs.extend(extra)
            entries.append((mint, address))
        if entries:
            chunks.append((refs, entries))
        prices: dict[str, list[dict]] = {}
        rejected: Counter = Counter()
        for addresses, entries in chunks:
            response = await self.rpc.accounts(addresses, parsed=True, priority=5)
            accounts = dict(zip(addresses, response["value"], strict=True))
            _, timestamp = checked_clock(accounts[CLOCK])
            observed = time.time()
            if abs(observed - timestamp) > 30:
                raise ValueError("RPC finalized bank is too old")
            for mint, address in entries:
                try:
                    account = accounts[address]
                    program = self.pool_catalog[address]["program"]
                    data = account_bytes(account, program, "PoolState", 1544 if program == CLMM else 637)
                    state = decode_pool(account)
                    if {state["mintA"], state["mintB"]} != {mint, USDC_MINT} or (
                        state["decimalsA"] != 6 or state["decimalsB"] != 6
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
                    prices.setdefault(mint, []).append(
                        {
                            "price": spot if state["mintB"] == USDC_MINT else 1 / spot,
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
    async def batch_quotes(self):
        batch = QuoteAccountBatch(self.rpc)
        try:
            yield partial(self.quote_pair, _rpc=batch)
        finally:
            await batch.close()

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
        pools = self.candidates.get(stock, [])
        if stock == WSOL:
            if not self.gas_pools:
                ray = Raydium(self.rpc, proxy=resolve_proxy("RAYDIUM"))
                try:
                    found = await ray.pools(WSOL, USDC_MINT)
                finally:
                    await ray.close()
                self.gas_pools = [p["id"] for p in found if p["programId"] in {CLMM, CPMM}][:2]
            pools = self.gas_pools
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
        rpc = _rpc if _rpc is not None else self.rpc
        initial = await rpc.accounts([pool], priority=priority)
        initial_state = decode_pool(initial["value"][0])
        program = initial["value"][0]["owner"]
        if program not in {CLMM, CPMM} or {source, target} != {
            initial_state["mintA"],
            initial_state["mintB"],
        }:
            raise ValueError("Direct pool pair/program mismatch")
        initial_state["observation"] = (
            initial_state["observation"]
            if program == CLMM
            else str(Pubkey.from_bytes(base64.b64decode(initial["value"][0]["data"][0])[296:328]))
        )
        bitmap = pda(CLMM, b"pool_tick_array_bitmap_extension", bytes(Pubkey.from_string(pool)))
        arrays: list[str] = []
        min_slot = initial["context"]["slot"]
        if program == CLMM:
            response = await rpc.accounts([pool, bitmap], min_slot=min_slot, priority=priority)
            min_slot = max(min_slot, response["context"]["slot"])
            account_bytes(response["value"][1], CLMM, "TickArrayBitmapExtension", 1832)
            params = self._worker_params(pool, response["value"][0], response["value"][1], source)
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
        accounts = dict(zip(refs, response["value"], strict=True))
        account_bytes(accounts[pool], program, "PoolState", 1544 if program == CLMM else 637)
        state = decode_pool(accounts[pool])
        state["observation"] = initial_state["observation"]
        for key in ("config", "vaultA", "vaultB", "mintA", "mintB"):
            if state[key] != initial_state[key]:
                raise ValueError("Pool routing changed during refresh")
        if state["status"] & (16 if program == CLMM else 4):
            raise ValueError("Pool swaps disabled")
        epoch, timestamp = checked_clock(accounts[CLOCK])
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
        validate_auxiliary(pool, program, state, accounts, arrays)
        direction = source == state["mintA"]
        if program == CPMM:
            state["quoteSupported"] = True
            frame = PreparedCpmmPools(self.calculator, [state])
            output, _, error = frame.quote_batch([(0, direction, amount, self.settings.policy.slippage_bps)])[
                0
            ]
            if error:
                raise ValueError("Unquotable CPMM amount")
            spot = dec(
                spot_b_per_a(
                    int(state["reserveA"]), int(state["reserveB"]), state["decimalsA"], state["decimalsB"]
                )
            )
        else:
            params = self._worker_params(pool, accounts[pool], accounts[bitmap], source)
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
        self.cached_prices[stock] = (observed, str(spot if state["mintB"] == USDC_MINT else 1 / spot))
        return {
            "inputMint": source,
            "outputMint": target,
            "inAmount": str(amount),
            "outAmount": str(output),
            "otherAmountThreshold": str(output * (10000 - self.settings.policy.slippage_bps) // 10000),
            "priceImpact": str(impact),
            "swapMode": "ExactIn",
            "router": "Direct Raydium",
            "source": "fresh local Raydium exact-in",
            "pool": pool,
            "kind": state["kind"],
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
