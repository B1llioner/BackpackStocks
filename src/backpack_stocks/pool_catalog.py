"""Bounded, read-only pool discovery and coherent per-pool RPC snapshots."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import struct
import uuid
from collections import Counter
from datetime import UTC, datetime

from solders.pubkey import Pubkey

from .backpack import Backpack
from .jupiter import Jupiter
from .models import TOKEN, TOKEN_2022, USDC_MINT, dec, raw
from .monitor import Asset, discover
from .pool_compute import PoolCalculator, PreparedCpmmPools
from .pool_math import cpmm_exact_in, spot_b_per_a
from .pool_state import PROGRAM_KINDS, decode_fee_config, decode_pool
from .raydium import Raydium
from .solana import SolanaRPC, decode_token
from .transport import RemoteError

CLOCK = "SysvarC1ock11111111111111111111111111111111"
SYSVAR = "Sysvar1111111111111111111111111111111111111"


def stamp() -> str:
    return datetime.now(UTC).isoformat()


def transfer_fee_profile(state: dict, epoch: int) -> dict:
    newer = state["newerTransferFee"]
    profile = newer if epoch >= int(newer["epoch"]) else state["olderTransferFee"]
    bps, maximum = int(profile["transferFeeBasisPoints"]), int(profile["maximumFee"])
    if not 0 <= bps <= 10000 or not 0 <= maximum < 2**64:
        raise ValueError("Invalid transfer fee profile")
    return {"bps": bps, "maximum": maximum}


def mint_capabilities(account: dict | None) -> dict:
    result: dict = {"verified": False, "quote_safe": False, "reason": "missing_or_unknown_mint"}
    if not account or account.get("owner") not in {TOKEN, TOKEN_2022}:
        return result
    info = account.get("data", {}).get("parsed", {}).get("info", {})
    if (
        account.get("data", {}).get("parsed", {}).get("type") != "mint"
        or not info.get("isInitialized")
        or type(info.get("decimals")) is not int
        or not 0 <= info["decimals"] <= 255
    ):
        return result
    result.update(verified=True, decimals=info["decimals"], program=account["owner"], info=info)
    extensions = {row["extension"]: row.get("state", {}) for row in info.get("extensions", [])}
    supported = {
        "metadataPointer",
        "tokenMetadata",
        "permanentDelegate",
        "defaultAccountState",
        "pausableConfig",
        "confidentialTransferMint",
        "scaledUiAmountConfig",
        "transferHook",
        "transferFeeConfig",
    }
    if set(extensions) - supported:
        return {**result, "reason": "unsupported_mint_extension"}
    if extensions.get("pausableConfig", {}).get("paused"):
        return {**result, "reason": "mint_paused"}
    if extensions.get("transferHook", {}).get("programId"):
        return {**result, "reason": "active_transfer_hook"}
    if extensions.get("defaultAccountState", {}).get("accountState", "initialized") != "initialized":
        return {**result, "reason": "default_account_frozen"}
    scaled = extensions.get("scaledUiAmountConfig")
    if scaled and (
        dec(str(scaled.get("multiplier", "0"))) != 1 or dec(str(scaled.get("newMultiplier", "0"))) != 1
    ):
        return {**result, "reason": "scaled_ui_not_one"}
    transfer = extensions.get("transferFeeConfig")
    if transfer:
        try:
            transfer_fee_profile(transfer, 0)
            transfer_fee_profile(transfer, 2**64 - 1)
        except (KeyError, ValueError, TypeError):
            return {**result, "reason": "invalid_transfer_fee_config"}
        result["transfer_fee_config"] = transfer
    return {**result, "quote_safe": True, "reason": None}


def enrich_snapshot(state: dict, accounts: dict, tokens: dict) -> dict:
    """Pool, vaults, config and Clock must come from one getMultipleAccounts."""
    a, b = decode_token(accounts[state["vaultA"]]), decode_token(accounts[state["vaultB"]])
    if a["mint"] != state["mintA"] or b["mint"] != state["mintB"]:
        raise ValueError("Vault mint mismatch")
    if a["owner"] != b["owner"]:
        raise ValueError("Vault authorities disagree")
    state.update(vaultAmountA=str(a["amount"]), vaultAmountB=str(b["amount"]))
    if accounts[CLOCK]["owner"] != SYSVAR:
        raise ValueError("Clock sysvar owner mismatch")
    clock = base64.b64decode(accounts[CLOCK]["data"][0], validate=True)
    if len(clock) != 40:
        raise ValueError("Invalid Clock account")
    state.update(
        epoch=struct.unpack_from("<Q", clock, 16)[0], chainTimestamp=struct.unpack_from("<q", clock, 32)[0]
    )
    if state.get("config"):
        state["fees"] = decode_fee_config(accounts[state["config"]], state["kind"])
    cap_a, cap_b = tokens.get(state["mintA"], {}), tokens.get(state["mintB"], {})
    safe = cap_a.get("quote_safe") and cap_b.get("quote_safe")
    if cap_a.get("decimals") != state["decimalsA"] or cap_b.get("decimals") != state["decimalsB"]:
        safe = False
    for side, cap in (("A", cap_a), ("B", cap_b)):
        state["transferFee" + side] = (
            transfer_fee_profile(cap["transfer_fee_config"], state["epoch"])
            if cap.get("transfer_fee_config")
            else {"bps": 0, "maximum": 0}
        )
    state["quoteSupported"] = False
    if state["kind"] == "clmm":
        state["quoteReason"] = (
            "CLMM requires tick arrays/bitmap, limit orders, dynamic fee and integer swap traversal"
        )
        return state
    if state["kind"] == "amm_v4":
        # Current SDK subtracts PnL; historical orderbook deployments need their
        # OpenOrders inventory accounted for too. Don't silently assume it is 0.
        state["reserveA"] = str(a["amount"] - int(state["needTakePnlA"]))
        state["reserveB"] = str(b["amount"] - int(state["needTakePnlB"]))
        if min(int(state["reserveA"]), int(state["reserveB"])) < 0:
            raise ValueError("AMM PNL exceeds vault balance")
        if min(int(state["reserveA"]), int(state["reserveB"])) > 0:
            state["priceBperA"] = spot_b_per_a(
                int(state["reserveA"]), int(state["reserveB"]), state["decimalsA"], state["decimalsB"]
            )
        state["quoteReason"] = "AMM v4 executable quote requires verified orderbook/PNL/fee semantics"
        return state
    for side, account in (("A", a), ("B", b)):
        if account["program"] != state["tokenProgram" + side]:
            raise ValueError("Vault token program mismatch")
        expected_authority = str(
            Pubkey.find_program_address(
                [b"vault_and_lp_mint_auth_seed"], Pubkey.from_string(accounts[state["config"]]["owner"])
            )[0]
        )
        if account["owner"] != expected_authority or account["delegate"] or account["close_authority"]:
            raise ValueError("Unexpected CPMM vault authority/control")
        reserve = account["amount"] - sum(
            int(state[k + side]) for k in ("protocolFees", "fundFees", "creatorFees")
        )
        if reserve < 0:
            raise ValueError("Accrued fees exceed vault balance")
        state["reserve" + side] = str(reserve)
    if min(int(state["reserveA"]), int(state["reserveB"])) <= 0:
        state["quoteReason"] = "empty_effective_reserves"
        return state
    state["priceBperA"] = spot_b_per_a(
        int(state["reserveA"]), int(state["reserveB"]), state["decimalsA"], state["decimalsB"]
    )
    if not safe:
        state["quoteReason"] = "unverified_or_unsupported_token_capabilities"
    elif state["status"] & 4 or state["openTime"] > state["chainTimestamp"]:
        state["quoteReason"] = "swap_disabled_or_not_open"
    else:
        state.update(quoteSupported=True, quoteReason=None, pricingModel="cp-swap-59fb845a-exact-in")
    return state


def cpmm_request(state: dict, input_mint: str, amount: int, *, slippage_bps: int = 50) -> tuple[tuple, dict]:
    if state["kind"] != "cpmm" or not state.get("quoteSupported"):
        raise ValueError("Pool state cannot produce a supported local quote")
    if (
        type(amount) is not int
        or not 0 < amount < 2**64
        or input_mint not in {state["mintA"], state["mintB"]}
        or type(slippage_bps) is not int
        or not 0 <= slippage_bps <= 500
    ):
        raise ValueError("Invalid mint or slippage")
    side, target = ("A", "B") if input_mint == state["mintA"] else ("B", "A")
    creator = state["fees"]["creatorFeeRate"] if state["enableCreatorFee"] else 0
    fee_in, fee_out = (
        state.get("transferFee" + side, {"bps": 0, "maximum": 0}),
        state.get("transferFee" + target, {"bps": 0, "maximum": 0}),
    )
    transfer_in = min(fee_in["maximum"], (amount * fee_in["bps"] + 9999) // 10000)
    return (
        (
            amount,
            int(state["reserve" + side]),
            int(state["reserve" + target]),
            state["fees"]["tradeFeeRate"],
            creator,
            state["feeOn"] == 0 or state["feeOn"] == (1 if side == "A" else 2),
            transfer_in,
            fee_out["bps"],
            fee_out["maximum"],
        ),
        {
            "input_mint": input_mint,
            "output_mint": state["mint" + target],
            "slippage_bps": slippage_bps,
            "source": "local_cpmm",
            "model": "snapshot_quote_not_execution",
        },
    )


def finish_cpmm_quote(result: dict, metadata: dict) -> dict:
    return {
        **result,
        **metadata,
        "minimum_output_raw": result["output_raw"] * (10000 - metadata["slippage_bps"]) // 10000,
    }


def quote_cpmm_state(state: dict, input_mint: str, amount: int, *, slippage_bps: int = 50) -> dict:
    row, metadata = cpmm_request(state, input_mint, amount, slippage_bps=slippage_bps)
    amount, rin, rout, trade, creator, creator_input, transfer_in, transfer_bps, transfer_max = row
    result = cpmm_exact_in(
        amount,
        rin,
        rout,
        trade,
        creator_fee_rate=creator,
        creator_on_input=creator_input,
        input_transfer_fee=transfer_in,
        output_transfer_bps=transfer_bps,
        output_transfer_maximum=transfer_max,
    )
    return finish_cpmm_quote(result, metadata)


async def collect_catalog(
    backpack: Backpack,
    raydium: Raydium,
    rpc: SolanaRPC,
    *,
    jupiter: Jupiter | None = None,
    amount_ui: str = "1",
    concurrency: int = 8,
    progress=None,
) -> dict:
    if not 1 <= concurrency <= 20 or dec(amount_ui) <= 0:
        raise ValueError("Invalid concurrency or sample size")
    started = stamp()
    assets, securities = await asyncio.gather(backpack.assets(), backpack.securities())
    universe = discover(assets, securities)
    pools: dict[str, dict] = {}
    links: set[tuple[str, str, str]] = set()
    errors: list[dict] = []
    quotes: list[dict] = []
    semaphore = asyncio.Semaphore(concurrency)
    completed = 0

    async def discover_asset(asset):
        nonlocal completed
        async with semaphore:
            try:
                rows = await raydium.pools(asset.mint, None)
                for row in rows:
                    address = row["id"]
                    pools[address] = {
                        "address": address,
                        "venue": "Raydium",
                        "discovery": row,
                        "program": row.get("programId"),
                        "kind": "unverified",
                        "verified": False,
                        "mintA": row["mintA"]["address"],
                        "mintB": row["mintB"]["address"],
                    }
                    links.add((asset.symbol, address, "raydium_index"))
            except Exception as error:
                errors.append(
                    {"symbol": asset.symbol, "stage": "raydium_index", "error": type(error).__name__}
                )
            completed += 1
            if progress:
                progress("raydium", completed, len(universe))

    await asyncio.gather(*(discover_asset(a) for a in universe.values()))
    if jupiter:
        for index, asset in enumerate(universe.values(), 1):
            try:
                amount = raw(amount_ui, asset.decimals)
                q = await jupiter.quote_pair(asset.mint, USDC_MINT, amount, priority=10)
                route = q.get("routePlan", [])
                quotes.append(
                    {
                        "id": str(uuid.uuid4()),
                        "symbol": asset.symbol,
                        "source": "jupiter",
                        "input_mint": asset.mint,
                        "output_mint": USDC_MINT,
                        "input_raw": str(amount),
                        "output_raw": q["outAmount"],
                        "minimum_output_raw": q.get("otherAmountThreshold"),
                        "observed_at": stamp(),
                        "details": {
                            "quantity_unit": "native_decimal_units_before_scaled_ui",
                            **{
                                k: q[k]
                                for k in (
                                    "routePlan",
                                    "router",
                                    "priceImpact",
                                    "priceImpactPct",
                                    "_request_started_at",
                                )
                                if k in q
                            },
                        },
                    }
                )
                for step in route:
                    info = step["swapInfo"]
                    address = info["ammKey"]
                    Pubkey.from_string(address)
                    pools.setdefault(
                        address,
                        {
                            "address": address,
                            "venue": info["label"],
                            "program": None,
                            "kind": "other",
                            "verified": False,
                            "mintA": info["inputMint"],
                            "mintB": info["outputMint"],
                            "discovery": {"route": info},
                        },
                    )
                    links.add((asset.symbol, address, "jupiter_route"))
            except Exception as error:
                errors.append({"symbol": asset.symbol, "stage": "jupiter", "error": type(error).__name__})
            if progress:
                progress("jupiter", index, len(universe))
    return await _read_chain(
        universe,
        pools,
        links,
        quotes,
        errors,
        rpc,
        started,
        amount_ui,
        progress,
        {
            "backpack": backpack.http.status(),
            "raydium": raydium.http.status(),
            "jupiter": jupiter.status() if jupiter else None,
        },
    )


async def refresh_catalog(catalog: dict, rpc: SolanaRPC, *, amount_ui: str = "1", progress=None) -> dict:
    """Refresh onchain state without re-discovering or re-consuming Jupiter quotes."""
    if dec(amount_ui) <= 0:
        raise ValueError("Positive native-decimal sample size required")
    universe = {
        a["symbol"]: Asset(a["symbol"], a["mint"], a["decimals"], dec("0"), dec("0"), dec("0"), False, ())
        for a in catalog["assets"]
    }
    pools = {
        p["address"]: {k: v for k, v in p.items() if k not in {"decoded", "snapshot", "reason"}}
        for p in catalog["pools"]
    }
    for pool in pools.values():
        pool.update(verified=False, kind="unverified")
    result = await _read_chain(
        universe,
        pools,
        set(tuple(row) for row in catalog["links"]),
        [q for q in catalog["quotes"] if q["source"] == "jupiter"],
        [e for e in catalog["errors"] if e["stage"] in {"raydium_index", "jupiter"}],
        rpc,
        stamp(),
        amount_ui,
        progress,
        {"jupiter": None, "raydium": None, "backpack": None},
    )
    result["discovery_started_at"] = catalog.get("discovery_started_at", catalog["started_at"])
    return result


async def _read_chain(universe, pools, links, quotes, errors, rpc, started, amount_ui, progress, limits):
    tokens: dict[str, dict] = {}
    pending_quotes = []
    addresses = sorted(pools)
    for start in range(0, len(addresses), 100):
        chunk = addresses[start : start + 100]
        response = await rpc.accounts(chunk, priority=10)
        for address, account in zip(chunk, response["value"], strict=True):
            pool = pools[address]
            pool["observed_owner"] = account["owner"] if account else None
            try:
                state = decode_pool(account) if account else None
                if state is None:
                    raise ValueError("Missing pool account")
                if {state["mintA"], state["mintB"]} != {pool["mintA"], pool["mintB"]}:
                    raise ValueError("Pool mint mismatch")
                pool.update(program=account["owner"], kind=state["kind"], verified=True, decoded=state)
            except (ValueError, TypeError, KeyError):
                pool["reason"] = "missing_account_or_unsupported_program_layout_pair"
                if account:
                    pool["program"] = account["owner"]
                    pool["kind"] = PROGRAM_KINDS.get(account["owner"], "other")
    mints = sorted(
        {
            USDC_MINT,
            *(a.mint for a in universe.values()),
            *(p[k] for p in pools.values() for k in ("mintA", "mintB")),
        }
    )
    for start in range(0, len(mints), 100):
        chunk = mints[start : start + 100]
        response = await rpc.accounts(chunk, parsed=True, priority=10)
        for mint, account in zip(chunk, response["value"], strict=True):
            tokens[mint] = {
                **mint_capabilities(account),
                "slot": response["context"]["slot"],
                "observed_at": stamp(),
            }
    verified = [p for p in pools.values() if p.get("decoded")]
    for start in range(0, len(verified), 24):
        group = verified[start : start + 24]
        refs = sorted(
            {
                CLOCK,
                *(p["address"] for p in group),
                *(
                    p["decoded"][k]
                    for p in group
                    for k in ("vaultA", "vaultB", "config")
                    if p["decoded"].get(k)
                ),
            }
        )
        try:
            response = await rpc.accounts(refs, priority=10)
        except RemoteError:
            errors.extend(
                {"pool": p["address"], "stage": "snapshot_rpc", "error": "RemoteError"} for p in group
            )
            if progress:
                progress("snapshots_partial", min(start + 24, len(verified)), len(verified))
            continue
        accounts = dict(zip(refs, response["value"], strict=True))
        for pool in group:
            try:
                state = decode_pool(accounts[pool["address"]])
                if {state["mintA"], state["mintB"]} != {pool["mintA"], pool["mintB"]}:
                    raise ValueError("Pool pair changed")
                state = enrich_snapshot(state, accounts, tokens)
                encoded = json.dumps(state, sort_keys=True, allow_nan=False).encode()
                pool["snapshot"] = {
                    "slot": response["context"]["slot"],
                    "observed_at": stamp(),
                    "digest": hashlib.sha256(encoded).hexdigest(),
                    "state": state,
                }
                if state.get("quoteSupported"):
                    for asset in universe.values():
                        if asset.mint not in {state["mintA"], state["mintB"]}:
                            continue
                        try:
                            row, metadata = cpmm_request(state, asset.mint, raw(amount_ui, asset.decimals))
                        except ValueError:
                            errors.append(
                                {
                                    "pool": pool["address"],
                                    "symbol": asset.symbol,
                                    "stage": "local_quote",
                                    "error": "amount_not_quotable",
                                }
                            )
                            continue
                        pending_quotes.append((pool, asset, row, metadata))
            except (ValueError, TypeError, KeyError) as error:
                pool["reason"] = "snapshot_validation_failed"
                errors.append({"pool": pool["address"], "stage": "snapshot", "error": type(error).__name__})
        if progress:
            progress("snapshots", min(start + 24, len(verified)), len(verified))
    calculator = PoolCalculator()
    prepared_pools = {p["address"]: p for p, _, _, _ in pending_quotes}
    pool_indices = {address: index for index, address in enumerate(prepared_pools)}
    prepared = PreparedCpmmPools(calculator, [p["snapshot"]["state"] for p in prepared_pools.values()])
    jobs = [
        (
            pool_indices[p["address"]],
            asset.mint == p["snapshot"]["state"]["mintA"],
            row[0],
            metadata["slippage_bps"],
        )
        for p, asset, row, metadata in pending_quotes
    ]
    results = await asyncio.to_thread(prepared.quote_batch, jobs)
    for (pool, asset, row, metadata), (out, minimum, math_error) in zip(pending_quotes, results, strict=True):
        if math_error:
            errors.append(
                {
                    "pool": pool["address"],
                    "symbol": asset.symbol,
                    "stage": "local_quote",
                    "error": "unquotable_input",
                }
            )
            continue
        quote = {**metadata, "input_raw": row[0], "output_raw": out, "minimum_output_raw": minimum}
        quotes.append(
            {
                **quote,
                "id": str(uuid.uuid4()),
                "symbol": asset.symbol,
                "pool_address": pool["address"],
                "observed_at": pool["snapshot"]["observed_at"],
                "slot": pool["snapshot"]["slot"],
                "details": quote,
            }
        )
    return {
        "scan_id": str(uuid.uuid4()),
        "started_at": started,
        "finished_at": stamp(),
        "assets": [{"symbol": a.symbol, "mint": a.mint, "decimals": a.decimals} for a in universe.values()],
        "tokens": tokens,
        "pools": list(pools.values()),
        "links": sorted(links),
        "quotes": quotes,
        "errors": errors,
        "sampling": {
            "native_decimal_amount": amount_ui,
            "scaled_ui_applied": False,
            "jupiter_quotes_reused": limits.get("jupiter") is None and bool(quotes),
        },
        "compute": {
            "backend": calculator.backend,
            "layout": "prepared_pool_frames",
            "workers": 4,
            "cpmm_jobs": len(pending_quotes),
            "parallel_threshold": prepared.threshold,
        },
        "summary": {
            "equities": len(universe),
            "pools": len(pools),
            "kinds": dict(Counter(p["kind"] for p in pools.values())),
            "verified_pools": sum(p["verified"] for p in pools.values()),
            "snapshots": sum("snapshot" in p for p in pools.values()),
            "local_quote_supported": sum(
                p.get("snapshot", {}).get("state", {}).get("quoteSupported", False) for p in pools.values()
            ),
            "quotes": len(quotes),
            "errors": len(errors),
            "raydium_index_complete": not any(e["stage"] == "raydium_index" for e in errors),
            "other_venues_complete": False,
        },
        "limits": {**limits, "rpc": rpc.http.status()},
        "mutations": 0,
    }
