import base64
import copy
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from solders.pubkey import Pubkey
from test_direct import fixture

from backpack_stocks.clmm_worker import ClmmCalculationError
from backpack_stocks.direct import DirectRaydium
from backpack_stocks.models import Settings
from backpack_stocks.pool_bank_feed import PoolBankFeed
from backpack_stocks.raydium import CLMM


@pytest.fixture
async def banks(monkeypatch):
    saved = fixture()
    q = saved["quote"]
    monkeypatch.setattr("backpack_stocks.direct.time.time", lambda: q["_observed_at"])
    final = saved["rpc_records"][-1]
    rows = dict(zip(final["addresses"], final["result"]["value"], strict=True))
    rpc = SimpleNamespace(
        accounts=AsyncMock(
            side_effect=lambda refs, **kwargs: {
                "context": {"slot": q["contextSlot"]},
                "value": [copy.deepcopy(rows.get(ref)) for ref in refs],
            }
        )
    )
    router = DirectRaydium(Settings(dex_backend="raydium"), rpc)
    router.pool_catalog[q["pool"]] = {"program": CLMM}
    # The public fixture already captures the SDK's canonical two addresses.
    # No HTTP or worker process is needed to exercise the fresh bank validation.
    router.worker = SimpleNamespace(call=AsyncMock(return_value={"addresses": q["_direct"]["arrays"]}))
    feed = PoolBankFeed(rpc, 8)
    key = q["pool"], q["inputMint"], q["outputMint"], 10
    original = await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
    feed.remember(key, original)
    rpc.accounts.reset_mock()
    router.worker.call.reset_mock()
    try:
        yield router, feed, rpc, key, original, rows, q
    finally:
        await feed.close()


async def test_shrinking_hint_trims_known_addresses_in_one_fresh_atomic_read(banks):
    router, feed, rpc, key, previous, _, q = banks
    # Model a previously wider verified direction; only its prefix is needed
    # now. Unneeded old addresses must not inflate the new atomic read.
    extra = [str(Pubkey.new_unique()) for _ in range(6)]
    previous["arrays"] = [*previous["arrays"], *extra]
    assert previous["array_limit"] == 8
    router.array_hints[key[:2]] = 2
    current = await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
    assert current is not previous and current["array_limit"] == 2
    assert current["arrays"] == q["_direct"]["arrays"] and current["arrays_verified"]
    rpc.accounts.assert_awaited_once()
    request = rpc.accounts.await_args
    assert request.kwargs["parsed"] and request.kwargs["min_slot"] == q["contextSlot"]
    assert not set(extra) & set(request.args[0])
    assert set(request.args[0]) == set(current["accounts"])
    router.worker.call.assert_awaited_once()
    assert router.worker.call.await_args.args[0]["maxArrays"] == 2


async def test_growing_hint_preserves_the_ready_minimum_window_without_a_discovery_read(banks):
    router, feed, rpc, key, previous, _, q = banks
    previous["array_limit"] = 2
    router.array_hints[key[:2]] = 3
    current = await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
    assert current is not previous and current["array_limit"] == 2
    assert current["arrays"] == previous["arrays"]
    rpc.accounts.assert_awaited_once()
    assert rpc.accounts.await_args.kwargs["parsed"]
    router.worker.call.assert_awaited_once()
    assert router.worker.call.await_args.args[0]["maxArrays"] == 2


@pytest.mark.parametrize("tampered", ["vault", "clock", "mint", "fees"])
async def test_hint_reuse_still_validates_the_new_atomic_bank_and_refuses_invalid_data(banks, tampered):
    router, feed, rpc, key, previous, rows, q = banks
    previous["array_limit"] = 2
    router.array_hints[key[:2]] = 3
    if tampered == "vault":
        rows[previous["state"]["vaultA"]]["data"]["parsed"]["info"]["owner"] = str(Pubkey.new_unique())
        message = "vault ownership"
    elif tampered == "clock":
        clock = next(account for account in rows.values() if account and account["owner"].startswith("Sysvar"))
        clock["data"]["parsed"]["info"]["unixTimestamp"] -= 60
        message = "bank is too old"
    elif tampered == "mint":
        rows[key[1]]["data"]["parsed"]["info"]["decimals"] -= 1
        message = "Unsafe mint"
    else:
        config = rows[previous["state"]["config"]]
        encoded = bytearray(base64.b64decode(config["data"][0]))
        encoded[47:51] = struct.pack("<I", 1_000_000)
        config["data"][0] = base64.b64encode(encoded).decode()
        message = "[Ff]ee"
    with pytest.raises(ValueError, match=message):
        await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
    rpc.accounts.assert_awaited_once()
    assert rpc.accounts.await_args.kwargs["parsed"]


async def test_changed_canonical_addresses_rediscover_a_complete_bank_instead_of_reusing_old_bytes(banks):
    router, feed, rpc, key, previous, _, q = banks
    previous["array_limit"] = 2
    router.array_hints[key[:2]] = 3
    router.worker.call.side_effect = [
        {"addresses": [str(Pubkey.new_unique())]},
        {"addresses": q["_direct"]["arrays"]},
        {"addresses": q["_direct"]["arrays"]},
    ]
    current = await router._pool_bank(*key[:3], int(q["inAmount"]), priority=10, _rpc=feed)
    assert current is not previous and current["arrays_verified"]
    assert current["array_limit"] == 3 and current["arrays"] == q["_direct"]["arrays"]
    assert [call.kwargs.get("parsed", False) for call in rpc.accounts.await_args_list] == [True, False, True]
    assert [call.args[0]["maxArrays"] for call in router.worker.call.await_args_list] == [2, 3, 3]


async def test_larger_exact_amount_keeps_the_explicit_full_bank_fallback(banks):
    router, feed, rpc, key, previous, _, q = banks
    previous["array_limit"] = 2
    router.array_hints[key[:2]] = 3
    requested = int(q["inAmount"]) * 10

    async def quote(pool, source, target, amount, bank):
        assert (pool, source, target, amount) == (*key[:3], requested)
        if bank["array_limit"] == 2:
            raise ClmmCalculationError("insufficient_tick_coverage")
        assert bank["array_limit"] == router.settings.direct.max_tick_arrays
        return {**q, "inAmount": str(amount)}

    router.quote_bank = AsyncMock(side_effect=quote)
    result = await router.pool_quote(*key[:3], requested, priority=10, _rpc=feed)
    assert result["inAmount"] == str(requested)
    assert router.quote_bank.await_count == 2
    assert rpc.accounts.await_count == 2
    assert [call.args[0]["maxArrays"] for call in router.worker.call.await_args_list] == [8, 8]
    assert set(rpc.accounts.await_args.args[0]) == set(previous["accounts"])
