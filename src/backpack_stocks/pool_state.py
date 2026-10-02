from __future__ import annotations

import base64
import hashlib
import struct

from solders.pubkey import Pubkey

from .raydium import CLMM, decode_clmm

CPMM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
AMM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
PROGRAM_KINDS = {CLMM: "clmm", CPMM: "cpmm", AMM_V4: "amm_v4"}


def decode_pool(account: dict) -> dict:
    program = account["owner"]
    if program == CLMM:
        return {"kind": "clmm", **decode_clmm(account)}
    data = base64.b64decode(account["data"][0], validate=True)

    def pub(offset: int) -> str:
        return str(Pubkey.from_bytes(data[offset : offset + 32]))

    def u64(offset: int) -> int:
        return struct.unpack_from("<Q", data, offset)[0]

    if program == CPMM:
        if len(data) != 637 or data[:8] != hashlib.sha256(b"account:PoolState").digest()[:8]:
            raise ValueError("Unsupported CPMM pool layout")
        if data[389] > 2 or data[390] > 1:
            raise ValueError("Unsupported CPMM fee mode")
        return {
            "kind": "cpmm",
            "config": pub(8),
            "vaultA": pub(72),
            "vaultB": pub(104),
            "mintA": pub(168),
            "mintB": pub(200),
            "tokenProgramA": pub(232),
            "tokenProgramB": pub(264),
            "decimalsA": data[331],
            "decimalsB": data[332],
            "status": data[329],
            "protocolFeesA": str(u64(341)),
            "protocolFeesB": str(u64(349)),
            "fundFeesA": str(u64(357)),
            "fundFeesB": str(u64(365)),
            "openTime": u64(373),
            "feeOn": data[389],
            "enableCreatorFee": bool(data[390]),
            "creatorFeesA": str(u64(397)),
            "creatorFeesB": str(u64(405)),
        }
    if program == AMM_V4:
        if len(data) != 752:
            raise ValueError("Unsupported AMM v4 pool layout")
        return {
            "kind": "amm_v4",
            "vaultA": pub(336),
            "vaultB": pub(368),
            "mintA": pub(400),
            "mintB": pub(432),
            "openOrders": pub(496),
            "market": pub(528),
            "marketProgram": pub(560),
            "decimalsA": u64(32),
            "decimalsB": u64(40),
            "status": u64(0),
            "tradeFeeNumerator": str(u64(144)),
            "tradeFeeDenominator": str(u64(152)),
            "swapFeeNumerator": str(u64(176)),
            "swapFeeDenominator": str(u64(184)),
            "needTakePnlA": str(u64(192)),
            "needTakePnlB": str(u64(200)),
            "openTime": u64(224),
        }
    raise ValueError("No decoder for pool program")


def decode_fee_config(account: dict, kind: str) -> dict:
    b = base64.b64decode(account["data"][0], validate=True)
    if b[:8] != hashlib.sha256(b"account:AmmConfig").digest()[:8]:
        raise ValueError("Fee config discriminator mismatch")
    if kind == "cpmm" and account["owner"] == CPMM and len(b) == 236:
        result = {
            "tradeFeeRate": struct.unpack_from("<Q", b, 12)[0],
            "protocolFeeRate": struct.unpack_from("<Q", b, 20)[0],
            "fundFeeRate": struct.unpack_from("<Q", b, 28)[0],
            "creatorFeeRate": struct.unpack_from("<Q", b, 108)[0],
        }
        if (
            result["tradeFeeRate"] + result["creatorFeeRate"] >= 1_000_000
            or result["protocolFeeRate"] + result["fundFeeRate"] > 1_000_000
        ):
            raise ValueError("Invalid CPMM fee config")
        return result
    if kind == "clmm" and account["owner"] == CLMM and len(b) == 117:
        return {
            "tradeFeeRate": struct.unpack_from("<I", b, 47)[0],
            "protocolFeeRate": struct.unpack_from("<I", b, 43)[0],
            "fundFeeRate": struct.unpack_from("<I", b, 53)[0],
        }
    raise ValueError("Unsupported fee config layout")
