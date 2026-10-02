from __future__ import annotations

import base64
import hashlib
import struct
from decimal import Decimal, localcontext

from solders.pubkey import Pubkey

from .models import STOCK_MINT, USDC_MINT
from .solana import SolanaRPC
from .transport import Transport

CLMM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"


def decode_clmm(account: dict) -> dict:
    # SDK cc33ec28a8921a35609e83293e9e07ad830b0779, PoolInfoLayout.
    b = base64.b64decode(account["data"][0], validate=True)
    if account["owner"] != CLMM or len(b) != 1544:
        raise ValueError("Unsupported Raydium pool owner/layout")
    if b[:8] != hashlib.sha256(b"account:PoolState").digest()[:8]:
        raise ValueError("CLMM discriminator mismatch")

    def pub(pos):
        return str(Pubkey.from_bytes(b[pos : pos + 32]))

    da, db = b[233], b[234]
    sqrt = int.from_bytes(b[253:269], "little")
    with localcontext() as ctx:
        ctx.prec = 60
        price = (Decimal(sqrt) / 2**64) ** 2 * Decimal(10) ** (da - db)
    return {
        "config": pub(9),
        "mintA": pub(73),
        "mintB": pub(105),
        "vaultA": pub(137),
        "vaultB": pub(169),
        "observation": pub(201),
        "decimalsA": da,
        "decimalsB": db,
        "tickSpacing": struct.unpack_from("<H", b, 235)[0],
        "liquidity": str(int.from_bytes(b[237:253], "little")),
        "sqrtPriceX64": str(sqrt),
        "tick": struct.unpack_from("<i", b, 269)[0],
        "status": b[389],
        "feeOn": b[390],
        "priceBperA": str(price),
        "dynamicFeeControl": struct.unpack_from("<I", b, 1102)[0],
        "quoteSupported": False,
        "quoteReason": "Spot state only; execution requires ticks, dynamic fees and transfer extensions",
    }


class Raydium:
    def __init__(self, rpc: SolanaRPC, transport: Transport | None = None, *, proxy: str | None = None):
        self.rpc = rpc
        self.http = transport or Transport("https://api-v3.raydium.io", interval=1.05, proxy=proxy)

    async def close(self) -> None:
        await self.http.close()

    async def pools(self, mint: str = STOCK_MINT, quote_mint: str | None = USDC_MINT) -> list[dict]:
        Pubkey.from_string(mint)
        if quote_mint:
            Pubkey.from_string(quote_mint)
        result: dict[str, dict] = {}
        for page in range(1, 21):
            params = {
                "mint1": mint,
                "poolType": "all",
                "poolSortField": "liquidity",
                "sortType": "desc",
                "pageSize": 100,
                "page": page,
            }
            if quote_mint:
                params["mint2"] = quote_mint
            response = await self.http.request("GET", "/pools/info/mint", params=params, priority=10)
            if not response.get("success"):
                raise ValueError("Raydium discovery failed")
            rows = response["data"]["data"]
            if not isinstance(rows, list) or len(rows) > 100:
                raise ValueError("Invalid Raydium pool page")
            if type(response["data"].get("hasNextPage")) is not bool:
                raise ValueError("Missing Raydium pagination completeness flag")
            for row in rows:
                pair = {row["mintA"]["address"], row["mintB"]["address"]}
                if mint not in pair or (quote_mint and quote_mint not in pair):
                    continue  # The API can include unrelated quote mints.
                Pubkey.from_string(row["id"])
                for token in pair:
                    Pubkey.from_string(token)
                result[row["id"]] = row
            if not response["data"].get("hasNextPage"):
                return list(result.values())
        raise ValueError("Raydium pool pagination exceeded bounded limit")

    async def pool_onchain(self, address: str) -> dict:
        r = await self.rpc.accounts([address])
        decoded = decode_clmm(r["value"][0])
        if {decoded["mintA"], decoded["mintB"]} != {STOCK_MINT, USDC_MINT}:
            raise ValueError("Pool mint mismatch")
        return {"address": address, "slot": r["context"]["slot"], **decoded}
