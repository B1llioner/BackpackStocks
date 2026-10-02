from __future__ import annotations

import base64
import struct

from solders.pubkey import Pubkey

from .models import ATA, STOCK_MINT, TOKEN, TOKEN_2022, USDC_MINT, dec
from .transport import RemoteError, Transport


def associated_address(wallet: str, mint: str, token_program: str) -> str:
    return str(
        Pubkey.find_program_address(
            [
                bytes(Pubkey.from_string(wallet)),
                bytes(Pubkey.from_string(token_program)),
                bytes(Pubkey.from_string(mint)),
            ],
            Pubkey.from_string(ATA),
        )[0]
    )


def decode_token(account: dict | None) -> dict:
    if account is None:
        return {"amount": 0, "owner": None, "mint": None}
    if account["owner"] not in {TOKEN, TOKEN_2022}:
        raise ValueError("Not a supported token account")
    data = base64.b64decode(account["data"][0], validate=True)
    if len(data) < 165 or data[108] != 1:
        raise ValueError("Uninitialized or frozen token account")
    return {
        "mint": str(Pubkey.from_bytes(data[:32])),
        "owner": str(Pubkey.from_bytes(data[32:64])),
        "amount": struct.unpack_from("<Q", data, 64)[0],
        "delegate": struct.unpack_from("<I", data, 72)[0],
        "close_authority": struct.unpack_from("<I", data, 129)[0],
        # Changing delegate/authority/extension bytes is forbidden by swap validation.
        "control": data[:64] + data[72:],
        "program": account["owner"],
    }


def assert_stock_mint(account: dict) -> dict:
    if account["owner"] != TOKEN_2022:
        raise ValueError("WEN.US token program changed")
    info = account["data"]["parsed"]["info"]
    if info["decimals"] != 6 or not info.get("isInitialized"):
        raise ValueError("Unexpected WEN.US mint precision/state")
    extensions = {e["extension"]: e.get("state", {}) for e in info.get("extensions", [])}
    allowed = {
        "metadataPointer",
        "permanentDelegate",
        "defaultAccountState",
        "pausableConfig",
        "confidentialTransferMint",
        "transferHook",
        "scaledUiAmountConfig",
        "tokenMetadata",
    }
    if set(extensions) - allowed:
        raise ValueError("New unsupported Token-2022 extension")
    scaled = extensions.get("scaledUiAmountConfig")
    if not scaled or dec(str(scaled["multiplier"])) != 1 or dec(str(scaled["newMultiplier"])) != 1:
        raise ValueError("Unsupported scaled UI multiplier")
    if extensions.get("pausableConfig", {}).get("paused"):
        raise ValueError("Mint is paused")
    if extensions.get("transferHook", {}).get("programId"):
        raise ValueError("Active transfer hook unsupported")
    if extensions.get("defaultAccountState", {}).get("accountState") != "initialized":
        raise ValueError("Default token account state unsupported")
    return info


class SolanaRPC:
    def __init__(
        self,
        url: str,
        transport: Transport | None = None,
        *,
        interval: float = 0.30,
        proxy: str | None = None,
    ):
        self.http = transport or Transport(
            url, interval=interval, byte_budget=75_000_000, max_response_bytes=2_000_000, proxy=proxy
        )
        self.sequence = 0

    async def close(self) -> None:
        await self.http.close()

    async def call(self, method: str, params: list, *, mutation: bool = False, priority: int = 0):
        self.sequence += 1
        result = await self.http.request(
            "POST",
            "",
            body={"jsonrpc": "2.0", "id": self.sequence, "method": method, "params": params},
            mutation=mutation,
            priority=priority,
        )
        if "error" in result:
            # sendTransaction's explicit RPC rejection is not silently retried either.
            raise RemoteError(f"RPC {method} rejected request")
        return result["result"]

    async def accounts(
        self, addresses: list[str], *, parsed: bool = False, min_slot: int | None = None, priority: int = 0
    ) -> dict:
        config: dict = {"encoding": "jsonParsed" if parsed else "base64", "commitment": "finalized"}
        if min_slot is not None:
            config["minContextSlot"] = min_slot
        return await self.call("getMultipleAccounts", [addresses, config], priority=priority)

    async def check_mint(self, mint: str = STOCK_MINT) -> dict:
        result = await self.accounts([mint], parsed=True)
        assert_stock_mint(result["value"][0])
        return result

    async def wallet_snapshot(self, wallet: str, mint: str = STOCK_MINT) -> dict:
        addresses = [
            wallet,
            associated_address(wallet, mint, TOKEN_2022),
            associated_address(wallet, USDC_MINT, TOKEN),
        ]
        result = await self.accounts(addresses)
        payer, stock, usdc = result["value"]
        if not payer or payer["owner"] != "11111111111111111111111111111111":
            raise ValueError("Wallet must be a funded system account")
        tokens = [decode_token(stock), decode_token(usdc)]
        for t, expected_mint in zip(tokens, [mint, USDC_MINT], strict=True):
            if t["owner"] is not None and (t["owner"] != wallet or t["mint"] != expected_mint):
                raise ValueError("Invalid wallet ATA")
        return {
            "addresses": addresses,
            "accounts": result["value"],
            "slot": result["context"]["slot"],
            "lamports": payer["lamports"],
            "stock": tokens[0]["amount"],
            "usdc": tokens[1]["amount"],
        }

    async def transaction(self, signature: str, *, priority: int = 0) -> dict | None:
        return await self.call(
            "getTransaction",
            [
                signature,
                {"encoding": "jsonParsed", "commitment": "finalized", "maxSupportedTransactionVersion": 1},
            ],
            priority=priority,
        )

    async def simulate(self, transaction: str, addresses: list[str], min_slot: int) -> dict:
        return await self.call(
            "simulateTransaction",
            [
                transaction,
                {
                    "encoding": "base64",
                    "sigVerify": False,
                    "commitment": "finalized",
                    "minContextSlot": min_slot,
                    "accounts": {"encoding": "base64", "addresses": addresses},
                },
            ],
        )

    async def send(self, transaction: str, min_slot: int) -> str:
        return await self.call(
            "sendTransaction",
            [
                transaction,
                {
                    "encoding": "base64",
                    "skipPreflight": False,
                    "maxRetries": 0,
                    "preflightCommitment": "finalized",
                    "minContextSlot": min_slot,
                },
            ],
            mutation=True,
        )

    async def signature_status(self, signature: str) -> dict | None:
        result = await self.call("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])
        return result["value"][0]


def transaction_deltas(tx: dict, wallet: str, mint: str = STOCK_MINT) -> dict:
    if tx["meta"]["err"] is not None:
        raise ValueError("Onchain transaction failed")
    meta = tx["meta"]

    def total(rows: list[dict], mint: str) -> int:
        return sum(
            int(r["uiTokenAmount"]["amount"]) for r in rows if r.get("owner") == wallet and r["mint"] == mint
        )

    return {
        "stock": total(meta.get("postTokenBalances", []), mint)
        - total(meta.get("preTokenBalances", []), mint),
        "usdc": total(meta.get("postTokenBalances", []), USDC_MINT)
        - total(meta.get("preTokenBalances", []), USDC_MINT),
        "fee": meta["fee"],
    }
