from __future__ import annotations

import base64
import time
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from .feed import StockFeed
from .models import Observation, dec
from .transport import Transport


def signing_message(instruction: str, params: dict, timestamp: int, window: int = 5000) -> bytes:
    def value(v: Any) -> str:
        if isinstance(v, bool):
            return str(v).lower()
        if isinstance(v, float) or isinstance(v, (dict, list)):
            raise ValueError("Signing supports scalar strings/integers/bools only")
        return str(v)

    fields = [
        f"{k}={value(v)}" for k, v in sorted(params.items()) if v is not None and k != "recipientInformation"
    ]
    return "&".join(
        [f"instruction={instruction}", *fields, f"timestamp={timestamp}", f"window={window}"]
    ).encode()


class BackpackAuth:
    def __init__(self, seed_base64: str):
        seed = base64.b64decode(seed_base64, validate=True)
        if len(seed) != 32:
            raise ValueError("Backpack key must be a base64-encoded 32-byte Ed25519 seed")
        self._key = Ed25519PrivateKey.from_private_bytes(seed)
        self.public_key = base64.b64encode(
            self._key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        ).decode()

    def headers(self, instruction: str, params: dict, *, timestamp: int | None = None) -> dict:
        timestamp = int(time.time() * 1000) if timestamp is None else timestamp
        signature = self._key.sign(signing_message(instruction, params, timestamp))
        return {
            "X-API-Key": self.public_key,
            "X-Signature": base64.b64encode(signature).decode(),
            "X-Timestamp": str(timestamp),
            "X-Window": "5000",
        }


class Backpack:
    def __init__(
        self,
        auth: BackpackAuth | None = None,
        transport: Transport | None = None,
        *,
        interval: float = 0.05,
        proxy: str | None = None,
        ws_proxy: str | None = None,
    ):
        self.auth = auth
        self.http = transport or Transport("https://api.backpack.exchange", interval=interval, proxy=proxy)
        self.http.limiter.categories["historical_market_data"] = 2.05
        self.stock_feed = StockFeed(proxy=proxy if ws_proxy is None else ws_proxy)

    async def close(self) -> None:
        await self.stock_feed.close()
        await self.http.close()

    async def public(self, path: str, **params: Any) -> Any:
        historical = path in {"/api/v1/klines", "/api/v1/trades"}
        return await self.http.request(
            "GET", path, params=params or None, category="historical_market_data" if historical else None
        )

    async def private(self, method: str, path: str, instruction: str, **params: Any) -> Any:
        auth = self.auth
        if auth is None:
            raise ValueError("Authenticated Backpack client required")
        params = {k: v for k, v in params.items() if v is not None}
        return await self.http.request(
            method,
            path,
            headers=lambda: auth.headers(instruction, params),
            params=params if method == "GET" else None,
            body=params if method != "GET" else None,
        )

    async def assets(self) -> list[dict]:
        return await self.public("/api/v1/assets")

    async def securities(self) -> list[dict]:
        return await self.public("/api/v1/securities")

    async def sessions(self) -> list[dict]:
        return await self.public("/api/v1/market-sessions")

    async def holidays(self) -> list[dict]:
        return await self.public("/api/v1/market-holidays")

    async def markets(self) -> list[dict]:
        return await self.public("/api/v1/markets")

    async def depth(self, symbol: str, limit: int = 100) -> dict:
        return await self.public("/api/v1/depth", symbol=symbol, limit=limit)

    async def ticker(self, symbol: str, source: str = "External") -> dict:
        return await self.public("/api/v1/ticker", symbol=symbol, source=source)

    async def tickers(self) -> list[dict]:
        return await self.public("/api/v1/tickers", source="External", interval="1d")

    async def server_time(self) -> int:
        return int(await self.public("/api/v1/time"))

    async def vaults(self) -> list[dict]:
        return await self.public("/api/v1/vaults")

    async def vault_mint(self, vault_id: int, symbol: str, quantity: str) -> None:
        # Vault shares are not automatically equities. Caller must verify vaultToken separately.
        await self.private(
            "POST",
            "/api/v1/vault/mint",
            "vaultMint",
            vaultId=vault_id,
            symbol=symbol,
            quantity=str(dec(quantity)),
            autoBorrow=False,
            autoLendRedeem=False,
        )

    async def balances(self) -> dict:
        return await self.private("GET", "/api/v1/capital", "balanceQuery")

    async def account(self) -> dict:
        return await self.private("GET", "/api/v1/account", "accountQuery")

    async def max_order_quantity(self, symbol: str, side: str, price: str) -> dict:
        if side not in {"Bid", "Ask"}:
            raise ValueError("Invalid order side")
        return await self.private(
            "GET",
            "/api/v1/account/limits/order",
            "maxOrderQuantity",
            symbol=symbol,
            side=side,
            price=price,
            reduceOnly=False,
            autoBorrow=False,
            autoBorrowRepay=False,
            autoLendRedeem=False,
        )

    async def submit_rfq(
        self, quantity: str, client_id: int, side: str = "Bid", *, symbol: str = "WEN.US_USDC_RFQ"
    ) -> dict:
        if side not in {"Bid", "Ask"} or not 0 <= client_id < 2**32:
            raise ValueError("Invalid RFQ side/client ID")
        return await self.private(
            "POST",
            "/api/v1/rfq",
            "rfqSubmit",
            symbol=symbol,
            side=side,
            quantity=str(dec(quantity)),
            clientId=client_id,
            executionMode="AwaitAccept",
            autoBorrow=False,
            autoLend=False,
            autoLendRedeem=False,
            autoBorrowRepay=False,
        )

    async def rfqs(self, rfq_id: str | None = None, *, symbol: str = "WEN.US_USDC_RFQ") -> list[dict]:
        return await self.private("GET", "/api/v1/rfqs", "rfqQuery", rfqId=rfq_id, symbol=symbol)

    async def accept_rfq(self, rfq_id: str, quote_id: str) -> dict:
        return await self.private("POST", "/api/v1/rfq/accept", "quoteAccept", rfqId=rfq_id, quoteId=quote_id)

    async def cancel_rfq(self, rfq_id: str) -> dict:
        return await self.private("POST", "/api/v1/rfq/cancel", "rfqCancel", rfqId=rfq_id)

    async def rfq_fills(self, rfq_id: str) -> list[dict]:
        return await self.private(
            "GET", "/wapi/v1/history/rfq/fill", "rfqFillHistoryQueryAll", rfqId=rfq_id, limit=1000
        )

    async def rfq_history(self, *, symbol: str = "WEN.US_USDC_RFQ", rfq_id: str | None = None) -> list[dict]:
        return await self.private(
            "GET", "/wapi/v1/history/rfq", "rfqHistoryQueryAll", symbol=symbol, rfqId=rfq_id, limit=1000
        )

    async def withdraw(self, quantity: str, address: str, client_id: str) -> dict:
        return await self.private(
            "POST",
            "/wapi/v1/capital/withdrawals",
            "withdraw",
            symbol="WEN.US",
            blockchain="Solana",
            quantity=quantity,
            address=address,
            clientId=client_id,
            autoBorrow=False,
            autoLendRedeem=False,
        )

    async def withdrawals(self, client_id: str) -> list[dict]:
        return await self.private(
            "GET", "/wapi/v1/capital/withdrawals", "withdrawalQueryAll", clientId=client_id, limit=1000
        )

    async def deposits(self, signature: str | None = None) -> list[dict]:
        return await self.private(
            "GET", "/wapi/v1/capital/deposits", "depositQueryAll", transactionHash=signature, limit=1000
        )

    async def deposit_address(self) -> dict:
        return await self.private(
            "GET", "/wapi/v1/capital/deposit/address", "depositAddressQuery", blockchain="Solana"
        )

    async def order(self, symbol: str, side: str, quantity: str, price: str, client_id: int) -> dict:
        return await self.private(
            "POST",
            "/api/v1/order",
            "orderExecute",
            symbol=symbol,
            side=side,
            orderType="Limit",
            quantity=quantity,
            price=price,
            clientId=client_id,
            timeInForce="IOC",
            autoBorrow=False,
            autoLend=False,
            autoLendRedeem=False,
        )

    async def query_order(
        self, symbol: str, *, client_id: int | None = None, order_id: str | None = None
    ) -> dict:
        if (client_id is None) == (order_id is None):
            raise ValueError("Provide exactly one order selector")
        return await self.private(
            "GET", "/api/v1/order", "orderQuery", symbol=symbol, clientId=client_id, orderId=order_id
        )

    async def stock_prices(self) -> AsyncIterator[Observation]:
        await self.stock_feed.subscribe(["WEN"])
        version = -1
        while True:
            if self.stock_feed.version != version:
                version = self.stock_feed.version
                if "WEN" in self.stock_feed.latest:
                    yield self.stock_feed.latest["WEN"]
            await self.stock_feed.wait(version)


def vwap(levels: list[list[str]], quantity: str) -> Decimal:
    remaining, total = dec(quantity), Decimal(0)
    for price, size, *_ in levels:
        take = min(remaining, dec(size))
        total += take * dec(price)
        remaining -= take
        if remaining == 0:
            return total / dec(quantity)
    raise ValueError("Insufficient orderbook depth")
