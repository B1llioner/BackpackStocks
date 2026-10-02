from __future__ import annotations

import time

from .limiter import PriorityLimiter
from .models import STOCK_MINT, USDC_MINT, impact_percent
from .transport import Transport


class Jupiter:
    def __init__(
        self,
        api_key: str | None = None,
        interval: float = 2.1,
        transport: Transport | None = None,
        *,
        proxy: str | None = None,
    ):
        self.http = transport or Transport("https://api.jup.ag/swap/v2", proxy=proxy, rate_headers=True)
        self.http.rate_headers = True
        self._headers = {"x-api-key": api_key} if api_key else {}
        self.limiter = self.http.limiter
        # Keyless 0.5 RPS; Free with API key 1 RPS (official 60-second window).
        self.limiter.configure(interval if api_key else max(2.1, interval))
        # A conservative dedicated execute bucket: keyless 20 RPS, keyed Free 50 RPS.
        self.execute_limiter = PriorityLimiter(0.021 if api_key else 0.055)

    @property
    def interval(self) -> float:
        return self.limiter.interval

    async def close(self) -> None:
        await self.execute_limiter.close()
        await self.http.close()

    def status(self) -> dict:
        return {**self.http.status(), "execute": self.execute_limiter.status()}

    async def _get(self, path: str, params: dict, *, priority: int = 0, trace: dict | None = None) -> dict:
        async def admitted():
            if trace is not None:
                trace["request_started_at"] = time.time()

        return await self.http.request(
            "GET",
            path,
            params=params,
            headers=self._headers,
            before_attempt=admitted,
            priority=priority,
        )

    async def prices(self, mints: list[str]) -> dict:
        if not 1 <= len(mints) <= 50:
            raise ValueError("Price API accepts between 1 and 50 mints")
        result = await self._get("https://api.jup.ag/price/v3", {"ids": ",".join(mints)}, priority=10)
        if not isinstance(result, dict) or result.get("error"):
            raise ValueError("Invalid Jupiter price response")
        return result

    async def quote(self, amount: int, *, reverse: bool = False) -> dict:
        if amount <= 0:
            raise ValueError("Positive raw input amount required")
        source, target = (USDC_MINT, STOCK_MINT) if reverse else (STOCK_MINT, USDC_MINT)
        return await self.quote_pair(source, target, amount)

    async def quote_pair(self, source: str, target: str, amount: int, *, priority: int = 0) -> dict:
        if amount <= 0:
            raise ValueError("Positive raw input amount required")
        trace: dict = {}
        q = await self._get(
            "/order",
            {"inputMint": source, "outputMint": target, "amount": str(amount)},
            priority=priority,
            trace=trace,
        )
        self.validate_quote(q, source, target, amount)
        if q.get("transaction"):
            raise ValueError("Quote-only request unexpectedly returned a transaction")
        return {**q, "_request_started_at": trace["request_started_at"]} if priority == 10 else q

    async def build(self, amount: int, wallet: str, slippage_bps: int) -> dict:
        q = await self._get(
            "/build",
            {
                "inputMint": STOCK_MINT,
                "outputMint": USDC_MINT,
                "amount": str(amount),
                "taker": wallet,
                "slippageBps": slippage_bps,
            },
        )
        self.validate_quote(q, STOCK_MINT, USDC_MINT, amount)
        if q.get("swapMode") != "ExactIn" or q.get("slippageBps") != slippage_bps:
            raise ValueError("Unexpected swap mode/slippage")
        return q

    async def execute(self, signed_transaction: str, request_id: str) -> dict:
        # Managed path is exposed as a client API. Engine uses validated self-built transactions.
        return await self.http.request(
            "POST",
            "/execute",
            headers=self._headers,
            body={"signedTransaction": signed_transaction, "requestId": request_id},
            limiter=self.execute_limiter,
            bucket="execute",
        )

    async def build_v1(self, amount: int, wallet: str, slippage_bps: int) -> dict:
        return await self.build_v1_stock(STOCK_MINT, amount, wallet, slippage_bps)

    async def build_v1_stock(self, mint: str, amount: int, wallet: str, slippage_bps: int) -> dict:
        # Compatibility route with pinned official IDL. V2 route_v2 has no current published IDL here.
        q = await self.quote_v1_stock(mint, amount, slippage_bps)
        instructions = await self.http.request(
            "POST",
            "https://api.jup.ag/swap/v1/swap-instructions",
            headers=self._headers,
            body={
                "quoteResponse": {k: v for k, v in q.items() if not k.startswith("_")},
                "userPublicKey": wallet,
                "useSharedAccounts": False,
                "wrapAndUnwrapSol": False,
                "dynamicComputeUnitLimit": True,
            },
            mutation=False,
            # Complete an acquired quote before admitting another batch quote.
            priority=-1,
        )
        return {**q, **instructions}

    async def quote_v1(self, amount: int, slippage_bps: int) -> dict:
        return await self.quote_v1_stock(STOCK_MINT, amount, slippage_bps)

    async def quote_v1_stock(self, mint: str, amount: int, slippage_bps: int) -> dict:
        from solders.pubkey import Pubkey

        Pubkey.from_string(mint)
        if (
            type(amount) is not int
            or not 0 < amount < 2**63
            or type(slippage_bps) is not int
            or not 1 <= slippage_bps <= 500
        ):
            raise ValueError("Invalid stock swap amount/slippage")
        trace: dict = {}
        q = await self._get(
            "https://api.jup.ag/swap/v1/quote",
            {
                "inputMint": mint,
                "outputMint": USDC_MINT,
                "amount": str(amount),
                "slippageBps": slippage_bps,
                "instructionVersion": "V1",
                "onlyDirectRoutes": "true",
                "dexes": "Raydium CLMM",
            },
            trace=trace,
        )
        self.validate_quote(q, mint, USDC_MINT, amount)
        if q.get("instructionVersion") != "V1" or q.get("swapMode") != "ExactIn":
            raise ValueError("Unsupported quote instruction format")
        return {**q, "_quote_started_at": trace["request_started_at"]}

    @staticmethod
    def validate_quote(q: dict, source: str, target: str, amount: int) -> None:
        if (
            q.get("error")
            or q.get("errorCode")
            or q.get("inputMint") != source
            or q.get("outputMint") != target
        ):
            raise ValueError("Jupiter returned an error or wrong mint")
        if int(q["inAmount"]) != amount or int(q["outAmount"]) <= 0:
            raise ValueError("Jupiter amount mismatch/no route")
        impact_percent(q)
