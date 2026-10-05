"""Request-level replay derived from data/jupiter, independent of the trading engine.

Wallet connect is local address discovery. Trigger V2 login signs a server challenge.
Swap execution accepts the exact reviewed/partially signed server transaction.
The separate jupiter_swap module checks simulation before local wallet signing.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any

import httpx
from solders.keypair import Keypair
from solders.message import to_bytes_versioned
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction

from .jupiter_curl import AsyncHttpClient, CurlClient, CurlTransportError
from .proxy import ProxyLogRedactor, checked_proxy
from .rejection import api_error_metadata
from .transport import retry_after_seconds

SOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
ORDER_BASES = {
    "website": "https://api.jup.ag/swap/v2a",
    "developer": "https://api.jup.ag/swap/v2",
    "ultra": "https://ultra-api.jup.ag",
}
TRIGGER_BASE = "https://trigger-api.jup.ag"
SECRET_FIELDS = re.compile(
    r"^(authorization|cookie|set-cookie|x-api-key|apiKey|swapApiKey|access[_-]?token|refresh[_-]?token|"
    r"authToken|sessionToken|idToken|csrfToken|turnstileToken|token|password|privateKey|secret|seed|mnemonic|"
    r"signature|signedTransaction|jwt|x-turnstile-token|proxy-authorization|proxy-authenticate|"
    r"SOLANA_WALLET_KEY|JUPITER_REPLAY_KEYPAIR|keypair)$",
    re.I,
)


class ReplayError(Exception):
    """Public, credential-free failure."""

    def __init__(self, message: str, *, code: str | None = None, status_code=None, api_code=None):
        super().__init__(message)
        self.code, self.status_code, self.api_code = code, status_code, api_code


def sanitized(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: "[REDACTED]" if SECRET_FIELDS.fullmatch(k) else sanitized(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitized(v) for v in value]
    return value


def raw_amount(value: str, decimals: int) -> int:
    if type(decimals) is not int or not 0 <= decimals <= 18:
        raise ValueError("decimals must be an integer between 0 and 18")
    # UI locale separators are accepted without float rounding or thousands grouping.
    if len(value) > 100 or not re.fullmatch(r"\d+(?:[.,]\d+)?", value):
        raise ValueError("amount must be a positive decimal without grouping or exponent")
    try:
        with localcontext() as context:
            context.prec = 120
            scaled = Decimal(value.replace(",", ".")) * (10**decimals)
    except InvalidOperation as error:
        raise ValueError("invalid decimal amount") from error
    if scaled != scaled.to_integral_value() or not 0 < scaled <= 2**64 - 1:
        raise ValueError("amount exceeds token precision or u64 range")
    return int(scaled)


def captured_frontend_key(root: Path) -> str:
    """Read only the publishable fallback config embedded in the archived public JS.

    Does not read cookies, a user profile, .env, or a private developer credential.
    Reject changed/unpinned source rather than guessing another key from a bundle.
    """
    manifest = json.loads((root / "js-manifest.json").read_text())
    keys: set[str] = set()
    for item in manifest:
        if not Path(item["file"]).name.endswith(".js") or "vendor-tanstack-runtime" not in item["file"]:
            continue
        source_path = (root / item["file"]).resolve()
        if not source_path.is_relative_to(root.resolve()):
            raise ReplayError("Invalid source path in archive")
        source = source_path.read_bytes()
        if hashlib.sha256(source).hexdigest() != item["sha256"]:
            raise ReplayError("Frontend source digest mismatch")
        keys.add(frontend_key_from_source(source))
    if len(keys) != 1:
        raise ReplayError("Captured frontend config is ambiguous/missing; recapture current public sources")
    return keys.pop()


def frontend_key_from_source(source: bytes) -> str:
    """Extract the public swapApiKey fallback without executing downloaded JavaScript."""
    keys = set(
        re.findall(
            r"apiKey\s*:\s*[\w$]+\.swapApiKey\s*\?\?\s*['\"`]([\w.-]+)['\"`]",
            source.decode("utf-8"),
        )
    )
    if len(keys) != 1:
        raise ReplayError("Public frontend config is ambiguous/missing; inspect the new JS build")
    return keys.pop()


@dataclass(frozen=True)
class SwapInput:
    input_mint: str
    output_mint: str
    amount: str
    amount_decimals: int
    profile: str = "ultra"
    swap_mode: str = "ExactIn"
    wallet: str | None = None
    has_enough_balance: bool = True
    max_transaction_version: int = 0
    slippage_bps: int = 50
    broadcast_fee_type: str = "maxCap"
    priority_fee_lamports: int | None = 1_000_000
    jito_tip_lamports: int | None = None
    use_wsol: bool = False
    exclude_dexes: tuple[str, ...] = ()
    exclude_routers: tuple[str, ...] = ()
    require_durability: bool | None = None

    def params(self) -> dict[str, str]:
        for address in (self.input_mint, self.output_mint, self.wallet):
            if address is not None:
                Pubkey.from_string(address)
        if self.input_mint == self.output_mint:
            raise ValueError("input and output mints must differ")
        if self.profile not in {"ultra", "manual"} or self.swap_mode not in {"ExactIn", "ExactOut"}:
            raise ValueError("invalid profile or swap mode")
        if any(type(v) is not bool for v in (self.has_enough_balance, self.use_wsol)):
            raise ValueError("balance and wSOL flags must be boolean")
        if self.require_durability is not None and type(self.require_durability) is not bool:
            raise ValueError("durability flag must be boolean")
        if type(self.max_transaction_version) is not int or self.max_transaction_version not in {0, 1}:
            raise ValueError("transaction version must be 0 or 1")
        if type(self.slippage_bps) is not int or not 0 <= self.slippage_bps <= 10_000:
            raise ValueError("slippage bps must be an integer between 0 and 10000")
        if self.broadcast_fee_type not in {"maxCap", "exactFee"}:
            raise ValueError("invalid broadcast fee type")
        for fee in (self.priority_fee_lamports, self.jito_tip_lamports):
            if fee is not None and (type(fee) is not int or not 0 <= fee <= 2**64 - 1):
                raise ValueError("fees must be unsigned integer lamports")
        if self.jito_tip_lamports is not None and self.jito_tip_lamports < 1000:
            raise ValueError("enabled Jito tip must be at least 1000 lamports")
        if self.profile == "ultra" and self.swap_mode == "ExactOut":
            raise ValueError("ExactOut requires the manual profile in the captured UI")
        taker = self.wallet if self.has_enough_balance or self.swap_mode == "ExactOut" else None
        p = {
            "amount": str(raw_amount(self.amount, self.amount_decimals)),
            "inputMint": self.input_mint,
            "outputMint": self.output_mint,
            "swapMode": self.swap_mode,
            "maxSupportedTransactionVersion": str(self.max_transaction_version),
        }
        if taker:
            p["taker"] = taker
        if self.require_durability is not None:
            p["requireDurability"] = str(self.require_durability).lower()
        if self.profile == "manual":
            p.update(
                broadcastFeeType=self.broadcast_fee_type,
                excludeDexes=",".join(self.exclude_dexes),
                excludeRouters=",".join(self.exclude_routers),
                slippageBps=str(self.slippage_bps),
                useWsol=str(self.use_wsol).lower(),
            )
            for name, value in (
                ("priorityFeeLamports", self.priority_fee_lamports),
                ("jitoTipLamports", self.jito_tip_lamports),
            ):
                if value is not None:
                    p[name] = str(value)
        return p


@dataclass(repr=False)
class AuthSession:
    address: str
    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_at: str

    def __repr__(self) -> str:
        return f"AuthSession(address={self.address!r}, expires_at={self.expires_at!r})"

    @classmethod
    def parse(cls, address: str, data: dict) -> AuthSession:
        if data.get("tokenType") != "Bearer":
            raise ReplayError("Unexpected auth token type")
        if not all(
            isinstance(data.get(k), str) and data[k] for k in ("accessToken", "refreshToken", "expiresAt")
        ):
            raise ReplayError("Incomplete auth session")
        try:
            expires = datetime.fromisoformat(data["expiresAt"].replace("Z", "+00:00"))
            if expires.tzinfo is None or expires <= datetime.now(UTC):
                raise ValueError
        except ValueError as error:
            raise ReplayError("Invalid/expired auth expiry") from error
        return cls(address, data["accessToken"], data["refreshToken"], data["expiresAt"])


class JupiterReplay:
    def __init__(
        self,
        *,
        backend: str = "website",
        api_key: str | None = None,
        website_key: str | None = None,
        proxy: str | None = None,
        audit: Path | None = None,
        client: AsyncHttpClient | None = None,
        transport: str = "httpx",
        interval: float = 2.1,
    ):
        if backend not in ORDER_BASES:
            raise ValueError("unknown backend")
        if transport not in {"httpx", "curl_cffi"}:
            raise ValueError("unknown HTTP transport")
        if client is not None and transport != "httpx":
            raise ValueError("An injected test client cannot select another transport")
        validated_proxy = checked_proxy(proxy)
        if client is not None and proxy is not None:
            raise ValueError("Pass either a proxy or an injected test client; a client would ignore proxy")
        if client is None and validated_proxy is None:
            raise ReplayError("Jupiter replay requires an explicit HTTP(S) proxy")
        self.backend = backend
        self.base = ORDER_BASES[backend]
        self.api_key = api_key
        self.website_key = website_key
        self.audit = audit
        self.proxied = validated_proxy is not None
        self.transport = "injected" if client is not None else transport
        self._proxy_redactor = ProxyLogRedactor(validated_proxy)
        try:
            self.http: AsyncHttpClient
            if client is not None:
                self.http = client
            elif transport == "curl_cffi":
                assert validated_proxy is not None
                self.http = CurlClient(validated_proxy)
            else:
                self.http = httpx.AsyncClient(
                    timeout=30, trust_env=False, proxy=validated_proxy, follow_redirects=False
                )
        except ImportError:
            self._proxy_redactor.close()
            raise ReplayError("Install the Jupiter extra: pip install -e '.[jupiter]'") from None
        except Exception:
            self._proxy_redactor.close()
            raise ReplayError("Could not initialize the proxy transport") from None
        self._owns_client = client is None
        self.interval = interval
        self._last_start = 0.0
        self._defer_until = 0.0
        self.responses_429 = 0
        self.session: AuthSession | None = None
        self._lock = asyncio.Lock()
        self._in_flight = asyncio.Semaphore(4)
        self.queued = 0

    async def close(self) -> None:
        try:
            if self._owns_client:
                await self.http.aclose()
        finally:
            self._proxy_redactor.close()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        payload: dict | None = None,
        authenticated: bool = False,
        allow_status: tuple[int, ...] = (),
        audit_label: str | None = None,
        allow_api_error: bool = False,
        interval_s: float | None = None,
        before_send: Callable[[], None] | None = None,
        timeout: float | None = None,
    ) -> dict:
        endpoint = audit_label or url.split("?")[0]
        headers = {"accept": "application/json"}
        if url.startswith("https://api.jup.ag/") and self.api_key:
            headers["x-api-key"] = self.api_key
        if url.startswith(TRIGGER_BASE + "/") and self.backend == "website" and self.website_key:
            # Captured production FF client uses the same publishable fallback as swap-api.
            headers["x-api-key"] = self.website_key
        if authenticated:
            if self.session is None or not url.startswith(TRIGGER_BASE + "/"):
                raise ReplayError("Missing scoped Trigger session")
            headers["authorization"] = "Bearer " + self.session.access_token
        # Pace starts, not entire HTTP round trips. Slow responses must not hold
        # the admission lock while another permitted read could be in flight.
        self.queued += 1
        try:
            await self._in_flight.acquire()
        except BaseException:
            self.queued -= 1
            raise
        waiting = True
        try:
            async with self._lock:
                while True:
                    delay = max(
                        (self.interval if interval_s is None else interval_s)
                        - (time.monotonic() - self._last_start),
                        self._defer_until - time.monotonic(),
                    )
                    if delay <= 0:
                        break
                    await asyncio.sleep(delay)
                self._last_start = time.monotonic()
                if before_send:
                    before_send()
            self.queued -= 1
            waiting = False
            try:
                options = {"timeout": timeout} if timeout is not None else {}
                response = await self.http.request(
                    method, url, params=params, json=payload, headers=headers, **options
                )
            except (httpx.HTTPError, CurlTransportError):
                # Never automatically retry a POST; its outcome may be unknown.
                raise ReplayError(
                    f"{method} {endpoint}: proxy/transport failure; no automatic retry",
                    code="jupiter_transport_error",
                ) from None
            if response.status_code == 429:
                self.responses_429 += 1
                wait = retry_after_seconds(getattr(response, "headers", {}).get("Retry-After"))
                self._defer_until = max(
                    self._defer_until, time.monotonic() + (wait if wait is not None else 30)
                )
        finally:
            if waiting:
                self.queued -= 1
            self._in_flight.release()
        try:
            data = response.json()
        except ValueError:
            data = {"nonJson": True, "bytes": len(response.content)}
        if self.audit:
            self.audit.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "time": datetime.now(UTC).isoformat(),
                "method": method,
                "url": audit_label or url,
                "params": params,
                "body": payload,
                "status": response.status_code,
                "response": data,
                "proxied": self.proxied,
                "transport": self.transport,
            }
            with self.audit.open("a") as handle:
                handle.write(json.dumps(sanitized(record)) + "\n")
        if response.status_code in allow_status and isinstance(data, dict):
            return {"httpStatus": response.status_code, "response": data}
        if not 200 <= response.status_code < 300:
            detail = api_error_metadata(data)
            raise ReplayError(
                f"{method} {endpoint}: HTTP {response.status_code}; no automatic retry",
                code={
                    "no_route": "jupiter_no_route",
                    "insufficient_liquidity": "jupiter_insufficient_liquidity",
                }.get(detail.get("reason", ""), "jupiter_http_error"),
                status_code=response.status_code,
                api_code=detail.get("api_code"),
            )
        if not isinstance(data, dict) or data.get("nonJson") or (data.get("error") and not allow_api_error):
            detail = api_error_metadata(data)
            raise ReplayError(
                "API returned an invalid/error response; inspect sanitized audit",
                code={
                    "no_route": "jupiter_no_route",
                    "insufficient_liquidity": "jupiter_insufficient_liquidity",
                }.get(detail.get("reason", ""), "jupiter_invalid_response"),
                status_code=response.status_code,
                api_code=detail.get("api_code"),
            )
        return data

    async def holdings(self, address: str) -> dict:
        Pubkey.from_string(address)
        return await self.request(
            "GET", f"https://ultra-api.jup.ag/holdings/{address}", params={"clientPlatform": "jupiter.web"}
        )

    async def order(self, swap: SwapInput) -> dict:
        params = swap.params()
        if self.backend != "developer":
            params["clientPlatform"] = "jupiter.web.swap_page"
        if self.backend == "website" and self.website_key:
            params["apiKey"] = self.website_key
        started_at = 0.0

        def mark_started() -> None:
            nonlocal started_at
            started_at = time.time()

        quote = await self.request("GET", self.base + "/order", params=params, before_send=mark_started)
        for key in ("inputMint", "outputMint", "swapMode"):
            if quote.get(key) != params[key]:
                raise ReplayError(f"Order differs from requested {key}", code="jupiter_order_pair_mismatch")
        side = "inAmount" if swap.swap_mode == "ExactIn" else "outAmount"
        if quote.get(side) != params["amount"]:
            raise ReplayError("Order differs from requested amount", code="jupiter_order_amount_mismatch")
        if not isinstance(quote.get("requestId"), str) or not quote["requestId"]:
            raise ReplayError("Order has no requestId", code="jupiter_order_missing_request_id")
        if params.get("taker") and quote.get("taker") != params["taker"]:
            raise ReplayError("Order taker differs from wallet", code="jupiter_order_wallet_mismatch")
        return {
            "backend": self.backend,
            "params": sanitized(params),
            "receivedAt": time.time(),
            "requestedAt": started_at,
            "quote": quote,
        }

    async def login(self, wallet: Keypair) -> AuthSession:
        address = str(wallet.pubkey())
        challenge = await self.request(
            "POST", TRIGGER_BASE + "/v2/auth/challenge", payload={"type": "message", "walletPubkey": address}
        )
        message = challenge.get("challenge")
        if challenge.get("type") != "message" or not isinstance(message, str):
            raise ReplayError("Expected a message challenge; transaction fallback requires separate review")
        if (
            not 0 < len(message.encode()) <= 4096
            or address not in message
            or not re.search("jup|jupiter", message, re.I)
        ):
            raise ReplayError("Challenge does not identify Jupiter and the requested wallet")
        message_bytes = message.encode("utf-8")
        signature = wallet.sign_message(message_bytes)
        if not signature.verify(wallet.pubkey(), message_bytes):
            raise ReplayError("Local message signature verification failed")
        data = await self.request(
            "POST",
            TRIGGER_BASE + "/v2/auth/verify",
            payload={
                "authMode": "access_refresh",
                "type": "message",
                "walletPubkey": address,
                "signature": str(signature),  # Base58, as the browser's SP.encode.
            },
        )
        if data.get("authMode") != "access_refresh":
            raise ReplayError("Unexpected auth mode")
        self.session = AuthSession.parse(address, data)
        return self.session

    async def refresh(self) -> AuthSession:
        if self.session is None:
            raise ReplayError("No session to refresh")
        address = self.session.address
        data = await self.request(
            "POST", TRIGGER_BASE + "/v2/auth/refresh", payload={"refreshToken": self.session.refresh_token}
        )
        self.session = AuthSession.parse(address, data)
        return self.session

    async def logout(self) -> None:
        if self.session:
            await self.request(
                "POST", TRIGGER_BASE + "/v2/auth/logout", payload={"refreshToken": self.session.refresh_token}
            )
            self.session = None

    async def trigger_vault(self) -> dict:
        result = await self.request(
            "GET", TRIGGER_BASE + "/v2/vault", authenticated=True, allow_status=(404,)
        )
        if result.get("httpStatus") == 404:
            if result.get("response") != {"error": "Vault not found"}:
                raise ReplayError("Unexpected vault 404 response")
            return {"phase": "not_registered", "registered": False}
        return {"phase": "ready", "registered": True, "vault": result}

    async def execute(
        self,
        prepared: dict,
        signed_transaction: str,
        *,
        allow_execute: bool = False,
        max_age_s: float = 30,
        on_submit: Callable[[], None] | None = None,
    ) -> dict:
        if not allow_execute:
            raise ReplayError("Explicit execution flag required")
        if (
            prepared.get("backend") != self.backend
            or not 0 <= time.time() - prepared["receivedAt"] <= max_age_s
        ):
            raise ReplayError("Order backend differs or order expired; acquire and review a new order")
        quote = prepared["quote"]
        if not quote.get("transaction") or not quote.get("taker"):
            raise ReplayError("Preview order cannot be executed")
        # Signed bytes must contain the exact reviewed server transaction message.
        try:
            unsigned = VersionedTransaction.from_bytes(base64.b64decode(quote["transaction"], validate=True))
            signed = VersionedTransaction.from_bytes(base64.b64decode(signed_transaction, validate=True))
        except (ValueError, TypeError) as error:
            raise ReplayError("Unsupported transaction bytes/version; no submission") from error
        if to_bytes_versioned(signed.message) != to_bytes_versioned(unsigned.message):
            raise ReplayError("Signed transaction changed the reviewed message")
        from .jupiter_wallet import verify_swap_signatures

        verify_swap_signatures(signed, quote, require_taker=True)
        signers = signed.message.account_keys[: signed.message.header.num_required_signatures]
        for pubkey, old, new in zip(signers, unsigned.signatures, signed.signatures, strict=True):
            if str(pubkey) != quote["taker"] and old != new:
                raise ReplayError("Signed transaction changed a non-wallet signature")
        params = {"clientPlatform": "jupiter.web"} if self.backend != "developer" else {}
        if self.backend == "website" and self.website_key:
            params["apiKey"] = self.website_key

        def before_send() -> None:
            from .jupiter_swap import check_fresh_order

            check_fresh_order(prepared)
            if on_submit:
                on_submit()

        return await self.request(
            "POST",
            self.base + "/execute",
            params=params,
            payload={
                "signedTransaction": signed_transaction,
                "requestId": quote["requestId"],
            },
            allow_api_error=True,
            before_send=before_send,
            timeout=92,  # Captured frontend submitSwap uses the same managed landing timeout.
        )
