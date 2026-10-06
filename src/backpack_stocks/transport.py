from __future__ import annotations

import asyncio
import math
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from .limiter import PriorityLimiter, RequestScheduler
from .observability import event
from .proxy import ProxyLogRedactor, checked_proxy
from .rejection import api_error_metadata, classify_rejection


class RemoteError(RuntimeError):
    """Intentionally contains no request URL, headers, body or remote error payload."""

    def __init__(self, message: str, *, status_code: int | None = None, api_code=None, reason=None):
        super().__init__(message)
        self.status_code, self.api_code, self.reason = status_code, api_code, reason


class UnknownOutcome(RemoteError):
    """Mutation may have reached the server. Reconcile instead of resubmitting."""


def retry_after_seconds(value: str | None, *, now: float | None = None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = parsedate_to_datetime(value).timestamp() - (now if now is not None else time.time())
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0, seconds) if math.isfinite(seconds) else None


class Transport:
    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        interval: float = 0,
        byte_budget: int | None = None,
        max_response_bytes: int = 5_000_000,
        proxy: str | None = None,
        rate_headers: bool = False,
        rate_limit_fallback_s: float | None = None,
        oldest_every: int | None = None,
    ):
        if rate_limit_fallback_s is not None and (
            not math.isfinite(rate_limit_fallback_s) or rate_limit_fallback_s <= 0
        ):
            raise ValueError("Rate-limit fallback must be a positive finite delay")
        proxy = checked_proxy(proxy)
        if client is not None and proxy is not None:
            raise ValueError("Configure the proxy on the supplied HTTP client")
        self.proxied = proxy is not None
        self._redactor = ProxyLogRedactor(proxy)
        self.client = client or httpx.AsyncClient(
            base_url=base_url,
            timeout=20,
            follow_redirects=False,
            trust_env=False,
            proxy=proxy,
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=8, keepalive_expiry=60),
        )
        self.limiter = PriorityLimiter(interval, oldest_every=oldest_every)
        self.in_flight = asyncio.Semaphore(8)
        self.requests = RequestScheduler(self.in_flight)
        self.responses_429 = 0
        self.byte_budget, self.max_response_bytes = byte_budget, max_response_bytes
        self.byte_history: deque[tuple[float, int]] = deque()
        self.rate_headers = rate_headers
        self.rate_limit_fallback_s = rate_limit_fallback_s
        self.remote_budgets: dict[str, dict[str, float]] = {}
        self.last_rejection: dict[str, str | int] | None = None

    async def bandwidth_slot(self) -> None:
        if self.byte_budget is None:
            return
        while True:
            now = time.monotonic()
            while self.byte_history and self.byte_history[0][0] <= now - 30:
                self.byte_history.popleft()
            if sum(size for _, size in self.byte_history) < self.byte_budget:
                return
            await asyncio.sleep(min(1, max(0, self.byte_history[0][0] + 30 - now)))

    def status(self) -> dict:
        return {
            **self.limiter.status(),
            "responses_429_all_buckets": self.responses_429,
            "max_connections": 8,
            "response_cap_bytes": self.max_response_bytes,
            "byte_budget_per_30s": self.byte_budget,
            "bytes_last_30s": sum(size for stamp, size in self.byte_history if stamp > time.monotonic() - 30),
            "proxied": self.proxied,
            "remote_budgets": self.remote_budgets,
            "rate_limit_fallback_s": self.rate_limit_fallback_s,
            "last_rejection": self.last_rejection,
        }

    async def close(self) -> None:
        await self.requests.close()
        await self.limiter.close()
        await self.client.aclose()
        self._redactor.close()

    def remote_budget(self, response: httpx.Response, bucket: str, limiter: PriorityLimiter) -> float:
        if not self.rate_headers or response.status_code not in {200, 429}:
            return 0
        values: dict[str, float] = {}
        for name in ("remaining", "current", "reset"):
            try:
                value = float(response.headers[f"x-ratelimit-{name}"])
                if math.isfinite(value):
                    values[name] = value
            except (KeyError, ValueError, OverflowError):
                pass
        if values:
            self.remote_budgets[bucket] = {**values, "observed_at": time.time()}
        delay = max(0, values.get("reset", 0) - time.time())
        if response.status_code == 200 and values.get("remaining", math.inf) <= 0:
            limiter.defer(delay + 0.05 if delay else max(1, limiter.interval))
        return delay

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        body: Any = None,
        headers: dict | Callable[[], dict] | None = None,
        mutation: bool | None = None,
        before_attempt: Callable[[], Awaitable[None]] | None = None,
        priority: int = 0,
        category: str | None = None,
        limiter: PriorityLimiter | None = None,
        bucket: str = "main",
    ) -> Any:
        admission = limiter or self.limiter
        writes = mutation if mutation is not None else method != "GET"
        for attempt in range(1 if writes else 3):
            try:
                await self.bandwidth_slot()
                start = await self.requests.acquire(admission, priority, category)
                try:
                    await self.bandwidth_slot()
                    if before_attempt is not None:
                        await before_attempt()
                    await start.start()
                    current_headers = headers() if callable(headers) else headers
                    content = bytearray()
                    async with self.client.stream(
                        method, path, params=params, json=body, headers=current_headers
                    ) as received:
                        try:
                            async for chunk in received.aiter_bytes(chunk_size=65536):
                                content.extend(chunk)
                                if len(content) > self.max_response_bytes:
                                    cls = UnknownOutcome if writes else RemoteError
                                    raise cls("API response exceeds configured size limit")
                        finally:
                            if self.byte_budget is not None:
                                self.byte_history.append(
                                    (time.monotonic(), len(content) + len(received.request.content))
                                )
                        decoded_headers = {
                            k: v
                            for k, v in received.headers.items()
                            if k.lower() not in {"content-encoding", "content-length", "transfer-encoding"}
                        }
                        response = httpx.Response(
                            received.status_code,
                            headers=decoded_headers,
                            content=bytes(content),
                            request=received.request,
                        )
                finally:
                    start.release()
            except httpx.HTTPError:
                admission.record_failure()
                if writes:
                    raise UnknownOutcome("Network error after mutation submission") from None
                if attempt == 2:
                    raise RemoteError("Read request failed") from None
                await asyncio.sleep(0.25 * 2**attempt)
                continue
            rejection = classify_rejection(response)
            if rejection:
                self.last_rejection = rejection
            if rejection and rejection["kind"] in {"interactive_check", "firewall_deny"}:
                cls = UnknownOutcome if writes else RemoteError
                if rejection["kind"] == "interactive_check":
                    raise cls("Interactive browser verification required; API request stopped")
                raise cls("Upstream firewall denied this route; API request stopped")
            if response.status_code in {401, 403}:
                kind = rejection["kind"] if rejection else "access_rejected"
                raise RemoteError(f"API access rejected: {kind} (HTTP {response.status_code})")
            reset_delay = self.remote_budget(response, bucket, admission)
            if response.status_code >= 500 or response.status_code == 429:
                delay = retry_after_seconds(response.headers.get("Retry-After"))
                if delay is None:
                    delay = (
                        self.rate_limit_fallback_s
                        if response.status_code == 429
                        and reset_delay <= 0
                        and self.rate_limit_fallback_s is not None
                        else (1 if response.status_code == 429 else 0.25) * 2**attempt
                    )
                if response.status_code == 429:
                    delay = max(delay, reset_delay)
                paced_delay = delay + random.uniform(0, min(0.1, delay * 0.1))
                if response.status_code == 429:
                    self.responses_429 += 1
                    admission.responses_429 += 1
                    admission.rate_limit_backoff(paced_delay)
                else:
                    admission.record_failure()
                    admission.defer(paced_delay)
                event(
                    "http_backoff",
                    level="WARNING",
                    component="transport",
                    code="rate_limit" if response.status_code == 429 else "server_error",
                    status_code=response.status_code,
                    seconds=delay,
                    attempt=attempt + 1,
                )
                if writes:
                    raise UnknownOutcome(f"Mutation response HTTP {response.status_code}")
                if attempt < 2:
                    continue
            if response.status_code >= 300:
                try:
                    detail = api_error_metadata(response.json())
                except ValueError:
                    detail = {}
                event(
                    "http_rejected",
                    level="WARNING",
                    component="transport",
                    status_code=response.status_code,
                    **detail,
                )
                raise RemoteError(
                    f"API rejected request: HTTP {response.status_code}",
                    status_code=response.status_code,
                    **detail,
                )
            if not response.content:
                admission.record_success()
                return None
            try:
                result = response.json()
            except ValueError:
                cls = UnknownOutcome if writes else RemoteError
                raise cls("Invalid JSON response") from None
            admission.record_success()
            return result
        raise RemoteError("Read attempts exhausted")
