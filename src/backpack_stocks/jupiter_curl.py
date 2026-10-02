"""Optional curl_cffi transport for Jupiter's browser-independent HTTP flow."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypedDict, cast
from urllib.parse import unquote, urlsplit, urlunsplit

from .proxy import checked_proxy

if TYPE_CHECKING:
    from curl_cffi.requests.session import HttpMethod


class _TimeoutOptions(TypedDict, total=False):
    timeout: float


class HttpResponse(Protocol):
    @property
    def status_code(self) -> int: ...

    @property
    def content(self) -> bytes: ...

    def json(self) -> Any: ...


class AsyncHttpClient(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
    ) -> HttpResponse: ...

    async def aclose(self) -> None: ...


class CurlTransportError(Exception):
    """Credential-free transport failure; never expose the native curl error."""


class CurlClient:
    def __init__(self, proxy: str):
        # Lazy import keeps the regular httpx path usable without the optional extra.
        from curl_cffi import AsyncSession, CurlHttpVersion, CurlOpt
        from curl_cffi.requests.exceptions import RequestException

        validated = checked_proxy(proxy)
        if validated is None:
            raise ValueError("curl_cffi replay requires an explicit HTTP(S) proxy")
        parsed = urlsplit(validated)
        server = urlunsplit((parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], "", "", ""))
        auth = None
        if parsed.username is not None:
            auth = (unquote(parsed.username), unquote(parsed.password or ""))
        self._request_error = RequestException
        self.session: AsyncSession[Any] = AsyncSession(
            proxy=server,
            proxy_auth=auth,
            impersonate="chrome",
            timeout=30,
            trust_env=False,
            verify=True,
            allow_redirects=False,
            retry=0,
            debug=False,
            http_version=CurlHttpVersion.V2TLS,
            # libcurl itself honours NO_PROXY; an explicit empty list prevents bypass.
            curl_options={CurlOpt.NOPROXY: ""},
        )

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        headers: dict | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        if method not in {"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "TRACE", "PATCH", "QUERY"}:
            raise ValueError("Unsupported HTTP method")
        try:
            options: _TimeoutOptions = {"timeout": timeout} if timeout is not None else {}
            return await self.session.request(
                cast("HttpMethod", method), url, params=params, json=json, headers=headers, **options
            )
        except self._request_error:
            raise CurlTransportError("curl_cffi proxy/transport failure") from None

    async def aclose(self) -> None:
        await self.session.close()
