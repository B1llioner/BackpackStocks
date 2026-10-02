from __future__ import annotations

import base64
import logging
import os
from urllib.parse import unquote, urlsplit

PROXY_ENV_NAMES = (
    "OUTBOUND_PROXY_URL",
    "BACKPACK_PROXY_URL",
    "BACKPACK_WS_PROXY_URL",
    "JUPITER_PROXY_URL",
    "SOLANA_PROXY_URL",
    "RAYDIUM_PROXY_URL",
    "TELEGRAM_PROXY_URL",
)


def checked_proxy(value: str | None) -> str | None:
    if value is None or value.strip().lower() in {"", "direct"}:
        return None
    try:
        url = urlsplit(value)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
            or (url.port is not None and not 0 < url.port < 65536)
            or any(char.isspace() or ord(char) < 32 for char in value)
        ):
            raise ValueError
    except ValueError:
        raise ValueError("Proxy must be a valid HTTP(S) proxy URL") from None
    return value


def resolve_proxy(provider: str) -> str | None:
    name = f"{provider.upper()}_PROXY_URL"
    if name not in PROXY_ENV_NAMES:
        raise ValueError("Unknown proxy provider")
    if os.environ.get(name, "").strip():
        return checked_proxy(os.environ[name])
    if provider.upper() == "BACKPACK_WS" and os.environ.get("BACKPACK_PROXY_URL", "").strip():
        return checked_proxy(os.environ["BACKPACK_PROXY_URL"])
    return checked_proxy(os.environ.get("OUTBOUND_PROXY_URL"))


class ProxyLogRedactor(logging.Filter):
    """Mask proxy credentials, including Basic auth produced by HTTP/WS libraries."""

    def __init__(self, proxy: str | None):
        super().__init__()
        self.secrets: set[str] = set()
        self.loggers: list[logging.Logger] = []
        if not proxy:
            return
        self.secrets.add(proxy)
        url = urlsplit(proxy)
        if url.username is not None:
            user, password = unquote(url.username), unquote(url.password or "")
            self.secrets.update(
                {url.netloc.rsplit("@", 1)[0], base64.b64encode(f"{user}:{password}".encode()).decode()}
            )
            if password:
                self.secrets.update({password, url.password or ""})
        names = {"httpx", "httpcore", "websockets", "websockets.client"}
        names.update(
            name
            for name in logging.Logger.manager.loggerDict
            if name.startswith(("httpcore.", "websockets."))
        )
        names.update({"httpcore.connection", "httpcore.http11", "httpcore.http2", "httpcore.proxy"})
        self.loggers = [logging.getLogger(name) for name in names]
        for logger in self.loggers:
            logger.addFilter(self)

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for secret in sorted(self.secrets, key=len, reverse=True):
            message = message.replace(secret, "<redacted>")
        record.msg, record.args = message, ()
        return True

    def close(self) -> None:
        for logger in self.loggers:
            logger.removeFilter(self)
