"""One explicit HTTP(S) proxy selection for the standalone Jupiter flow."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

from .proxy import checked_proxy

JUPITER_PROXY_ENV_NAMES = ("JUPITER_REPLAY_PROXY", "JUPITER_PROXY_URL", "OUTBOUND_PROXY_URL")


def replay_proxy(environ: Mapping[str, str] | None = None, *, env_name: str | None = None) -> str:
    values = os.environ if environ is None else environ
    if env_name is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
        raise ValueError("Invalid proxy environment variable name")
    names = (env_name,) if env_name else JUPITER_PROXY_ENV_NAMES
    for name in names:
        value = values.get(name)
        if value is None or not value.strip():
            continue
        proxy = checked_proxy(value)
        if proxy is None:
            raise ValueError("Jupiter replay requires a proxy; DIRECT is disabled")
        return proxy
    raise ValueError("Set JUPITER_REPLAY_PROXY, JUPITER_PROXY_URL or OUTBOUND_PROXY_URL before networking")
