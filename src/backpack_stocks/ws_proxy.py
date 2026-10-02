"""Scoped websockets 15 HTTP CONNECT cancellation compatibility fix.

The upstream 15 implementation leaves its transport open when its response future is
cancelled; late proxy callbacks then mutate an already completed future. Preserve
proxy/auth/TLS behaviour and abort the transport on cancellation.
"""

from __future__ import annotations

import websockets
import websockets.asyncio.client as client

NativeProxyConnection = client.HTTPProxyConnection


class CancellationSafeProxyConnection(NativeProxyConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.response.add_done_callback(self._completed)

    def _completed(self, future):
        if future.cancelled() and getattr(self, "transport", None) is not None:
            self.transport.abort()

    def run_parser(self) -> None:
        if not self.response.done():
            super().run_parser()

    def connection_lost(self, exc) -> None:
        if not self.response.done():
            super().connection_lost(exc)
            if exc is None:
                self.run_parser()


def install() -> None:
    # Version-specific internal hook; do not patch future versions with different internals.
    if websockets.__version__.startswith("15."):
        client.HTTPProxyConnection = CancellationSafeProxyConnection  # type: ignore[misc]
