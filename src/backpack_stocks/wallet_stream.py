"""Finalized public transaction hints; the HTTP reader verifies every transaction."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from urllib.parse import urlsplit, urlunsplit

import websockets

from .observability import error_code, event
from .transport import RemoteError
from .ws_proxy import install


class WalletStream:
    def __init__(self, url: str, proxy: str | None, on_signature: Callable[[str, str], None]):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("Wallet stream requires an HTTPS RPC endpoint")
        self.url = urlunsplit(parsed._replace(scheme="wss"))
        self.proxy, self.on_signature = proxy, on_signature
        self.addresses: dict[str, str] = {}
        self.connected = False
        self.subscribed = 0
        self.last_error: str | None = None
        install()

    def update(self, addresses: dict[str, str]) -> None:
        if len(addresses) > 512:
            raise ValueError("Too many wallet subscriptions")
        self.addresses = dict(addresses)

    def ingest(self, payload: dict, subscriptions: dict[int, str]) -> None:
        if payload.get("method") != "logsNotification":
            return
        params = payload.get("params")
        if not isinstance(params, dict) or type(params.get("subscription")) is not int:
            raise ValueError("Invalid wallet notification")
        wallet = subscriptions.get(params["subscription"])
        if wallet is None:
            return
        result = params.get("result")
        if not isinstance(result, dict) or not isinstance(result.get("value"), dict):
            raise ValueError("Invalid wallet notification value")
        signature = result["value"].get("signature")
        if not isinstance(signature, str):
            raise ValueError("Invalid wallet signature")
        self.on_signature(wallet, signature)

    async def run(self) -> None:
        while True:
            if not self.addresses:
                await asyncio.sleep(1)
                continue
            try:
                async with websockets.connect(
                    self.url, proxy=self.proxy, open_timeout=15, max_size=256_000, max_queue=16
                ) as socket:
                    self.connected = True
                    pending: dict[int, str] = {}
                    subscriptions: dict[int, str] = {}
                    sent: set[str] = set()
                    sequence = 0
                    event("wallet_ws_connected", component="wallet_watch", ws_connected=True)
                    while True:
                        for address in sorted(self.addresses.keys() - sent):
                            sequence += 1
                            pending[sequence] = self.addresses[address]
                            await socket.send(
                                json.dumps(
                                    {
                                        "jsonrpc": "2.0",
                                        "id": sequence,
                                        "method": "logsSubscribe",
                                        "params": [{"mentions": [address]}, {"commitment": "finalized"}],
                                    }
                                )
                            )
                            sent.add(address)
                            await asyncio.sleep(0.05)
                        try:
                            message = await asyncio.wait_for(socket.recv(), 5)
                        except TimeoutError:
                            if pending:
                                raise RemoteError("Wallet subscriptions not acknowledged") from None
                            continue
                        payload = json.loads(message)
                        if not isinstance(payload, dict):
                            raise ValueError("Invalid wallet stream response")
                        identity = payload.get("id")
                        if type(identity) is int and identity in pending:
                            if "error" in payload or type(payload.get("result")) is not int:
                                raise RemoteError("Wallet subscription rejected")
                            subscriptions[payload["result"]] = pending.pop(identity)
                            self.subscribed = len(subscriptions)
                            self.last_error = None
                            if not pending:
                                event("wallet_ws_subscribed", component="wallet_watch", quantity=self.subscribed)
                        else:
                            self.ingest(payload, subscriptions)
            except Exception as error:
                self.last_error = error_code(error)
                event("wallet_ws_error", level="WARNING", component="wallet_watch", code=self.last_error)
            finally:
                self.connected = False
                self.subscribed = 0
            # HTTP pagination continues throughout a socket outage.
            await asyncio.sleep(20)
