from __future__ import annotations

import base64
import os
from contextlib import asynccontextmanager
from pathlib import Path

from solders.keypair import Keypair

from .backpack import Backpack, BackpackAuth
from .direct import DirectRaydium
from .direct_execution import DirectSwapGuard, WalletLease
from .engine import Engine
from .jupiter import Jupiter
from .live import LiveBroker
from .market import PaperBroker, PublicMarket, ReplayMarket
from .models import Settings
from .monitor import SpreadMonitor
from .observability import configure, event
from .proxy import resolve_proxy
from .solana import SolanaRPC
from .store import Store
from .swap_guard import SwapGuard
from .transfer import USDCTransfer
from .wallet_watch import WalletJournal, WalletWatcher


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Required environment variable is missing: {name}")
    return value


def public_clients(settings: Settings) -> tuple[Backpack, Jupiter | DirectRaydium, SolanaRPC]:
    # Validate all routes before opening clients; a configured route never falls back to direct.
    bp_proxy, ws_proxy, rpc_proxy = (resolve_proxy(name) for name in ("BACKPACK", "BACKPACK_WS", "SOLANA"))
    rpc = SolanaRPC(settings.rpc_url, interval=settings.rpc_interval_s, proxy=rpc_proxy)
    dex = (
        DirectRaydium(settings, rpc)
        if settings.dex_backend == "raydium"
        else Jupiter(
            os.environ.get(settings.jupiter_key_env),
            settings.jupiter_interval_s,
            proxy=resolve_proxy("JUPITER"),
        )
    )
    return (
        Backpack(interval=settings.backpack_interval_s, proxy=bp_proxy, ws_proxy=ws_proxy or "DIRECT"),
        dex,
        rpc,
    )


@asynccontextmanager
async def runtime(settings: Settings):
    if settings.log_file:
        configure(settings.log_file)
        event("runtime_start", mode=settings.mode, size_usdc=settings.monitor.notional_usdc)
    store = Store(settings.database, settings)
    clients: list[Backpack | Jupiter | DirectRaydium | SolanaRPC] = []
    market: ReplayMarket | PublicMarket | None = None
    monitor: SpreadMonitor | None = None
    watcher: WalletWatcher | None = None
    broker: PaperBroker | LiveBroker
    wallet_lease: WalletLease | None = None
    wallet_reserved = False
    try:
        # Paper never reads the Solana signing key; private Backpack reads are opt-in.
        if settings.source == "replay":
            market = ReplayMarket(settings, store)
            broker = PaperBroker(settings, market)
        else:
            backpack, jupiter, rpc = public_clients(settings)
            clients.extend([backpack, jupiter, rpc])
            if settings.backpack_private_reads:
                backpack.auth = BackpackAuth(required_env(settings.backpack_seed_env))
            elif settings.monitor.enabled and settings.monitor.rfq_validation:
                seed = os.environ.get(settings.backpack_seed_env)
                if seed:
                    backpack.auth = BackpackAuth(seed)
            market = PublicMarket(settings, store, backpack, jupiter)
            await market.start()
            if settings.mode == "paper":
                broker = PaperBroker(settings, market)
            else:
                backpack.auth = BackpackAuth(required_env(settings.backpack_seed_env))
                encoded = base64.b64decode(required_env(settings.wallet_seed_env), validate=True)
                if len(encoded) == 32:
                    key = Keypair.from_seed(encoded)
                elif len(encoded) == 64:
                    key = Keypair.from_bytes(encoded)
                else:
                    raise ValueError("Solana key must contain 32 seed bytes or 64 keypair bytes")
                if str(key.pubkey()) != settings.wallet:
                    raise ValueError("Solana signing key does not match configured wallet")
                wallet_lease = WalletLease(str(key.pubkey()))
                WalletLease.reserve(str(key.pubkey()), "fsm", settings.database, "runtime", recover=True)
                wallet_reserved = True
                broker = LiveBroker(
                    settings,
                    backpack,
                    jupiter,
                    rpc,
                    (
                        DirectSwapGuard(jupiter, rpc, key, settings)
                        if isinstance(jupiter, DirectRaydium)
                        else SwapGuard(rpc, key, settings.policy)
                    ),
                    USDCTransfer(rpc, key, settings.policy),
                )
            if settings.monitor.enabled:
                monitor = SpreadMonitor(settings, backpack, jupiter, rpc)
                market.monitor = monitor
                await monitor.start()
        engine = Engine(settings, store, market, broker)
        engine.monitor = monitor
        if settings.source == "public":
            watcher = WalletWatcher(
                WalletJournal(Path(settings.database).parent / "wallet-watch.sqlite3"), rpc, monitor
            )
            engine.wallet_watch = watcher
            if monitor:
                monitor.quote_observer = watcher.capture_quote
            watcher.start()
        yield engine
    finally:
        if monitor:
            monitor.quote_observer = None
        if watcher:
            await watcher.close()
        if monitor:
            await monitor.close()
        if market:
            await market.close()
        for client in reversed(clients):
            await client.close()
        if wallet_lease:
            if wallet_reserved and store.active() is None:
                assert settings.wallet is not None
                WalletLease.clear(settings.wallet, "fsm", settings.database, "runtime")
            wallet_lease.close()
        store.close()
