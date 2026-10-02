from __future__ import annotations

import asyncio
import contextlib
import secrets
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException

from .models import Settings
from .runtime import required_env, runtime


def create_app(settings: Settings, *, runtime_factory=runtime, token: str | None = None) -> FastAPI:
    access_token = token if token is not None else required_env(settings.api_token_env)
    if len(access_token) < 24:
        raise ValueError("API bearer token must be at least 24 characters")

    @asynccontextmanager
    async def lifespan(app):
        async with runtime_factory(settings) as engine:
            app.state.engine = engine

            async def worker():
                while True:
                    await engine.tick()
                    await asyncio.sleep(settings.poll_s)

            task = asyncio.create_task(worker())
            try:
                yield
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(title="Backpack Stocks", lifespan=lifespan, docs_url=None, redoc_url=None)

    def authorize(authorization: str | None = Header(default=None)):
        if not authorization or not secrets.compare_digest(authorization, "Bearer " + access_token):
            raise HTTPException(status_code=401, detail="Unauthorized")

    @app.get("/v1/status", dependencies=[Depends(authorize)])
    async def status():
        engine = app.state.engine
        return engine.status()

    @app.get("/v1/events", dependencies=[Depends(authorize)])
    async def events():
        return app.state.engine.store.events()

    @app.get("/v1/spreads", dependencies=[Depends(authorize)])
    async def spreads():
        monitor = app.state.engine.monitor
        if monitor is None:
            raise HTTPException(status_code=409, detail="Public multi-asset monitor is not enabled")
        return monitor.report()

    @app.get("/v1/spread", dependencies=[Depends(authorize)])
    async def quote():
        engine = app.state.engine
        if engine.monitor is not None:
            from .monitor import FORWARD

            report = engine.monitor.report("WEN.US")
            engine.monitor.request_refresh("WEN.US")
            result = next(
                (
                    r
                    for r in report["rows"]
                    if r["direction"] == FORWARD and r["fresh"] and r.get("profit_raw", 0) > 0
                ),
                None,
            )
            if result is None:
                raise HTTPException(status_code=409, detail="No RFQ-validated positive spread after costs")
            return result
        async with engine.lock:
            try:
                result = await engine.scan()
                if result.profit_raw <= 0:
                    raise HTTPException(status_code=409, detail="No positive spread after costs")
                return result.model_dump()
            except ValueError as e:
                raise HTTPException(status_code=409, detail=str(e)) from None

    async def control(action: str, identity: str | None):
        if not identity or len(identity) > 128:
            raise HTTPException(status_code=400, detail="Idempotency-Key required, maximum 128 chars")
        engine = app.state.engine
        # Serialize request caching with the execution owner; action itself takes engine.lock.
        key = action + ":" + identity
        async with app.state.control_lock:
            previous = engine.store.idempotent(key)
            if previous:
                return previous
            result = await (engine.stop() if action == "stop" else engine.resume())
            engine.store.remember_request(key, result)
            return result

    app.state.control_lock = asyncio.Lock()

    @app.post("/v1/stop", dependencies=[Depends(authorize)])
    async def stop(idempotency_key: str | None = Header(default=None)):
        return await control("stop", idempotency_key)

    @app.post("/v1/resume", dependencies=[Depends(authorize)])
    async def resume(idempotency_key: str | None = Header(default=None)):
        return await control("resume", idempotency_key)

    return app
