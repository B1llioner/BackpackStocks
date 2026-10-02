"""PostgreSQL market data only; SQLite remains the execution journal."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from importlib.resources import files

from psycopg import AsyncConnection
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from solders.pubkey import Pubkey


def instant(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("An aware UTC timestamp is required")
    return result.astimezone(UTC)


def number(value) -> Decimal | None:
    if value is None:
        return None
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Nonfinite amount")
    return result


async def migrate_local(dsn: str) -> dict:
    """Explicit migration for the dedicated local database, never an arbitrary host."""
    cfg = conninfo_to_dict(dsn)
    if (
        cfg.get("host") != "127.0.0.1"
        or cfg.get("port") != "55432"
        or cfg.get("dbname") != "backpack_stocks"
        or cfg.get("user") != "backpack_migrator"
    ):
        raise ValueError("Migration is restricted to the dedicated local PostgreSQL")
    sql = files("backpack_stocks").joinpath("migrations/001_pool_catalog.sql").read_text()
    checksum = hashlib.sha256(sql.encode()).hexdigest()
    conn = await AsyncConnection.connect(
        dsn, connect_timeout=5, options="-c statement_timeout=15000 -c lock_timeout=3000"
    )
    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(824782101)")
            await conn.execute("CREATE SCHEMA IF NOT EXISTS market_data")
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS market_data.schema_migrations ("
                "version integer PRIMARY KEY CHECK(version > 0), checksum text NOT NULL, "
                "applied_at timestamptz NOT NULL DEFAULT now())"
            )
            row = await (
                await conn.execute(
                    "SELECT checksum FROM market_data.schema_migrations WHERE version=%s", (1,)
                )
            ).fetchone()
            if row:
                if row[0] != checksum:
                    raise ValueError("Applied migration checksum changed; add a new migration")
                return {"version": 1, "applied": False}
            await conn.execute(sql, prepare=False)
            await conn.execute(
                "INSERT INTO market_data.schema_migrations(version,checksum) VALUES(%s,%s)", (1, checksum)
            )
        return {"version": 1, "applied": True}
    finally:
        await conn.close()


class PoolRepository:
    def __init__(self, dsn: str):
        self.pool = AsyncConnectionPool[AsyncConnection[dict]](
            dsn,
            min_size=1,
            max_size=4,
            open=False,
            timeout=10,
            kwargs={
                "row_factory": dict_row,
                "connect_timeout": 5,
                "application_name": "backpack_pool_catalog",
                "options": "-c statement_timeout=10000 -c lock_timeout=2000",
            },
        )

    async def __aenter__(self):
        await self.pool.open(wait=True)
        return self

    async def __aexit__(self, *args):
        await self.pool.close()

    async def save_catalog(self, catalog: dict) -> dict:
        # Validate keys before issuing writes; all public content remains bound data.
        for mint in catalog["tokens"]:
            Pubkey.from_string(mint)
        for p in catalog["pools"]:
            for key in ("address", "mintA", "mintB"):
                Pubkey.from_string(p[key])
        observed = instant(catalog["finished_at"])
        scan = catalog["scan_id"]
        async with self.pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO market_data.scan_runs(id,started_at,finished_at,status,summary) VALUES(%s,%s,%s,%s,%s) "
                "ON CONFLICT(id) DO UPDATE SET finished_at=EXCLUDED.finished_at,status=EXCLUDED.status,summary=EXCLUDED.summary",
                (
                    scan,
                    instant(catalog["started_at"]),
                    observed,
                    "partial" if catalog["errors"] else "complete",
                    Jsonb(catalog["summary"]),
                ),
            )
            await cur.executemany(
                "INSERT INTO market_data.tokens(mint,decimals,token_program,chain_verified,slot,info,observed_at) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(mint) DO UPDATE SET decimals=EXCLUDED.decimals,"
                "token_program=EXCLUDED.token_program,chain_verified=EXCLUDED.chain_verified,slot=EXCLUDED.slot,"
                "info=EXCLUDED.info,observed_at=EXCLUDED.observed_at WHERE market_data.tokens.observed_at <= EXCLUDED.observed_at",
                [
                    (
                        mint,
                        t.get("decimals"),
                        t.get("program"),
                        t.get("verified", False),
                        t.get("slot"),
                        Jsonb(t),
                        instant(t["observed_at"]),
                    )
                    for mint, t in catalog["tokens"].items()
                ],
            )
            await cur.executemany(
                "INSERT INTO market_data.equities(symbol,mint,decimals,metadata,observed_at) VALUES(%s,%s,%s,%s,%s) "
                "ON CONFLICT(symbol) DO UPDATE SET mint=EXCLUDED.mint,decimals=EXCLUDED.decimals,metadata=EXCLUDED.metadata,"
                "observed_at=EXCLUDED.observed_at WHERE market_data.equities.observed_at <= EXCLUDED.observed_at",
                [(a["symbol"], a["mint"], a["decimals"], Jsonb(a), observed) for a in catalog["assets"]],
            )
            await cur.executemany(
                "INSERT INTO market_data.pools(address,program,kind,mint_a,mint_b,venue,chain_verified,discovery,reason,observed_at) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(address) DO UPDATE SET program=EXCLUDED.program,kind=EXCLUDED.kind,"
                "mint_a=EXCLUDED.mint_a,mint_b=EXCLUDED.mint_b,venue=EXCLUDED.venue,chain_verified=EXCLUDED.chain_verified,"
                "discovery=EXCLUDED.discovery,reason=EXCLUDED.reason,observed_at=EXCLUDED.observed_at "
                "WHERE market_data.pools.observed_at <= EXCLUDED.observed_at",
                [
                    (
                        p["address"],
                        p.get("program"),
                        p["kind"],
                        p["mintA"],
                        p["mintB"],
                        p["venue"],
                        p["verified"],
                        Jsonb(p["discovery"]),
                        p.get("reason"),
                        observed,
                    )
                    for p in catalog["pools"]
                ],
            )
            await cur.executemany(
                "INSERT INTO market_data.asset_pools(symbol,pool_address,source,observed_at) VALUES(%s,%s,%s,%s) "
                "ON CONFLICT(symbol,pool_address,source) DO UPDATE SET observed_at=GREATEST(market_data.asset_pools.observed_at,EXCLUDED.observed_at)",
                [(symbol, address, source, observed) for symbol, address, source in catalog["links"]],
            )
            snapshots = []
            for p in catalog["pools"]:
                if "snapshot" not in p:
                    continue
                s, state = p["snapshot"], p["snapshot"]["state"]
                snapshots.append(
                    (
                        scan,
                        p["address"],
                        s["slot"],
                        s["digest"],
                        instant(s["observed_at"]),
                        number(state.get("priceBperA")),
                        number(state.get("reserveA")),
                        number(state.get("reserveB")),
                        number(state.get("sqrtPriceX64")),
                        number(state.get("liquidity")),
                        Jsonb(state),
                        state["quoteSupported"],
                        state.get("quoteReason"),
                    )
                )
            await cur.executemany(
                "INSERT INTO market_data.pool_snapshots(scan_id,pool_address,slot,digest,observed_at,price_b_per_a,"
                "reserve_a,reserve_b,sqrt_price_x64,liquidity,state,quote_supported,reason) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(pool_address,slot,digest) DO NOTHING",
                snapshots,
            )
            await cur.executemany(
                "INSERT INTO market_data.quotes(id,scan_id,symbol,source,pool_address,input_mint,output_mint,input_raw,output_raw,"
                "minimum_output_raw,observed_at,slot,details) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(id) DO NOTHING",
                [
                    (
                        q["id"],
                        scan,
                        q["symbol"],
                        q["source"],
                        q.get("pool_address"),
                        q["input_mint"],
                        q["output_mint"],
                        number(q["input_raw"]),
                        number(q["output_raw"]),
                        number(q.get("minimum_output_raw")),
                        instant(q["observed_at"]),
                        q.get("slot"),
                        Jsonb(q["details"]),
                    )
                    for q in catalog["quotes"]
                ],
            )
        return {"scan_id": scan, **catalog["summary"]}

    async def status(self) -> dict:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT (SELECT count(*) FROM market_data.equities) equities,"
                "(SELECT count(*) FROM market_data.pools) pools,(SELECT count(*) FROM market_data.pool_snapshots) snapshots,"
                "(SELECT count(*) FROM market_data.quotes) quotes,(SELECT count(*) FROM market_data.scan_runs) scans"
            )
            counts = await cursor.fetchone()
            last = await (
                await conn.execute(
                    "SELECT id,finished_at,status,summary FROM market_data.scan_runs ORDER BY finished_at DESC NULLS LAST LIMIT 1"
                )
            ).fetchone()
            return {**(counts or {}), "last_scan": last}

    async def list_pools(self, symbol: str) -> list[dict]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT p.address,p.venue,p.kind,p.mint_a,p.mint_b,p.chain_verified,"
                "array_agg(ap.source ORDER BY ap.source) sources,max(ap.observed_at) last_seen,s.slot,s.observed_at,s.price_b_per_a,s.quote_supported,s.reason "
                "FROM market_data.asset_pools ap JOIN market_data.pools p ON p.address=ap.pool_address "
                "LEFT JOIN LATERAL (SELECT slot,observed_at,price_b_per_a,quote_supported,reason FROM market_data.pool_snapshots "
                "WHERE pool_address=p.address ORDER BY observed_at DESC,id DESC LIMIT 1) s ON true "
                "WHERE ap.symbol=%s GROUP BY p.address,s.slot,s.observed_at,s.price_b_per_a,s.quote_supported,s.reason ORDER BY p.kind,p.address",
                (symbol,),
            )
            return await cursor.fetchall()

    async def latest_state(self, address: str, *, max_age_s: int = 10) -> dict:
        Pubkey.from_string(address)
        if type(max_age_s) is not int or not 1 <= max_age_s <= 60:
            raise ValueError("Snapshot age limit must be 1..60 seconds")
        async with self.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT slot,observed_at,state FROM market_data.pool_snapshots "
                    "WHERE pool_address=%s ORDER BY observed_at DESC,id DESC LIMIT 1",
                    (address,),
                )
            ).fetchone()
        if not row or not 0 <= (datetime.now(UTC) - row["observed_at"]).total_seconds() <= max_age_s:
            raise ValueError("Missing or stale pool snapshot; refresh before quoting")
        return row
