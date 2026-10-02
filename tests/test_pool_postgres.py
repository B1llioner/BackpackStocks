"""Opt-in checks of the dedicated LOCAL database, never a production DSN."""

import copy
import json
import os
import uuid
from pathlib import Path

import pytest

from backpack_stocks.environment import load_environment

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    os.environ.get("POOL_POSTGRES_TESTS") != "1", reason="Local PostgreSQL integration opt-in"
)


def local_dsn():
    from psycopg.conninfo import conninfo_to_dict

    load_environment(ROOT / ".env", ["POSTGRES_DSN"])
    dsn = os.environ["POSTGRES_DSN"]
    cfg = conninfo_to_dict(dsn)
    assert (cfg.get("host"), cfg.get("port"), cfg.get("dbname"), cfg.get("user")) == (
        "127.0.0.1",
        "55432",
        "backpack_stocks",
        "backpack_catalog",
    )
    return dsn


async def test_runtime_role_has_only_market_data_permissions():
    from psycopg.errors import InsufficientPrivilege

    from backpack_stocks.postgres import PoolRepository

    async with PoolRepository(local_dsn()) as repo:
        async with repo.pool.connection() as conn:
            r = await (
                await conn.execute(
                    "SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user"
                )
            ).fetchone()
            assert r and not any(r.values())
            r = await (
                await conn.execute(
                    "SELECT has_schema_privilege(current_user,'public','CREATE') public_create,"
                    "has_schema_privilege(current_user,'market_data','CREATE') schema_create,"
                    "has_table_privilege(current_user,'market_data.pools','INSERT') insert_ok,"
                    "has_table_privilege(current_user,'market_data.pools','DELETE') delete_ok,"
                    "has_table_privilege(current_user,'market_data.schema_migrations','UPDATE') migration_update"
                )
            ).fetchone()
            assert r == {
                "public_create": False,
                "schema_create": False,
                "insert_ok": True,
                "delete_ok": False,
                "migration_update": False,
            }
            with pytest.raises(InsufficientPrivilege):
                async with conn.transaction():
                    await conn.execute("CREATE TABLE market_data.permission_probe(id integer)")


async def test_import_is_idempotent_and_decimal_storage_is_exact():
    from backpack_stocks.postgres import PoolRepository

    catalog = json.loads((ROOT / "research/pool-pricing/catalog-refreshed.json").read_text())
    async with PoolRepository(local_dsn()) as repo:
        before = await repo.status()
        await repo.save_catalog(catalog)
        assert before == await repo.status()
        async with repo.pool.connection() as conn:
            # No binary float types are used for amounts or prices.
            r = await (
                await conn.execute(
                    "SELECT data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
                    ("market_data", "quotes", "input_raw"),
                )
            ).fetchone()
            assert r == {"data_type": "numeric"}
            q = catalog["quotes"][0]
            r = await (
                await conn.execute(
                    "SELECT input_raw,output_raw FROM market_data.quotes WHERE id=%s", (q["id"],)
                )
            ).fetchone()
            assert int(r["input_raw"]) == int(q["input_raw"]) and int(r["output_raw"]) == int(q["output_raw"])


async def test_failed_import_rolls_back_the_entire_scan():
    from psycopg.errors import CheckViolation

    from backpack_stocks.postgres import PoolRepository

    bad = copy.deepcopy(json.loads((ROOT / "research/pool-pricing/catalog-refreshed.json").read_text()))
    bad["scan_id"] = str(uuid.uuid4())
    bad["quotes"][0].update(id=str(uuid.uuid4()), input_raw="-1")
    async with PoolRepository(local_dsn()) as repo:
        before = await repo.status()
        with pytest.raises(CheckViolation):
            await repo.save_catalog(bad)
        assert await repo.status() == before


async def test_bound_symbols_and_stale_quotes():
    from backpack_stocks.postgres import PoolRepository

    async with PoolRepository(local_dsn()) as repo:
        before = await repo.status()
        assert await repo.list_pools("WEN.US'; DROP TABLE market_data.pools; --") == []
        pools = await repo.list_pools("WEN.US")
        assert pools
        with pytest.raises(ValueError, match="stale"):
            await repo.latest_state(pools[0]["address"], max_age_s=1)
        assert await repo.status() == before


async def test_migration_repeated_and_external_target_rejected():
    from backpack_stocks.postgres import migrate_local

    local_dsn()
    load_environment(ROOT / "state/postgres/migration.env", ["POSTGRES_MIGRATION_DSN"])
    assert await migrate_local(os.environ["POSTGRES_MIGRATION_DSN"]) == {"version": 1, "applied": False}
    with pytest.raises(ValueError, match="restricted"):
        await migrate_local("postgresql://backpack_migrator@192.0.2.1:55432/backpack_stocks")
