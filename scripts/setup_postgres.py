"""Generate project-local PostgreSQL credentials without displaying them.

Run once before docker compose; existing files are never overwritten.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

from dotenv import set_key


def main() -> None:
    os.umask(0o077)
    env = Path(".env.postgres")
    if env.exists():
        raise SystemExit("PostgreSQL configuration already exists; preserved")
    state = Path("state/postgres")
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    admin, app = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    with env.open("x") as f:
        f.write(
            f"POSTGRES_USER=backpack_migrator\nPOSTGRES_DB=backpack_stocks\nPOSTGRES_PASSWORD={admin}\nBACKPACK_POSTGRES_APP_PASSWORD={app}\n"
        )
    admin_env = state / "migration.env"
    with admin_env.open("x") as f:
        f.write(
            f"POSTGRES_MIGRATION_DSN=postgresql://backpack_migrator:{admin}@127.0.0.1:55432/backpack_stocks\n"
        )
    local = Path(".env")
    local.touch(mode=0o600, exist_ok=True)
    set_key(local, "POSTGRES_DSN", f"postgresql://backpack_catalog:{app}@127.0.0.1:55432/backpack_stocks")
    local.chmod(0o600)
    print("Local PostgreSQL credentials saved privately; application role has no DDL privileges")


if __name__ == "__main__":
    main()
