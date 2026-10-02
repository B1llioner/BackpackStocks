"""Download the pinned public Jupiter config source; no wallet, cookies or account reads."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from backpack_stocks.environment import load_environment
from backpack_stocks.jupiter_curl import CurlClient
from backpack_stocks.jupiter_proxy import JUPITER_PROXY_ENV_NAMES, replay_proxy
from backpack_stocks.jupiter_replay import captured_frontend_key

ROOT = Path(__file__).resolve().parents[1]


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--data-dir", type=Path, default=Path("data/jupiter"))
    args = parser.parse_args()
    load_environment(args.env_file, JUPITER_PROXY_ENV_NAMES)
    source = json.loads((ROOT / "src/backpack_stocks/jupiter_frontend_source.json").read_text())
    client = CurlClient(replay_proxy())
    try:
        response = await client.request("GET", source["url"], timeout=20)
        if response.status_code != 200:
            raise ValueError(f"Public frontend source HTTP {response.status_code}; no automatic retry")
        content = response.content
        if len(content) > 8_000_000 or hashlib.sha256(content).hexdigest() != source["sha256"]:
            raise ValueError("Public frontend source changed; update the reviewed source pin")
        target = args.data_dir / source["file"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        (args.data_dir / "js-manifest.json").write_text(json.dumps([source], indent=2))
        captured_frontend_key(args.data_dir)  # Verify extraction without displaying the publishable key.
        print("Jupiter frontend config ready (public source, SHA-256 verified).")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
