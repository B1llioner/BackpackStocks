"""Live smoke via an authenticated loopback CONNECT proxy; no user secrets or swap execution.

Only recorded Jupiter/browser hosts and port 443 are allowed. The relay forwards TLS
bytes without inspecting them. Requires the project's venv and Node >=20/Playwright
for --browser; it never reads .env or production proxy credentials.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from solders.keypair import Keypair

from backpack_stocks.jupiter_replay import (
    SOL,
    USDC,
    JupiterReplay,
    SwapInput,
    captured_frontend_key,
    frontend_key_from_source,
)


async def run(args: argparse.Namespace) -> None:
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    hosts = {"jup.ag", "api.jup.ag", "ultra-api.jup.ag", "trigger-api.jup.ag"}
    endpoints = json.loads((args.data_dir / "endpoints.json").read_text())
    hosts.update(urlsplit(row["endpoint"]).hostname for row in endpoints)
    expected = "Basic " + base64.b64encode(b"local-fixture:local-test-password").decode()
    connects: Counter[str] = Counter()
    rejected: Counter[str] = Counter()
    handlers: set[asyncio.Task] = set()

    async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while chunk := await reader.read(65536):
            writer.write(chunk)
            await writer.drain()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream = None
        pipes = []
        task = asyncio.current_task()
        handlers.add(task)
        try:
            data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            rows = data.decode("latin1").split("\r\n")
            method, target, _ = rows[0].split(" ")
            headers = {
                name.lower(): value.strip()
                for name, value in (row.split(":", 1) for row in rows[1:] if ":" in row)
            }
            if headers.get("proxy-authorization", "") != expected:
                writer.write(
                    b'HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm="fixture"\r\nContent-Length: 0\r\n\r\n'
                )
                await writer.drain()
                return
            host, port = target.rsplit(":", 1)
            if method != "CONNECT" or port != "443" or host not in hosts:
                rejected[host] += 1
                writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return
            upstream_reader, upstream = await asyncio.wait_for(asyncio.open_connection(host, 443), 20)
            connects[host] += 1
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            pipes = [
                asyncio.create_task(relay(reader, upstream)),
                asyncio.create_task(relay(upstream_reader, writer)),
            ]
            await asyncio.wait(pipes, return_when=asyncio.FIRST_COMPLETED, timeout=120)
        except (OSError, ValueError, TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            for pipe in pipes:
                pipe.cancel()
            await asyncio.gather(*pipes, return_exceptions=True)
            for stream in (upstream, writer):
                if stream is not None:
                    stream.close()
            handlers.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, limit=16384)
    port = server.sockets[0].getsockname()[1]
    proxy = f"http://local-fixture:local-test-password@127.0.0.1:{port}"
    report = {
        "observedAt": datetime.now(UTC).isoformat(),
        "proxy": "authenticated loopback HTTP CONNECT",
        "tlsVerification": True,
        "transport": args.transport,
        "steps": [],
        "browser": None,
    }
    # Force the difficult case: native libcurl must not honour NO_PROXY and bypass the relay.
    previous_no_proxy = os.environ.get("NO_PROXY")
    if args.transport == "curl_cffi":
        os.environ["NO_PROXY"] = "*"
        report["noProxyBypassTest"] = True
    client = None
    try:
        client = JupiterReplay(proxy=proxy, transport=args.transport, audit=output / "replay.jsonl")
        manifest = json.loads((args.data_dir / "js-manifest.json").read_text())
        runtime = next(row for row in manifest if "vendor-tanstack-runtime" in row["file"])
        response = await client.http.request("GET", runtime["url"], headers={"accept": "text/javascript"})
        digest = hashlib.sha256(response.content).hexdigest()
        if response.status_code != 200 or digest != runtime["sha256"]:
            raise RuntimeError("Public frontend JS changed; source inspection required")
        client.website_key = frontend_key_from_source(response.content)
        if client.website_key != captured_frontend_key(args.data_dir):
            raise RuntimeError("HTTP frontend config differs from pinned archive")
        report["frontendSource"] = {
            "url": runtime["url"],
            "status": response.status_code,
            "sha256": digest,
            "keyMatchesArchive": True,
            "javascriptExecuted": False,
        }
        for profile in ("ultra", "manual"):
            result = await client.order(SwapInput(SOL, USDC, "0.01", 9, profile=profile))
            report["steps"].append(
                {
                    "step": "frontend-" + profile,
                    "success": True,
                    "inAmount": result["quote"]["inAmount"],
                    "backend": result["backend"],
                }
            )
        wallet = Keypair()
        await client.holdings(str(wallet.pubkey()))
        report["steps"].append({"step": "holdings", "success": True})
        await client.login(wallet)
        vault = await client.trigger_vault()
        await client.refresh()
        await client.logout()
        report["steps"].append(
            {
                "step": "challenge-sign-verify-vault-refresh-logout",
                "success": True,
                "vaultPhase": vault["phase"],
            }
        )
        if args.browser:
            env = {
                **os.environ,
                "JUPITER_REPLAY_PROXY": proxy,
                "JUPITER_DATA": str(args.data_dir),
                "JUPITER_PROOF_OUTPUT": str(output),
            }
            child = await asyncio.create_subprocess_exec(
                args.node,
                "scripts/jupiter_frontend_key_probe.cjs",
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(child.communicate(), 120)
            if child.returncode:
                raise RuntimeError(
                    "Proxied browser proof failed; no credential-bearing browser output printed"
                )
            proof = json.loads((output / "frontend-key-proof.json").read_text())
            if not all(
                row["status"] == 200
                and row["contextRequestStatus"] == 200
                and row["contextRequestMatchesArchive"]
                for row in proof["contexts"]
            ):
                raise RuntimeError("Browser/page-request proxy check did not pass")
            report["browser"] = {
                "success": True,
                "contexts": len(proof["contexts"]),
                "contextRequestDownload": True,
            }
        if not {"jup.ag", "api.jup.ag", "ultra-api.jup.ag", "trigger-api.jup.ag"} <= connects.keys():
            raise RuntimeError("A required Jupiter host did not pass through the proxy")
        report["success"] = True
    finally:
        try:
            if client and client.session:
                await client.logout()
        finally:
            if client:
                await client.close()
            if args.transport == "curl_cffi":
                if previous_no_proxy is None:
                    os.environ.pop("NO_PROXY", None)
                else:
                    os.environ["NO_PROXY"] = previous_no_proxy
            server.close()
            await server.wait_closed()
            for task in list(handlers):
                task.cancel()
            await asyncio.gather(*list(handlers), return_exceptions=True)
            report["connectsByHost"] = dict(connects)
            report["rejectedHosts"] = dict(rejected)
            (output / "summary.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data/jupiter"))
    parser.add_argument("--output", type=Path, default=Path("data/jupiter/proxy-smoke"))
    parser.add_argument("--transport", choices=("httpx", "curl_cffi"), default="httpx")
    parser.add_argument("--browser", action="store_true")
    parser.add_argument("--node", default="node")
    try:
        asyncio.run(run(parser.parse_args()))
    except Exception as error:
        raise SystemExit(f"Proxy smoke failed ({type(error).__name__}); see sanitized summary") from None
