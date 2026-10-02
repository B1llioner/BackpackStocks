"""Read-only gateway checks and bounded download of public Jupiter client files.

Run from the workspace with its venv. No wallets, signed requests, API-key
creation, CAPTCHA solving, or transaction submission occur here.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx

from backpack_stocks.environment import load_environment
from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.proxy import PROXY_ENV_NAMES, ProxyLogRedactor, resolve_proxy
from backpack_stocks.rejection import classify_rejection

PUBLIC_FILES = {
    "docs-index.txt": "https://developers.jup.ag/docs/llms.txt",
    "rate-limits.md": "https://developers.jup.ag/docs/portal/rate-limits.md",
    "api-keys.md": "https://developers.jup.ag/docs/portal/api-keys.md",
    "responses.md": "https://developers.jup.ag/docs/portal/responses.md",
    "setup.md": "https://developers.jup.ag/docs/portal/setup.md",
    "firewall.md": "https://developers.jup.ag/docs/portal/firewall.md",
    "swap.html": "https://jup.ag/swap/SOL-USDC",
    "portal.html": "https://developers.jup.ag/portal",
    "sign-in.html": "https://developers.jup.ag/sign-in?callbackUrl=/portal",
}


async def bounded_get(client: httpx.AsyncClient, url: str) -> tuple[httpx.Response, bytes]:
    chunks = bytearray()
    async with client.stream("GET", url) as response:
        async for chunk in response.aiter_bytes(chunk_size=65536):
            chunks.extend(chunk)
            if len(chunks) > 8_000_000:
                raise ValueError("Public research file exceeds size cap")
    return response, bytes(chunks)


def classify(response: httpx.Response, content: bytes) -> str:
    # HTTPX already decoded streamed bytes; preserve diagnostic headers without
    # attempting gzip/br decompression a second time.
    headers = {
        k: v
        for k, v in response.headers.items()
        if k.lower() not in {"content-encoding", "content-length", "transfer-encoding"}
    }
    diagnostic = classify_rejection(httpx.Response(response.status_code, headers=headers, content=content))
    if diagnostic:
        return str(diagnostic["kind"])
    return "allowed" if response.is_success else "http_error"


async def run(output: Path, *, download: bool, probe: bool = True) -> None:
    load_environment(".env", ["JUPITER_API_KEY", *PROXY_ENV_NAMES])
    proxy = resolve_proxy("JUPITER")
    redactor = ProxyLogRedactor(proxy)
    output.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []
    key = os.environ.get("JUPITER_API_KEY")
    try:
        async with httpx.AsyncClient(
            proxy=proxy, trust_env=False, timeout=20, follow_redirects=False
        ) as client:
            # Each API call is spaced. Invalid-key probe is a single explicit diagnostic.
            checks = (
                [
                    ("keyless-price", {}),
                    ("invalid-key-price", {"x-api-key": "backpack-stocks-invalid-diagnostic-key"}),
                    *(([("configured-key-price", {"x-api-key": key})]) if key else []),
                ]
                if probe
                else []
            )
            for label, headers in checks:
                url = f"https://api.jup.ag/price/v3?ids={USDC_MINT}"
                start = time.monotonic()
                response = await client.get(url, headers=headers)
                record = {
                    "step": label,
                    "method": "GET",
                    "endpoint": "/price/v3",
                    "status": response.status_code,
                    "classification": classify(response, response.content),
                    "content_type": response.headers.get("content-type"),
                    "latency_s": round(time.monotonic() - start, 3),
                    "headers": {
                        name: response.headers[name]
                        for name in (
                            "retry-after",
                            "x-ratelimit-remaining",
                            "x-ratelimit-current",
                            "x-ratelimit-reset",
                            "cf-mitigated",
                        )
                        if name in response.headers
                    },
                }
                if response.is_success:
                    data = response.json()
                    record["price_present"] = isinstance(data, dict) and USDC_MINT in data
                records.append(record)
                await asyncio.sleep(2.1)
            if probe:
                response = await client.get(
                    "https://api.jup.ag/swap/v2/order",
                    params={"inputMint": STOCK_MINT, "outputMint": USDC_MINT, "amount": "1000000"},
                    headers={"x-api-key": key} if key else {},
                )
                item = {
                    "step": "quote-only",
                    "method": "GET",
                    "endpoint": "/swap/v2/order",
                    "status": response.status_code,
                    "classification": classify(response, response.content),
                }
                if response.is_success:
                    data = response.json()
                    item.update(
                        input_matches=data.get("inputMint") == STOCK_MINT,
                        output_matches=data.get("outputMint") == USDC_MINT,
                        amount_matches=data.get("inAmount") == "1000000",
                        positive_output=int(data.get("outAmount", 0)) > 0,
                        transaction_present=bool(data.get("transaction")),
                    )
                records.append(item)
            manifest = []
            html_pages: dict[str, str] = {}
            if download:
                for name, url in PUBLIC_FILES.items():
                    response, content = await bounded_get(client, url)
                    record = {
                        "file": name,
                        "url": url,
                        "status": response.status_code,
                        "classification": classify(response, content),
                        "size": len(content),
                        "sha256": hashlib.sha256(content).hexdigest(),
                    }
                    if response.is_redirect:
                        record["redirect"] = response.headers.get("location")
                    if response.is_success:
                        (output / name).write_bytes(content)
                        if name.endswith(".html"):
                            html_pages[url] = content.decode(errors="replace")
                    manifest.append(record)
                # Public scripts only, fetched as text and never executed.
                script_urls: list[str] = []
                for page, html in html_pages.items():
                    sources = re.findall(r'<script[^>]+src=["\']([^"\']+)', html)
                    sources += [
                        src for src in re.findall(r'href=["\']([^"\']+)', html) if src.endswith(".js")
                    ]
                    for src in sources:
                        url = urljoin(page, src.replace("&amp;", "&"))
                        parsed = urlsplit(url)
                        if (
                            parsed.scheme == "https"
                            and parsed.hostname in {"jup.ag", "developers.jup.ag"}
                            and url not in script_urls
                        ):
                            script_urls.append(url)
                total = 0
                # Favor page entry chunks plus some shared runtime/vendor files.
                entries = [
                    url
                    for url in script_urls
                    if any(x in url for x in ("page-", "layout-", "webpack-", "main-app"))
                ]
                entries += [url for url in script_urls if url not in entries]
                index = 0
                visited: set[str] = set()
                while entries and index < 60:
                    if total >= 40_000_000:
                        break
                    url = entries.pop(0)
                    if url in visited:
                        continue
                    visited.add(url)
                    response, content = await bounded_get(client, url)
                    name = f"client-{index:02}-{Path(urlsplit(url).path).name}"
                    if response.is_success:
                        (output / name).write_bytes(content)
                        total += len(content)
                        source = content.decode(errors="replace")
                        imports = re.findall(r'(?:from|import\s*\()\s*["\'`]([^"\'`]+\.js)["\'`]', source)
                        imports += re.findall(
                            r'["\'`]((?:\./|assets/)[^"\'`]*(?:SwapForm|SpotSwap|client-bootstrap)[^"\'`]*\.js)["\'`]',
                            source,
                        )
                        for dependency in imports:
                            base = "https://jup.ag/" if dependency.startswith("assets/") else url
                            child = urljoin(base, dependency)
                            if (
                                urlsplit(child).hostname in {"jup.ag", "developers.jup.ag"}
                                and child not in visited
                                and child not in entries
                            ):
                                entries.append(child)
                    manifest.append(
                        {
                            "file": name,
                            "url": url,
                            "status": response.status_code,
                            "size": len(content),
                            "sha256": hashlib.sha256(content).hexdigest(),
                        }
                    )
                    index += 1
                (output / "download-manifest.json").write_text(json.dumps(manifest, indent=2))
            result = {
                "observed_at_utc": datetime.now(UTC).isoformat(),
                "proxied": proxy is not None,
                "configured_api_key": bool(key),
                "steps": records,
                "mutations": 0,
                "scripts_executed": 0,
            }
            if probe:
                (output / "gateway-probe.json").write_text(json.dumps(result, indent=2))
                print(json.dumps(result, indent=2))
            if download:
                print(f"Downloaded public files: {len(manifest)}; output: {output}")
    finally:
        redactor.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("research/jupiter-access"))
    parser.add_argument("--download", action="store_true")
    parser.add_argument(
        "--skip-probe", action="store_true", help="Download public client files without repeating API probes"
    )
    args = parser.parse_args()
    try:
        asyncio.run(run(args.output, download=args.download, probe=not args.skip_probe))
    except Exception as error:
        raise SystemExit(
            f"Read-only research stopped ({type(error).__name__}); no secret details printed"
        ) from None
