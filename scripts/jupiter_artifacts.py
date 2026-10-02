"""Build sanitized HAR, endpoint inventory and capture coverage from archived records."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from backpack_stocks.jupiter_replay import sanitized


def lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def run(root: Path) -> None:
    # Extend redaction to any newly identified credential field, preserving raw public JS.
    for pattern in ("*.jsonl", "responses/*.json"):
        for file in root.glob(pattern):
            if file.suffix == ".jsonl":
                file.write_text("".join(json.dumps(sanitized(x)) + "\n" for x in lines(file)))
            else:
                file.write_text(json.dumps(sanitized(json.loads(file.read_text())), indent=2))
    requests = lines(root / "requests.jsonl")
    responses = {r["id"]: r for r in lines(root / "responses.jsonl")}
    failures = {r["id"]: r for r in lines(root / "failures.jsonl")}
    groups: dict[tuple[str, str], dict] = {}
    entries = []
    for request in requests:
        url = urlsplit(request["url"])
        response = responses.get(request["id"], {})
        failed = failures.get(request["id"])
        endpoint = url.scheme + "://" + url.netloc + url.path
        group = groups.setdefault(
            (request["method"], endpoint),
            {
                "method": request["method"],
                "endpoint": endpoint,
                "count": 0,
                "queryFields": set(),
                "bodyFields": set(),
                "statusCounts": Counter(),
                "exampleRequestIds": [],
            },
        )
        group["count"] += 1
        group["queryFields"].update(k for k, _ in parse_qsl(url.query, keep_blank_values=True))
        if isinstance(request.get("body"), dict):
            group["bodyFields"].update(request["body"])
        group["statusCounts"][str(response.get("status", "failed" if failed else "pending/closed"))] += 1
        if len(group["exampleRequestIds"]) < 3:
            group["exampleRequestIds"].append(request["id"])
        headers = response.get("headers", {})
        content = {"size": response.get("bytes", -1), "mimeType": headers.get("content-type", "")}
        if response.get("file"):
            content["_bodyFile"] = response["file"]
        body = request.get("body")
        req = {
            "method": request["method"],
            "url": request["url"],
            "httpVersion": "HTTP/2",
            "headers": [{"name": k, "value": v} for k, v in request["headers"].items()],
            "cookies": [],
            "queryString": [{"name": k, "value": v} for k, v in parse_qsl(url.query, keep_blank_values=True)],
            "headersSize": -1,
            "bodySize": -1,
        }
        if body is not None:
            req["postData"] = {
                "mimeType": request["headers"].get("content-type", ""),
                "text": json.dumps(body),
            }
        entries.append(
            {
                "startedDateTime": request["time"],
                "time": 0,
                "request": req,
                "response": {
                    "status": response.get("status", 0),
                    "statusText": "",
                    "httpVersion": "HTTP/2",
                    "headers": [{"name": k, "value": v} for k, v in headers.items()],
                    "cookies": [],
                    "content": content,
                    "redirectURL": headers.get("location", ""),
                    "headersSize": -1,
                    "bodySize": response.get("bytes", -1),
                },
                "cache": {},
                "timings": {"send": 0, "wait": 0, "receive": 0},
                "_requestId": request["id"],
                "_resourceType": request["resourceType"],
                "_captureState": "response" if response else "failed" if failed else "pending/closed",
                "_failure": failed,
            }
        )
    har = {
        "log": {
            "version": "1.2",
            "creator": {"name": "Jupiter isolated Playwright recorder", "version": "1"},
            "entries": entries,
            "_note": "Sanitized metadata HAR. Response bytes in _bodyFile; exact HTTP version/timings were not recorded, placeholders used. Non-JSON bodies omitted. Raw cookies/tokens/signatures are redacted.",
        }
    }
    (root / "network.har").write_text(json.dumps(har))
    inventory = [
        {
            **g,
            "queryFields": sorted(g["queryFields"]),
            "bodyFields": sorted(g["bodyFields"]),
            "statusCounts": dict(g["statusCounts"]),
        }
        for g in groups.values()
    ]
    (root / "endpoints.json").write_text(json.dumps(inventory, indent=2))
    manifest = json.loads((root / "js-manifest.json").read_text())
    summary = {
        "browserRequests": len(requests),
        "responses": len(responses),
        "failedRequests": len(failures),
        "withoutResponseOrFailure": sum(
            r["id"] not in responses and r["id"] not in failures for r in requests
        ),
        "jsFiles": len(manifest),
        "jsBytes": sum(r["bytes"] for r in manifest),
        "websocketEvents": len(lines(root / "websockets.jsonl")),
        "endpointGroups": len(inventory),
        "redaction": "Credentials removed from records; original public JS preserved for exact source analysis.",
    }
    (root / "coverage.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data/jupiter"))
    run(parser.parse_args().root)
