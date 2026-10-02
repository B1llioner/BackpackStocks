from __future__ import annotations

import httpx


def api_error_metadata(payload: object) -> dict:
    """Convert remote errors to fixed labels; never expose their free-form text."""
    if not isinstance(payload, dict):
        return {}
    code = payload.get("errorCode", payload.get("code"))
    error = payload.get("error")
    if isinstance(error, dict):
        code = error.get("code", code)
        message = error.get("message", "")
    else:
        message = payload.get("message", error)
    known = {
        "INSUFFICIENT_BALANCE": "insufficient_balance",
        "INSUFFICIENT_FUNDS": "insufficient_balance",
        "INSUFFICIENT_COLLATERAL": "insufficient_collateral",
        "INVALID_QUANTITY": "invalid_quantity",
        "INVALID_SYMBOL": "invalid_symbol",
        "MARKET_CLOSED": "market_closed",
        "COULD_NOT_FIND_ANY_ROUTE": "no_route",
        "NO_ROUTES_FOUND": "no_route",
        "TOKEN_NOT_TRADABLE": "token_not_tradable",
    }
    result = {}
    if type(code) is int and 0 <= code <= 999999:
        result["api_code"] = str(code)
    elif isinstance(code, str) and code in known:
        result.update(api_code=code, reason=known[code])
    if isinstance(message, str) and len(message) <= 8192:
        normalized = message.lower()
        if message in known:
            result["reason"] = known[message]
        elif any(text in normalized for text in ("insufficient balance", "insufficient funds")):
            result["reason"] = "insufficient_balance"
        elif "insufficient collateral" in normalized:
            result["reason"] = "insufficient_collateral"
        elif "rfq" in normalized and "expired" in normalized:
            result["reason"] = "rfq_expired"
        elif any(text in normalized for text in ("no route found", "could not find any route")):
            result["reason"] = "no_route"
    return result


def _known_message(response: httpx.Response) -> str:
    if len(response.content) > 8192:
        return ""
    try:
        payload = response.json()
    except ValueError:
        return ""
    message = payload.get("message") if isinstance(payload, dict) else None
    return message if isinstance(message, str) else ""


def classify_rejection(response: httpx.Response) -> dict[str, str | int] | None:
    """Only fixed diagnostic labels escape; never response bodies, headers or tokens.

    An edge checkpoint can return 429 without exhausting an API quota. Explicit
    protection headers take precedence over status-code interpretation.
    """
    status = response.status_code
    vercel = response.headers.get("x-vercel-mitigated", "").strip().lower()
    cloudflare = response.headers.get("cf-mitigated", "").strip().lower()
    result: dict[str, str | int] = {"status": status}
    if cloudflare == "challenge":
        return {**result, "kind": "interactive_check", "provider": "cloudflare"}
    if vercel == "challenge":
        return {**result, "kind": "interactive_check", "provider": "vercel"}
    if status >= 400 and vercel == "deny":
        return {**result, "kind": "firewall_deny", "provider": "vercel"}
    if status >= 400 and vercel == "rate_limit":
        return {**result, "kind": "rate_limit", "provider": "vercel", "reason": "firewall_rule"}
    html = "text/html" in response.headers.get("content-type", "").lower()
    if html:
        body = response.content.lower()
        if any(marker in body for marker in (b"cf-chl-", b"challenge-platform")):
            return {**result, "kind": "interactive_check", "provider": "cloudflare"}
        if b"vercel security checkpoint" in body:
            return {**result, "kind": "interactive_check", "provider": "vercel"}
        if b"captcha" in body:
            return {**result, "kind": "interactive_check", "provider": "unknown"}
    if status not in {401, 403, 429}:
        return None
    message = _known_message(response)
    if status == 403 and message.startswith("[TURNSTILE]"):
        return {**result, "kind": "interactive_check", "provider": "turnstile"}
    if status == 401:
        return {**result, "kind": "authentication", "provider": "api"}
    if status == 403:
        reason = {
            "API key does not have access to this endpoint": "missing_permission",
            "Request blocked by firewall": "firewall_rule",
        }.get(message, "unspecified")
        return {**result, "kind": "permissions_or_firewall", "provider": "api", "reason": reason}
    reason = {
        "[API Gateway] Too many requests": "plan_quota",
        "Too many requests": "firewall_rule",
    }.get(message, "unspecified")
    return {**result, "kind": "rate_limit", "provider": "api", "reason": reason}
