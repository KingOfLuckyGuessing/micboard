"""Centralised debug logging for Planning Center (PCO) API calls.

Why this exists
---------------
PCO calls used to be scattered across three places, and only
``planning_center._make_pco_request`` logged anything -- and even that
only logged a summary ("200 - 3 items returned"), never the body.
The scheduler (``pco_scheduler``) and several Tornado handlers called
``session.get`` directly, so those responses were invisible in the log.

Every live PCO call now goes through one of these helpers, so turning
debug on once shows every request and response, in the same format,
in ``micboard.log`` (and on stdout -- see ``config.logging_init``).

How to turn it on
-----------------
Set one of these -- no code change needed:

    PCO_DEBUG=1     # request + response summary + 2,000-char body preview
    PCO_DEBUG=full  # request + response summary + full body (up to 50,000 chars)

or, equivalently, in ``config.json``:

    "integrations": {"planning_center": {"debug": true}}       # preview mode
    "integrations": {"planning_center": {"debug": "full"}}    # full-body mode

Any other value (``0``, ``false``, ``off``, unset) disables it. Even
with debug off, non-2xx responses other than 404 still log a one-line
warning with a short body preview, because a PCO error body is usually
what explains a failure -- and 404 is normal here (plans with no
assignments yet), so it stays quiet.

What is NOT logged
-------------------
Authorization headers and any JSON field whose name looks like a
token/secret/password are replaced with ``[REDACTED]`` before logging.
Response bodies *do* contain the names PCO returns (that is the point
of this logger), so treat ``micboard.log`` as containing personal
data and do not post it publicly.
"""

import json
import logging
import os
import time
from typing import Any, Optional

_PREVIEW_LIMIT = 2000
_FULL_LIMIT = 50000
_ERROR_PREVIEW_LIMIT = 1000

_SENSITIVE_KEYS = ("token", "secret", "password", "client_secret", "access_token")


def _config_debug_value() -> Any:
    """Read the config.json debug flag lazily (config may not be loaded yet)."""
    try:
        import config  # local import: config imports tornado_server, which imports planning_center

        tree = getattr(config, "config_tree", None)
        if isinstance(tree, dict):
            return tree.get("integrations", {}).get("planning_center", {}).get("debug")
    except Exception:
        pass
    return None


def debug_level() -> str:
    """Return 'off', 'summary' (preview) or 'full'."""
    raw = os.environ.get("PCO_DEBUG")
    if raw is None:
        raw = _config_debug_value()
    if raw is True:
        return "summary"
    if raw is False or raw is None:
        return "off"
    value = str(raw).strip().lower()
    if value in ("1", "true", "yes", "on", "summary", "preview"):
        return "summary"
    if value in ("full", "all", "body"):
        return "full"
    return "off"


def is_pco_debug_enabled() -> bool:
    return debug_level() != "off"


def _redact_headers(headers) -> dict:
    if not headers:
        return {}
    return {
        k: ("[REDACTED]" if "auth" in k.lower() or "token" in k.lower() else v)
        for k, v in dict(headers).items()
    }


def _redact_json(obj: Any, _depth: int = 0) -> Any:
    if _depth > 10:
        return obj
    if isinstance(obj, dict):
        return {
            k: ("[REDACTED]" if any(s in k.lower() for s in _SENSITIVE_KEYS) else _redact_json(v, _depth + 1))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact_json(item, _depth + 1) for item in obj]
    return obj


def _summarise_json(data: Any) -> str:
    """One-line shape summary, e.g. 'data: 3 items, meta: {...}, errors: [...]'."""
    if not isinstance(data, dict):
        return f"body is {type(data).__name__}, not a JSON object"
    parts = []
    if "data" in data:
        value = data["data"]
        if isinstance(value, list):
            parts.append(f"data: {len(value)} items")
            # Show just ids/titles of the first few items, not whole records.
            heads = []
            for item in value[:3]:
                if isinstance(item, dict):
                    attrs = item.get("attributes") or {}
                    heads.append(str(attrs.get("title") or attrs.get("name") or attrs.get("dates") or item.get("id")))
            if heads:
                parts.append(f"first items: {heads}")
        else:
            parts.append("data: single object")
    if "meta" in data:
        parts.append(f"meta: {data['meta']}")
    if "errors" in data:
        parts.append(f"errors: {data['errors']}")
    if "included" in data and isinstance(data["included"], list):
        parts.append(f"included: {len(data['included'])} items")
    return ", ".join(parts) if parts else f"keys: {sorted(data.keys())}"


def _body_text(response, limit: int) -> tuple[str, bool]:
    """Return (possibly truncated) body text and whether it was truncated."""
    try:
        text = response.text
    except Exception:
        return "", False
    if text is None:
        return "", False
    if not isinstance(text, str):
        text = str(text)
    # Redact secrets in JSON bodies; leave non-JSON (e.g. error HTML) as-is.
    try:
        parsed = json.loads(text)
        text = json.dumps(_redact_json(parsed), indent=2)
    except Exception:
        pass
    if len(text) > limit:
        return text[:limit] + f"\n... [truncated: showing {limit} of {len(text)} chars]", True
    return text, False


def log_pco_request(method: str, url: str, params=None, headers=None, caller: Optional[str] = None) -> None:
    if not is_pco_debug_enabled():
        return
    prefix = f"PCO DEBUG [{caller}] " if caller else "PCO DEBUG "
    logging.info(
        "%sREQUEST %s %s params=%s headers=%s",
        prefix,
        method,
        url,
        json.dumps(params, default=str) if params else None,
        _redact_headers(headers),
    )


def log_pco_response(response, caller: Optional[str] = None, elapsed_ms: Optional[float] = None,
                     error: Optional[str] = None) -> None:
    """Log a PCO response (or a transport error).

    Always warns, with a short preview, on non-2xx errors other than
    404. Full/summary logging happens only when PCO_DEBUG is on.
    """
    prefix = f"PCO DEBUG [{caller}] " if caller else "PCO DEBUG "
    if error is not None:
        logging.error("%sREQUEST FAILED: %s", prefix, error)
        return

    status = getattr(response, "status_code", "?")
    elapsed = f" in {elapsed_ms:.0f}ms" if elapsed_ms is not None else ""
    url = getattr(response, "url", "") or ""

    level = debug_level()
    if level == "off":
        # Still surface real errors: the body is usually the explanation.
        if isinstance(status, int) and status >= 400 and status != 404:
            body, _ = _body_text(response, _ERROR_PREVIEW_LIMIT)
            logging.warning("%sRESPONSE %s %s%s body=%s", prefix, status, url, elapsed, body)
        return

    # Summary line, with a JSON-shape summary when the body parses.
    # Deliberately parse response.text, not response.json(): calling
    # .json() here would be harmless for real responses but would
    # consume a mocked .json() side_effect in tests.
    summary = ""
    try:
        text = getattr(response, "text", None)
        parsed = json.loads(text) if isinstance(text, str) else None
        if parsed is not None:
            summary = " | " + _summarise_json(parsed)
    except Exception:
        pass
    logging.info("%sRESPONSE %s %s%s%s", prefix, status, url, elapsed, summary)

    limit = _FULL_LIMIT if level == "full" else _PREVIEW_LIMIT
    body, _truncated = _body_text(response, limit)
    if body:
        logging.info("%sRESPONSE BODY%s:\n%s", prefix, " (full)" if level == "full" else " (preview)", body)


def pco_get(session, url: str, params=None, timeout: int = 10, caller: Optional[str] = None):
    """Drop-in replacement for ``session.get`` for PCO calls.

    Logs the request and response via the helpers above, times the
    call, and returns the response unchanged -- it never swallows
    exceptions or changes status handling, so existing call sites keep
    their own 404 / raise_for_status / retry behaviour.
    """
    log_pco_request("GET", url, params, getattr(session, "headers", None), caller=caller)
    started = time.monotonic()
    try:
        response = session.get(url, params=params, timeout=timeout)
    except Exception as exc:
        log_pco_response(None, caller=caller, elapsed_ms=(time.monotonic() - started) * 1000, error=str(exc))
        raise
    log_pco_response(response, caller=caller, elapsed_ms=(time.monotonic() - started) * 1000)
    return response
