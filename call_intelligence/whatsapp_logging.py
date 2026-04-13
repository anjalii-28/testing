"""
Structured WhatsApp traffic logging (Meta Cloud API): file logger + Error Log on failures.

Site config (optional):
  call_intelligence_whatsapp_log_all_traffic_to_error_log: bool — also write successful sends to Error Log (noisy).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import frappe


def _site_config_get(key: str, default: Any = None) -> Any:
    """site_config may be absent on frappe.local in some contexts (e.g. early init)."""
    sc = getattr(frappe.local, "site_config", None)
    if isinstance(sc, dict):
        return sc.get(key, default)
    return default


def _conf_bool(key: str, default: bool = False) -> bool:
    v = frappe.conf.get(key) or _site_config_get(key, default)
    if isinstance(v, str):
        return v.lower() in ("1", "true", "yes")
    return bool(v)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_response_json(response_text: str) -> Any:
    """Return parsed JSON or the original string if not valid JSON."""
    raw = response_text or ""
    try:
        return json.loads(raw)
    except Exception:
        return raw


def classify_meta_cloud_response(
    *,
    status_code: int,
    response_text: str,
    kind: str,
) -> dict[str, Any]:
    """
    Best-effort hints for Desk debugging (24h session vs token vs template).
    `likely_outside_24h_window` is True only when the error body suggests session / re-engagement rules.
    """
    out: dict[str, Any] = {
        "likely_outside_24h_window": None,
        "likely_invalid_token": False,
        "likely_template_issue": False,
        "meta_error_code": None,
        "meta_error_message": None,
    }
    if status_code == 0:
        out["meta_error_message"] = "request failed before HTTP response (client/network/exception)"
        return out
    if status_code < 400:
        out["likely_outside_24h_window"] = False
        return out

    t = (response_text or "").lower()
    err_code = ""
    err_msg = ""
    try:
        d = json.loads(response_text or "{}")
        if isinstance(d, dict):
            err = d.get("error")
            if isinstance(err, dict):
                err_code = str(err.get("code") or err.get("error_subcode") or "")
                err_msg = str(err.get("message") or err.get("error_user_msg") or "")
    except Exception:
        pass

    out["meta_error_code"] = err_code or None
    out["meta_error_message"] = (err_msg or t)[:800] or None

    # Token / auth (Meta Graph)
    if err_code in ("190", "102", "463", "467") or "oauth" in t or "access token" in t or "invalid token" in t:
        out["likely_invalid_token"] = True

    session_hint = (
        "131047" in t
        or "131026" in t
        or "24 hour" in t
        or "24-hour" in t
        or "re-engagement" in t
        or ("business initiated" in t and "template" in t)
    )
    if session_hint:
        out["likely_outside_24h_window"] = True
    elif out["likely_invalid_token"]:
        out["likely_outside_24h_window"] = False
    elif kind == "text" and status_code in (400, 403):
        out["likely_outside_24h_window"] = None
    else:
        out["likely_outside_24h_window"] = False

    # Template send failures (approved template name / params / language)
    if kind == "template":
        out["likely_template_issue"] = not out["likely_invalid_token"]
    elif err_code.startswith("132"):
        out["likely_template_issue"] = True

    return out


def log_whatsapp_cloud_outbound(
    *,
    kind: str,
    to_digits: str,
    status_code: int,
    response_text: str,
    ok: bool,
    reference_doctype: str | None = None,
    reference_name: str | None = None,
    extra: dict[str, Any] | None = None,
    skip_error_log: bool = False,
) -> None:
    """Log one Cloud API POST result: phone, time, status, full JSON, session/token/template hints."""
    parsed = parse_response_json(response_text)
    hints = classify_meta_cloud_response(status_code=status_code, response_text=response_text, kind=kind)
    record: dict[str, Any] = {
        "direction": "outbound",
        "channel": "whatsapp_cloud",
        "kind": kind,
        "timestamp_utc": _now_iso(),
        "to_phone_digits": str(to_digits or ""),
        "http_status": int(status_code),
        "ok": bool(ok),
        "response_json": parsed,
        "failure_hints": hints,
        "reference_doctype": str(reference_doctype or "").strip() or None,
        "reference_name": str(reference_name or "").strip() or None,
    }
    if extra:
        record["extra"] = extra

    line = json.dumps(record, default=str, ensure_ascii=False)
    frappe.logger("call_intelligence.whatsapp_traffic").info(line)

    if skip_error_log:
        return
    log_all = _conf_bool("call_intelligence_whatsapp_log_all_traffic_to_error_log", False)
    if not ok or status_code >= 400 or log_all:
        frappe.log_error(
            title="WhatsApp Outbound (Cloud)",
            message=line,
        )


def log_whatsapp_inbound_message(
    *,
    message_id: str,
    phone_digits: str,
    text: str,
    message_type: str,
    extra: dict[str, Any] | None = None,
) -> None:
    record: dict[str, Any] = {
        "direction": "inbound",
        "channel": "whatsapp_cloud",
        "timestamp_utc": _now_iso(),
        "message_id": str(message_id or "").strip(),
        "from_phone_digits": str(phone_digits or ""),
        "message_type": str(message_type or ""),
        "text": (text or "")[:8000],
    }
    if extra:
        record["extra"] = extra
    line = json.dumps(record, default=str, ensure_ascii=False)
    frappe.logger("call_intelligence.whatsapp_traffic").info(line)

    if _conf_bool("call_intelligence_whatsapp_log_inbound_to_error_log", False):
        frappe.log_error(title="WhatsApp Inbound (Cloud)", message=line)
