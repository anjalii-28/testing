"""
Lead lifecycle hooks — outbound webhooks (configurable via Call Intelligence Settings).
"""

from __future__ import annotations

import json
from typing import Any

import frappe


def on_lead_after_insert(doc, event: str | None = None) -> None:
    """
    Fired after Lead insert. Sends structured JSON if webhook enabled; notifies desk operator on WhatsApp.
    """
    try:
        settings = _get_settings()
    except Exception:
        settings = None

    if settings and settings.enable_lead_webhook and (settings.webhook_url or "").strip():
        payload = _build_lead_webhook_payload(doc)
        secret = None
        try:
            secret = settings.get_password("webhook_secret")
        except Exception:
            secret = None
        _post_webhook(settings.webhook_url.strip(), payload, secret)

    try:
        from call_intelligence.whatsapp_integration import send_operator_new_lead_notification

        send_operator_new_lead_notification(doc.name)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "on_lead_after_insert operator WhatsApp")


def _get_settings():
    if not frappe.db.exists("DocType", "Call Intelligence Settings"):
        return None
    return frappe.get_single("Call Intelligence Settings")


def _build_lead_webhook_payload(doc) -> dict[str, Any]:
    """Minimal contract for downstream automation (extend as needed)."""
    return {
        "event": "lead_created",
        "lead": doc.name,
        "call_id": getattr(doc, "call_id", None),
        "sentiment": getattr(doc, "sentiment", None),
        "outcome": getattr(doc, "outcome", None),
        "lead_name": getattr(doc, "lead_name", None),
        "mobile_no": getattr(doc, "mobile_no", None),
    }


def _post_webhook(url: str, payload: dict[str, Any], secret: str | None) -> None:
    try:
        import requests
    except ImportError:
        frappe.log_error("requests is required for Call Intelligence webhooks", "Call Intelligence Webhook")
        return

    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-Webhook-Secret"] = secret

    try:
        resp = requests.post(url, data=json.dumps(payload), headers=headers, timeout=15)
        if not resp.ok:
            frappe.log_error(
                message=f"Webhook HTTP {resp.status_code}: {resp.text[:500]}",
                title="Call Intelligence Webhook",
            )
    except Exception as e:
        frappe.log_error(message=frappe.get_traceback(), title=f"Call Intelligence Webhook: {e!s}")
