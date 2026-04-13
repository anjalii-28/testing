"""
Notify hf-agents-lead-qualification-agent after WhatsApp-driven Lead status updates.

Configure on the Frappe site (``site_config.json`` / ``common_site_config.json``), e.g.::

    bench --site mysite.local set-config call_intelligence_lead_agent_status_url \\
      "https://<agent-host>/whatsapp-lead/webhooks/frappe/lead-status"

No shared secret — guest-style POST (use only behind Cloudflare / private network in production).

If URL is missing, no HTTP call is made.
"""

from __future__ import annotations

from typing import Any

import frappe


def get_lead_agent_status_webhook_url() -> str:
    return str(frappe.conf.get("call_intelligence_lead_agent_status_url") or "").strip()


def lead_status_snapshot(lead_name: str) -> str:
    """Best-effort current status string before an update (workflow_state, status, lead_status)."""
    row = frappe.db.get_value(
        "Lead",
        lead_name,
        ["status", "workflow_state", "lead_status"],
        as_dict=True,
    )
    if not row:
        return ""
    return str(
        row.get("workflow_state")
        or row.get("status")
        or row.get("lead_status")
        or ""
    ).strip()


def notify_lead_qualification_agent_after_status_change(
    lead_name: str,
    final_status: str,
    *,
    previous_status: str | None = None,
    phone: str | None = None,
    user_message: str | None = None,
) -> None:
    """
    POST JSON to the agent's ``/whatsapp-lead/webhooks/frappe/lead-status`` when URL is set.

    Failures are logged; they do not raise to the WhatsApp webhook caller.
    """
    url = get_lead_agent_status_webhook_url()
    if not url:
        return

    final = str(final_status or "").strip()
    if not final:
        return

    payload: dict[str, Any] = {
        "lead_id": lead_name,
        "status": final,
    }
    prev = str(previous_status or "").strip()
    if prev:
        payload["previous_status"] = prev
    ph = str(phone or "").strip()
    if ph:
        payload["phone"] = ph
    um = str(user_message or "").strip()
    if um:
        payload["user_message"] = um

    try:
        import requests

        resp = requests.post(
            url.rstrip("/"),
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code >= 400:
            frappe.log_error(
                title="lead_agent_status_notify_http",
                message=f"status={resp.status_code} url={url!r} body={resp.text[:2000]!r}",
            )
    except Exception:
        frappe.log_error(
            title="lead_agent_status_notify_failed",
            message=frappe.get_traceback(),
        )
