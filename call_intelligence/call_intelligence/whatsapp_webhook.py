"""
WhatsApp Cloud API webhook — inbound text (1/2/3, yes/no, …) + interactive button_reply → Lead.

Endpoint:
  POST https://<site>/api/method/call_intelligence.whatsapp_webhook.whatsapp_webhook

GET: Meta hub.challenge verification when call_intelligence_whatsapp_cloud_verify_token is set.

Note: Primary Meta URL often points to call_intelligence.api.whatsapp_cloud_webhook → whatsapp_inbound.
This endpoint mirrors the same mapping for alternate routing.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import frappe
from werkzeug.wrappers import Response

from call_intelligence.lead_qualification_agent_client import (
    lead_status_snapshot,
    notify_lead_qualification_agent_after_status_change,
)
from call_intelligence.whatsapp_inbound import normalize_whatsapp_phone
from call_intelligence.whatsapp_integration import (
    _digits,
    _store_communication,
    apply_lead_whatsapp_followup_action,
    canonical_whatsapp_action,
    get_whatsapp_cloud_verify_token,
    normalize_whatsapp_reply_to_action,
    send_whatsapp_reply_confirmation,
    webhook_privileged_session,
)

logger = logging.getLogger("call_intelligence.whatsapp_webhook")


def _root_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("entry"), list):
        return payload
    inner = payload.get("data")
    if isinstance(inner, dict) and isinstance(inner.get("entry"), list):
        return inner
    return payload


def _find_lead_name_by_whatsapp_phone(phone_raw: str) -> str | None:
    """Match Lead by whatsapp_no (digits), then fall back to existing phone fields."""
    from call_intelligence.whatsapp_integration import _find_lead_by_phone

    digits = normalize_whatsapp_phone(phone_raw)
    if not digits or len(digits) < 7:
        return None

    suffix = digits[-10:] if len(digits) >= 10 else digits
    meta = frappe.get_meta("Lead")

    if meta.has_field("whatsapp_no"):
        try:
            rows = frappe.db.sql(
                """
				SELECT name FROM `tabLead`
				WHERE RIGHT(REGEXP_REPLACE(IFNULL(`whatsapp_no`, ''), '[^0-9]', ''), 10) = %s
				LIMIT 1
				""",
                (suffix,),
                as_dict=True,
            )
            if rows:
                return rows[0]["name"]
        except Exception:
            frappe.log_error(title="whatsapp_webhook lead lookup whatsapp_no")

    return _find_lead_by_phone(digits)


def _is_probably_outgoing_message(msg: dict[str, Any], value: dict[str, Any]) -> bool:
    """Skip if sender matches business display number (echo / system)."""
    meta = value.get("metadata") or {}
    if not isinstance(meta, dict):
        return False
    biz = _digits(str(meta.get("display_phone_number") or ""))
    sender = _digits(str(msg.get("from") or ""))
    return bool(biz and sender and biz == sender)


def _iter_interactive_button_messages(payload: dict[str, Any]):
    """Yield (phone, button_id) for inbound interactive button_reply messages."""
    root = _root_payload(payload)
    entries = root.get("entry") or []
    if not isinstance(entries, list):
        return

    for ent in entries:
        if not isinstance(ent, dict):
            continue
        for ch in ent.get("changes") or []:
            if not isinstance(ch, dict):
                continue
            val = ch.get("value") or {}
            if not isinstance(val, dict):
                continue
            if val.get("statuses") and not val.get("messages"):
                continue

            for msg in val.get("messages") or []:
                if not isinstance(msg, dict):
                    continue
                if str(msg.get("type") or "").lower() != "interactive":
                    continue

                if _is_probably_outgoing_message(msg, val):
                    logger.info("whatsapp_webhook skip outgoing-like message from=%s", msg.get("from"))
                    continue

                inter = msg.get("interactive") or {}
                br = inter.get("button_reply") or {}
                button_id = str(br.get("id") or "").strip()
                if not button_id:
                    continue

                phone = str(msg.get("from") or "").strip()
                if not phone:
                    continue

                yield phone, button_id


def _iter_text_messages(payload: dict[str, Any]):
    """Inbound type=text — body may be 1, 2, 3, yes, confirm, etc."""
    root = _root_payload(payload)
    entries = root.get("entry") or []
    if not isinstance(entries, list):
        return

    for ent in entries:
        if not isinstance(ent, dict):
            continue
        for ch in ent.get("changes") or []:
            if not isinstance(ch, dict):
                continue
            val = ch.get("value") or {}
            if not isinstance(val, dict):
                continue
            if val.get("statuses") and not val.get("messages"):
                continue

            for msg in val.get("messages") or []:
                if not isinstance(msg, dict):
                    continue
                if str(msg.get("type") or "").lower() != "text":
                    continue
                if _is_probably_outgoing_message(msg, val):
                    continue
                body = str((msg.get("text") or {}).get("body") or "").strip()
                if not body:
                    continue
                if not normalize_whatsapp_reply_to_action(body):
                    continue
                phone = str(msg.get("from") or "").strip()
                if not phone:
                    continue
                yield phone, body


def _process_one_reply(
    phone: str,
    action: str,
    stored_content: str,
    msg_type: str,
) -> bool:
    lead_name = _find_lead_name_by_whatsapp_phone(phone)
    if not lead_name:
        logger.warning("whatsapp_webhook no Lead for phone=%s", phone)
        return False
    try:
        _store_communication(
            "Received",
            stored_content,
            "Lead",
            lead_name,
            normalize_whatsapp_phone(phone),
            msg_type=msg_type,
            provider="whatsapp_cloud",
        )
        prev_status = lead_status_snapshot(lead_name)
        final = apply_lead_whatsapp_followup_action(lead_name, action)
        if final:
            notify_lead_qualification_agent_after_status_change(
                lead_name,
                final,
                previous_status=prev_status or None,
                phone=normalize_whatsapp_phone(phone),
                user_message=stored_content,
            )
        try:
            frappe.log_error("SENDING WHATSAPP REPLY", "WA DEBUG")
            send_whatsapp_reply_confirmation(
                normalize_whatsapp_phone(phone),
                lead_name,
                action,
            )
        except Exception:
            frappe.log_error(
                title="whatsapp_webhook confirmation send",
                message=frappe.get_traceback(),
            )
        logger.info(
            "whatsapp_webhook lead=%s action=%s final=%s",
            lead_name,
            action,
            final,
        )
        return True
    except Exception:
        frappe.log_error(title="whatsapp_webhook lead update", message=frappe.get_traceback())
        return False


def _whatsapp_webhook_post() -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        payload = frappe.request.get_json(silent=True) or {}
        if not payload and frappe.request.form:
            payload = dict(frappe.request.form)
    except Exception:
        payload = {}

    raw_preview = json.dumps(payload, default=str, indent=2) if payload else "{}"
    if len(raw_preview) > 12000:
        raw_preview = raw_preview[:12000] + "...(truncated)"
    logger.info("whatsapp_webhook payload:\n%s", raw_preview)

    if not isinstance(payload, dict) or not payload:
        return {"status": "success", "processed": 0}

    with webhook_privileged_session():
        processed = 0

        for phone, button_id in _iter_interactive_button_messages(payload):
            action = canonical_whatsapp_action(button_id)
            if not action:
                logger.info("whatsapp_webhook skip unmapped button_id=%s", button_id)
                continue
            if _process_one_reply(
                phone,
                action,
                f"Button reply: {button_id}",
                "interactive",
            ):
                processed += 1

        for phone, body in _iter_text_messages(payload):
            action = normalize_whatsapp_reply_to_action(body)
            if not action:
                continue
            if _process_one_reply(phone, action, body, "text"):
                processed += 1

        return {"status": "success", "processed": processed}


@frappe.whitelist(allow_guest=True)
def whatsapp_webhook():
    """
    WhatsApp Cloud webhook: text + interactive → Lead workflow / reschedule.

    Full method path: call_intelligence.whatsapp_webhook.whatsapp_webhook
    """
    try:
        if frappe.request.method == "GET":
            q = frappe.form_dict
            expected = get_whatsapp_cloud_verify_token()
            mode = str(q.get("hub.mode") or "")
            provided = str(
                q.get("hub.verify_token")
                or q.get("hub.verifyToken")
                or q.get("hub.verify-token")
                or ""
            ).strip()
            challenge = q.get("hub.challenge")

            # Browser/manual hit (no hub.* params): return a helpful OK response.
            if not mode and not provided and not challenge:
                return {
                    "ok": True,
                    "hint": "Meta verification requires GET params hub.mode, hub.verify_token, hub.challenge; "
                    "POST is used for inbound messages.",
                    "verify_token_configured": bool(expected),
                }

            if expected and mode == "subscribe" and provided == expected and challenge:
                return Response(str(challenge), mimetype="text/plain", status=200)
            return {"ok": False, "reason": "verification_failed"}

        if frappe.request.method != "POST":
            return {"status": "success", "processed": 0}

        return _whatsapp_webhook_post()
    except Exception:
        frappe.log_error(title="whatsapp_webhook", message=frappe.get_traceback())
        return {"status": "success", "processed": 0}
