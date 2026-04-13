"""
WhatsApp Cloud inbound webhook handler.

Parses Meta webhook payloads (entry->changes->value->messages),
finds matching Lead by phone, stores inbound Communications,
and applies reply interpretation (buttons + text) to update Lead status.
"""

from __future__ import annotations

import json
import re
from typing import Any

import frappe

from call_intelligence.lead_qualification_agent_client import (
    lead_status_snapshot,
    notify_lead_qualification_agent_after_status_change,
)
from call_intelligence.whatsapp_integration import (
    _find_lead_by_phone,
    _handle_keyword_reply,
    _store_communication,
    apply_normalized_reply_to_lead,
    is_operator_inbound_phone,
    normalize_user_reply,
    resolve_lead_for_operator_inbound,
    send_whatsapp_reply_confirmation,
    webhook_privileged_session,
)
from call_intelligence.whatsapp_logging import (
    log_whatsapp_inbound_message,
    log_whatsapp_reply_processing,
)


def normalize_whatsapp_phone(phone: str | None) -> str:
    """Strip all non-digits from a phone number string."""
    if not phone:
        return ""
    return re.sub(r"\D", "", str(phone))


def _extract_display_text_and_type(msg: dict[str, Any]) -> tuple[str, str]:
    """Human-readable content for Communication + Meta message type."""
    msg_type = str(msg.get("type") or "unknown").lower()
    text = ""

    if msg_type == "text":
        text = str((msg.get("text") or {}).get("body") or "").strip()
    elif msg_type in ("image", "video", "document", "audio"):
        media = msg.get(msg_type) or {}
        text = str(media.get("caption") or "[{}]".format(msg_type)).strip()
    elif msg_type == "button":
        b = msg.get("button") or {}
        text = str(b.get("payload") or b.get("text") or "").strip()
    elif msg_type == "interactive":
        inter = msg.get("interactive") or {}
        btn_reply = inter.get("button_reply") or inter.get("list_reply") or {}
        text = str(btn_reply.get("title") or btn_reply.get("id") or "").strip()
    else:
        text = str(msg.get("text") or msg_type)

    return text, msg_type


def _message_should_be_processed(msg: dict[str, Any]) -> bool:
    """Skip unsupported payloads (no text body and no interactive reply id)."""
    if not isinstance(msg, dict):
        return False
    msg_type = str(msg.get("type") or "").lower()
    if msg_type == "text":
        return bool(str((msg.get("text") or {}).get("body") or "").strip())
    if msg_type == "interactive":
        inter = msg.get("interactive") or {}
        br = inter.get("button_reply") if isinstance(inter.get("button_reply"), dict) else {}
        lr = inter.get("list_reply") if isinstance(inter.get("list_reply"), dict) else {}
        return bool(str(br.get("id") or "").strip() or str(lr.get("id") or "").strip())
    if msg_type == "button":
        b = msg.get("button") or {}
        return bool(str(b.get("payload") or b.get("text") or "").strip())
    # Optional: allow media with caption
    if msg_type in ("image", "video", "document", "audio"):
        media = msg.get(msg_type) or {}
        return bool(str(media.get("caption") or "").strip())
    return False


def process_inbound_whatsapp_cloud_webhook(payload: dict[str, Any]) -> int:
    """
    Process a Meta WhatsApp Cloud webhook POST payload.
    Returns count of messages handled (skipped/ignored inbound items are not counted).
    """
    if not isinstance(payload, dict):
        return 0

    with webhook_privileged_session():
        processed = 0
        entries = payload.get("entry") or []
        for entry in entries:
            for change in (entry.get("changes") or []):
                value = change.get("value") or {}
                if change.get("field") != "messages" and value.get("messaging_product") != "whatsapp":
                    if not value.get("messages"):
                        continue

                messages = value.get("messages") or []
                contacts = {
                    c.get("wa_id"): (c.get("profile") or {}).get("name", "")
                    for c in (value.get("contacts") or [])
                }

                for msg in messages:
                    try:
                        if _process_single_message(msg, contacts, value):
                            processed += 1
                    except Exception:
                        frappe.log_error(title="WA inbound message", message=frappe.get_traceback())
        return processed


def _process_single_message(msg: dict, contacts: dict, value: dict) -> bool:
    """
    Handle one Meta message dict. Returns True if the message was accepted (stored or logged path).
    """
    msg_id = str(msg.get("id") or "").strip()
    from_num = str(msg.get("from") or "").strip()
    timestamp = str(msg.get("timestamp") or "")

    phone_digits = normalize_whatsapp_phone(from_num)
    contact_name = contacts.get(from_num, "")

    # Desk operator: route replies to the Lead by Ref: line or last-notified cache (not patient workflow).
    if is_operator_inbound_phone(phone_digits):
        text, msg_type = _extract_display_text_and_type(msg)
        media_url = ""
        mtype = str(msg.get("type") or "").lower()
        if mtype in ("image", "video", "document", "audio"):
            media = msg.get(mtype) or {}
            media_url = str(media.get("url") or media.get("link") or "")
        if not str(text).strip():
            frappe.logger("call_intelligence.whatsapp").info(
                "operator_inbound_skip_empty id=%s", msg_id
            )
            return False
        raw_for_log = json.dumps(msg, default=str)[:12000]
        log_whatsapp_inbound_message(
            message_id=msg_id,
            phone_digits=phone_digits,
            text=text,
            message_type=msg_type,
            extra={"contact_name": contact_name, "timestamp": timestamp, "operator": True},
        )
        lead_name = resolve_lead_for_operator_inbound(text, phone_digits)
        if not lead_name:
            log_whatsapp_reply_processing(
                phone_digits=phone_digits,
                raw_message_preview=raw_for_log[:2000],
                normalized_action=None,
                lead_name=None,
                final_status=None,
                note="operator_no_lead_ref",
            )
            return True
        _store_communication(
            direction="Received",
            content=text,
            reference_doctype="Lead",
            reference_name=lead_name,
            phone=phone_digits,
            msg_type=msg_type,
            media_url=media_url,
            provider="whatsapp_cloud",
        )
        log_whatsapp_reply_processing(
            phone_digits=phone_digits,
            raw_message_preview=raw_for_log[:2000],
            normalized_action=None,
            lead_name=lead_name,
            final_status=None,
            note="operator_reply",
        )
        return True

    if not _message_should_be_processed(msg):
        frappe.logger("call_intelligence.whatsapp").info(
            "inbound_skip_unsupported type=%s id=%s", msg.get("type"), msg_id
        )
        return False

    text, msg_type = _extract_display_text_and_type(msg)
    media_url = ""
    mtype = str(msg.get("type") or "").lower()
    if mtype in ("image", "video", "document", "audio"):
        media = msg.get(mtype) or {}
        media_url = str(media.get("url") or media.get("link") or "")

    raw_for_log = json.dumps(msg, default=str)[:12000]

    log_whatsapp_inbound_message(
        message_id=msg_id,
        phone_digits=phone_digits,
        text=text,
        message_type=msg_type,
        extra={"contact_name": contact_name, "timestamp": timestamp},
    )

    normalized_action = normalize_user_reply(msg)

    if not phone_digits:
        log_whatsapp_reply_processing(
            phone_digits="",
            raw_message_preview=raw_for_log[:2000],
            normalized_action=normalized_action,
            lead_name=None,
            final_status=None,
            note="missing_phone_digits",
        )
        return True

    lead_name = _find_lead_by_phone(phone_digits)

    if not lead_name:
        lead_name = _maybe_auto_create_lead(phone_digits, contact_name)
        if not lead_name:
            frappe.logger("call_intelligence.whatsapp").warning(
                "inbound_no_lead_match phone=%s msg=%s", phone_digits, text[:80]
            )
            log_whatsapp_reply_processing(
                phone_digits=phone_digits,
                raw_message_preview=raw_for_log[:2000],
                normalized_action=normalized_action,
                lead_name=None,
                final_status=None,
                note="no_lead_match",
            )
            return True

    _store_communication(
        direction="Received",
        content=text,
        reference_doctype="Lead",
        reference_name=lead_name,
        phone=phone_digits,
        msg_type=msg_type,
        media_url=media_url,
        provider="whatsapp_cloud",
    )

    prev_status = lead_status_snapshot(lead_name)
    final_status = ""
    if normalized_action:
        final_status = apply_normalized_reply_to_lead(lead_name, normalized_action)
        if final_status:
            notify_lead_qualification_agent_after_status_change(
                lead_name,
                final_status,
                previous_status=prev_status or None,
                phone=phone_digits,
                user_message=text,
            )
        try:
            frappe.log_error("SENDING WHATSAPP REPLY", "WA DEBUG")
            send_whatsapp_reply_confirmation(phone_digits, lead_name, normalized_action)
        except Exception:
            frappe.log_error(
                title="WA inbound confirmation send",
                message=frappe.get_traceback(),
            )
    else:
        _handle_keyword_reply(lead_name, text)

    log_whatsapp_reply_processing(
        phone_digits=phone_digits,
        raw_message_preview=raw_for_log[:2000],
        normalized_action=normalized_action,
        lead_name=lead_name,
        final_status=final_status or None,
        note=None,
    )

    return True


def _maybe_auto_create_lead(phone_digits: str, contact_name: str) -> str | None:
    try:
        settings = frappe.get_single("Call Intelligence Settings")
        if not getattr(settings, "create_lead_when_no_match_for_issue", 0):
            return None
        lead = frappe.new_doc("Lead")
        display = contact_name or "Unknown"
        parts = display.split()
        lead.first_name = parts[0] if parts else "Unknown"
        if len(parts) > 1:
            lead.last_name = " ".join(parts[1:])
        if hasattr(lead, "mobile_no"):
            lead.mobile_no = phone_digits
        try:
            lead.set("phone_number", phone_digits)
        except Exception:
            pass
        # Patient 360 "Leads" list filters by ci_record_type in ("lead", "leads") when the field exists.
        # Ensure webhook-created Leads are included.
        try:
            meta = frappe.get_meta("Lead")
            if meta.has_field("ci_record_type"):
                lead.set("ci_record_type", "lead")
            if meta.has_field("status") and not getattr(lead, "status", None):
                lead.status = "Lead"
            if meta.has_field("workflow_state") and not lead.get("workflow_state"):
                lead.set("workflow_state", "Open")
        except Exception:
            pass
        lead.insert(ignore_permissions=True)
        lead_name = lead.name
        frappe.logger("call_intelligence.whatsapp").info(
            "auto_created_lead name=%s phone=%s", lead_name, phone_digits
        )
        return lead_name
    except Exception:
        frappe.log_error(title="WA auto-create lead", message=frappe.get_traceback())
        return None
