"""
WhatsApp integration: Meta WhatsApp Cloud API only.

Site-config keys (set via bench set-config or site_config.json):
  call_intelligence_whatsapp_cloud_access_token      Meta Cloud API bearer token
  call_intelligence_whatsapp_cloud_phone_number_id   Sending phone-number ID (Meta)
  call_intelligence_whatsapp_cloud_verify_token      Webhook verification token
  call_intelligence_whatsapp_admin_number            E.164 test/admin destination
  call_intelligence_whatsapp_test_mode               1 = always send to admin number
  call_intelligence_whatsapp_preflight_media_url     1 = HEAD-check media URLs
  call_intelligence_whatsapp_operator_notify         1 = WhatsApp new-lead summary to operator (default 1)
  call_intelligence_whatsapp_operator_number         Operator digits (digits only; 10-digit IN → 91 prefix)
  call_intelligence_whatsapp_operator_cta_delay        Optional seconds before first operator message (default 0, max 3)
  call_intelligence_whatsapp_operator_between_messages Seconds between intro and CTA+buttons (default 1, max 3)
"""

from __future__ import annotations

import json
import re
import time
from contextlib import contextmanager
from typing import Any

import frappe

_GRAPH_BASE = "https://graph.facebook.com/v18.0"


@contextmanager
def webhook_privileged_session():
    """
    Meta webhooks call whitelisted methods as Guest; Lead / Communication / get_meta need
    a real user context. Restore previous session after the block.
    """
    prev = frappe.session.user
    frappe.set_user("Administrator")
    try:
        yield
    finally:
        frappe.set_user(prev)


# ── Config ────────────────────────────────────────────────────────────────────

def _conf(key: str, default: Any = None) -> Any:
    try:
        return frappe.conf.get(key) or default
    except Exception:
        return default


def get_whatsapp_cloud_verify_token() -> str:
    """
    Meta GET verification must match this string (hub.verify_token).
    Priority: site_config call_intelligence_whatsapp_cloud_verify_token, then
    Call Intelligence Settings.whatsapp_cloud_verify_token.
    """
    t = str(_conf("call_intelligence_whatsapp_cloud_verify_token") or "").strip()
    if t:
        return t
    try:
        v = frappe.db.get_single_value("Call Intelligence Settings", "whatsapp_cloud_verify_token")
        if v:
            return str(v).strip()
    except Exception:
        pass
    return ""


def _conf_bool(key: str, default: bool = False) -> bool:
    v = _conf(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return bool(v)
    return str(v).strip().lower() in ("1", "true", "yes")

def _wa_debug_enabled() -> bool:
    return _conf_bool("call_intelligence_whatsapp_debug_send", False)


def _digits(phone: str | None) -> str:
    if not phone:
        return ""
    return re.sub(r"\D", "", str(phone))


def _log(msg: str, title: str = "call_intelligence.whatsapp") -> None:
    frappe.logger("call_intelligence.whatsapp").info("%s: %s", title, str(msg)[:4000])


def is_cloud_configured() -> bool:
    token = _conf("call_intelligence_whatsapp_cloud_access_token") or \
            _conf("call_intelligence_whatsapp_cloud_token")
    phone_id = _conf("call_intelligence_whatsapp_cloud_phone_number_id") or \
               _conf("call_intelligence_whatsapp_cloud_phone_id")
    return bool(token and str(token).strip() and phone_id and str(phone_id).strip())


def is_twilio_configured() -> bool:
    sid = str(_conf("call_intelligence_twilio_account_sid") or "").strip()
    token = str(_conf("call_intelligence_twilio_auth_token") or "").strip()
    frm = str(_conf("call_intelligence_twilio_from_number") or "").strip()
    return bool(sid and token and frm)


def get_whatsapp_test_mode() -> bool:
    return _conf_bool("call_intelligence_whatsapp_test_mode", False)


def get_admin_destination_number() -> str:
    num = _conf("call_intelligence_whatsapp_admin_number", "")
    return str(num).strip() if num else ""


def _get_cloud_token() -> str:
    return str(
        _conf("call_intelligence_whatsapp_cloud_access_token") or
        _conf("call_intelligence_whatsapp_cloud_token") or ""
    ).strip()


def _get_cloud_phone_id() -> str:
    return str(
        _conf("call_intelligence_whatsapp_cloud_phone_number_id") or
        _conf("call_intelligence_whatsapp_cloud_phone_id") or ""
    ).strip()


# ── Communication storage ─────────────────────────────────────────────────────

def _safe_communication_medium() -> str:
    """
    Frappe's Communication.communication_medium (often labeled "Type" in Desk) only allows
    values like Email, Chat, Phone, SMS — not "WhatsApp". Pick a valid option from meta.
    """
    try:
        meta = frappe.get_meta("Communication")
        if not meta.has_field("communication_medium"):
            return ""
        raw = meta.get_field("communication_medium").options or ""
        opts = [x.strip() for x in raw.split("\n") if x.strip()]
        blocked = {"whatsapp", "WhatsApp"}
        opts = [o for o in opts if o not in blocked]
        for pref in ("Chat", "SMS", "Phone", "Other", "Email"):
            if pref in opts:
                return pref
        if opts:
            return opts[0]
        # Error message allows empty Type; use if nothing else matches
        return ""
    except Exception:
        return "Chat"


def _store_communication(
    direction: str,
    content: str,
    reference_doctype: str,
    reference_name: str,
    phone: str = "",
    msg_type: str = "text",
    media_url: str = "",
    provider: str = "whatsapp_cloud",
) -> None:
    try:
        comm = frappe.new_doc("Communication")
        comm.communication_type = "Communication"
        comm.sent_or_received = direction
        comm.content = str(content or "")[:4000]
        comm.reference_doctype = reference_doctype
        comm.reference_name = reference_name
        comm.phone_no = (_digits(phone) or "")[:20]
        comm.subject = "[WhatsApp/{}] {}".format(provider, msg_type)
        # Desk field "Type" = communication_medium; must never be "WhatsApp" (invalid in standard Frappe).
        comm.communication_medium = _safe_communication_medium()
        comm.insert(ignore_permissions=True)
    except Exception:
        frappe.log_error(title="WhatsApp store_communication", message=frappe.get_traceback())


# ── Cloud API send ─────────────────────────────────────────────────────────────

def _cloud_send_raw(to_digits: str, payload: dict, ref_dt: str = "", ref_name: str = "", kind: str = "text") -> dict:
    import requests as _req
    phone_id = _get_cloud_phone_id()
    token = _get_cloud_token()
    url = "{}/{}/messages".format(_GRAPH_BASE, phone_id)
    headers = {"Authorization": "Bearer {}".format(token), "Content-Type": "application/json"}
    status_code = 0
    response_text = ""
    try:
        resp = _req.post(url, json=payload, headers=headers, timeout=20)
        status_code = resp.status_code
        response_text = resp.text
        if _wa_debug_enabled():
            frappe.log_error(
                "STATUS: {}, RESPONSE: {}".format(status_code, (response_text or "")[:4000]),
                "WA SEND DEBUG",
            )
    except Exception as e:
        response_text = str(e)
        if _wa_debug_enabled():
            frappe.log_error(
                "EXCEPTION: {}".format(response_text[:4000]),
                "WA SEND DEBUG",
            )
    ok = 200 <= status_code < 300
    try:
        from call_intelligence.whatsapp_logging import log_whatsapp_cloud_outbound
        log_whatsapp_cloud_outbound(kind=kind, to_digits=to_digits, status_code=status_code,
                                    response_text=response_text, ok=ok,
                                    reference_doctype=ref_dt or None, reference_name=ref_name or None)
    except Exception:
        pass
    return {"ok": ok, "status_code": status_code, "response": response_text[:2000]}


def send_whatsapp_cloud_text_with_fallback(
    message: str,
    to_e164: str,
    reference_doctype: str = "",
    reference_name: str = "",
) -> dict[str, Any]:
    to_digits = _digits(to_e164)
    if not to_digits:
        return {"ok": False, "error": "no destination number", "provider": "whatsapp_cloud"}
    if not _get_cloud_phone_id():
        return {"ok": False, "error": "cloud_phone_id not configured", "provider": "whatsapp_cloud"}

    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_digits,
        "type": "text",
        "text": {"preview_url": False, "body": str(message).strip()},
    }
    result = _cloud_send_raw(to_digits, payload, reference_doctype, reference_name, "text")

    if result["ok"]:
        if reference_doctype and reference_name:
            _store_communication("Sent", message, reference_doctype, reference_name, to_digits)
        return {"ok": True, "provider": "whatsapp_cloud", "destination": to_digits, "response": result}

    raw = str(result.get("response") or "").lower()
    is_window_error = "131047" in raw or "131026" in raw or "24 hour" in raw or "re-engagement" in raw
    if is_window_error:
        tmpl = _cloud_send_raw(to_digits, {
            "messaging_product": "whatsapp", "to": to_digits, "type": "template",
            "template": {"name": "hello_world", "language": {"code": "en_US"}},
        }, reference_doctype, reference_name, "template")
        return {"ok": tmpl.get("ok", False), "provider": "whatsapp_cloud", "destination": to_digits,
                "response": tmpl, "fallback": True, "text_attempt_failed": True}

    return {"ok": False, "provider": "whatsapp_cloud", "destination": to_digits,
            "response": result, "error_hint": str(result.get("response") or "")[:300]}


def send_whatsapp_message_impl(
    message: str,
    reference_doctype: str,
    reference_name: str,
) -> dict[str, Any]:
    """
    Twilio path when Cloud API is not configured. This build is Cloud-primary; keep a safe stub.
    """
    dest = get_admin_destination_number()
    if not is_twilio_configured():
        return {
            "ok": False,
            "destination": dest,
            "response": {"error": "configure_whatsapp_cloud_or_twilio_in_site_config"},
        }
    _log("Twilio outbound not implemented; set WhatsApp Cloud credentials instead.", "call_intelligence.whatsapp")
    return {
        "ok": False,
        "destination": dest,
        "response": {"error": "twilio_outbound_not_implemented"},
    }


def process_inbound_twilio_webhook(payload: dict[str, Any]) -> None:
    if not payload:
        return
    _log("Twilio inbound webhook ignored (use WhatsApp Cloud webhook).", "call_intelligence.whatsapp.twilio")


def _effective_whatsapp_destination(phone: str) -> str:
    """In test mode, send to admin number when configured."""
    if get_whatsapp_test_mode():
        admin = get_admin_destination_number()
        if admin:
            return admin
    return phone


def send_whatsapp_message(
    phone: str,
    text: str,
    buttons: list[tuple[str, str]] | None = None,
    *,
    reference_doctype: str = "",
    reference_name: str = "",
) -> dict[str, Any]:
    """
    Send a WhatsApp Cloud API message (plain text or interactive reply buttons).

    buttons: up to 3 items as (id, title); title max 20 chars (Meta limit).
    """
    dest = _effective_whatsapp_destination(phone)
    to_digits = _digits(dest)
    if not to_digits:
        return {"ok": False, "error": "no destination number", "provider": "whatsapp_cloud"}
    if not _get_cloud_phone_id():
        return {"ok": False, "error": "cloud_phone_id not configured", "provider": "whatsapp_cloud"}

    text = str(text or "").strip()
    if not text:
        return {"ok": False, "error": "empty text", "provider": "whatsapp_cloud"}

    if not buttons:
        return send_whatsapp_cloud_text_with_fallback(
            message=text,
            to_e164=dest,
            reference_doctype=reference_doctype,
            reference_name=reference_name,
        )

    btns: list[dict[str, Any]] = []
    for bid, title in buttons[:3]:
        btns.append(
            {
                "type": "reply",
                "reply": {
                    "id": str(bid)[:256],
                    "title": str(title).strip()[:20],
                },
            }
        )

    payload: dict[str, Any] = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to_digits,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": text[:1024]},
            "action": {"buttons": btns},
        },
    }

    result = _cloud_send_raw(to_digits, payload, reference_doctype, reference_name, "interactive")
    ok = bool(result.get("ok"))
    if ok and reference_doctype and reference_name:
        _store_communication(
            "Sent",
            text,
            reference_doctype,
            reference_name,
            to_digits,
            msg_type="interactive",
            provider="whatsapp_cloud",
        )

    return {
        "ok": ok,
        "provider": "whatsapp_cloud",
        "destination": to_digits,
        "response": result,
        "test_mode": get_whatsapp_test_mode(),
    }


def send_whatsapp_reply_confirmation(
    phone: str,
    lead_name: str,
    action: str,
) -> dict[str, Any]:
    """
    After a normalized inbound reply updates the Lead, send a short confirmation via Cloud API.
    Uses send_whatsapp_message → Sent Communication when delivery succeeds.
    """
    a = str(action or "").strip().lower()
    if a == "yes":
        msg = "Thank you for confirming. Your request is confirmed and our team will proceed shortly."
    elif a == "no":
        msg = "Thank you for your response. We have marked this as cancelled. If you need anything later, just reply."
    elif a == "reschedule":
        msg = "Thank you for your response. We will help you reschedule. Please share a convenient date and time."
    else:
        msg = "Thank you for your response. Please reply with 1 (confirm), 2 (cancel), or 3 (reschedule)."
    phone = str(phone or "").strip()
    lead_name = str(lead_name or "").strip()
    if not phone or not lead_name:
        return {"ok": False, "error": "missing_phone_or_lead"}
    if _wa_debug_enabled():
        frappe.log_error("SEND FUNCTION TRIGGERED", "WA DEBUG")
    return send_whatsapp_message(
        phone,
        msg,
        buttons=None,
        reference_doctype="Lead",
        reference_name=lead_name,
    )


def map_action_to_workflow(action: str | None) -> str | None:
    """
    Map normalized WhatsApp action tokens to Lead status/workflow targets.

    Note: actual Workflow transitions (when configured) are applied via apply_workflow(doc, action).
    This mapping is used as a fallback when no workflow transition is available.
    """
    a = str(action or "").strip().lower()
    if a == "yes":
        return "Interested"  # or correct workflow action
    if a == "no":
        return "Cold"
    return None


DEFAULT_FOLLOWUP_SUMMARY = (
    "You may require a follow-up consultation based on your recent visit."
)
DEFAULT_FOLLOWUP_CTA = "Would you like to proceed?"

# Internal template keys (never shown in WhatsApp copy — classification only).
_CARE_FOLLOW_UP = "FOLLOW_UP"
_CARE_POST_CARE = "POST_CARE_FOLLOW_UP"
_CARE_SCAN = "SCAN_REQUIRED"
_CARE_INPATIENT = "INPATIENT_PROCEDURE"
_CARE_MEDICATION = "MEDICATION_REVIEW"
_CARE_GENERAL = "GENERAL_CONSULTATION"

# Exact customer-facing copy: (message 1 plain text, message 2 + interactive buttons).
CARE_MESSAGING_TEMPLATE_PARTS: dict[str, tuple[str, str]] = {
    _CARE_FOLLOW_UP: (
        "You may require a follow-up consultation based on your recent visit.",
        "Would you like to proceed?",
    ),
    _CARE_POST_CARE: (
        "We recommend a post-care follow-up to monitor your recovery.",
        "Would you like to proceed?",
    ),
    _CARE_SCAN: (
        "You are advised to undergo a diagnostic scan.\n\n"
        "Please complete the scan and revert for further consultation.",
        "Would you like to proceed?",
    ),
    _CARE_INPATIENT: (
        "Your condition may require an inpatient procedure.",
        "Would you like to proceed with further arrangements?",
    ),
    _CARE_MEDICATION: (
        "You have been prescribed medication.\n\n"
        "Please follow the prescription and revisit for evaluation.",
        "Would you like to proceed?",
    ),
    _CARE_GENERAL: (
        "A consultation is recommended based on your recent interaction.",
        "Would you like to proceed?",
    ),
}

_WHATSAPP_PLAIN_TEXT_MAX = 4096
_WHATSAPP_INTERACTIVE_BODY_MAX = 1024

_STANDARD_WA_BUTTONS: list[tuple[str, str]] = [
    ("yes", "Confirm"),
    ("no", "Cancel"),
    ("reschedule", "Reschedule"),
]


def _lead_text_blob_for_template(lead) -> str:
    parts: list[str] = []
    for attr in (
        "ci_ai_summary",
        "ci_lead_notes",
        "ci_action_description",
        "ci_sentiment_summary",
        "ci_services",
        "transcript",
    ):
        if hasattr(lead, attr):
            v = getattr(lead, attr, None)
            if v and str(v).strip():
                parts.append(str(v))
    return "\n".join(parts).lower()




def _lead_field_text(lead, fieldname: str) -> str:
    if not hasattr(lead, fieldname):
        return ""
    v = getattr(lead, fieldname, None)
    return str(v or "").strip()


def _extract_label_value(text: str, label: str) -> str:
    if not text:
        return ""
    m = re.search(rf"{re.escape(label)}\s*:\s*([^\n]+)", text, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def _two_word_topic(*values: str) -> str:
    """Pick a short human phrase (max 2 words) for 'regarding your ...'."""
    stop = {
        "diagnosis", "follow", "follow-up", "required", "true", "false", "fhir",
        "patient", "insurance", "eligible", "status", "regarding", "your"
    }
    for raw in values:
        if not raw:
            continue
        s = re.sub(r"[^A-Za-z0-9\s-]", " ", str(raw)).strip().lower()
        words = [w for w in s.split() if w and w not in stop]
        if not words:
            continue
        return " ".join(words[:2])
    return ""


def _rich_followup_intro_from_lead(lead, template_intro: str) -> str:
    """Build richer WhatsApp intro from Lead/Encounter-derived details when present."""
    summary_blob = "\n".join(
        x
        for x in (
            _lead_field_text(lead, "ci_ai_summary"),
            _lead_field_text(lead, "ci_lead_notes"),
            _lead_field_text(lead, "transcript"),
        )
        if x
    )

    diagnosis = (
        _lead_field_text(lead, "diagnosis")
        or _lead_field_text(lead, "ci_diagnosis")
        or _extract_label_value(summary_blob, "Diagnosis")
    )
    service = _lead_field_text(lead, "ci_services")
    insurance = (
        _lead_field_text(lead, "insurance_eligibility_status")
        or _extract_label_value(summary_blob, "Insurance")
    )

    followup_days = ""
    m_days = re.search(r"days\s*=\s*(\d+)", summary_blob, re.IGNORECASE)
    if m_days:
        followup_days = m_days.group(1)

    detail_lines: list[str] = []
    topic = _two_word_topic(diagnosis, service, summary_blob)
    if topic:
        detail_lines.append(f"Regarding your {topic}, we recommend a follow-up consultation.")
    elif diagnosis:
        detail_lines.append(f"Regarding: {diagnosis}.")

    if service:
        detail_lines.append(f"Recommended service: {service}.")
    if followup_days:
        detail_lines.append(f"Suggested follow-up in {followup_days} day(s).")
    if insurance:
        detail_lines.append(f"Insurance status: {insurance}.")

    if not detail_lines:
        return template_intro
    return template_intro + "\n\n" + "\n".join(detail_lines)

def classify_care_messaging_template(lead) -> str:
    """
    Map Lead clinical text to a strict messaging template key (internal — not user-visible).
    Order: most specific signals first.
    """
    t = _lead_text_blob_for_template(lead)
    if not t.strip():
        return _CARE_GENERAL

    if re.search(
        r"post[-\s]?care|monitor\s+your\s+recovery|after\s+surgery|postoperative|post[-\s]?op",
        t,
    ):
        return _CARE_POST_CARE
    if re.search(
        r"diagnostic\s+scan|undergo.*\bscan\b|\bscan\b|imaging|\bmri\b|ct\s*scan|\bct\b|"
        r"ultrasound|x[- ]?ray|radiolog|sonograph",
        t,
    ):
        return _CARE_SCAN
    if re.search(r"inpatient|admission|\badmit\b|hospitaliz|\bward\b", t):
        return _CARE_INPATIENT
    if re.search(r"\bsurgery\b|inpatient\s+procedure|operation\b", t) and "outpatient" not in t:
        return _CARE_INPATIENT
    if re.search(
        r"medication|prescrib|prescription|pharmacy|\btablets?\b|insulin|antibiotic|dosage",
        t,
    ):
        return _CARE_MEDICATION
    if re.search(
        r"follow[-\s]?up|followup|\breview\b|routine\s+diabetes|\bdiabetes\b|recheck|surveillance",
        t,
    ):
        return _CARE_FOLLOW_UP
    return _CARE_GENERAL


def get_care_template_message_parts(lead) -> tuple[str, str, str]:
    """
    Returns (internal_template_key, intro_plain_text, cta_for_second_message).

    First WhatsApp: ``intro`` only. Second: ``cta`` + Confirm / Cancel / Reschedule.
    No ``Ref:`` line — operator threading uses cache + optional explicit Ref in replies.
    """
    key = classify_care_messaging_template(lead)
    pair = CARE_MESSAGING_TEMPLATE_PARTS.get(key) or CARE_MESSAGING_TEMPLATE_PARTS[_CARE_GENERAL]
    intro, cta = _rich_followup_intro_from_lead(lead, pair[0].strip()), pair[1].strip()
    if len(intro) > _WHATSAPP_PLAIN_TEXT_MAX:
        intro = intro[: _WHATSAPP_PLAIN_TEXT_MAX - 1] + "…"
    if len(cta) > _WHATSAPP_INTERACTIVE_BODY_MAX:
        cta = cta[: _WHATSAPP_INTERACTIVE_BODY_MAX - 1] + "…"
    return key, intro, cta


def _send_whatsapp_care_template_two_step(
    to_e164: str,
    lead_name: str,
    intro: str,
    cta: str,
    *,
    delay_seconds: float = 1.0,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Plain intro, then CTA + standard buttons. Both logged against the Lead."""
    r1 = send_whatsapp_message(
        to_e164,
        intro,
        buttons=None,
        reference_doctype="Lead",
        reference_name=lead_name,
    )
    if not r1.get("ok"):
        return r1, {}
    try:
        d = float(delay_seconds or 0)
        if d > 0:
            time.sleep(min(d, 3.0))
    except Exception:
        time.sleep(1.0)
    r2 = send_whatsapp_message(
        to_e164,
        cta,
        buttons=list(_STANDARD_WA_BUTTONS),
        reference_doctype="Lead",
        reference_name=lead_name,
    )
    return r1, r2


def _lead_phone_for_outbound(lead) -> str:
    for attr in ("whatsapp_no", "mobile_no", "phone_number", "phone"):
        if hasattr(lead, attr):
            v = getattr(lead, attr, None)
            if v:
                s = str(v).strip()
                if s:
                    return s
    return ""


@frappe.whitelist()
def send_lead_whatsapp_followup_flow(
    lead_name: str,
    summary_text: str | None = None,
    cta_text: str | None = None,
    *,
    delay_seconds: float = 1.0,
) -> dict[str, Any]:
    """
    Two WhatsApp messages: (1) template intro plain text, (2) CTA + Confirm / Cancel / Reschedule.

    Uses :func:`get_care_template_message_parts` when overrides are omitted. If ``summary_text``
    or ``cta_text`` is set, uses them as message 1 / message 2 (defaulting missing parts to
    :data:`DEFAULT_FOLLOWUP_SUMMARY` / :data:`DEFAULT_FOLLOWUP_CTA`).
    """
    if getattr(frappe, "session", None) and frappe.session.user == "Guest":
        # Do not allow unauthenticated callers to trigger outbound messages.
        frappe.throw("Login required", frappe.AuthenticationError)

    lead = frappe.get_doc("Lead", lead_name)
    phone = _lead_phone_for_outbound(lead)
    if not phone:
        _log("followup_flow skip no phone lead=%s" % lead_name)
        return {"ok": False, "error": "no_phone_on_lead", "lead": lead_name}

    if not is_cloud_configured():
        return {"ok": False, "error": "whatsapp_cloud_not_configured", "lead": lead_name}

    if summary_text is not None or cta_text is not None:
        intro = (summary_text or "").strip() or DEFAULT_FOLLOWUP_SUMMARY
        cta = (cta_text or "").strip() or DEFAULT_FOLLOWUP_CTA
        template_key = "CUSTOM"
    else:
        template_key, intro, cta = get_care_template_message_parts(lead)

    if len(intro) > _WHATSAPP_PLAIN_TEXT_MAX:
        intro = intro[: _WHATSAPP_PLAIN_TEXT_MAX - 1] + "…"
    if len(cta) > _WHATSAPP_INTERACTIVE_BODY_MAX:
        cta = cta[: _WHATSAPP_INTERACTIVE_BODY_MAX - 1] + "…"

    r1, r2 = _send_whatsapp_care_template_two_step(
        phone, lead_name, intro, cta, delay_seconds=delay_seconds
    )
    return {
        "ok": bool(r2.get("ok")) if r2 else bool(r1.get("ok")),
        "lead": lead_name,
        "template": template_key,
        "context": r1,
        "cta": r2,
    }


def pick_lead_status_for_whatsapp_button(button_id: str) -> str | None:
    """
    Map yes/no to Lead.status option values present on the doctype.
    yes → Interested (or first allowed); no → Cold, else Do Not Contact / Not Interested.
    """
    meta = frappe.get_meta("Lead")
    if not meta.has_field("status"):
        return None
    field = meta.get_field("status")
    opts = [x.strip() for x in (field.options or "").split("\n") if x.strip()]
    if not opts:
        return None

    bid = str(button_id or "").lower()
    if bid == "yes":
        for c in ("Interested", "Open", "Replied"):
            if c in opts:
                return c
        return "Interested" if "Interested" in opts else None

    if bid == "no":
        for c in ("Cold", "Do Not Contact", "Not Interested"):
            if c in opts:
                return c
        return None

    return None


def normalize_whatsapp_reply_to_action(msg_text: str | None) -> str | None:
    """
    Map inbound TEXT or button labels to a canonical action.

    yes  ← 1, yes, confirm, y
    no   ← 2, no, cancel, n
    reschedule ← 3, reschedule
    """
    if msg_text is None:
        return None
    raw = str(msg_text).strip()
    if not raw:
        return None
    first_line = raw.split("\n")[0].strip()
    t = first_line.lower()
    t_nospace = re.sub(r"[\s\u200d\uFE0F]", "", t)
    yes_set = frozenset({"1", "yes", "confirm", "y"})
    no_set = frozenset({"2", "no", "cancel", "n"})
    re_set = frozenset({"3", "reschedule"})

    for candidate in (t, t_nospace):
        if candidate in yes_set:
            return "yes"
        if candidate in no_set:
            return "no"
        if candidate in re_set:
            return "reschedule"
    first_word = t.split()[0] if t else ""
    if first_word in yes_set:
        return "yes"
    if first_word in no_set:
        return "no"
    if first_word in re_set:
        return "reschedule"
    return None


def canonical_whatsapp_action(token: str | None) -> str | None:
    """Button id or text → yes | no | reschedule (also accepts raw yes/no/reschedule)."""
    n = normalize_whatsapp_reply_to_action(token)
    if n:
        return n
    s = str(token or "").strip().lower()
    if s in ("yes", "no", "reschedule"):
        return s
    return None


# ── Queries ────────────────────────────────────────────────────────────────────

def list_whatsapp_communications_for_lead(lead_name: str, limit: int = 80) -> list[dict[str, Any]]:
    try:
        rows = frappe.get_all(
            "Communication",
            filters={"reference_doctype": "Lead", "reference_name": lead_name,
                     "subject": ["like", "[WhatsApp/%"]},
            fields=["name", "creation", "content", "sent_or_received", "subject", "phone_no"],
            order_by="creation asc", limit=limit,
        )
        result = []
        for r in rows:
            direction = str(r.get("sent_or_received") or "").lower()
            subj = str(r.get("subject") or "")
            result.append({
                "name": r.get("name"), "creation": str(r.get("creation") or ""),
                "content": r.get("content") or "",
                "direction": "outgoing" if direction == "sent" else "incoming",
                "type": "outgoing" if direction == "sent" else "incoming",
                "provider": "whatsapp_cloud",
                "msg_type": "image" if "image" in subj.lower() else "text",
                "phone": r.get("phone_no") or "",
            })
        return result
    except Exception:
        frappe.log_error(title="list_whatsapp_comms", message=frappe.get_traceback())
        return []


# ── Internal helpers ───────────────────────────────────────────────────────────

def _find_lead_by_phone(phone_digits: str) -> str | None:
    """Resolve Lead by normalized digits: whatsapp_no → mobile_no → phone_number → phone."""
    if not phone_digits or len(phone_digits) < 7:
        return None
    suffix = phone_digits[-10:] if len(phone_digits) >= 10 else phone_digits
    fields = ("whatsapp_no", "mobile_no", "phone_number", "phone")
    for field in fields:
        try:
            meta = frappe.get_meta("Lead")
            if not meta.has_field(field):
                continue
            rows = frappe.db.sql(
                "SELECT name FROM `tabLead` WHERE RIGHT(REGEXP_REPLACE(`{}`, '[^0-9]', ''), 10) = %s LIMIT 1".format(
                    field
                ),
                (suffix,),
                as_dict=True,
            )
            if rows:
                return rows[0]["name"]
        except Exception:
            pass
    return None


def normalize_user_reply(msg: dict[str, Any] | None) -> str | None:
    """
    Map Meta inbound message → 'yes' | 'no' | 'reschedule' | None.

    - Interactive: use button_reply.id (or list_reply.id) when present.
    - Text: lower/strip and map 1/yes/confirm, 2/no/cancel, 3/reschedule.
    """
    if not isinstance(msg, dict):
        return None

    inter = msg.get("interactive")
    if isinstance(inter, dict):
        br = inter.get("button_reply")
        if isinstance(br, dict):
            bid = str(br.get("id") or "").strip().lower()
            if bid:
                if bid in ("yes", "no", "reschedule"):
                    return bid
                return _normalize_text_token(bid)
        lr = inter.get("list_reply")
        if isinstance(lr, dict):
            lid = str(lr.get("id") or "").strip().lower()
            if lid:
                if lid in ("yes", "no", "reschedule"):
                    return lid
                return _normalize_text_token(lid)

    msg_type = str(msg.get("type") or "").lower()
    if msg_type == "text":
        body = str((msg.get("text") or {}).get("body") or "").strip()
        return _normalize_text_token(body)

    if msg_type == "button":
        b = msg.get("button") or {}
        raw = str(b.get("payload") or b.get("text") or "").strip()
        return _normalize_text_token(raw)

    return None


def _normalize_text_token(text: str) -> str | None:
    if not text:
        return None
    s = str(text).lower().strip()
    if not s:
        return None

    yes_tokens = {"1", "yes", "y", "confirm", "confirmed", "yeah", "yep"}
    no_tokens = {"2", "no", "n", "cancel", "cancelled", "nope", "nah"}
    reschedule_tokens = {"3", "reschedule", "reschedule_requested"}

    if s in yes_tokens:
        return "yes"
    if s in no_tokens:
        return "no"
    if s in reschedule_tokens:
        return "reschedule"

    return None


def _status_option_exists(doctype: str, value: str) -> bool:
    try:
        meta = frappe.get_meta(doctype)
        if not meta.has_field("status"):
            return False
        raw = meta.get_field("status").options or ""
        opts = {x.strip() for x in raw.split("\n") if x.strip()}
        return value in opts
    except Exception:
        return False


def apply_normalized_reply_to_lead(lead_name: str, action: str) -> str:
    """
    If an active Workflow exists on Lead, try apply_workflow(doc, action) and fall back to a mapped
    action name when the workflow does not define yes/no/reschedule transitions.
    Otherwise set Lead.status: yes→Interested, no→Cold, reschedule→Open (when options allow).
    Returns final status field value (best effort) or ''.
    """
    from frappe.model.workflow import (
        WorkflowTransitionError,
        apply_workflow,
        get_workflow,
        get_workflow_name,
    )

    action = str(action or "").strip().lower()
    if action not in ("yes", "no", "reschedule"):
        return ""

    # Keep booking_status aligned with user response for Patient 360 panels.
    _handle_keyword_reply(lead_name, action)

    prev_user = frappe.session.user
    frappe.set_user("Administrator")
    try:
        wf_name = get_workflow_name("Lead")
        doc = frappe.get_doc("Lead", lead_name)

        if wf_name:
            try:
                out = apply_workflow(doc, action)
                if out:
                    wf = get_workflow("Lead")
                    sf = wf.workflow_state_field
                    return str(out.get("status") or out.get(sf) or "")
                return ""
            except WorkflowTransitionError:
                pass
            except Exception:
                frappe.logger("call_intelligence.whatsapp").exception(
                    "apply_workflow failed lead=%s action=%s", lead_name, action
                )
                # If the workflow doesn't define actions named yes/no/reschedule, try mapped names.
                mapped_action = map_action_to_workflow(action)
                if mapped_action and mapped_action != action:
                    try:
                        out2 = apply_workflow(doc, mapped_action)
                        if out2:
                            wf = get_workflow("Lead")
                            sf = wf.workflow_state_field
                            return str(out2.get("status") or out2.get(sf) or "")
                        return ""
                    except Exception:
                        frappe.logger("call_intelligence.whatsapp").exception(
                            "apply_workflow failed lead=%s action=%s mapped_action=%s",
                            lead_name,
                            action,
                            mapped_action,
                        )

        doc = frappe.get_doc("Lead", lead_name)
        preferred = map_action_to_workflow(action) or ("Open" if action == "reschedule" else None)
        meta_lead = frappe.get_meta("Lead")
        if preferred and meta_lead.has_field("status"):
            if _status_option_exists("Lead", preferred):
                doc.status = preferred
            elif action == "reschedule":
                for candidate in ("Open", "Replied", "Interested", "Lead"):
                    if candidate and _status_option_exists("Lead", candidate):
                        doc.status = candidate
                        break
            elif action in ("yes", "no"):
                mapped = pick_lead_status_for_whatsapp_button(
                    "yes" if action == "yes" else "no"
                )
                if mapped:
                    doc.status = mapped

        doc.flags.ignore_permissions = True
        doc.save()
        return str(doc.get("status") or "")
    finally:
        frappe.set_user(prev_user)


def _handle_keyword_reply(lead_name: str, body: str) -> None:
    text = body.strip().lower()
    kmap = {
        "confirm": ("confirmed", "Booked"),
        "confirmed": ("confirmed", "Booked"),
        "yes": ("confirmed", "Booked"),
        "1": ("confirmed", "Booked"),
        "cancel": ("cancelled", "Cancelled"),
        "cancelled": ("cancelled", "Cancelled"),
        "no": ("cancelled", "Cancelled"),
        "2": ("cancelled", "Cancelled"),
        "reschedule": ("reschedule_requested", "Reschedule Requested"),
        "3": ("reschedule_requested", "Reschedule Requested"),
    }
    matched = kmap.get(text)
    if not matched:
        return
    flow_state, booking_status = matched
    try:
        lead = frappe.get_doc("Lead", lead_name)
        meta = frappe.get_meta("Lead")
        if meta.has_field("whatsapp_flow_state"):
            lead.whatsapp_flow_state = flow_state
        if meta.has_field("booking_status"):
            lead.booking_status = booking_status
        lead.flags.ignore_permissions = True
        lead.save()
        frappe.db.commit()
        _log("keyword_reply lead={} flow={} booking={}".format(lead_name, flow_state, booking_status))
    except Exception:
        frappe.log_error(title="handle_keyword_reply", message=frappe.get_traceback())


def apply_lead_whatsapp_followup_action(lead_name: str, action: str) -> str:
    """
    Apply Lead workflow for yes/no, or booking flow for reschedule.
    Tries both raw tokens (yes/no) and mapped action names for workflows that don't use yes/no.

    action: yes | no | reschedule
    """
    from frappe.model.workflow import apply_workflow

    action = str(action or "").strip().lower()
    if action not in ("yes", "no", "reschedule"):
        return ""

    # Always persist booking intent for Patient 360 even when workflow/state handling differs.
    _handle_keyword_reply(lead_name, action)

    if action == "reschedule":
        return "reschedule_requested"

    frappe.flags.ignore_permissions = True
    final = ""
    try:
        doc = frappe.get_doc("Lead", lead_name)
        wf_name = frappe.db.get_value(
            "Workflow",
            {"document_type": "Lead", "is_active": 1},
            "name",
        )
        if wf_name:
            try:
                apply_workflow(doc, action)
                doc.save()
                doc.reload()
                final = str(
                    getattr(doc, "workflow_state", None)
                    or getattr(doc, "status", None)
                    or ""
                )
                return final
            except Exception:
                frappe.log_error(
                    title="apply_lead_whatsapp_followup_action workflow",
                    message=frappe.get_traceback(),
                )
            mapped_action = map_action_to_workflow(action)
            if mapped_action and mapped_action != action:
                try:
                    apply_workflow(doc, mapped_action)
                    doc.save()
                    doc.reload()
                    final = str(
                        getattr(doc, "workflow_state", None)
                        or getattr(doc, "status", None)
                        or ""
                    )
                    return final
                except Exception:
                    frappe.log_error(
                        title="apply_lead_whatsapp_followup_action workflow mapped",
                        message=frappe.get_traceback(),
                    )
                doc.reload()

        status_val = pick_lead_status_for_whatsapp_button(action)
        if status_val and frappe.get_meta("Lead").has_field("status"):
            doc.status = status_val
        doc.save()
        doc.reload()
        final = str(
            getattr(doc, "status", None)
            or getattr(doc, "workflow_state", None)
            or ""
        )
        return final
    finally:
        frappe.flags.ignore_permissions = False


# ── Operator alerts (new Lead → WhatsApp; reply routing for CRM timeline) ───

_OPERATOR_CACHE_PREFIX = "ci_wa_op_last:"


def should_notify_operator_on_new_lead() -> bool:
    """Notify desk operator on new Lead when Cloud API is configured (default on)."""
    return _conf_bool("call_intelligence_whatsapp_operator_notify", True)


def get_operator_destination_number() -> str:
    """
    Operator WhatsApp destination (digits only, no +). Meta expects full country code.

    Default: 91 + 9334796806. Override via ``call_intelligence_whatsapp_operator_number``.
    Ten-digit values are assumed India (+91).
    """
    raw = str(_conf("call_intelligence_whatsapp_operator_number") or "").strip()
    if not raw:
        return "919334796806"
    d = _digits(raw)
    if len(d) == 10:
        return "91" + d
    return d


def is_operator_inbound_phone(phone_digits: str) -> bool:
    """True if inbound Meta ``from`` matches the configured operator number."""
    op = _digits(get_operator_destination_number())
    if not op or not phone_digits:
        return False
    if phone_digits == op:
        return True
    if len(phone_digits) >= 10 and len(op) >= 10:
        return phone_digits[-10:] == op[-10:]
    return False


def _cache_operator_last_lead(operator_digits: str, lead_name: str) -> None:
    try:
        frappe.cache().set_value(
            _OPERATOR_CACHE_PREFIX + operator_digits,
            lead_name,
            expires_in_sec=86400 * 7,
        )
    except Exception:
        pass


def extract_lead_name_from_operator_message(text: str) -> str | None:
    """
    Parse a Lead id from operator reply (e.g. ``Ref: LEAD-2025-00001`` or bare ``CRM-LEAD-...``).
    """
    if not text:
        return None
    s = text.strip()
    patterns = (
        r"\bRef:\s*([A-Z0-9][A-Z0-9-]+)\b",
        r"\bLead:\s*([A-Z0-9][A-Z0-9-]+)\b",
        r"\b(CRM-LEAD-\d{4}-\d{5})\b",
        r"\b(LEAD-\d{4}-\d{5})\b",
    )
    for pat in patterns:
        m = re.search(pat, s, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            if frappe.db.exists("Lead", name):
                return name
    return None


def resolve_lead_for_operator_inbound(text: str, operator_digits: str) -> str | None:
    name = extract_lead_name_from_operator_message(text)
    if name:
        return name
    try:
        cached = frappe.cache().get_value(_OPERATOR_CACHE_PREFIX + operator_digits)
    except Exception:
        cached = None
    if cached and frappe.db.exists("Lead", cached):
        return str(cached)
    return None


def send_operator_new_lead_notification(lead_name: str) -> dict[str, Any]:
    """
    After a new Lead is inserted: two WhatsApp messages to the operator.

    1) Template intro only (no ``Ref:`` line). 2) CTA + Confirm / Cancel / Reschedule.
    Lead association for replies uses cache; operator can still paste ``Ref:`` in free text.
    """
    if not should_notify_operator_on_new_lead():
        return {"ok": False, "skipped": True, "reason": "operator_notify_disabled"}
    dest = get_operator_destination_number()
    if not dest:
        return {"ok": False, "skipped": True, "reason": "no_operator_number"}
    if not is_cloud_configured():
        return {"ok": False, "skipped": True, "reason": "whatsapp_cloud_not_configured"}
    try:
        lead = frappe.get_doc("Lead", lead_name)
    except Exception:
        return {"ok": False, "error": "lead_not_found"}

    template_key, intro, cta = get_care_template_message_parts(lead)
    op_digits = _digits(dest)
    _cache_operator_last_lead(op_digits, lead_name)

    try:
        try:
            pre = float(_conf("call_intelligence_whatsapp_operator_cta_delay") or 0.0)
            if pre > 0:
                time.sleep(min(pre, 3.0))
        except Exception:
            pass

        between = 1.0
        try:
            between = float(_conf("call_intelligence_whatsapp_operator_between_messages") or 1.0)
        except Exception:
            pass

        r1, r2 = _send_whatsapp_care_template_two_step(
            dest, lead_name, intro, cta, delay_seconds=between
        )
        return {
            "ok": bool(r2.get("ok")) if r2 else bool(r1.get("ok")),
            "lead": lead_name,
            "template": template_key,
            "context": r1,
            "cta": r2,
        }
    except Exception:
        frappe.log_error(frappe.get_traceback(), "send_operator_new_lead_notification")
        return {"ok": False, "error": "send_failed"}
