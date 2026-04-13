"""
Isolated WhatsApp demo: Demo Patient lead + structured options + auto-replies.

Uses admin/test phone (same as send_whatsapp_message destination) so inbound mapping matches.
Does not modify send_whatsapp_message — only calls it.
"""

from __future__ import annotations

import re
from typing import Any

import frappe
from frappe import _

from call_intelligence.whatsapp_inbound import normalize_whatsapp_phone

DEMO_LEAD_NAME = "Demo Patient"
DEMO_SOURCE = "WhatsApp Demo"

DEMO_PROMPT_MESSAGE = """Hi 👋 Your appointment is scheduled.

Please reply with:
1️⃣ Confirm
2️⃣ Cancel
3️⃣ Reschedule"""


def _log_demo(msg: str, title: str = "demo_whatsapp") -> None:
    frappe.logger("call_intelligence.demo_whatsapp").info("%s: %s", title, msg[:4000])


def _demo_phone_digits() -> str:
    from call_intelligence.whatsapp_integration import get_admin_destination_number

    return normalize_whatsapp_phone(get_admin_destination_number())


def _set_if_has_field(doc, fieldname: str, value: Any) -> bool:
    if frappe.get_meta(doc.doctype).has_field(fieldname):
        doc.set(fieldname, value)
        return True
    return False


def _apply_demo_source(lead) -> None:
    """Set Lead.source to WhatsApp Demo when the field exists (Link or Data)."""
    if not frappe.get_meta("Lead").has_field("source"):
        return
    try:
        if frappe.db.exists("Lead Source", DEMO_SOURCE):
            lead.source = DEMO_SOURCE
            return
    except Exception:
        pass
    try:
        lead.source = DEMO_SOURCE
    except Exception:
        pass


def create_demo_patient_impl() -> dict[str, Any]:
    """Create or refresh the demo Lead (same phone as webhook / outbound test number)."""
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)

    phone_digits = _demo_phone_digits()
    if not phone_digits or len(phone_digits) < 10:
        frappe.throw(
            _("Set call_intelligence_whatsapp_admin_number to a full test number (digits-only match for inbound).")
        )

    existing = frappe.db.get_value("Lead", {"lead_name": DEMO_LEAD_NAME}, "name")
    if not existing:
        existing = frappe.db.get_value(
            "Lead",
            {"first_name": "Demo", "last_name": "Patient", "phone_number": phone_digits},
            "name",
        )
    if not existing:
        existing = frappe.db.get_value("Lead", {"phone_number": phone_digits}, "name")

    company = None
    try:
        company = frappe.db.get_value("Company", {}, "name")
    except Exception:
        pass

    if existing:
        lead = frappe.get_doc("Lead", existing)
        if hasattr(lead, "first_name"):
            lead.first_name = "Demo"
        if hasattr(lead, "last_name"):
            lead.last_name = "Patient"
        _set_if_has_field(lead, "phone_number", phone_digits)
        if hasattr(lead, "mobile_no"):
            lead.mobile_no = phone_digits
        if hasattr(lead, "phone"):
            lead.phone = phone_digits
        _set_if_has_field(lead, "booking_status", "Pending")
        _set_if_has_field(lead, "ci_record_type", "lead")
        _apply_demo_source(lead)
        lead.flags.ignore_permissions = True
        lead.save()
        frappe.db.commit()
        _log_demo(f"demo_lead_refreshed name={lead.name} phone={phone_digits}", "create_demo_patient")
        return {"lead_name": lead.name, "phone": phone_digits}

    lead = frappe.new_doc("Lead")
    if hasattr(lead, "first_name"):
        lead.first_name = "Demo"
    if hasattr(lead, "last_name"):
        lead.last_name = "Patient"
    if company and hasattr(lead, "company"):
        lead.company = company
    if hasattr(lead, "mobile_no"):
        lead.mobile_no = phone_digits
    if hasattr(lead, "phone"):
        lead.phone = phone_digits
    _set_if_has_field(lead, "phone_number", phone_digits)
    _set_if_has_field(lead, "booking_status", "Pending")
    _set_if_has_field(lead, "ci_record_type", "lead")
    _apply_demo_source(lead)

    lead.insert(ignore_permissions=True)
    frappe.db.commit()
    _log_demo(f"demo_lead_created name={lead.name} phone={phone_digits}", "create_demo_patient")
    return {"lead_name": lead.name, "phone": phone_digits}


def send_demo_whatsapp_message_impl(lead_name: str | None = None) -> dict[str, Any]:
    """Send the structured demo prompt via existing send_whatsapp_message API."""
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)

    lead_name = str(lead_name or "").strip()
    if not lead_name or not frappe.db.exists("Lead", lead_name):
        frappe.throw(_("Valid lead_name is required"))

    ln = frappe.db.get_value("Lead", lead_name, "lead_name")
    if ln != DEMO_LEAD_NAME:
        frappe.throw(_("Demo messages can only be sent for the Demo Patient lead."))

    from call_intelligence.api import send_whatsapp_message

    out = send_whatsapp_message(
        message=DEMO_PROMPT_MESSAGE,
        phone="",
        reference_doctype="Lead",
        reference_name=lead_name,
    )

    lead = frappe.get_doc("Lead", lead_name)
    _set_if_has_field(lead, "whatsapp_flow_state", "awaiting_confirmation")
    lead.flags.ignore_permissions = True
    lead.save()
    frappe.db.commit()

    _log_demo(f"demo_prompt_sent lead={lead_name} ok={out.get('ok')}", "send_demo_whatsapp")
    return {"ok": bool(out.get("ok")), "lead_name": lead_name, "send_result": out}
