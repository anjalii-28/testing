"""Idempotent minimal demo records (safe on repeated migrate)."""

from __future__ import annotations

import frappe

DEMO_LEAD_EMAIL = "ci.demo.lead@example.com"


def ensure_demo_data() -> None:
    """Insert a sample Lead when Company and ERPNext Lead exist."""
    if frappe.flags.in_install:
        return
    if frappe.db.exists("Lead", {"email_id": DEMO_LEAD_EMAIL}):
        return

    if not frappe.db.exists("DocType", "Lead"):
        return

    company = frappe.defaults.get_user_default("Company") or frappe.db.get_value("Company", {}, "name")
    if not company:
        return

    doc = frappe.get_doc(
        {
            "doctype": "Lead",
            "lead_name": "CI Demo Lead",
            "email_id": DEMO_LEAD_EMAIL,
            "status": "Open",
            "source": "Demo",
            "company": company,
        }
    )
    try:
        doc.insert(ignore_permissions=True)
        frappe.db.commit()
    except Exception:
        frappe.log_error(frappe.get_traceback(), "call_intelligence: demo Lead insert skipped")
