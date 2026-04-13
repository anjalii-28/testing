"""
Whitelisted helpers: WhatsApp gateway → Lead workflow via frappe.model.workflow.apply_workflow only.

Do not set Lead.status or Lead.workflow_state directly — use workflow actions.
"""

from __future__ import annotations

import re
from typing import Any

import frappe
from frappe import _

ACTION_YES = "yes"
ACTION_NO = "no"


def _digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def _apply_workflow_action_impl(doctype: str, docname: str, action: str) -> dict[str, Any]:
    from frappe.model.workflow import WorkflowTransitionError, apply_workflow

    doc = frappe.get_doc(doctype, docname)
    try:
        apply_workflow(doc, action)
    except WorkflowTransitionError as e:
        return {"ok": False, "error": "transition_not_allowed", "detail": str(e)}
    except Exception:
        frappe.log_error(title="apply_workflow_action")
        return {"ok": False, "error": "apply_failed", "detail": frappe.get_traceback()}

    doc.reload()
    # Sync Lead.status with workflow_state so list views reflect the change.
    # Some UIs show "status" column, not workflow_state.
    if doctype == "Lead":
        try:
            meta = frappe.get_meta("Lead")
            if meta.has_field("status") and meta.has_field("workflow_state"):
                ws = str(doc.get("workflow_state") or "").strip()
                if ws:
                    doc.status = ws
                    doc.save(ignore_permissions=True)
                    doc.reload()
        except Exception:
            frappe.log_error(title="apply_workflow_action: sync status", message=frappe.get_traceback())
    return {
        "ok": True,
        "doctype": doctype,
        "name": docname,
        "workflow_state": doc.get("workflow_state"),
        "action": action,
    }


@frappe.whitelist(allow_guest=True)
def apply_workflow_action(doctype, docname, action):
    """
    Run a workflow action on a document (uses apply_workflow — no direct field writes).

    POST body example:
    {"doctype": "Lead", "docname": "CRM-LEAD-00001", "action": "yes"}
    """
    frappe.set_user("Administrator")  # bypass permission

    dt = str(doctype or "").strip()
    dn = str(docname or "").strip()
    act = str(action or "").strip()
    if not dt or not dn or not act:
        return {"ok": False, "error": "missing_args"}

    if not frappe.db.exists(dt, dn):
        return {"ok": False, "error": "doc_not_found"}

    return _apply_workflow_action_impl(dt, dn, act)


@frappe.whitelist()
def find_lead_name_by_phone(phone: str | None = None) -> dict[str, Any]:
    """
    Resolve latest Lead by phone / mobile (digits-normalized).
    Returns { "ok", "lead_name"? , "error"? }
    """
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)

    d = _digits(str(phone or ""))
    if len(d) < 7:
        return {"ok": False, "error": "invalid_phone"}

    leads = frappe.get_all(
        "Lead",
        fields=["name", "phone", "mobile_no", "modified"],
        order_by="modified desc",
        limit=50,
    )
    for row in leads:
        mp = _digits(str(row.get("mobile_no") or ""))
        pp = _digits(str(row.get("phone") or ""))
        if d == mp or d == pp or (len(d) >= 10 and (mp.endswith(d) or pp.endswith(d))):
            return {"ok": True, "lead_name": row.name}

    return {"ok": False, "error": "lead_not_found"}


@frappe.whitelist()
def apply_lead_whatsapp_button(lead_name: str | None = None, button_id: str | None = None) -> dict[str, Any]:
    """Map Meta button id (yes/no) to workflow actions — delegates to apply_workflow_action."""
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)

    lid = str(lead_name or "").strip()
    bid = str(button_id or "").strip().lower()
    if not lid or not frappe.db.exists("Lead", lid):
        return {"ok": False, "error": "invalid_lead"}

    if bid not in (ACTION_YES, ACTION_NO):
        return {"ok": False, "error": "invalid_button"}

    action = ACTION_YES if bid == ACTION_YES else ACTION_NO
    return _apply_workflow_action_impl("Lead", lid, action)
