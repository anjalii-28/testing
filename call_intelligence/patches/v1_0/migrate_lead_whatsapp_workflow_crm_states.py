"""
One-time migration: WhatsApp Open/Interested/Cold → Open/Interested/Cold on Lead workflow.

Safe to re-run (idempotent).
"""

from __future__ import annotations

import frappe

WF_NAME = "Lead WhatsApp Follow-up"

STATE_OPEN = "Open"
STATE_INTERESTED = "Interested"
STATE_COLD = "Cold"

LEGACY_MAP = {
    "WhatsApp Open": STATE_OPEN,
    "WhatsApp Interested": STATE_INTERESTED,
    "WhatsApp Cold": STATE_COLD,
}


def _ensure_states() -> None:
    for name in (STATE_OPEN, STATE_INTERESTED, STATE_COLD):
        if not frappe.db.exists("Workflow State", name):
            frappe.get_doc({"doctype": "Workflow State", "workflow_state_name": name}).insert(
                ignore_permissions=True
            )


def _migrate() -> None:
    if not frappe.db.exists("Workflow", WF_NAME):
        return

    wf = frappe.get_doc("Workflow", WF_NAME)
    changed = False
    for row in wf.states:
        if row.state in LEGACY_MAP:
            row.state = LEGACY_MAP[row.state]
            changed = True
    for row in wf.transitions:
        if row.state in LEGACY_MAP:
            row.state = LEGACY_MAP[row.state]
            changed = True
        if row.next_state in LEGACY_MAP:
            row.next_state = LEGACY_MAP[row.next_state]
            changed = True
    if changed:
        wf.save(ignore_permissions=True)

    for old, new in LEGACY_MAP.items():
        frappe.db.sql(
            "update `tabLead` set workflow_state=%s where workflow_state=%s",
            (new, old),
        )


def execute() -> None:
    _ensure_states()
    _migrate()
    frappe.db.commit()
