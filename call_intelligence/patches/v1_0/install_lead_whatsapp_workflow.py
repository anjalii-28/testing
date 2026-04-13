"""
Install Lead workflow for WhatsApp Yes/No follow-up (code-only, no Desk UI).

CRM-aligned workflow states:
  Open (default) --yes--> Interested
  Open --no--> Cold

Workflow field: workflow_state (created by Workflow if missing)

Legacy renames (WhatsApp Open → Open, etc.) are handled by
`migrate_lead_whatsapp_workflow_crm_states` patch.

Idempotent: safe to re-run.
"""

from __future__ import annotations

import frappe

WF_NAME = "Lead WhatsApp Follow-up"

STATE_OPEN = "Open"
STATE_INTERESTED = "Interested"
STATE_COLD = "Cold"

ACTION_YES = "yes"
ACTION_NO = "no"


def _ensure_workflow_state(name: str) -> None:
    if frappe.db.exists("Workflow State", name):
        return
    frappe.get_doc(
        {
            "doctype": "Workflow State",
            "workflow_state_name": name,
        }
    ).insert(ignore_permissions=True)


def _ensure_workflow_action(name: str) -> None:
    if frappe.db.exists("Workflow Action Master", name):
        return
    frappe.get_doc(
        {
            "doctype": "Workflow Action Master",
            "workflow_action_name": name,
        }
    ).insert(ignore_permissions=True)


def _deactivate_other_lead_workflows() -> None:
    frappe.db.sql(
        """
		update `tabWorkflow`
		set is_active = 0
		where document_type = %s and name != %s and is_active = 1
		""",
        ("Lead", WF_NAME),
    )


def _create_workflow() -> None:
    _deactivate_other_lead_workflows()

    wf = frappe.new_doc("Workflow")
    wf.workflow_name = WF_NAME
    wf.document_type = "Lead"
    wf.is_active = 1
    wf.workflow_state_field = "workflow_state"
    wf.override_status = 1
    wf.send_email_alert = 0

    wf.append(
        "states",
        {
            "state": STATE_OPEN,
            "doc_status": "0",
            "allow_edit": "All",
        },
    )
    wf.append(
        "states",
        {
            "state": STATE_INTERESTED,
            "doc_status": "0",
            "allow_edit": "All",
        },
    )
    wf.append(
        "states",
        {
            "state": STATE_COLD,
            "doc_status": "0",
            "allow_edit": "All",
        },
    )

    wf.append(
        "transitions",
        {
            "state": STATE_OPEN,
            "action": ACTION_YES,
            "next_state": STATE_INTERESTED,
            "allowed": "All",
            "allow_self_approval": 1,
        },
    )
    wf.append(
        "transitions",
        {
            "state": STATE_OPEN,
            "action": ACTION_NO,
            "next_state": STATE_COLD,
            "allowed": "All",
            "allow_self_approval": 1,
        },
    )

    wf.insert(ignore_permissions=True)


def execute() -> None:
    for s in (STATE_OPEN, STATE_INTERESTED, STATE_COLD):
        _ensure_workflow_state(s)
    _ensure_workflow_action(ACTION_YES)
    _ensure_workflow_action(ACTION_NO)

    if frappe.db.exists("Workflow", WF_NAME):
        return

    _create_workflow()
    frappe.db.commit()
