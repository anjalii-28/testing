"""
Programmatic Custom Field definitions for Lead and Issue.
Used by patches for idempotent field sync; Custom Field rows also ship in fixtures.

insert_after: targets ERPNext CRM Lead / Support Issue. Adjust if your site differs.
"""

import frappe

# First custom block attaches after this standard Lead field (ERPNext CRM).
LEAD_INSERT_AFTER = "lead_owner"
# Issue: attach after subject (Support Issue).
ISSUE_INSERT_AFTER = "subject"

MODULE = "Call Intelligence"


def get_lead_custom_fields():
    """Section breaks + Patient 360 / call intelligence fields on Lead."""
    return [
        {
            "fieldname": "call_data_section",
            "label": "Call Data",
            "fieldtype": "Section Break",
            "insert_after": LEAD_INSERT_AFTER,
            "collapsible": 1,
        },
        {
            "fieldname": "phone_number",
            "label": "Phone Number",
            "fieldtype": "Data",
            "insert_after": "call_data_section",
            "reqd": 0,
            "description": "Normalized digits-only phone number for call matching (WhatsApp-ready).",
        },
        {
            "fieldname": "call_id",
            "label": "Call ID",
            "fieldtype": "Data",
            "insert_after": "phone_number",
        },
        {
            "fieldname": "call_time",
            "label": "Call Time",
            "fieldtype": "Datetime",
            "insert_after": "call_id",
        },
        {
            "fieldname": "call_timestamp",
            "label": "Call Timestamp",
            "fieldtype": "Datetime",
            "insert_after": "call_time",
            "description": "Canonical timestamp field for call records (used by Call Log).",
        },
        {
            "fieldname": "transcript",
            "label": "Transcript",
            "fieldtype": "Long Text",
            "insert_after": "call_timestamp",
        },
        {
            "fieldname": "chatwoot_conversation_id",
            "label": "Chatwoot Conversation ID",
            "fieldtype": "Data",
            "insert_after": "transcript",
            "description": "Reserved for Chatwoot / omnichannel integration.",
        },
        {
            "fieldname": "ai_insights_section",
            "label": "AI Insights",
            "fieldtype": "Section Break",
            "insert_after": "chatwoot_conversation_id",
            "collapsible": 1,
        },
        {
            "fieldname": "sentiment",
            "label": "Sentiment",
            "fieldtype": "Select",
            "options": "Positive\nNegative\nNeutral",
            "insert_after": "ai_insights_section",
        },
        {
            "fieldname": "intent",
            "label": "Intent",
            "fieldtype": "Data",
            "insert_after": "sentiment",
        },
        {
            "fieldname": "priority_score",
            "label": "Priority Score",
            "fieldtype": "Float",
            "insert_after": "intent",
        },
        {
            "fieldname": "outcome_section",
            "label": "Outcome",
            "fieldtype": "Section Break",
            "insert_after": "priority_score",
            "collapsible": 1,
        },
        {
            "fieldname": "outcome",
            "label": "Outcome",
            "fieldtype": "Select",
            "options": "BOOKED\nNOT\nPENDING\nUNKNOWN",
            "insert_after": "outcome_section",
        },
        {
            "fieldname": "lead_status",
            "label": "Lead Status",
            "fieldtype": "Select",
            "options": "Lead\nFollow-up Required\nConfirmed\nOpportunity",
            "insert_after": "outcome",
            "default": "Lead",
            "description": "Custom call-level status (separate from ERPNext Lead.status).",
        },
        {
            "fieldname": "booking_status",
            "label": "Booking Status",
            "fieldtype": "Select",
            "options": "Pending\nConfirmed\nCancelled\nReschedule Requested",
            "insert_after": "lead_status",
            "default": "Pending",
            "description": "Updated by WhatsApp keyword replies (confirm/cancel/reschedule).",
        },
        {
            "fieldname": "whatsapp_priority",
            "label": "WhatsApp Priority",
            "fieldtype": "Select",
            "options": "\nLow\nMedium\nHigh",
            "insert_after": "booking_status",
            "description": "Set when patient requests a call via WhatsApp.",
        },
        {
            "fieldname": "whatsapp_flow_state",
            "label": "WhatsApp Flow State",
            "fieldtype": "Select",
            "options": "\nawaiting_confirmation\nconfirmed\ncancelled\nreschedule_requested",
            "insert_after": "whatsapp_priority",
            "description": "Interactive WhatsApp demo / booking flow state.",
        },
        {
            "fieldname": "appointment_date",
            "label": "Appointment Date",
            "fieldtype": "Date",
            "insert_after": "outcome",
        },
        {
            "fieldname": "ci_record_type",
            "label": "AI Record Type",
            "fieldtype": "Data",
            "insert_after": "appointment_date",
            "description": "From call JSON recordType (lead / ticket). Patient 360 display only.",
        },
        {
            "fieldname": "ci_doctor",
            "label": "AI Doctor",
            "fieldtype": "Data",
            "insert_after": "ci_record_type",
        },
        {
            "fieldname": "ci_ai_department",
            "label": "AI Department",
            "fieldtype": "Data",
            "insert_after": "ci_doctor",
        },
        {
            "fieldname": "ci_ai_location",
            "label": "AI Location",
            "fieldtype": "Data",
            "insert_after": "ci_ai_department",
        },
        {
            "fieldname": "ci_services",
            "label": "AI Services",
            "fieldtype": "Data",
            "insert_after": "ci_ai_location",
        },
        {
            "fieldname": "ci_sentiment_summary",
            "label": "AI Sentiment Summary",
            "fieldtype": "Text",
            "insert_after": "ci_services",
        },
        {
            "fieldname": "ci_ai_summary",
            "label": "AI Summary",
            "fieldtype": "Text",
            "insert_after": "ci_sentiment_summary",
        },
        {
            "fieldname": "ci_call_solution",
            "label": "AI Call Solution",
            "fieldtype": "Long Text",
            "insert_after": "ci_ai_summary",
        },
        {
            "fieldname": "ci_action_required",
            "label": "AI Action Required",
            "fieldtype": "Data",
            "insert_after": "ci_call_solution",
        },
        {
            "fieldname": "ci_action_description",
            "label": "AI Action Description",
            "fieldtype": "Text",
            "insert_after": "ci_action_required",
        },
        {
            "fieldname": "ci_lead_notes",
            "label": "AI Lead Notes",
            "fieldtype": "Long Text",
            "insert_after": "ci_action_description",
        },
    ]


def get_issue_custom_fields():
    """
    Sectioned call-intelligence fields on Issue (mirrors structured call JSON).
    Standard Issue already has: priority (Issue Priority link), customer_name, lead — avoid those fieldnames.
    """
    return [
        {
            "fieldname": "ci_section_call_info",
            "label": "Call Info",
            "fieldtype": "Section Break",
            "insert_after": ISSUE_INSERT_AFTER,
            "collapsible": 1,
        },
        {
            "fieldname": "custom_lead",
            "label": "Lead",
            "fieldtype": "Link",
            "options": "Lead",
            "insert_after": "ci_section_call_info",
            "description": "Linked CRM Lead (same phone); also set standard Issue.lead in API when present.",
        },
        {
            "fieldname": "ci_call_id",
            "label": "Call ID",
            "fieldtype": "Data",
            "insert_after": "custom_lead",
            "unique": 1,
            "description": "External call id; de-duplication key when set.",
        },
        {
            "fieldname": "ci_phone_number",
            "label": "Phone Number",
            "fieldtype": "Data",
            "insert_after": "ci_call_id",
        },
        {
            "fieldname": "ci_customer_name",
            "label": "Customer Name",
            "fieldtype": "Data",
            "insert_after": "ci_phone_number",
        },
        {
            "fieldname": "ci_call_classification",
            "label": "Call Classification",
            "fieldtype": "Select",
            "options": "Enquiry\nDiscussion\nComplaint\nFollow-up",
            "insert_after": "ci_customer_name",
        },
        {
            "fieldname": "ci_ticket_type",
            "label": "Call Intelligence Ticket Type",
            "fieldtype": "Select",
            "options": "Appointment Booking\nFollow-up\nOther",
            "insert_after": "ci_call_classification",
            "default": "Other",
        },
        {
            "fieldname": "booking_status",
            "label": "Booking Status",
            "fieldtype": "Select",
            "options": "Pending\nConfirmed\nCancelled\nReschedule Requested",
            "insert_after": "ci_ticket_type",
            "default": "Pending",
            "description": "Updated by WhatsApp keyword replies.",
        },
        {
            "fieldname": "ci_filename",
            "label": "Filename",
            "fieldtype": "Data",
            "insert_after": "booking_status",
        },
        {
            "fieldname": "ci_call_timestamp",
            "label": "Timestamp",
            "fieldtype": "Datetime",
            "insert_after": "ci_filename",
        },
        {
            "fieldname": "ci_section_action",
            "label": "Action Required",
            "fieldtype": "Section Break",
            "insert_after": "ci_call_timestamp",
            "collapsible": 1,
        },
        {
            "fieldname": "ci_action_required",
            "label": "Action Required",
            "fieldtype": "Select",
            "options": "Yes\nNo",
            "insert_after": "ci_section_action",
        },
        {
            "fieldname": "ci_action_description",
            "label": "Action Description",
            "fieldtype": "Text",
            "insert_after": "ci_action_required",
        },
        {
            "fieldname": "ci_section_medical",
            "label": "Medical Context",
            "fieldtype": "Section Break",
            "insert_after": "ci_action_description",
            "collapsible": 1,
        },
        {
            "fieldname": "ci_department_to_handle",
            "label": "Department To Handle",
            "fieldtype": "Data",
            "insert_after": "ci_section_medical",
        },
        {
            "fieldname": "ci_department",
            "label": "Department",
            "fieldtype": "Data",
            "insert_after": "ci_department_to_handle",
        },
        {
            "fieldname": "ci_doctor_name",
            "label": "Doctor Name",
            "fieldtype": "Data",
            "insert_after": "ci_department",
        },
        {
            "fieldname": "ci_priority_level",
            "label": "Priority",
            "fieldtype": "Select",
            "options": "Low\nMedium\nHigh",
            "insert_after": "ci_doctor_name",
            "description": "Call-intent priority (not the same as ERPNext Issue Priority link above).",
        },
        {
            "fieldname": "ci_section_ai",
            "label": "AI Insights",
            "fieldtype": "Section Break",
            "insert_after": "ci_priority_level",
            "collapsible": 1,
        },
        {
            "fieldname": "ci_sentiment_label",
            "label": "Sentiment",
            "fieldtype": "Select",
            "options": "Positive\nNeutral\nNegative",
            "insert_after": "ci_section_ai",
        },
        {
            "fieldname": "ci_sentiment_summary",
            "label": "Sentiment Summary",
            "fieldtype": "Text",
            "insert_after": "ci_sentiment_label",
        },
        {
            "fieldname": "ci_outcome",
            "label": "Outcome",
            "fieldtype": "Select",
            "options": "Escalated\nResolved\nUnknown",
            "insert_after": "ci_sentiment_summary",
            "description": "Ticket-level outcome (call pipeline outcomes are mapped into these).",
        },
        {
            "fieldname": "ci_follow_up_required",
            "label": "Follow Up Required",
            "fieldtype": "Check",
            "insert_after": "ci_outcome",
            "default": "0",
        },
        {
            "fieldname": "ci_section_transcript",
            "label": "Transcript",
            "fieldtype": "Section Break",
            "insert_after": "ci_follow_up_required",
            "collapsible": 1,
        },
        {
            "fieldname": "ci_transcript",
            "label": "Transcript",
            "fieldtype": "Long Text",
            "insert_after": "ci_section_transcript",
        },
        {
            "fieldname": "ci_call_solution",
            "label": "Call Solution",
            "fieldtype": "Long Text",
            "insert_after": "ci_transcript",
        },
        {
            "fieldname": "ci_ticket_notes",
            "label": "Ticket Notes",
            "fieldtype": "Long Text",
            "insert_after": "ci_call_solution",
        },
    ]


def ensure_custom_field(dt: str, field_def: dict) -> None:
    """
    Idempotent: create or update Custom Field for DocType dt.
    """
    fieldname = field_def["fieldname"]
    existing = frappe.db.get_value(
        "Custom Field",
        {"dt": dt, "fieldname": fieldname},
        "name",
    )
    row = {
        "doctype": "Custom Field",
        "dt": dt,
        "module": MODULE,
        **field_def,
    }
    if existing:
        doc = frappe.get_doc("Custom Field", existing)
        doc.update(field_def)
        doc.save(ignore_permissions=True)
    else:
        # Avoid clashing with standard DocType fields (not stored as Custom Field rows).
        if frappe.get_meta(dt).has_field(fieldname):
            return
        doc = frappe.get_doc(row)
        doc.insert(ignore_permissions=True)


def resolve_issue_insert_after() -> str:
    meta = frappe.get_meta("Issue")
    names = {f.fieldname for f in meta.fields}
    if ISSUE_INSERT_AFTER in names:
        return ISSUE_INSERT_AFTER
    for candidate in ("customer", "status", "raised_by"):
        if candidate in names:
            return candidate
    return meta.fields[-1].fieldname if meta.fields else "name"


def install_all_custom_fields() -> None:
    """Apply all Lead + Issue custom fields."""
    lead_anchor = resolve_lead_insert_after()
    lead_fields = get_lead_custom_fields()
    if lead_fields and lead_fields[0].get("fieldname") == "call_data_section":
        lead_fields[0]["insert_after"] = lead_anchor

    issue_anchor = resolve_issue_insert_after()
    issue_fields = get_issue_custom_fields()
    if issue_fields and issue_fields[0].get("fieldname") == "call_intelligence_section":
        issue_fields[0]["insert_after"] = issue_anchor

    for fd in lead_fields:
        ensure_custom_field("Lead", fd)
    for fd in issue_fields:
        ensure_custom_field("Issue", fd)
    frappe.clear_cache(doctype="Lead")
    frappe.clear_cache(doctype="Issue")


def resolve_lead_insert_after() -> str:
    """
    If lead_owner is missing (customized site), fall back to a safe field.
    """
    meta = frappe.get_meta("Lead")
    names = {f.fieldname for f in meta.fields}
    if LEAD_INSERT_AFTER in names:
        return LEAD_INSERT_AFTER
    for candidate in ("mobile_no", "phone", "status", "source"):
        if candidate in names:
            return candidate
    return meta.fields[-1].fieldname if meta.fields else "name"
