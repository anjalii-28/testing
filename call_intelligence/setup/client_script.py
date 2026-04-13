"""
Install Client Scripts: Lead → Patient 360; Issue → Lead / Patient 360.
Idempotent: upsert by name or by marker in script body.
"""

import textwrap

import frappe

MODULE = "Call Intelligence"

# Keep stable name so migrate updates the same Client Script row (no duplicates).
LEAD_SCRIPT_NAME = "Call Intelligence Lead Create Ticket"
ISSUE_SCRIPT_NAME = "Call Intelligence Issue Lead Navigation"

LEAD_PATIENT_360_SCRIPT = textwrap.dedent(
    """
	frappe.ui.form.on('Lead', {
		refresh(frm) {
			if (frm.is_new()) {
				return;
			}
			frm.add_custom_button(__('Open Patient 360'), function () {
				frappe.route_options = frappe.route_options || {};
				frappe.route_options.lead_name = frm.doc.name;
				frappe.set_route('patient-360');
			}, __('Patient 360'));
			frm.add_custom_button(__('Create Ticket'), function () {
				frappe.call({
					method: 'call_intelligence.api.create_issue',
					args: {
						lead_id: frm.doc.name,
					},
					freeze: true,
					freeze_message: __('Creating Issue...'),
					callback(r) {
						if (!r.exc && r.message && r.message.name) {
							frappe.set_route('Form', 'Issue', r.message.name);
						}
					},
				});
			}, __('Actions'));
		},
	});
	"""
).strip()

ISSUE_LEAD_NAV_SCRIPT = textwrap.dedent(
    """
	frappe.ui.form.on('Issue', {
		refresh(frm) {
			const lead = frm.doc.custom_lead;
			if (!lead) {
				return;
			}
			frm.add_custom_button(__('Open Patient 360'), function () {
				frappe.route_options = frappe.route_options || {};
				frappe.route_options.lead_name = lead;
				frappe.set_route('patient-360');
			}, __('Lead'));
			frm.add_custom_button(__('Open Lead'), function () {
				frappe.set_route('Form', 'Lead', lead);
			}, __('Lead'));
		},
	});
	"""
).strip()


def _upsert_client_script(
    *,
    name: str,
    dt: str,
    script: str,
    marker: str,
) -> None:
    if frappe.db.exists("Client Script", name):
        doc = frappe.get_doc("Client Script", name)
        doc.script = script
        doc.enabled = 1
        doc.view = "Form"
        doc.dt = dt
        doc.module = MODULE
        doc.save(ignore_permissions=True)
        return

    for row in frappe.get_all(
        "Client Script",
        filters={"dt": dt, "module": MODULE},
        fields=["name", "script"],
    ):
        if row.script and marker in row.script:
            doc = frappe.get_doc("Client Script", row.name)
            doc.script = script
            doc.enabled = 1
            doc.view = "Form"
            doc.save(ignore_permissions=True)
            return

    frappe.get_doc(
        {
            "doctype": "Client Script",
            "name": name,
            "dt": dt,
            "view": "Form",
            "enabled": 1,
            "module": MODULE,
            "script": script,
        }
    ).insert(ignore_permissions=True)


def install_lead_patient_360_script() -> None:
    _upsert_client_script(
        name=LEAD_SCRIPT_NAME,
        dt="Lead",
        script=LEAD_PATIENT_360_SCRIPT,
        marker="call_intelligence.api.create_issue",
    )


def install_issue_lead_navigation_script() -> None:
    _upsert_client_script(
        name=ISSUE_SCRIPT_NAME,
        dt="Issue",
        script=ISSUE_LEAD_NAV_SCRIPT,
        marker="patient-360",
    )


# Backwards compatibility for patches that still import the old name
def install_lead_create_ticket_script() -> None:
    install_lead_patient_360_script()
