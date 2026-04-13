"""
Call Intelligence — hooks, fixtures, doc events.
"""

from . import __version__ as app_version

app_name = "call_intelligence"
app_title = "Call Intelligence"
app_publisher = "Call Intelligence"
app_description = "Call Intelligence + Patient 360 CRM (Leads, Issues, AI ingestion)"
app_email = "support@example.com"
app_license = "MIT"
app_version = app_version

# Required apps (Lead/Issue live in ERPNext stack)
required_apps = ["erpnext"]

# Loaded on `bench migrate` — order: charts/cards → dashboard → workspace shell.
# Re-export with: bench --site <site> export-fixtures --app call_intelligence
fixtures = [
    {"doctype": "Dashboard Chart", "filters": [["module", "=", "Call Intelligence"]]},
    {"doctype": "Number Card", "filters": [["module", "=", "Call Intelligence"]]},
    {"doctype": "Dashboard", "filters": [["module", "=", "Call Intelligence"]]},
    {"doctype": "Workspace", "filters": [["name", "=", "Call Intelligence"]]},
    {"doctype": "Workspace Sidebar", "filters": [["name", "=", "Call Intelligence"]]},
    {"doctype": "Custom Field", "filters": [["module", "=", "Call Intelligence"]]},
    {"doctype": "Client Script", "filters": [["module", "=", "Call Intelligence"]]},
]

after_migrate = ["call_intelligence.setup.post_migrate.run"]

doc_events = {
    "Lead": {
        "after_insert": "call_intelligence.webhooks.on_lead_after_insert",
    },
}

# Lead list: row link → Patient 360 (see public/js/lead_list_patient360.js)
doctype_list_js = {"Lead": "public/js/lead_list_patient360.js"}

# Desk shell aligned with default Frappe light theme — see public/css/custom.css
app_include_css = ["/assets/call_intelligence/css/custom.css"]

# Sidebar styling + section headers (MAIN / CRM / SYSTEM) when Workspace Sidebar is available.
app_include_js = [
    "/assets/call_intelligence/js/ci_desk_sidebar_css.js",
    "/assets/call_intelligence/js/ci_desk_sidebar_flat_sections.js",
]

# Patient 360 Dashboard: shared parsers/renderers (page CSS must live in page/*.css — Frappe ignores page_css hooks)
page_js = {"patient-360-dashboard": "public/js/patient_chat.js"}

# Future: SLA / lifecycle hooks on Issue
# doc_events["Issue"] = {"on_update": "call_intelligence.hooks_issue.on_issue_update"}
