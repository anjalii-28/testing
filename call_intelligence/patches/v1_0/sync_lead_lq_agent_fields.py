"""Install Lead Qualification (AI) custom fields (ci_lq_*)."""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_all_custom_fields

    install_all_custom_fields()
    frappe.clear_cache(doctype="Lead")
