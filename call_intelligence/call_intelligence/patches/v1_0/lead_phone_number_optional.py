"""
Post-migrate patch: make Lead phone_number custom field optional (reqd=0).

NOTE: This patch file was not included in the original repository export.
Stub — re-runs install_all_custom_fields which already sets reqd=0 for phone_number.
"""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_all_custom_fields

    install_all_custom_fields()
