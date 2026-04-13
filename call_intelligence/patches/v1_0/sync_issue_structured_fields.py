"""
Post-migrate patch: sync structured AI fields on existing Issue records.

NOTE: This patch file was not included in the original repository export.
Stub — re-runs install_all_custom_fields to ensure structured fields are present.
"""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_all_custom_fields

    install_all_custom_fields()
