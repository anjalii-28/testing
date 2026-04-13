"""
Post-migrate patch: add WhatsApp booking fields (booking_status, whatsapp_priority)
to Lead and Issue.

NOTE: This patch file was not included in the original repository export.
Stub — re-runs install_all_custom_fields which includes these fields.
"""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_all_custom_fields

    install_all_custom_fields()
