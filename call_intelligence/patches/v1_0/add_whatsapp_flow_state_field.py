"""
Post-migrate patch: add whatsapp_flow_state field to Lead
(interactive WhatsApp booking flow state machine).

NOTE: This patch file was not included in the original repository export.
Stub — re-runs install_all_custom_fields which includes whatsapp_flow_state.
"""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_all_custom_fields

    install_all_custom_fields()
