"""Add Patient 360 / AI JSON mirror fields (runs install_all_custom_fields)."""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_all_custom_fields

    install_all_custom_fields()
