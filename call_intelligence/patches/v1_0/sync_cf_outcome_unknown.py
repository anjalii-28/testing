"""Refresh Lead custom fields (adds UNKNOWN to Outcome select)."""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_all_custom_fields

    install_all_custom_fields()
