"""Post-migrate: desk icons, optional Workspace Sidebar scope fix, demo data."""

from __future__ import annotations

import frappe


def run() -> None:
    _ensure_workspace_sidebar_global()
    _refresh_desk_icons()
    from call_intelligence.setup import demo_data

    demo_data.ensure_demo_data()


def _ensure_workspace_sidebar_global() -> None:
    name = "Call Intelligence"
    if not frappe.db.exists("Workspace Sidebar", name):
        return
    if not frappe.db.has_column("Workspace Sidebar", "for_user"):
        return
    frappe.db.set_value("Workspace Sidebar", name, "for_user", None)


def _refresh_desk_icons() -> None:
    try:
        from frappe.desk.doctype.desktop_icon.desktop_icon import (
            clear_desktop_icons_cache,
            create_desktop_icons_from_workspace,
        )

        create_desktop_icons_from_workspace()
        clear_desktop_icons_cache()
    except Exception:
        frappe.log_error(frappe.get_traceback(), "call_intelligence: create_desktop_icons_from_workspace")
    frappe.clear_cache()
