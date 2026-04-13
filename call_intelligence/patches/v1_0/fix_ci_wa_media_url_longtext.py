"""
Widen Communication.ci_wa_media_url: default Data fields are VARCHAR(140) and break long CDN URLs.

Frappe does not allow changing Custom Field from Data → Long Text. Keep fieldtype Data and set
``length`` to 1000 (Frappe's maximum for Data/VARCHAR), then widen the database column.
"""

import frappe

_MEDIA_URL_DB_LEN = 1000


def execute():
    from call_intelligence.setup.custom_fields import ensure_custom_field, get_communication_whatsapp_media_fields

    name = frappe.db.get_value(
        "Custom Field",
        {"dt": "Communication", "fieldname": "ci_wa_media_url"},
        "name",
    )
    if name:
        doc = frappe.get_doc("Custom Field", name)
        if doc.fieldtype == "Data":
            doc.length = _MEDIA_URL_DB_LEN
            doc.save(ignore_permissions=True)
    else:
        for fd in get_communication_whatsapp_media_fields():
            if fd.get("fieldname") == "ci_wa_media_url":
                ensure_custom_field("Communication", fd)
                break

    frappe.clear_cache(doctype="Communication")

    if not frappe.db.has_column("Communication", "ci_wa_media_url"):
        return

    try:
        if frappe.db.db_type == "mariadb":
            frappe.db.sql_ddl(
                f"ALTER TABLE `tabCommunication` MODIFY COLUMN `ci_wa_media_url` VARCHAR({_MEDIA_URL_DB_LEN})"
            )
        elif frappe.db.db_type == "postgres":
            frappe.db.sql_ddl(
                f'ALTER TABLE "tabCommunication" ALTER COLUMN "ci_wa_media_url" TYPE VARCHAR({_MEDIA_URL_DB_LEN})'
            )
    except Exception:
        frappe.log_error(
            title="fix_ci_wa_media_url_longtext: ALTER optional",
            message=frappe.get_traceback(),
        )
