"""
Communication doctype — Desk field "Type" is `communication_medium`.
Values like "WhatsApp" are not in Frappe's allowed list; coerce before validate/insert.
"""

from __future__ import annotations

import frappe


def sanitize_communication_medium(doc, event: str | None = None) -> None:
    """
    If anything sets communication_medium to WhatsApp, map to Chat (or first valid option).
    Runs on validate (insert + save) so all callers are covered.
    """
    m = getattr(doc, "communication_medium", None)
    if m is None:
        return
    s = str(m).strip()
    if s.lower() != "whatsapp":
        return
    try:
        meta = frappe.get_meta("Communication")
        if not meta.has_field("communication_medium"):
            return
        raw = meta.get_field("communication_medium").options or ""
        opts = [x.strip() for x in raw.split("\n") if x.strip()]
        opts = [o for o in opts if o.lower() != "whatsapp"]
        for pref in ("Chat", "SMS", "Phone", "Other", "Email"):
            if pref in opts:
                doc.communication_medium = pref
                return
        doc.communication_medium = opts[0] if opts else ""
    except Exception:
        doc.communication_medium = "Chat"
