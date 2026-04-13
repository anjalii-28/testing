"""Add Communication custom fields for WhatsApp rich media (image / document) logging."""

import frappe


def execute():
    from call_intelligence.setup.custom_fields import install_communication_whatsapp_media_fields

    install_communication_whatsapp_media_fields()
