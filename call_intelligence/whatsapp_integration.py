"""
WhatsApp integration layer: Cloud API + Twilio fallback.

IMPORTANT: This module was not included in the original repository export.
Implement each function below to restore full WhatsApp functionality.

The module is imported by `api.py` for both outbound sending and inbound routing.

Environment / site-config keys expected:
  call_intelligence_whatsapp_cloud_token         WhatsApp Cloud API bearer token
  call_intelligence_whatsapp_cloud_phone_id      Sending phone number ID (Meta)
  call_intelligence_whatsapp_cloud_verify_token  Webhook verification token
  call_intelligence_whatsapp_admin_number        E.164 destination for test sends
  call_intelligence_twilio_account_sid           Twilio account SID (fallback)
  call_intelligence_twilio_auth_token            Twilio auth token (fallback)
  call_intelligence_twilio_from_number           Twilio WhatsApp sender number
  call_intelligence_whatsapp_test_mode           "1" to enable test mode

All keys live in site_config.json (set via bench set-config or directly).
See legacy/configs/example.site_config.json for a template.
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def is_cloud_configured() -> bool:
    """Return True when WhatsApp Cloud API credentials are present in site config."""
    raise NotImplementedError("Implement is_cloud_configured in whatsapp_integration.py")


def is_twilio_configured() -> bool:
    """Return True when Twilio credentials are present in site config."""
    raise NotImplementedError("Implement is_twilio_configured in whatsapp_integration.py")


def get_whatsapp_test_mode() -> bool:
    """Return True when call_intelligence_whatsapp_test_mode == '1' in site config."""
    raise NotImplementedError("Implement get_whatsapp_test_mode in whatsapp_integration.py")


def get_admin_destination_number() -> str:
    """
    Return the E.164 admin/test destination number from site config.

    Used as the fixed recipient for outbound sends during development / testing.
    """
    raise NotImplementedError("Implement get_admin_destination_number in whatsapp_integration.py")


# ---------------------------------------------------------------------------
# Outbound sending
# ---------------------------------------------------------------------------

def send_whatsapp_message_impl(
    message: str,
    reference_doctype: str,
    reference_name: str,
) -> dict[str, Any]:
    """
    Send a WhatsApp message via Twilio and return a result dict with keys:
      ok (bool), destination (str), response (dict)

    Args:
        message: Plain-text message body.
        reference_doctype: Frappe DocType the message is linked to (e.g. "Lead").
        reference_name: Document name (e.g. "CRM-LEAD-2025-00001").
    """
    raise NotImplementedError("Implement send_whatsapp_message_impl in whatsapp_integration.py")


def send_whatsapp_cloud_text_with_fallback(
    message: str,
    to_e164: str,
    reference_doctype: str,
    reference_name: str,
) -> dict[str, Any]:
    """
    Attempt to send a free-form text message via WhatsApp Cloud API.
    Fall back to an approved template when the 24-hour window is closed.

    Returns a dict with keys:
      ok (bool), provider (str), destination (str), response (dict),
      fallback (bool, optional), text_attempt_failed (bool, optional),
      error_hint (str, optional)
    """
    raise NotImplementedError(
        "Implement send_whatsapp_cloud_text_with_fallback in whatsapp_integration.py"
    )


# ---------------------------------------------------------------------------
# Inbound routing (Twilio)
# ---------------------------------------------------------------------------

def process_inbound_twilio_webhook(payload: dict[str, Any]) -> None:
    """
    Process an inbound Twilio WhatsApp webhook payload.

    Args:
        payload: Form-encoded dict from Twilio (frappe.local.form_dict).
    """
    raise NotImplementedError(
        "Implement process_inbound_twilio_webhook in whatsapp_integration.py"
    )


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def list_whatsapp_communications_for_lead(lead_name: str, limit: int = 80) -> list[dict[str, Any]]:
    """
    Return a list of WhatsApp Communication records linked to the given Lead.

    Each dict should contain at minimum: name, creation, content, direction.

    Args:
        lead_name: Lead name (e.g. CRM-LEAD-...).
        limit: Max rows to return (clamped by callers of the whitelisted API).
    """
    # Stub: return empty so Patient 360 and desk APIs load; replace with a Communication query when ready.
    return []
