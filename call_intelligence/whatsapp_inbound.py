"""
WhatsApp Cloud inbound webhook handler.

IMPORTANT: This module was not included in the original repository export.
You must implement `process_inbound_whatsapp_cloud_webhook` for WhatsApp inbound
functionality to work. The function is called by `api.whatsapp_cloud_webhook`
on every inbound POST from Meta's WhatsApp Cloud API.

Expected behaviour:
  - Parse the Meta webhook payload (entry → changes → value → messages)
  - For each inbound message, find or create the matching Lead by phone number
  - Store the message as a Communication linked to the Lead
  - Update `booking_status` or `whatsapp_flow_state` when keyword replies are
    detected (e.g. "confirm", "cancel", "reschedule")

See Meta's WhatsApp Business API documentation for payload shape:
  https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/payload-examples

Replace this stub with your real implementation before deploying.
"""

from __future__ import annotations

from typing import Any


def process_inbound_whatsapp_cloud_webhook(payload: dict[str, Any]) -> None:
    """
    Process an inbound WhatsApp Cloud webhook payload.

    Args:
        payload: Parsed JSON body sent by Meta to the webhook endpoint.

    Raises:
        NotImplementedError: This stub must be replaced with a real implementation.
    """
    raise NotImplementedError(
        "whatsapp_inbound.process_inbound_whatsapp_cloud_webhook is not implemented. "
        "See call_intelligence/whatsapp_inbound.py for instructions."
    )
