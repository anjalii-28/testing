# Copyright (c) 2025, Call Intelligence and contributors
# See license.txt

from frappe.tests.utils import FrappeTestCase

from call_intelligence.whatsapp_integration import normalize_user_reply


class TestWhatsAppNormalizeUserReply(FrappeTestCase):
    def test_interactive_button_id_yes_no(self):
        msg = {
            "type": "interactive",
            "interactive": {"button_reply": {"id": "yes", "title": "Yes"}},
        }
        self.assertEqual(normalize_user_reply(msg), "yes")
        msg["interactive"]["button_reply"]["id"] = "no"
        self.assertEqual(normalize_user_reply(msg), "no")

    def test_text_maps_to_yes_no_reschedule(self):
        for t in ("1", "yes", "YES", "confirm"):
            self.assertEqual(
                normalize_user_reply({"type": "text", "text": {"body": t}}),
                "yes",
                msg=t,
            )
        for t in ("2", "no", "cancel"):
            self.assertEqual(
                normalize_user_reply({"type": "text", "text": {"body": t}}),
                "no",
                msg=t,
            )
        self.assertEqual(
            normalize_user_reply({"type": "text", "text": {"body": "3"}}),
            "reschedule",
        )
        self.assertEqual(
            normalize_user_reply({"type": "text", "text": {"body": "reschedule"}}),
            "reschedule",
        )

    def test_text_unknown_returns_none(self):
        self.assertIsNone(
            normalize_user_reply({"type": "text", "text": {"body": "maybe later"}})
        )

    def test_missing_keys_safe(self):
        self.assertIsNone(normalize_user_reply(None))
        self.assertIsNone(normalize_user_reply({}))
        self.assertIsNone(normalize_user_reply({"type": "text"}))
