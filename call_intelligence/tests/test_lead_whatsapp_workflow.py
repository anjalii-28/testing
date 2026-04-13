"""Lead WhatsApp workflow — transitions via apply_workflow_action only."""

from __future__ import annotations

import frappe
from frappe.tests.utils import FrappeTestCase

from call_intelligence.lead_whatsapp_workflow_api import apply_workflow_action

WF_NAME = "Lead WhatsApp Follow-up"


class TestLeadWhatsAppWorkflow(FrappeTestCase):
    def setUp(self):
        super().setUp()
        if not frappe.db.exists("Workflow", WF_NAME):
            self.skipTest(f"Workflow {WF_NAME} not installed — run bench migrate")

    def _minimal_lead(self):
        """Create a Lead; workflow_state defaults to Open via validate_workflow."""
        doc = frappe.get_doc(
            {
                "doctype": "Lead",
                "lead_name": f"WA Workflow Test {frappe.generate_hash(length=6)}",
                "status": "Lead",
            }
        )
        doc.insert(ignore_permissions=True, ignore_mandatory=True)
        doc.reload()
        return doc

    def test_yes_opens_to_interested(self):
        lead = self._minimal_lead()
        self.assertEqual(lead.get("workflow_state"), "Open")

        out = apply_workflow_action("Lead", lead.name, "yes")
        self.assertTrue(out.get("ok"), msg=out)

        lead.reload()
        self.assertEqual(lead.get("workflow_state"), "Interested")

    def test_no_opens_to_cold(self):
        lead = self._minimal_lead()
        out = apply_workflow_action("Lead", lead.name, "no")
        self.assertTrue(out.get("ok"), msg=out)
        lead.reload()
        self.assertEqual(lead.get("workflow_state"), "Cold")

    def test_second_yes_rejected(self):
        lead = self._minimal_lead()
        apply_workflow_action("Lead", lead.name, "yes")
        lead.reload()
        out = apply_workflow_action("Lead", lead.name, "yes")
        self.assertFalse(out.get("ok"))
