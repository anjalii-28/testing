"""Re-apply Lead Client Script (Run Lead Qualification button)."""


def execute():
    from call_intelligence.setup.client_script import install_lead_patient_360_script

    install_lead_patient_360_script()
