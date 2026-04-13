from __future__ import annotations

from typing import Any


SERIOUS_KEYWORDS = ("heart", "surgery", "critical")
QUALIFIED_MIN_SCORE = 0.6


def _follow_up_required(data: dict[str, Any]) -> bool:
    raw = data.get("follow_up")
    if not isinstance(raw, dict):
        return False
    v = raw.get("required")
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y")
    return False


def qualify_lead(data: dict[str, Any]) -> dict[str, Any]:
    """
    Basic rule-based Lead Qualification Agent.

    Rules:
    - follow_up.required == true  -> +0.5
    - insurance_eligibility_status == "eligible" -> +0.3
    - diagnosis contains serious keywords (heart, surgery, critical) -> +0.2
    """
    if not isinstance(data, dict):
        data = {}

    score = 0.0
    rationale_bits: list[str] = []

    if _follow_up_required(data):
        score += 0.5
        rationale_bits.append("Follow-up required (+0.5)")

    ins = str(data.get("insurance_eligibility_status") or "").strip().lower()
    if ins == "eligible":
        score += 0.3
        rationale_bits.append("Insurance eligible (+0.3)")

    diagnosis = str(data.get("diagnosis") or "").lower()
    if any(k in diagnosis for k in SERIOUS_KEYWORDS):
        score += 0.2
        rationale_bits.append("Serious diagnosis keyword match (+0.2)")

    score = round(min(max(score, 0.0), 1.0), 2)
    qualified = score >= QUALIFIED_MIN_SCORE

    action = "send_followup" if qualified else "none"
    rationale = (
        "; ".join(rationale_bits)
        if rationale_bits
        else "No qualification signals detected."
    )

    patient_id = str(data.get("patient_id") or "").strip()

    return {
        "patient_id": patient_id,
        "score": float(score),
        "qualified": bool(qualified),
        "action": action,
        "rationale": rationale,
    }
