"""
Rule-based lead qualification from patient / FHIR-like JSON.
"""

from __future__ import annotations

from typing import Any

SERIOUS_KEYWORDS = ("heart", "surgery", "critical")
QUALIFIED_MIN_SCORE = 0.8


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
    Score 0–1.0 from follow-up need, insurance eligibility, and diagnosis keywords.
    """
    if not isinstance(data, dict):
        data = {}

    score = 0.0
    parts: list[str] = []

    if _follow_up_required(data):
        score += 0.5
        parts.append("follow-up flagged (+0.5)")

    ins = str(data.get("insurance_eligibility_status") or "").strip().lower()
    if ins == "eligible":
        score += 0.3
        parts.append("insurance eligible (+0.3)")

    diagnosis = str(data.get("diagnosis") or "")
    dlow = diagnosis.lower()
    if any(kw in dlow for kw in SERIOUS_KEYWORDS):
        score += 0.2
        parts.append("serious diagnosis keyword (+0.2)")

    # Cap in case of future rule additions
    score = round(min(1.0, score), 2)

    qualified = score >= QUALIFIED_MIN_SCORE
    action = "send_followup" if qualified else "none"

    if parts:
        rationale = f"Score {score}: " + "; ".join(parts) + f". Threshold ≥{QUALIFIED_MIN_SCORE} → {'qualified' if qualified else 'not qualified'}."
    else:
        rationale = f"Score {score}: no positive rules matched. Threshold ≥{QUALIFIED_MIN_SCORE} → not qualified."

    patient_id = str(data.get("patient_id") or "").strip()

    return {
        "patient_id": patient_id,
        "score": float(score),
        "qualified": qualified,
        "action": action,
        "rationale": rationale,
    }
