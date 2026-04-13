"""
Medplum / FHIR helpers: build lead-qualification payload from Encounter + related resources.

Falls back to :func:`build_mock_payload` when parsing yields incomplete data or errors.
Optional HTTP fetch is skipped unless ``medplum_base_url`` + auth are configured (future).
"""

from __future__ import annotations

import base64
import json
import random
import re
from typing import Any

import frappe
from frappe.utils import strip_html


def _limit_words(phrase: str, max_words: int) -> str:
    words = [w for w in _normalize_ws(phrase).split() if w][:max_words]
    return " ".join(words)


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _condition_code_display(cond: dict[str, Any]) -> str:
    code = cond.get("code")
    if not isinstance(code, dict):
        return ""
    txt = str(code.get("text") or "").strip()
    if txt:
        return txt
    codings = code.get("coding")
    if isinstance(codings, list):
        for c in codings:
            if isinstance(c, dict):
                disp = str(c.get("display") or "").strip()
                if disp:
                    return disp
    return ""


def _condition_subject_matches(cond: dict[str, Any], patient_id: str | None) -> bool:
    if not patient_id:
        return False
    sub = cond.get("subject")
    ref = ""
    if isinstance(sub, dict):
        ref = str(sub.get("reference") or "")
    elif isinstance(sub, str):
        ref = sub
    if not ref:
        return False
    rid = ref_to_id(ref)
    return bool(rid == patient_id or ref.rstrip("/").endswith(patient_id))


def _condition_sort_key(cond: dict[str, Any]) -> tuple[int, int]:
    """Prefer active conditions; lower tuple sorts first."""
    cs = cond.get("clinicalStatus")
    if isinstance(cs, dict):
        codings = cs.get("coding")
        if isinstance(codings, list):
            for c in codings:
                if isinstance(c, dict) and str(c.get("code") or "").lower() == "active":
                    return (0, 0)
    return (1, 0)


def extract_diagnosis_from_fhir_bundle(bundle: dict[str, Any] | None, patient_id: str | None) -> str:
    """Best ``Condition.code`` text/display for the patient (prefers active conditions)."""
    if not bundle or not patient_id:
        return ""
    ranked: list[tuple[tuple, str]] = []
    for i, res in enumerate(_bundle_entries(bundle)):
        if str(res.get("resourceType") or "") != "Condition":
            continue
        if not _condition_subject_matches(res, patient_id):
            continue
        disp = _condition_code_display(res)
        if not disp:
            continue
        key = (*_condition_sort_key(res), i)
        ranked.append((key, disp))
    if not ranked:
        return ""
    ranked.sort(key=lambda x: x[0])
    return ranked[0][1]


def _service_request_code_display(sr: dict[str, Any]) -> str:
    code = sr.get("code")
    if not isinstance(code, dict):
        return ""
    txt = str(code.get("text") or "").strip()
    if txt:
        return txt
    codings = code.get("coding")
    if isinstance(codings, list):
        for c in codings:
            if isinstance(c, dict):
                disp = str(c.get("display") or "").strip()
                if disp:
                    return disp
    return ""


def _service_request_subject_matches(sr: dict[str, Any], patient_id: str | None) -> bool:
    if not patient_id:
        return True
    sub = sr.get("subject")
    ref = ""
    if isinstance(sub, dict):
        ref = str(sub.get("reference") or "")
    elif isinstance(sub, str):
        ref = sub
    if not ref:
        return False
    rid = ref_to_id(ref)
    return bool(rid == patient_id or ref.rstrip("/").endswith(patient_id))


def extract_service_request_code_text(bundle: dict[str, Any] | None, patient_id: str | None) -> str:
    """First ``ServiceRequest.code`` text/display for the patient (procedure / order label for messaging)."""
    if not bundle:
        return ""
    candidates: list[tuple[int, str]] = []
    for i, res in enumerate(_bundle_entries(bundle)):
        if str(res.get("resourceType") or "") != "ServiceRequest":
            continue
        if not _service_request_subject_matches(res, patient_id):
            continue
        disp = _service_request_code_display(res)
        if not disp:
            continue
        candidates.append((i, disp))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _diagnosis_from_fhir_for_whatsapp(fhir_text: str, *, max_words: int = 5) -> str:
    try:
        s = strip_html(fhir_text)
    except Exception:
        s = fhir_text
    s = _normalize_ws(s)
    if not s:
        return ""
    return _limit_words(s, max_words)


def _diagnosis_from_gemini_summary_text(raw: str, max_words: int = 5) -> str:
    """First meaningful phrase (before comma / ``with``), strip narrative prefixes, cap words."""
    if not raw or not str(raw).strip():
        return ""
    try:
        s = strip_html(str(raw))
    except Exception:
        s = str(raw)
    s = re.sub(r"[\r\n]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("*", "").replace("#", "")
    for stop in (". ", "! ", "? "):
        if stop in s:
            s = s.split(stop)[0].strip()
            break
    if "," in s:
        s = s.split(",")[0].strip()
    parts = re.split(r"(?i)\s+with\s+", s, maxsplit=1)
    s = parts[0].strip()
    s = re.sub(r"\([^)]{0,220}\)", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for p in (
        r"(?i)^the\s+patient\s+has\s+",
        r"(?i)^the\s+patient\s+is\s+",
        r"(?i)^patient\s+has\s+",
        r"(?i)^patient\s+is\s+",
        r"(?i)^has\s+active\s+",
        r"(?i)^has\s+",
    ):
        s = re.sub(p, "", s).strip()
    s = re.sub(r"(?i)^active\s+", "", s).strip()
    s = _normalize_ws(s)
    if not s:
        return ""
    return _limit_words(s, max_words)


def whatsapp_diagnosis_phrase(
    fhir_diagnosis: str | None,
    gemini_summary: str | None,
    *,
    max_words: int = 5,
) -> str:
    """
    WhatsApp line: prefer FHIR Condition text; else clean phrase from Gemini summary only.
    """
    fd = str(fhir_diagnosis or "").strip()
    if fd:
        return _diagnosis_from_fhir_for_whatsapp(fd, max_words=max_words)
    return _diagnosis_from_gemini_summary_text(str(gemini_summary or ""), max_words=max_words)


def ref_to_id(ref: str | None) -> str | None:
    if not ref:
        return None
    s = str(ref).strip()
    if not s:
        return None
    return s.split("/")[-1]


def normalize_phone_digits(raw: str | None) -> str:
    if not raw:
        return ""
    return "".join(c for c in str(raw) if c.isdigit())


def build_mock_payload(patient_id: str | None) -> dict[str, Any]:
    """Fallback when FHIR parsing fails or webhook sends minimal data.

    Random phone (91 + 10 digits) so repeated webhook tests create distinct Leads.
    No fixed demo names or diagnoses — placeholders only.
    """
    phone = "91" + str(random.randint(1000000000, 9999999999))
    pid = str(patient_id or "").strip()
    return {
        "patient_id": pid or "unknown",
        "patient_name": f"Patient {pid}" if pid else "Patient",
        "phone": phone,
        "diagnosis": "",
        "follow_up": {"required": True, "days": 7},
        "insurance_eligibility_status": "eligible",
    }


def _bundle_entries(data: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    if str(data.get("resourceType") or "") != "Bundle":
        return []
    ent = data.get("entry")
    if not isinstance(ent, list):
        return []
    out: list[dict[str, Any]] = []
    for e in ent:
        if isinstance(e, dict) and isinstance(e.get("resource"), dict):
            out.append(e["resource"])
    return out


def extract_encounter_id_from_webhook(webhook_data: dict[str, Any] | None) -> str | None:
    """Return ``Encounter.id`` from a webhook body (Encounter or Bundle), if present."""
    encounter, _bundle = extract_encounter_and_bundle(webhook_data or {})
    if not encounter:
        return None
    eid = str(encounter.get("id") or "").strip()
    return eid or None


def extract_encounter_and_bundle(webhook_data: dict[str, Any] | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """
    From webhook JSON, return (Encounter dict, Bundle dict or None).
    If body is a bare Encounter, bundle is None.
    """
    if not isinstance(webhook_data, dict):
        return None, None
    rtype = str(webhook_data.get("resourceType") or "")
    if rtype == "Encounter":
        return webhook_data, None
    if rtype == "Bundle":
        enc = None
        for res in _bundle_entries(webhook_data):
            if str(res.get("resourceType") or "") == "Encounter":
                enc = res
                break
        return enc, webhook_data
    if isinstance(webhook_data.get("resource"), dict):
        inner = webhook_data["resource"]
        if str(inner.get("resourceType") or "") == "Encounter":
            return inner, None
    return None, None


def _resources_by_type(bundle_entries: list[dict[str, Any]], rtype: str) -> list[dict[str, Any]]:
    return [r for r in bundle_entries if str(r.get("resourceType") or "") == rtype]


def _patient_name(patient: dict[str, Any]) -> str:
    names = patient.get("name")
    if isinstance(names, list) and names:
        n0 = names[0]
        if isinstance(n0, dict):
            t = str(n0.get("text") or "").strip()
            if t:
                return t
            fam = str(n0.get("family") or "").strip()
            given = n0.get("given")
            g = ""
            if isinstance(given, list) and given:
                g = str(given[0] or "").strip()
            parts = [p for p in (g, fam) if p]
            if parts:
                return " ".join(parts)
    return ""


def _patient_phone(patient: dict[str, Any]) -> str:
    telecom = patient.get("telecom")
    if not isinstance(telecom, list):
        return ""
    for t in telecom:
        if not isinstance(t, dict):
            continue
        if str(t.get("system") or "").lower() != "phone":
            continue
        v = str(t.get("value") or "").strip()
        if v:
            return v
    return ""


def _parse_attachment_json(docref: dict[str, Any]) -> dict[str, Any] | None:
    """Decode DocumentReference content attachment (base64 JSON case sheet)."""
    content = docref.get("content")
    if not isinstance(content, list):
        return None
    for block in content:
        if not isinstance(block, dict):
            continue
        att = block.get("attachment")
        if not isinstance(att, dict):
            continue
        url = str(att.get("url") or "").strip()
        if url and url.lower().endswith(".json"):
            # Future: fetch URL via medplum
            pass
        data_b64 = att.get("data")
        if isinstance(data_b64, str) and data_b64.strip():
            try:
                raw = base64.b64decode(data_b64, validate=False)
                txt = raw.decode("utf-8", errors="replace")
                parsed = json.loads(txt)
                return parsed if isinstance(parsed, dict) else None
            except (json.JSONDecodeError, ValueError, TypeError):
                continue
    return None


def _merge_loose_clinical_dict(target: dict[str, Any], blob: dict[str, Any]) -> None:
    """Merge common keys from case-sheet JSON (flat or nested)."""
    if not isinstance(blob, dict):
        return
    for k in (
        "patient_name",
        "phone",
        "diagnosis",
        "insurance_eligibility_status",
    ):
        if k in blob and blob[k] is not None and str(blob[k]).strip():
            target[k] = str(blob[k]).strip()

    fu = blob.get("follow_up")
    if isinstance(fu, dict):
        cur = target.setdefault("follow_up", {})
        if not isinstance(cur, dict):
            cur = {}
            target["follow_up"] = cur
        if "required" in fu:
            cur["required"] = bool(fu.get("required"))
        if fu.get("days") is not None:
            try:
                cur["days"] = int(fu["days"])
            except (TypeError, ValueError):
                pass


def _parse_careplan(careplan: dict[str, Any], target: dict[str, Any]) -> None:
    """Infer follow-up from CarePlan (activity, goal, period)."""
    fu = target.setdefault("follow_up", {})
    if not isinstance(fu, dict):
        fu = {}
        target["follow_up"] = fu

    status = str(careplan.get("status") or "").lower()
    if status in ("active", "on-hold", "completed"):
        fu.setdefault("required", True)

    days = None
    period = careplan.get("period")
    if isinstance(period, dict):
        end = period.get("end")
        start = period.get("start")
        if end and start:
            # crude: do not compute delta without date parsing; look in extension
            pass

    activity = careplan.get("activity")
    if isinstance(activity, list):
        for act in activity:
            if not isinstance(act, dict):
                continue
            det = act.get("detail")
            if isinstance(det, dict):
                desc = str(det.get("description") or "")
                m = re.search(r"(\d+)\s*(day|week)", desc, re.I)
                if m:
                    n = int(m.group(1))
                    unit = m.group(2).lower()
                    days = n * (7 if unit.startswith("w") else 1)
                    break
            sch = act.get("detail", {})
            if isinstance(sch, dict):
                per = sch.get("scheduledPeriod")
                if isinstance(per, dict) and per.get("end"):
                    fu.setdefault("required", True)

    ext = careplan.get("extension")
    if isinstance(ext, list):
        for ex in ext:
            if not isinstance(ex, dict):
                continue
            url = str(ex.get("url") or "")
            if "follow" in url.lower() or "days" in url.lower():
                v = ex.get("valueInteger") or ex.get("valueQuantity", {}).get("value")
                if v is not None:
                    try:
                        days = int(v)
                    except (TypeError, ValueError):
                        pass

    if days is not None:
        fu["days"] = days
    fu.setdefault("days", 7)


def _gather_bundle_resources(
    encounter: dict[str, Any],
    bundle: dict[str, Any] | None,
    patient_id: str | None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
    entries = _bundle_entries(bundle) if bundle else []
    patient: dict[str, Any] | None = None
    docrefs: list[dict[str, Any]] = []
    careplans: list[dict[str, Any]] = []

    pref = f"Patient/{patient_id}" if patient_id else None

    for res in entries:
        rt = str(res.get("resourceType") or "")
        if rt == "Patient" and patient_id:
            if ref_to_id(res.get("id")) == patient_id or res.get("id") == patient_id:
                patient = res
        elif rt == "DocumentReference":
            sub = res.get("subject")
            ref = ""
            if isinstance(sub, dict):
                ref = str(sub.get("reference") or "")
            if not pref or ref.endswith(patient_id or "") or not ref:
                docrefs.append(res)
        elif rt == "CarePlan":
            sub = res.get("subject")
            ref = ""
            if isinstance(sub, dict):
                ref = str(sub.get("reference") or "")
            if not pref or (patient_id and ref.endswith(patient_id)):
                careplans.append(res)

    # Encounter-only: still try first Patient in bundle
    if patient is None and entries:
        for res in entries:
            if str(res.get("resourceType") or "") == "Patient":
                patient = res
                break

    return patient, docrefs, careplans


def fetch_medplum_related_resources(patient_id: str | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
    """
    Optional Medplum REST fetch (disabled unless site config is set).

    Returns (document_references, care_plans, patient_resource).
    """
    if not patient_id:
        return [], [], None
    base = (
        frappe.conf.get("medplum_base_url")
        or frappe.conf.get("MEDPLUM_BASE_URL")
        or ""
    ).strip().rstrip("/")
    if not base:
        return [], [], None

    token = (
        frappe.conf.get("medplum_access_token")
        or frappe.conf.get("MEDPLUM_ACCESS_TOKEN")
        or ""
    ).strip()
    if not token:
        # Placeholder: client-credentials flow not implemented; keep empty.
        return [], [], None

    try:
        import requests
    except ImportError:
        return [], [], None

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/fhir+json"}
    pid = patient_id
    patient: dict[str, Any] | None = None
    docrefs: list[dict[str, Any]] = []
    careplans: list[dict[str, Any]] = []

    try:
        pr = requests.get(f"{base}/fhir/R4/Patient/{pid}", headers=headers, timeout=20)
        if pr.ok:
            patient = pr.json()
    except Exception:
        pass

    try:
        q = requests.get(
            f"{base}/fhir/R4/DocumentReference",
            headers=headers,
            params={"patient": pid, "_count": "20"},
            timeout=20,
        )
        if q.ok:
            bundle = q.json()
            if str(bundle.get("resourceType") or "") == "Bundle":
                for e in bundle.get("entry") or []:
                    if isinstance(e, dict) and isinstance(e.get("resource"), dict):
                        docrefs.append(e["resource"])
    except Exception:
        pass

    try:
        q = requests.get(
            f"{base}/fhir/R4/CarePlan",
            headers=headers,
            params={"patient": pid, "_count": "10"},
            timeout=20,
        )
        if q.ok:
            bundle = q.json()
            if str(bundle.get("resourceType") or "") == "Bundle":
                for e in bundle.get("entry") or []:
                    if isinstance(e, dict) and isinstance(e.get("resource"), dict):
                        careplans.append(e["resource"])
    except Exception:
        pass

    return docrefs, careplans, patient


def build_qualification_payload_from_fhir(
    encounter: dict[str, Any],
    webhook_bundle: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    """
    Build qualification payload from Encounter + embedded or fetched FHIR resources.

    Returns (payload_dict, True) on success, or (fallback_mock_payload, False) if parsing failed.
    """
    try:
        subject = encounter.get("subject")
        patient_ref = None
        if isinstance(subject, dict):
            patient_ref = subject.get("reference")
        elif isinstance(subject, str):
            patient_ref = subject
        patient_id = ref_to_id(patient_ref)

        patient, docrefs, careplans = _gather_bundle_resources(encounter, webhook_bundle, patient_id)

        # Optional API enrichment
        dr2, cp2, p2 = fetch_medplum_related_resources(patient_id)
        if p2 and not patient:
            patient = p2
        if dr2:
            docrefs.extend(dr2)
        if cp2:
            careplans.extend(cp2)

        payload: dict[str, Any] = {"patient_id": patient_id or ""}

        if patient:
            pn = _patient_name(patient)
            ph = _patient_phone(patient)
            if pn:
                payload["patient_name"] = pn
            if ph:
                payload["phone"] = normalize_phone_digits(ph) or ph

        for dr in docrefs:
            blob = _parse_attachment_json(dr)
            if blob:
                _merge_loose_clinical_dict(payload, blob)

        for cp in careplans:
            _parse_careplan(cp, payload)

        # Encounter extensions (optional)
        ext = encounter.get("extension")
        if isinstance(ext, list):
            for ex in ext:
                if not isinstance(ex, dict):
                    continue
                u = str(ex.get("url") or "").lower()
                if "insurance" in u or "coverage" in u:
                    v = ex.get("valueString") or ex.get("valueCode")
                    if v:
                        payload.setdefault("insurance_eligibility_status", str(v).strip())

        # Condition resources (preferred over Encounter reasonCode for diagnosis line)
        if webhook_bundle and patient_id:
            cdx = extract_diagnosis_from_fhir_bundle(webhook_bundle, patient_id)
            if cdx:
                payload["diagnosis"] = cdx

        # ReasonCode / type → diagnosis hint (if no Condition in bundle)
        reason = encounter.get("reasonCode")
        if isinstance(reason, list) and reason:
            rc0 = reason[0]
            if isinstance(rc0, dict):
                txt = str(rc0.get("text") or "").strip()
                coding = rc0.get("coding")
                if isinstance(coding, list) and coding:
                    disp = str(coding[0].get("display") or "").strip()
                    txt = txt or disp
                if txt and not payload.get("diagnosis"):
                    payload["diagnosis"] = txt

        fu = payload.get("follow_up")
        if isinstance(fu, dict):
            fu = dict(fu)
            fu.setdefault("days", 7)
            payload["follow_up"] = fu
        else:
            # Omit follow_up when unknown — webhook / soften can merge mock
            payload.pop("follow_up", None)

        # Minimum viable: need some identity + follow-up signal
        if not payload.get("patient_name") and patient_id:
            payload["patient_name"] = f"Patient {patient_id}"

        if not payload.get("phone"):
            payload["phone"] = normalize_phone_digits(str(payload.get("phone") or "")) or ""

        return payload, True
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Medplum FHIR payload build")
        pref = None
        try:
            sub = encounter.get("subject")
            if isinstance(sub, dict):
                pref = sub.get("reference")
            elif isinstance(sub, str):
                pref = sub
        except Exception:
            pref = None
        pid = ref_to_id(pref)
        return (
            {
                "patient_id": pid or "",
                "patient_name": f"Patient {pid}" if pid else "",
                "phone": "",
                "diagnosis": "",
            },
            False,
        )


def soften_payload_after_gemini(payload: dict[str, Any], patient_id_hint: str | None) -> dict[str, Any]:
    """Fill CRM contact fields after a successful Gemini merge — no demo clinical labels."""
    out = dict(payload)
    if not str(out.get("phone") or "").strip():
        out["phone"] = "91" + str(random.randint(1000000000, 9999999999))
    if not str(out.get("patient_name") or "").strip():
        out["patient_name"] = f"Patient {patient_id_hint}" if patient_id_hint else "Patient"
    fd = str(out.get("diagnosis") or "").strip()
    cs = str(out.get("case_sheet_summary") or "").strip()
    out["diagnosis"] = whatsapp_diagnosis_phrase(fd or None, cs or None, max_words=5)
    if not str(out.get("insurance_eligibility_status") or "").strip():
        out["insurance_eligibility_status"] = ""
    return out


def soften_payload_with_defaults(
    payload: dict[str, Any],
    patient_id_hint: str | None,
    *,
    skip_follow_up: bool = False,
) -> dict[str, Any]:
    """Fill missing phone/name from mock so CRM + dedupe do not break."""
    fb = build_mock_payload(patient_id_hint)
    out = dict(payload)
    if not str(out.get("phone") or "").strip():
        out["phone"] = fb["phone"]
    if not str(out.get("patient_name") or "").strip():
        out["patient_name"] = fb["patient_name"]
    if not skip_follow_up:
        fu = out.get("follow_up")
        if not isinstance(fu, dict):
            out["follow_up"] = dict(fb["follow_up"])
        else:
            fu = dict(fu)
            if "required" not in fu:
                fu["required"] = fb["follow_up"]["required"]
            if not fu.get("days"):
                fu["days"] = fb["follow_up"]["days"]
            out["follow_up"] = fu
    if not str(out.get("diagnosis") or "").strip():
        out["diagnosis"] = fb["diagnosis"]
    if not str(out.get("insurance_eligibility_status") or "").strip():
        out["insurance_eligibility_status"] = fb["insurance_eligibility_status"]
    return out
