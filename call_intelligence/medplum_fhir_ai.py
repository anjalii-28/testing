"""
FHIR case-sheet analysis via Gemini for Medplum Encounter → lead qualification.

Loads a FHIR Bundle (webhook or local file), extracts Composition and follow-up plan
sections, builds structured text, and calls Gemini 2.5 Flash. API key from
``GEMINI_API_KEY`` env or ``frappe.conf`` ``gemini_api_key`` / ``GEMINI_API_KEY``.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import frappe

from call_intelligence.medplum_fhir import ref_to_id

_LOG = "call_intelligence.medplum_fhir_ai"

_DEFAULT_MODEL = "gemini-2.5-flash"
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

_FOLLOW_UP_SECTION_HINTS = ("follow", "follow-up", "followup", "plan", "review", "advice")


def _module_dir() -> Path:
    return Path(__file__).resolve().parent


def default_fhir_bundle_path() -> Path:
    custom = (frappe.conf.get("medplum_fhir_bundle_path") or frappe.conf.get("MEDPLUM_FHIR_BUNDLE_PATH") or "").strip()
    if custom:
        return Path(custom)
    return _module_dir() / "fhir" / "02_fhir_bundle.json"


def default_prompt_path() -> Path:
    custom = (frappe.conf.get("medplum_fhir_prompt_path") or frappe.conf.get("MEDPLUM_FHIR_PROMPT_PATH") or "").strip()
    if custom:
        return Path(custom)
    return _module_dir() / "fhir" / "opd_fhir_prompt.md"


def _bundle_entry_resources(bundle: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(bundle, dict) or str(bundle.get("resourceType") or "") != "Bundle":
        return []
    ent = bundle.get("entry")
    if not isinstance(ent, list):
        return []
    out: list[dict[str, Any]] = []
    for e in ent:
        if isinstance(e, dict) and isinstance(e.get("resource"), dict):
            out.append(e["resource"])
    return out


def bundle_has_composition(bundle: dict[str, Any] | None) -> bool:
    for r in _bundle_entry_resources(bundle):
        if str(r.get("resourceType") or "") == "Composition":
            return True
    return False


def load_fhir_bundle_json(path: Path | str | None = None) -> dict[str, Any] | None:
    """Load a FHIR Bundle from disk. Returns None if missing or invalid."""
    p = Path(path) if path else default_fhir_bundle_path()
    if not p.is_file():
        frappe.logger(_LOG).warning("FHIR bundle file not found: %s", p)
        return None
    try:
        raw = p.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError) as e:
        frappe.logger(_LOG).error("Failed to read FHIR bundle %s: %s", p, e)
        return None


def resolve_case_sheet_bundle(webhook_bundle: dict[str, Any] | None) -> dict[str, Any] | None:
    """Prefer webhook Bundle if it contains a Composition; otherwise load local default bundle."""
    if bundle_has_composition(webhook_bundle):
        return webhook_bundle
    return load_fhir_bundle_json()


def _index_by_ref(resources: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    idx: dict[str, dict[str, Any]] = {}
    for r in resources:
        rt = str(r.get("resourceType") or "")
        rid = str(r.get("id") or "").strip()
        if rt and rid:
            idx[f"{rt}/{rid}"] = r
            idx[rid] = r
    return idx


def _ref_list(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, str) and val.strip():
        return [val.strip()]
    if isinstance(val, dict):
        r = str(val.get("reference") or "").strip()
        return [r] if r else []
    if isinstance(val, list):
        out: list[str] = []
        for x in val:
            if isinstance(x, dict):
                r = str(x.get("reference") or "").strip()
                if r:
                    out.append(r)
            elif isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out
    return []


def _is_follow_up_section(section: dict[str, Any]) -> bool:
    title = str(section.get("title") or "").lower()
    code = section.get("code")
    codings: list[str] = []
    if isinstance(code, dict):
        c = code.get("coding")
        if isinstance(c, list):
            for it in c:
                if isinstance(it, dict):
                    for k in ("display", "code"):
                        v = str(it.get(k) or "").lower()
                        if v:
                            codings.append(v)
        txt = str(code.get("text") or "").lower()
        if txt:
            codings.append(txt)
    blob = " ".join([title, *codings])
    return any(h in blob for h in _FOLLOW_UP_SECTION_HINTS)


def _resource_lines(label: str, res: dict[str, Any]) -> list[str]:
    rt = str(res.get("resourceType") or "")
    lines = [f"### {label} ({rt}/{res.get('id', '')})"]
    if rt == "Appointment":
        lines.append(
            f"status={res.get('status')!s} description={res.get('description')!s} "
            f"start={res.get('start')!s} end={res.get('end')!s}"
        )
    elif rt == "Task":
        lines.append(f"status={res.get('status')!s} intent={res.get('intent')!s} description={res.get('description')!s}")
    elif rt == "ServiceRequest":
        code = res.get("code") or {}
        ctext = ""
        if isinstance(code, dict):
            ctext = str(code.get("text") or "")
            if not ctext:
                codings = code.get("coding")
                if isinstance(codings, list) and codings and isinstance(codings[0], dict):
                    ctext = str(codings[0].get("display") or "")
        lines.append(f"status={res.get('status')!s} intent={res.get('intent')!s} code={ctext!s}")
    elif rt == "Observation":
        code = res.get("code") or {}
        ctext = ""
        if isinstance(code, dict):
            ctext = str(code.get("text") or "")
        vq = res.get("valueQuantity")
        vs = res.get("valueString")
        if isinstance(vq, dict):
            lines.append(f"code={ctext!s} value={vq.get('value')!s} {vq.get('unit')!s}")
        elif vs:
            lines.append(f"code={ctext!s} value={vs!s}")
        else:
            lines.append(json.dumps(res, default=str)[:1200])
    elif rt == "Condition":
        code = res.get("code") or {}
        ctext = ""
        if isinstance(code, dict):
            ctext = str(code.get("text") or "")
        lines.append(f"clinicalStatus={res.get('clinicalStatus')!s} code={ctext!s}")
    else:
        lines.append(json.dumps(res, default=str)[:2000])
    return lines


def _patient_lines(patient: dict[str, Any] | None) -> list[str]:
    if not patient:
        return ["### Patient", "(not found in bundle)"]
    lines = ["### Patient"]
    names = patient.get("name")
    if isinstance(names, list) and names:
        n0 = names[0]
        if isinstance(n0, dict):
            t = str(n0.get("text") or "").strip()
            if not t:
                fam = str(n0.get("family") or "")
                g = n0.get("given")
                g0 = str(g[0]) if isinstance(g, list) and g else ""
                t = " ".join(x for x in (g0, fam) if x).strip()
            if t:
                lines.append(f"name={t}")
    telecom = patient.get("telecom")
    if isinstance(telecom, list):
        for t in telecom:
            if isinstance(t, dict) and str(t.get("system") or "").lower() == "phone":
                lines.append(f"phone={t.get('value')!s}")
                break
    lines.append(f"id={patient.get('id')!s}")
    return lines


def build_structured_case_sheet_text(bundle: dict[str, Any]) -> str:
    """
    Extract Composition, diagnoses, observations, and follow-up–linked resources as plain text.
    """
    resources = _bundle_entry_resources(bundle)
    idx = _index_by_ref(resources)
    compositions = [r for r in resources if str(r.get("resourceType") or "") == "Composition"]
    comp = compositions[0] if compositions else None

    patient: dict[str, Any] | None = None
    if comp:
        pref = _ref_list(comp.get("subject"))
        if pref:
            patient = idx.get(pref[0]) or idx.get(ref_to_id(pref[0]) or "")
    if patient is None:
        for r in resources:
            if str(r.get("resourceType") or "") == "Patient":
                patient = r
                break

    parts: list[str] = []
    parts.append("# FHIR case sheet (structured extract)\n")
    parts.extend(_patient_lines(patient))
    parts.append("")

    if comp:
        parts.append(f"### Composition title={comp.get('title')!s} status={comp.get('status')!s}")
        parts.append("")

    conditions = [r for r in resources if str(r.get("resourceType") or "") == "Condition"]
    if conditions:
        parts.append("## Diagnoses / Conditions")
        for c in conditions:
            parts.extend(_resource_lines("Condition", c))
        parts.append("")

    observations = [r for r in resources if str(r.get("resourceType") or "") == "Observation"]
    if observations:
        parts.append("## Observations")
        for o in observations[:50]:
            parts.extend(_resource_lines("Observation", o))
        parts.append("")

    follow_refs: list[str] = []
    if comp:
        sections = comp.get("section")
        if isinstance(sections, list):
            for sec in sections:
                if not isinstance(sec, dict):
                    continue
                if _is_follow_up_section(sec):
                    follow_refs.extend(_ref_list(sec.get("entry")))

    parts.append("## Follow-up Plan section (references)")
    if follow_refs:
        for ref in follow_refs:
            parts.append(f"- {ref}")
    else:
        parts.append("(no Follow-up Plan section or no references)")
    parts.append("")

    parts.append("## Resolved follow-up related resources")
    want_types = ("Appointment", "Task", "ServiceRequest")
    seen: set[str] = set()
    for ref in follow_refs:
        target = idx.get(ref) or idx.get(ref_to_id(ref) or "")
        if not target:
            parts.append(f"(unresolved) {ref}")
            continue
        rt = str(target.get("resourceType") or "")
        if rt in want_types:
            key = f"{rt}/{target.get('id')}"
            if key in seen:
                continue
            seen.add(key)
            parts.extend(_resource_lines(rt, target))
            parts.append("")
    parts.append("## All Composition sections (overview)")
    if comp:
        sections = comp.get("section")
        if isinstance(sections, list):
            for sec in sections:
                if not isinstance(sec, dict):
                    continue
                st = str(sec.get("title") or "")
                txt = sec.get("text")
                div = ""
                if isinstance(txt, dict):
                    div = str(txt.get("div") or "")[:800]
                parts.append(f"- **{st}** {div}")

    return "\n".join(parts).strip()


def _get_gemini_api_key() -> str:
    # Prefer site_config so ``bench execute`` uses keys from sites/<site>/site_config.json.
    # ``GEMINI_API_KEY`` in the shell overrides only when site config is empty (CI / ad-hoc).
    return (
        str(frappe.conf.get("gemini_api_key") or frappe.conf.get("GEMINI_API_KEY") or "").strip()
        or os.environ.get("GEMINI_API_KEY", "").strip()
    )


def _get_gemini_model() -> str:
    return (
        str(frappe.conf.get("gemini_model") or frappe.conf.get("GEMINI_MODEL") or "").strip() or _DEFAULT_MODEL
    )


def _parse_gemini_json(text: str) -> dict[str, Any] | None:
    s = str(text or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}\s*$", s)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return None


def _normalize_analysis(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    if not raw or not isinstance(raw, dict):
        return None
    req = raw.get("follow_up_required")
    if isinstance(req, str):
        req = req.strip().lower() in ("1", "true", "yes", "y")
    elif not isinstance(req, bool):
        req = bool(req)

    ft = str(raw.get("follow_up_type") or "advisory").strip().lower()
    if ft not in ("appointment", "procedure", "advisory"):
        ft = "advisory"

    ur = str(raw.get("urgency") or "medium").strip().lower()
    if ur not in ("high", "medium", "low"):
        ur = "medium"

    summary = str(raw.get("case_sheet_summary") or raw.get("summary") or "").strip()
    return {
        "ok": True,
        "follow_up_required": req,
        "follow_up_type": ft,
        "urgency": ur,
        "summary": summary,
        "case_sheet_summary": summary,
    }


def _urgency_to_days(urgency: str) -> int:
    u = str(urgency or "").lower()
    if u == "high":
        return 3
    if u == "low":
        return 14
    return 7


def detect_fhir_structured_follow_up_signals(bundle: dict[str, Any]) -> dict[str, Any]:
    """
    Detect explicit FHIR follow-up signals in the bundle (not LLM output).

    - Any ``Appointment`` or ``ServiceRequest`` in bundle entries triggers a hard follow-up signal.
    - Also records whether those resource types appear under Composition Follow-up Plan section refs.
    """
    resources = _bundle_entry_resources(bundle)
    has_appt = any(str(r.get("resourceType") or "") == "Appointment" for r in resources)
    has_sr = any(str(r.get("resourceType") or "") == "ServiceRequest" for r in resources)

    appt_in_plan = False
    sr_in_plan = False
    idx = _index_by_ref(resources)
    compositions = [r for r in resources if str(r.get("resourceType") or "") == "Composition"]
    comp = compositions[0] if compositions else None
    if comp:
        sections = comp.get("section")
        if isinstance(sections, list):
            for sec in sections:
                if not isinstance(sec, dict):
                    continue
                if not _is_follow_up_section(sec):
                    continue
                for ref in _ref_list(sec.get("entry")):
                    target = idx.get(ref) or idx.get(ref_to_id(ref) or "")
                    if not isinstance(target, dict):
                        continue
                    rt = str(target.get("resourceType") or "")
                    if rt == "Appointment":
                        appt_in_plan = True
                    if rt == "ServiceRequest":
                        sr_in_plan = True

    force = bool(has_appt or has_sr)
    if has_appt:
        forced_type = "appointment"
    elif has_sr:
        forced_type = "procedure"
    else:
        forced_type = "appointment"
    return {
        "force": force,
        "follow_up_type": forced_type,
        "has_appointment": has_appt,
        "has_service_request": has_sr,
        "appointment_in_follow_up_section": appt_in_plan,
        "service_request_in_follow_up_section": sr_in_plan,
    }


def apply_fhir_follow_up_hard_override(gemini_result: dict[str, Any], bundle: dict[str, Any]) -> dict[str, Any]:
    """
    Post-Gemini safeguard: if the bundle contains ``Appointment`` or ``ServiceRequest`` resources,
    force ``follow_up_required`` and ``follow_up_type`` regardless of model output.
    """
    sig = detect_fhir_structured_follow_up_signals(bundle)
    if not sig.get("force"):
        return gemini_result
    out = dict(gemini_result)
    prior_req = out.get("follow_up_required")
    prior_type = str(out.get("follow_up_type") or "").lower()
    forced = str(sig.get("follow_up_type") or "appointment")
    out["follow_up_required"] = True
    out["follow_up_type"] = forced
    cs = str(out.get("case_sheet_summary") or out.get("summary") or "").strip()
    out["summary"] = cs
    out["case_sheet_summary"] = cs
    frappe.logger(_LOG).info(
        "FHIR follow-up hard override: bundle Appointment=%s ServiceRequest=%s; "
        "Follow-up Plan section → Appointment=%s ServiceRequest=%s; "
        "Gemini had follow_up_required=%s follow_up_type=%s → forced required=True type=%s",
        sig["has_appointment"],
        sig["has_service_request"],
        sig["appointment_in_follow_up_section"],
        sig["service_request_in_follow_up_section"],
        prior_req,
        prior_type,
        forced,
    )
    return out


def _call_gemini_generate(system_and_user: str) -> tuple[str | None, str | None]:
    api_key = _get_gemini_api_key()
    if not api_key:
        return None, "GEMINI_API_KEY not configured"

    model = _get_gemini_model()
    url = _GEMINI_URL.format(model=model)
    body: dict[str, Any] = {
        "contents": [{"role": "user", "parts": [{"text": system_and_user}]}],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }

    try:
        import requests
    except ImportError:
        return None, "requests not available"

    try:
        r = requests.post(
            url,
            params={"key": api_key},
            json=body,
            timeout=90,
            headers={"Content-Type": "application/json"},
        )
        if not r.ok:
            return None, f"Gemini HTTP {r.status_code}: {r.text[:500]}"
        data = r.json()
    except Exception as e:
        return None, str(e)

    candidates = data.get("candidates") if isinstance(data, dict) else None
    if not isinstance(candidates, list) or not candidates:
        return None, "Gemini returned no candidates"
    c0 = candidates[0]
    content = c0.get("content") if isinstance(c0, dict) else None
    parts = content.get("parts") if isinstance(content, dict) else None
    if not isinstance(parts, list) or not parts:
        return None, "Gemini returned empty content"
    p0 = parts[0]
    txt = p0.get("text") if isinstance(p0, dict) else None
    if not isinstance(txt, str):
        return None, "Gemini returned no text"
    return txt, None


def analyze_case_sheet(bundle: dict[str, Any] | None = None, bundle_path: str | None = None) -> dict[str, Any]:
    """
    Build structured text from ``bundle`` (or load from ``bundle_path`` / defaults), call Gemini, return:

    - On success: ``ok``, ``follow_up_required``, ``follow_up_type``, ``urgency``, ``summary``, ``follow_up_days``
    - On failure: ``ok: False``, ``error``
    """
    log = frappe.logger(_LOG)

    b = bundle
    if b is None and bundle_path:
        b = load_fhir_bundle_json(bundle_path)
    if b is None:
        b = load_fhir_bundle_json()

    if not b or not isinstance(b, dict):
        return {"ok": False, "error": "No FHIR bundle available"}

    case_text = build_structured_case_sheet_text(b)
    prompt_path = default_prompt_path()
    try:
        instructions = prompt_path.read_text(encoding="utf-8")
    except OSError as e:
        return {"ok": False, "error": f"Prompt file unreadable: {e}"}

    full_input = f"{instructions}\n\n--- STRUCTURED CASE SHEET ---\n\n{case_text}"
    log.info("Gemini FHIR input length=%s (prompt=%s)", len(full_input), prompt_path)
    log.info("Gemini FHIR structured case sheet excerpt:\n%s", case_text[:6000])

    raw_text, err = _call_gemini_generate(full_input)
    log.info("Gemini FHIR raw output: %s", (raw_text or "")[:8000])
    if err:
        log.error("Gemini FHIR call failed: %s", err)
        return {"ok": False, "error": err}

    parsed = _parse_gemini_json(raw_text or "")
    normalized = _normalize_analysis(parsed)
    if not normalized:
        log.error("Gemini FHIR JSON parse failed. Raw=%s", (raw_text or "")[:2000])
        return {"ok": False, "error": "Invalid Gemini JSON"}

    normalized = apply_fhir_follow_up_hard_override(normalized, b)
    normalized["follow_up_days"] = _urgency_to_days(normalized["urgency"])
    log.info(
        "Gemini FHIR parsed (after FHIR hard override): follow_up_required=%s follow_up_type=%s urgency=%s",
        normalized.get("follow_up_required"),
        normalized.get("follow_up_type"),
        normalized.get("urgency"),
    )
    return normalized


def merge_gemini_into_payload(payload: dict[str, Any], gemini: dict[str, Any]) -> dict[str, Any]:
    """Attach follow-up fields and case-sheet summary to qualification payload."""
    out = dict(payload)
    days = int(gemini.get("follow_up_days") or 7)
    out["follow_up"] = {
        "required": bool(gemini.get("follow_up_required")),
        "days": days,
        "type": str(gemini.get("follow_up_type") or ""),
        "urgency": str(gemini.get("urgency") or ""),
    }
    cs = str(gemini.get("case_sheet_summary") or gemini.get("summary") or "").strip()
    if cs:
        out["case_sheet_summary"] = cs
    # Consumed by trigger_lead_agent to bypass rule-based score threshold when Gemini required follow-up.
    out["ci_gemini_follow_up_required"] = bool(gemini.get("follow_up_required"))
    return out


def run_test_with_bundle(bundle_path: str | None = None) -> dict[str, Any]:
    """
    Load a local FHIR Bundle (default: ``fhir/02_fhir_bundle.json``), run Gemini, return decision + WhatsApp message.

    For ``bench execute`` / manual verification. Does not create a Lead.
    """
    from call_intelligence.medplum_fhir import (
        _patient_name,
        extract_diagnosis_from_fhir_bundle,
        extract_service_request_code_text,
        soften_payload_after_gemini,
    )

    bp = Path(bundle_path) if bundle_path else default_fhir_bundle_path()
    b = load_fhir_bundle_json(bp)
    if not b:
        return {"ok": False, "error": "Bundle file missing or invalid", "bundle_path": str(bp)}

    g = analyze_case_sheet(b)
    if not g.get("ok"):
        return {"ok": False, "gemini": g, "error": g.get("error"), "bundle_path": str(bp)}

    payload: dict[str, Any] = {"patient_id": "", "patient_name": "", "phone": "", "diagnosis": ""}
    for r in _bundle_entry_resources(b):
        if str(r.get("resourceType") or "") == "Patient":
            pn = _patient_name(r)
            if pn:
                payload["patient_name"] = pn
            pid = str(r.get("id") or "").strip()
            if pid:
                payload["patient_id"] = pid
            break

    pid = str(payload.get("patient_id") or "").strip()
    if pid:
        cdx = extract_diagnosis_from_fhir_bundle(b, pid)
        if cdx:
            payload["diagnosis"] = cdx

    payload = merge_gemini_into_payload(payload, g)
    pid_hint = str(payload.get("patient_id") or "").strip() or None
    payload = soften_payload_after_gemini(payload, pid_hint)
    sr_lbl = extract_service_request_code_text(b, pid_hint)
    if sr_lbl:
        payload["follow_up_procedure_label"] = sr_lbl

    from call_intelligence.api import format_lq_whatsapp_message

    result = {
        "patient_id": str(payload.get("patient_id") or ""),
        "score": 1.0,
        "qualified": True,
        "action": "send_followup",
        "rationale": "run_test_with_bundle",
    }
    msg = format_lq_whatsapp_message(payload, result)
    frappe.logger(_LOG).info(
        "run_test_with_bundle decision: follow_up_required=%s type=%s urgency=%s",
        g.get("follow_up_required"),
        g.get("follow_up_type"),
        g.get("urgency"),
    )
    frappe.logger(_LOG).info("run_test_with_bundle message: %s", msg)
    return {
        "ok": True,
        "bundle_path": str(bp),
        "gemini": g,
        "decision": {
            "follow_up_required": g.get("follow_up_required"),
            "follow_up_type": g.get("follow_up_type"),
            "urgency": g.get("urgency"),
            "case_sheet_summary": g.get("case_sheet_summary") or g.get("summary"),
        },
        "message": msg,
    }
