"""
Whitelisted API methods for AI ingestion and ticket creation.
"""

from __future__ import annotations

import datetime
import json
import logging
import math
from pathlib import Path
from typing import Any

from werkzeug.wrappers import Response

import frappe
from frappe import _
from frappe.utils import escape_html, strip_html

_ingest_logger = logging.getLogger("call_intelligence.ingest")

from call_intelligence.whatsapp_inbound import process_inbound_whatsapp_cloud_webhook
from call_intelligence.whatsapp_integration import (
    get_admin_destination_number,
    get_whatsapp_test_mode,
    is_cloud_configured,
    is_twilio_configured,
    list_whatsapp_communications_for_lead,
    process_inbound_twilio_webhook,
    send_whatsapp_cloud_text_with_fallback,
    send_whatsapp_message_impl,
)
from frappe.utils.data import format_date, format_datetime, get_datetime, get_datetime_str, getdate

ALLOWED_SENTIMENT = {"Positive", "Neutral", "Negative"}
ALLOWED_OUTCOME = {"BOOKED", "NOT", "PENDING", "UNKNOWN"}

ALLOWED_ISSUE_TICKET_OUTCOME = frozenset({"Escalated", "Resolved", "Unknown"})
ALLOWED_CALL_CLASSIFICATION = frozenset({"Enquiry", "Discussion", "Complaint", "Follow-up"})
ALLOWED_PRIORITY_LEVEL = frozenset({"Low", "Medium", "High"})


@frappe.whitelist()
def send_whatsapp_message(
    phone: str | None = None,
    message: str | None = None,
    reference_doctype: str | None = None,
    reference_name: str | None = None,
) -> dict[str, Any]:
    """
    Send WhatsApp from Patient 360 (and similar callers).

    - `phone` is accepted for future use but ignored; destination is fixed for testing.
    - Uses WhatsApp Cloud API when configured; otherwise falls back to Twilio integration.
    """
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)

    msg = str(message or "").strip()
    if not msg:
        frappe.throw(_("message is required"))

    dest = get_admin_destination_number()

    if is_cloud_configured():
        res = send_whatsapp_cloud_text_with_fallback(
            message=msg,
            to_e164=dest,
            reference_doctype=str(reference_doctype or "Lead").strip(),
            reference_name=str(reference_name or "").strip(),
        )
        out: dict[str, Any] = {
            "ok": bool(res.get("ok")),
            "provider": "whatsapp_cloud",
            "destination": dest,
            "response": res,
        }
        if res.get("error_hint"):
            out["error_hint"] = res.get("error_hint")
        if res.get("fallback"):
            out["fallback"] = res.get("fallback")
        if res.get("text_attempt_failed"):
            out["note"] = _(
                "Free-form text was not accepted; an approved template message was sent instead."
            )
        return out

    return {
        "provider": "twilio",
        **send_whatsapp_message_impl(
            msg,
            str(reference_doctype or "Lead").strip(),
            str(reference_name or "").strip(),
        ),
    }


@frappe.whitelist(allow_guest=True)
def whatsapp_cloud_webhook() -> Any:
    """
    Inbound WhatsApp Cloud webhook.

    - GET: verification — return raw hub.challenge as text/plain (Meta; /api/method otherwise wraps in JSON)
    - POST: inbound messages
    """
    # Verification handshake (Meta)
    if frappe.request.method == "GET":
        q = frappe.form_dict
        # `frappe.conf` merges site + common config.
        expected = str(frappe.conf.get("call_intelligence_whatsapp_cloud_verify_token") or "").strip()
        mode = str(q.get("hub.mode") or "")
        provided = str(
            q.get("hub.verify_token")
            or q.get("hub.verifyToken")
            or q.get("hub.verify-token")
            or ""
        ).strip()

        if not expected:
            return {"ok": False, "reason": "verify_token_not_set"}

        if mode == "subscribe" and provided == expected and q.get("hub.challenge"):
            # Must return werkzeug Response: whitelisted str would become JSON {"message": "..."}.
            return Response(str(q.get("hub.challenge")), mimetype="text/plain", status=200)
        return {"ok": False, "reason": "verification_failed"}

    if frappe.request.method != "POST":
        return {"ok": True, "hint": "POST webhook JSON"}

    # Debug: confirm Meta is POSTing (tail -f sites/<site>/logs/web.log). If nothing here → Meta / subscription.
    _wa_log = logging.getLogger("call_intelligence.whatsapp_webhook")
    frappe.logger().info("🔥 WEBHOOK HIT")
    try:
        raw_bytes = frappe.request.get_data()
        raw_txt = (raw_bytes or b"").decode("utf-8", errors="replace")
    except Exception:
        raw_txt = str(getattr(frappe.request, "data", None) or "")[:12000]
    if len(raw_txt) > 12000:
        raw_txt = raw_txt[:12000] + "...(truncated)"
    frappe.logger().info(f"RAW BODY: {raw_txt}")
    _wa_log.info("WEBHOOK HIT (Meta POST received) path=%s content_type=%s", frappe.request.path, frappe.request.content_type or "")

    payload = frappe.local.form_dict
    # form_dict may not contain JSON body; fallback to request data
    try:
        if not payload:
            payload = frappe.request.get_json(silent=True) or {}
    except Exception:
        payload = {}

    if payload:
        _wa_log.info("Parsed JSON payload keys: %s", list((payload or {}).keys()))

    try:
        process_inbound_whatsapp_cloud_webhook(dict(payload or {}))
        frappe.db.commit()
    except Exception:
        frappe.log_error(title="whatsapp_cloud_webhook_post", message=frappe.get_traceback())
    return {"ok": True}


def _str_clean(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _normalize_call_classification(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    for opt in ALLOWED_CALL_CLASSIFICATION:
        if opt.lower() == s.lower():
            return opt
    return None


def _normalize_yes_no(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    s = str(value).strip().lower()
    if s in ("yes", "y", "true", "1"):
        return "Yes"
    if s in ("no", "n", "false", "0"):
        return "No"
    if s in ("yes", "no"):
        return s.title()
    return None


def _normalize_priority_level(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    k = s.lower()
    if k in ("low", "l"):
        return "Low"
    if k in ("medium", "med", "m"):
        return "Medium"
    if k in ("high", "h"):
        return "High"
    if s in ALLOWED_PRIORITY_LEVEL:
        return s
    return None


def _normalize_issue_ticket_outcome(value: Any) -> str | None:
    """Map pipeline or ticket labels to Escalated | Resolved | Unknown."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    for opt in ALLOWED_ISSUE_TICKET_OUTCOME:
        if opt.lower() == s.lower():
            return opt
    up = s.upper()
    if up in ALLOWED_OUTCOME:
        if up == "BOOKED":
            return "Resolved"
        if up == "NOT":
            return "Escalated"
        if up in ("PENDING", "UNKNOWN"):
            return "Unknown"
    if up in ("ESCALATED", "COMPLAINT"):
        return "Escalated"
    if up in ("RESOLVED",):
        return "Resolved"
    return "Unknown"


def _normalize_follow_up_flag(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    s = str(value).strip().lower() if value is not None else ""
    if s in ("1", "true", "yes", "y"):
        return 1
    return 0


def _issue_subject_from_structured_data(data: dict[str, Any]) -> str:
    """
    Dynamic Issue subject from JSON (no generic 'Call intake #…' templates).

    Prefer: department_to_handle — priority — call_classification (skips empty segments).
    Fallback: phone / filename / call_id (logged when sparse).
    """
    dth = _str_clean(data.get("department_to_handle"))
    dept = _str_clean(data.get("department"))
    dept_part = dth or dept

    pl = _normalize_priority_level(data.get("priority")) or ""
    cls = _normalize_call_classification(data.get("call_classification")) or _str_clean(
        data.get("call_classification")
    )

    segments: list[str] = []
    if dept_part:
        segments.append(dept_part)
    if pl:
        segments.append(pl)
    if cls:
        segments.append(cls)

    if segments:
        subj = _("Call — {0}").format(" — ".join(segments))
    else:
        phone = _normalize_phone_for_ingest(data.get("phone_number"))
        fn = _str_clean(data.get("filename"))
        cid = _str_clean(data.get("call_id"))
        tail = phone or (Path(fn).stem if fn else "") or cid
        subj = _("Call — {0}").format(tail or _("unknown"))
        _ingest_logger.warning(
            "issue_subject_fallback_minimal_data keys=%s",
            list(data.keys())[:30],
        )

    if len(subj) > 140:
        subj = subj[:137] + "…"
    return subj


def _minimal_issue_description(data: dict[str, Any]) -> str:
    """Fallback when ticket_notes / LeadNotes / transcript are empty."""
    cls = _normalize_call_classification(data.get("call_classification")) or ""
    out = _normalize_issue_ticket_outcome(data.get("outcome")) or ""
    parts = [p for p in (cls, out) if p]
    if parts:
        return "<p>" + " · ".join(escape_html(p) for p in parts) + "</p>"
    return "<p></p>"


def _issue_description_from_call_data(data: dict[str, Any]) -> str:
    """
    Standard Issue.description: ticket_notes (or LeadNotes), else transcript excerpt.
    Full structured text remains in ci_ticket_notes / ci_transcript.
    """
    tn = data.get("ticket_notes")
    if tn is None and data.get("LeadNotes") is not None:
        tn = data.get("LeadNotes")
    body = str(tn).strip() if tn is not None else ""
    if not body:
        tr = data.get("transcript")
        if tr is not None and str(tr).strip():
            body = str(tr).strip()[:4000]
    if not body:
        return _minimal_issue_description(data)
    safe = escape_html(body)
    safe = safe.replace("\n", "<br/>")
    return f"<p>{safe}</p>"


def _ingest_log_sparse_fields(data: dict[str, Any], path: str | None = None) -> list[str]:
    """Log when important JSON keys are missing (does not block ingest)."""
    want = (
        "department_to_handle",
        "priority",
        "call_classification",
        "ticket_notes",
        "sentiment_label",
        "outcome",
    )
    missing: list[str] = []
    for key in want:
        if key == "sentiment_label" and (data.get("sentiment") or data.get("sentiment_label")):
            continue
        if key == "ticket_notes" and (data.get("ticket_notes") or data.get("LeadNotes")):
            continue
        if key == "department_to_handle" and (
            data.get("department_to_handle") or data.get("department")
        ):
            continue
        v = data.get(key)
        if v is None:
            missing.append(key)
        elif isinstance(v, str) and not v.strip():
            missing.append(key)
        elif isinstance(v, float) and math.isnan(v):
            missing.append(key)
    if missing:
        _ingest_logger.warning(
            "sparse_call_json path=%s missing=%s",
            path or "inline",
            ",".join(missing),
        )
    return missing


def _populate_issue_from_call_dict(issue: Any, data: dict[str, Any]) -> None:
    """Map call / extract JSON keys into Issue custom fields (structured; not a JSON dump)."""
    raw_phone = data.get("phone_number")
    cleaned = _normalize_phone_for_ingest(raw_phone)
    _set_if_has_field(issue, "ci_phone_number", cleaned)

    cid = data.get("call_id")
    if cid is not None and str(cid).strip():
        _set_if_has_field(issue, "ci_call_id", str(cid).strip())
    else:
        _clear_if_has_field(issue, "ci_call_id")

    fn = data.get("filename")
    if fn is not None and str(fn).strip():
        _set_if_has_field(issue, "ci_filename", str(fn).strip())
    else:
        _clear_if_has_field(issue, "ci_filename")

    cn = data.get("customer_name")
    if cn is not None and str(cn).strip():
        _set_if_has_field(issue, "ci_customer_name", str(cn).strip())
    else:
        _clear_if_has_field(issue, "ci_customer_name")

    cc = _normalize_call_classification(data.get("call_classification"))
    _set_if_has_field(issue, "ci_call_classification", cc)

    ar = _normalize_yes_no(data.get("action_required"))
    _set_if_has_field(issue, "ci_action_required", ar)

    if data.get("action_description") is not None:
        _set_if_has_field(issue, "ci_action_description", str(data.get("action_description") or "").strip())
    else:
        _clear_if_has_field(issue, "ci_action_description")

    dth = data.get("department_to_handle")
    if dth is not None and str(dth).strip():
        _set_if_has_field(issue, "ci_department_to_handle", str(dth).strip())
    else:
        _clear_if_has_field(issue, "ci_department_to_handle")

    dept = data.get("department")
    if dept is not None and str(dept).strip():
        _set_if_has_field(issue, "ci_department", str(dept).strip())
    else:
        _clear_if_has_field(issue, "ci_department")

    docn = data.get("doctor_name") or data.get("doctor") or data.get("primary_doctor")
    if docn is not None and str(docn).strip():
        _set_if_has_field(issue, "ci_doctor_name", str(docn).strip())
    else:
        _clear_if_has_field(issue, "ci_doctor_name")

    pl = _normalize_priority_level(data.get("priority"))
    _set_if_has_field(issue, "ci_priority_level", pl)

    sl = _normalize_sentiment(str(data.get("sentiment_label") or data.get("sentiment") or "").strip() or None)
    _set_if_has_field(issue, "ci_sentiment_label", sl)

    ss = data.get("sentiment_summary")
    if ss is not None and str(ss).strip():
        _set_if_has_field(issue, "ci_sentiment_summary", str(ss).strip())
    else:
        _clear_if_has_field(issue, "ci_sentiment_summary")

    oo = _normalize_issue_ticket_outcome(data.get("outcome"))
    _set_if_has_field(issue, "ci_outcome", oo or "Unknown")

    if frappe.get_meta("Issue").has_field("ci_follow_up_required"):
        _set_if_has_field(issue, "ci_follow_up_required", _normalize_follow_up_flag(data.get("follow_up_required")))

    tr = data.get("transcript")
    if tr is not None and str(tr).strip():
        _set_if_has_field(issue, "ci_transcript", str(tr).strip())
    else:
        _clear_if_has_field(issue, "ci_transcript")

    cs = data.get("call_solution")
    if cs is not None and str(cs).strip():
        _set_if_has_field(issue, "ci_call_solution", str(cs).strip())
    else:
        _clear_if_has_field(issue, "ci_call_solution")

    tn = data.get("ticket_notes")
    if tn is None and data.get("LeadNotes"):
        tn = data.get("LeadNotes")
    if tn is not None and str(tn).strip():
        _set_if_has_field(issue, "ci_ticket_notes", str(tn).strip())
    else:
        _clear_if_has_field(issue, "ci_ticket_notes")

    ts = data.get("timestamp") or data.get("call_time")
    ct = _normalize_call_time(ts)
    if ct is not None:
        _set_if_has_field(issue, "ci_call_timestamp", ct)
    else:
        _clear_if_has_field(issue, "ci_call_timestamp")

    _set_if_has_field(issue, "ci_ticket_type", str(data.get("ci_ticket_type") or "Other").strip() or "Other")


def _apply_ci_priority_level_to_issue(issue: Any, data: dict[str, Any]) -> None:
    """Map ci_priority_level to ERPNext Issue Priority link when names match (Low/Medium/High)."""
    pl = _normalize_priority_level(data.get("priority"))
    if not pl or not hasattr(issue, "priority"):
        return
    if frappe.db.exists("Issue Priority", pl):
        issue.priority = pl


def _call_extract_output_dirs() -> list[Path]:
    """Folders containing call pipeline *.json (beside bench / dashboard output)."""
    bench = Path(frappe.utils.get_bench_path()).resolve()
    parent = bench.parent
    candidates = [
        parent / "call-entity-extract" / "output",
        parent / "call-entity-extract backup" / "output",
        parent / "output" / "call-entity-extract backup",
        parent / "output",
    ]
    return [p for p in candidates if p.is_dir()]


def _first_json_file() -> Path | None:
    for directory in _call_extract_output_dirs():
        if not directory.is_dir():
            continue
        files = sorted(directory.glob("*.json"))
        if files:
            return files[0]
    return None


def _all_json_files() -> list[Path]:
    """Unique *.json paths from call-entity-extract output dirs (beside bench)."""
    seen: set[str] = set()
    out: list[Path] = []
    for directory in _call_extract_output_dirs():
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.json")):
            key = str(path.resolve())
            if key not in seen:
                seen.add(key)
                out.append(path)
    return out


def _normalize_phone_for_ingest(raw: Any) -> str | None:
    """
    First value from comma-separated phone_number, non-digits stripped.
    Returns digits-only string if length >= 10, else None (invalid / missing).
    """
    if raw is None:
        return None
    if isinstance(raw, float) and math.isnan(raw):
        return None
    s = str(raw).strip()
    if not s:
        return None
    low = s.lower()
    if low in ("nan", "none", "null", "-", "n/a", "na"):
        return None
    first = s.split(",")[0].strip()
    digits = "".join(c for c in first if c.isdigit())
    if len(digits) >= 10:
        return digits
    return None


def _apply_phone_to_lead(lead: Any, raw_phone: Any) -> None:
    """Set mobile_no / phone from normalized value, or clear to None when invalid."""
    cleaned = _normalize_phone_for_ingest(raw_phone)
    if hasattr(lead, "mobile_no"):
        lead.mobile_no = cleaned if cleaned else None
    if hasattr(lead, "phone"):
        lead.phone = cleaned if cleaned else None
    if hasattr(lead, "phone_number"):
        # Added by call_intelligence custom fields; required for call matching.
        lead.phone_number = cleaned if cleaned else None


def _json_call_id(data: dict[str, Any], path: Path | None) -> str | None:
    cid = data.get("call_id")
    if cid is not None and str(cid).strip():
        return str(cid).strip()
    fn = data.get("filename")
    if fn is not None and str(fn).strip():
        return Path(str(fn)).stem
    if path is not None:
        return path.stem
    return None


def _populate_lead_from_call_json(
    lead: Any,
    data: dict[str, Any],
    path: Path | None,
    *,
    set_status_from_outcome: bool,
) -> None:
    """
    Map each JSON key to the matching Lead field only.
    LeadNotes / transcript / call_solution / sentiment_summary stay distinct.
    Minimal JSON (call_*.json): LeadNotes may backfill transcript + ci_ai_summary only when those keys are absent.

    When a key is missing or empty, the corresponding Lead field is cleared so re-ingest fixes stale
    duplicates from older mapping logic (no need to delete Leads).
    """
    ln = data.get("LeadNotes")
    notes = str(ln).strip() if ln is not None and str(ln).strip() else ""
    if notes:
        _set_if_has_field(lead, "ci_lead_notes", notes)
    else:
        _clear_if_has_field(lead, "ci_lead_notes")

    tr = data.get("transcript")
    if tr is not None and str(tr).strip():
        _set_if_has_field(lead, "transcript", str(tr).strip())
    elif notes:
        _set_if_has_field(lead, "transcript", notes)
    else:
        _clear_if_has_field(lead, "transcript")

    sentiment = data.get("sentiment_label") or data.get("sentiment")
    norm_sent = _normalize_sentiment(str(sentiment) if sentiment is not None else None)
    if norm_sent:
        _set_if_has_field(lead, "sentiment", norm_sent)
    else:
        _clear_if_has_field(lead, "sentiment")

    outcome_raw = data.get("outcome")
    if outcome_raw is None:
        norm_out = "UNKNOWN"
    elif isinstance(outcome_raw, float) and math.isnan(outcome_raw):
        norm_out = "UNKNOWN"
    else:
        os = str(outcome_raw).strip()
        if not os or os.lower() == "nan":
            norm_out = "UNKNOWN"
        else:
            norm_out = _normalize_outcome(os) or "UNKNOWN"
    _set_if_has_field(lead, "outcome", norm_out)

    if set_status_from_outcome:
        lead.status = _lead_status_from_outcome(norm_out)
        _set_if_has_field(lead, "lead_status", _lead_status_from_outcome_ci(norm_out))

    ct = _normalize_call_time(data.get("timestamp") or data.get("call_time"))
    if ct is not None:
        _set_if_has_field(lead, "call_time", ct)
        _set_if_has_field(lead, "call_timestamp", ct)
    else:
        _clear_if_has_field(lead, "call_time")
        _clear_if_has_field(lead, "call_timestamp")

    dept = data.get("department") or data.get("department_to_handle")
    dept_s = str(dept).strip() if dept is not None else ""
    if dept_s:
        _set_if_has_field(lead, "intent", dept_s)
        _set_if_has_field(lead, "ci_ai_department", dept_s)
    else:
        _clear_if_has_field(lead, "ci_ai_department")
        _clear_if_has_field(lead, "intent")

    rt = data.get("recordType") or data.get("record_type")
    rt_norm = _normalize_p360_record_type_for_storage(rt)
    if rt_norm:
        _set_if_has_field(lead, "ci_record_type", rt_norm)
    else:
        _clear_if_has_field(lead, "ci_record_type")

    docn = data.get("doctor_name") or data.get("doctor") or data.get("primary_doctor")
    if docn is not None and str(docn).strip():
        _set_if_has_field(lead, "ci_doctor", str(docn).strip())
    else:
        _clear_if_has_field(lead, "ci_doctor")

    loc = data.get("location") or data.get("patient_location")
    if loc is not None and str(loc).strip():
        _set_if_has_field(lead, "ci_ai_location", str(loc).strip())
    else:
        _clear_if_has_field(lead, "ci_ai_location")

    serv = data.get("services")
    if serv is not None and str(serv).strip():
        _set_if_has_field(lead, "ci_services", str(serv).strip())
    else:
        _clear_if_has_field(lead, "ci_services")

    summ = data.get("summary")
    if summ is not None and str(summ).strip():
        _set_if_has_field(lead, "ci_ai_summary", str(summ).strip())
    else:
        cc = data.get("call_classification")
        if cc is not None and str(cc).strip():
            _set_if_has_field(lead, "ci_ai_summary", str(cc).strip())
        elif notes:
            _set_if_has_field(lead, "ci_ai_summary", notes)
        else:
            _clear_if_has_field(lead, "ci_ai_summary")

    ss = data.get("sentiment_summary")
    if ss is not None and str(ss).strip():
        _set_if_has_field(lead, "ci_sentiment_summary", str(ss).strip())
    else:
        _clear_if_has_field(lead, "ci_sentiment_summary")

    cs = data.get("call_solution")
    if cs is not None and str(cs).strip():
        _set_if_has_field(lead, "ci_call_solution", str(cs).strip())
    else:
        _clear_if_has_field(lead, "ci_call_solution")

    meta_lead_pf = frappe.get_meta("Lead")
    pl_n = _normalize_priority_level(data.get("priority"))
    if meta_lead_pf.has_field("priority_score"):
        if pl_n:
            score_map = {"Low": 0.33, "Medium": 0.66, "High": 1.0}
            if pl_n in score_map:
                _set_if_has_field(lead, "priority_score", score_map[pl_n])
            else:
                _clear_if_has_field(lead, "priority_score")
        else:
            _clear_if_has_field(lead, "priority_score")

    if data.get("action_required") is not None:
        _set_if_has_field(lead, "ci_action_required", str(data.get("action_required")).strip())
    else:
        pr = data.get("priority")
        if pr is not None and str(pr).strip():
            _set_if_has_field(lead, "ci_action_required", str(pr).strip())
        else:
            _clear_if_has_field(lead, "ci_action_required")

    if data.get("action_description") is not None:
        ad = data.get("action_description")
        _set_if_has_field(lead, "ci_action_description", str(ad).strip() if ad else "")
    else:
        cc = data.get("call_classification")
        if cc is not None and str(cc).strip():
            _set_if_has_field(lead, "ci_action_description", str(cc).strip())
        else:
            _clear_if_has_field(lead, "ci_action_description")

    ap_set = False
    for ap_key in ("appointment_date", "appointmentDate", "scheduled_date", "scheduledDate"):
        raw_ap = data.get(ap_key)
        if raw_ap is None or not str(raw_ap).strip():
            continue
        try:
            _set_if_has_field(lead, "appointment_date", getdate(raw_ap))
        except Exception:
            _set_if_has_field(lead, "appointment_date", str(raw_ap).strip())
        ap_set = True
        break
    if not ap_set:
        _clear_if_has_field(lead, "appointment_date")

    cid = _json_call_id(data, path)
    if cid:
        _set_if_has_field(lead, "call_id", cid)
    else:
        _clear_if_has_field(lead, "call_id")

    src = data.get("hospital_name") or data.get("source")
    if hasattr(lead, "source"):
        cur_src = (getattr(lead, "source", None) or "").strip()
        if not cur_src:
            if src and str(src).strip():
                lead.source = str(src).strip()
            elif data.get("source_type") and str(data.get("source_type")).strip():
                lead.source = str(data.get("source_type")).strip()
            elif set_status_from_outcome:
                lead.source = dept_s or "Call Intelligence"


def _normalize_call_time(value: Any) -> str | None:
    """Convert ISO-8601 to MySQL-safe naive datetime string (no tz offset)."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = get_datetime(s)
    except Exception:
        return None
    if dt is None:
        return None
    if isinstance(dt, datetime.datetime) and dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    elif isinstance(dt, datetime.date) and not isinstance(dt, datetime.datetime):
        dt = datetime.datetime.combine(dt, datetime.time.min)
    if not isinstance(dt, datetime.datetime):
        return None
    return get_datetime_str(dt)


_LEAD_STATUS_OPTIONS = frozenset(
    {
        "Lead",
        "Open",
        "Replied",
        "Opportunity",
        "Quotation",
        "Lost Quotation",
        "Interested",
        "Converted",
        "Do Not Contact",
    }
)


def _lead_status_from_outcome(outcome: str | None) -> str:
    """Map pipeline outcome to ERPNext Lead status (Select options)."""
    if outcome:
        o = str(outcome).strip()
        if o in _LEAD_STATUS_OPTIONS:
            return o
        up = o.upper()
        mapped = {"BOOKED": "Interested", "NOT": "Do Not Contact", "PENDING": "Open", "UNKNOWN": "Open"}
        if up in mapped:
            return mapped[up]
    return "Open"


def _lead_status_from_outcome_ci(outcome: str | None) -> str:
    """Map pipeline outcome to call-level `Lead.lead_status` options."""
    if not outcome:
        return "Lead"
    o = str(outcome).strip().upper()
    mapping = {
        "BOOKED": "Confirmed",
        "PENDING": "Follow-up Required",
        "NOT": "Lead",
        "UNKNOWN": "Lead",
    }
    return mapping.get(o, "Lead")


def _existing_lead_name_for_phone(phone: str | None) -> str | None:
    """Match by normalized digits-only phone (same as stored mobile_no)."""
    if not phone:
        return None
    if frappe.db.exists("Lead", {"mobile_no": phone}):
        return frappe.db.get_value("Lead", {"mobile_no": phone}, "name")
    if frappe.db.exists("Lead", {"phone": phone}):
        return frappe.db.get_value("Lead", {"phone": phone}, "name")
    return None


def _resolve_issue_priority(priority: str | None, by_lower: dict[str, str]) -> str | None:
    if not priority:
        return None
    raw = str(priority).strip()
    if not raw:
        return None
    if frappe.db.exists("Issue Priority", raw):
        return raw
    return by_lower.get(raw.lower())


def _priority_rank(value: str | None) -> int:
    """Higher rank means higher urgency."""
    v = str(value or "").strip().lower()
    if any(x in v for x in ("urgent", "critical", "emergency")):
        return 3
    if "high" in v:
        return 3
    if "medium" in v:
        return 2
    if "low" in v:
        return 1
    return 0


def _p360_best_priority_from_issue_row(ir: dict[str, Any]) -> str:
    """
    Prefer the stronger signal between ERPNext Issue.priority (link) and ci_priority_level (AI JSON).
    Using `priority or ci_priority_level` alone can hide Medium on the list when the link is set to a
    generic value with rank 0.
    """
    p = str(ir.get("priority") or "").strip()
    pl = str(ir.get("ci_priority_level") or "").strip()
    best = ""
    for c in (p, pl):
        if not c:
            continue
        if _priority_rank(c) > _priority_rank(best):
            best = c
    if best:
        return best
    return pl or p


@frappe.whitelist()
def get_patient_data() -> dict[str, Any]:
    """Read the first *.json from call-entity-extract/output (beside bench)."""
    path = _first_json_file()
    if not path:
        frappe.throw(
            _("No JSON file found. Add *.json under call-entity-extract/output (next to your bench folder).")
        )
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        frappe.throw(_("Invalid JSON in {0}: {1}").format(path.name, str(e)))

    if not isinstance(data, dict):
        frappe.throw(_("JSON root must be an object"))

    return {
        "name": data.get("customer_name") or "",
        "phone": str(data.get("phone_number") or "").strip(),
        "department": data.get("department") or "",
        "status": str(data.get("outcome") or "").strip(),
        "sentiment": str(data.get("sentiment_label") or "").strip(),
    }


def _p360_desc_snippet(html: str | None, max_len: int = 240) -> str:
    if not html:
        return ""
    t = strip_html(str(html)).strip()
    if len(t) > max_len:
        return t[: max_len - 1] + "…"
    return t


def _p360_skip_tag_val(v: str) -> bool:
    if not v or not str(v).strip():
        return True
    lo = str(v).strip().lower()
    return lo in ("unknown", "n/a", "na", "none", "—")


def _p360_patient_type_label(record_type: str) -> str:
    """Friendly label from JSON recordType / Lead.ci_record_type."""
    k = (record_type or "").strip().lower()
    if k in ("ticket", "tickets"):
        return _("Ticket")
    if k in ("lead", "leads"):
        return _("Outpatient")
    s = (record_type or "").strip()
    return s if s else _("Outpatient")


def _p360_build_tags(
    sentiment: str,
    outcome: str,
    department: str,
    services: str,
    issue0: dict[str, Any] | None,
) -> list[str]:
    """Chips for Patient 360 — from Lead + first Issue (JSON-backed fields)."""
    tags: list[str] = []
    for v in (sentiment, outcome, department, services):
        vs = str(v or "").strip()
        if not _p360_skip_tag_val(vs):
            tags.append(vs)
    if issue0:
        for key in ("call_classification", "ticket_type", "department_to_handle"):
            vs = str(issue0.get(key) or "").strip()
            if vs and not _p360_skip_tag_val(vs):
                tags.append(vs)
    seen: set[str] = set()
    out: list[str] = []
    for t in tags:
        tl = t.lower()
        if tl not in seen and len(out) < 10:
            seen.add(tl)
            out.append(t)
    return out


def _normalize_p360_record_type_for_storage(value: Any) -> str | None:
    """Map JSON recordType to Lead.ci_record_type: lead | ticket (incl. tickets → ticket)."""
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    if s in ("lead", "leads"):
        return "lead"
    if s in ("ticket", "tickets", "issue", "issues"):
        return "ticket"
    return s


def _p360_norm_ws(s: Any) -> str:
    return " ".join(str(s or "").split()).strip().lower()


def _p360_issue_story_fingerprint(iss: dict[str, Any]) -> str:
    """Stable key for duplicate Issues (ignores newlines, minor spacing, Issue creation time)."""
    notes = str(iss.get("ticket_notes") or "").strip()
    tr = str(iss.get("transcript") or "").strip()
    narrative = notes if len(notes) >= len(tr) else tr
    if not narrative:
        narrative = notes or tr
    return "||".join(
        [
            _p360_norm_ws(narrative),
            _p360_norm_ws(iss.get("customer_name")),
            _p360_norm_ws(iss.get("doctor_name")),
            _p360_norm_ws(iss.get("department") or iss.get("department_to_handle")),
        ]
    )


def _dedupe_p360_issue_dicts(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Drop duplicate Issues for Patient 360 (newest first from query).
    ci_call_id wins; else same story fingerprint (normalized narrative + patient/doctor/dept).
    """
    seen_ci: set[str] = set()
    seen_fp: set[str] = set()
    out: list[dict[str, Any]] = []
    for iss in issues:
        cid = (iss.get("ci_call_id") or "").strip()
        if cid:
            if cid in seen_ci:
                continue
            seen_ci.add(cid)
            out.append(iss)
            continue
        fp = _p360_issue_story_fingerprint(iss)
        if fp in seen_fp:
            continue
        seen_fp.add(fp)
        out.append(iss)
    return out


def _p360_normalize_phone_key(phone: str | None) -> str:
    """Last 10 digits for overlap matching (e.g. India); empty if too short."""
    if not phone:
        return ""
    digits = "".join(c for c in str(phone) if c.isdigit())
    if len(digits) >= 10:
        return digits[-10:]
    if len(digits) >= 8:
        return digits
    return ""


GENERIC_P360_LEAD_NAMES = frozenset(
    {
        "unknown",
        "unknown caller",
        "caller",
        "nan",
        "-",
        "n/a",
        "na",
        "test",
        "none",
    }
)


def _p360_normalize_lead_name_key(name: Any) -> str:
    """Stable key for mutual exclusion; skip short / generic names."""
    if name is None:
        return ""
    s = str(name).strip().lower()
    if len(s) < 5 or s in GENERIC_P360_LEAD_NAMES:
        return ""
    return s


def _p360_ticket_phone_keys() -> set[str]:
    """Phones that already have a ticket-type Lead (JSON recordType ticket wins over the Leads tab)."""
    meta = frappe.get_meta("Lead")
    if not meta.has_field("ci_record_type"):
        return set()
    rows = frappe.get_all(
        "Lead",
        filters={"ci_record_type": ["in", ["ticket", "tickets"]]},
        fields=["mobile_no", "phone"],
        limit_page_length=0,
    )
    keys: set[str] = set()
    for r in rows:
        ph = (r.get("mobile_no") or r.get("phone") or "").strip()
        k = _p360_normalize_phone_key(ph)
        if k:
            keys.add(k)
    return keys


def _p360_ticket_name_keys() -> set[str]:
    """Non-generic lead names on ticket-type Leads (same person must not appear under Leads tab)."""
    meta = frappe.get_meta("Lead")
    if not meta.has_field("ci_record_type"):
        return set()
    rows = frappe.get_all(
        "Lead",
        filters={"ci_record_type": ["in", ["ticket", "tickets"]]},
        fields=["lead_name"],
        limit_page_length=0,
    )
    keys: set[str] = set()
    for r in rows:
        k = _p360_normalize_lead_name_key(r.get("lead_name"))
        if k:
            keys.add(k)
    return keys


def _p360_row_is_medplum_encounter_lead(row: dict[str, Any]) -> bool:
    """Medplum ingest creates distinct Lead rows per event; never hide them as ticket dupes."""
    cid = str(row.get("call_id") or "").strip()
    if cid.startswith("medplum-Encounter:"):
        return True
    src = str(row.get("source") or "").strip().lower()
    return "medplum" in src


def _p360_exclude_lead_rows_if_ticket_wins(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop lead-type rows when ticket JSON already owns that contact (phone or non-generic name)."""
    meta = frappe.get_meta("Lead")
    if not meta.has_field("ci_record_type"):
        return rows
    ticket_phones = _p360_ticket_phone_keys()
    ticket_names = _p360_ticket_name_keys()
    if not ticket_phones and not ticket_names:
        return rows
    out: list[dict[str, Any]] = []
    for r in rows:
        if _p360_row_is_medplum_encounter_lead(r):
            out.append(r)
            continue
        ph = (r.get("mobile_no") or r.get("phone") or "").strip()
        k = _p360_normalize_phone_key(ph)
        if k and k in ticket_phones:
            continue
        ln = _p360_normalize_lead_name_key(r.get("lead_name"))
        if ln and ln in ticket_names:
            continue
        out.append(r)
    return out


def _p360_winner_lead_batch(batch: list[dict[str, Any]]) -> dict[str, Any]:
    """When duplicate phones exist: keep ticket-type Lead (JSON ticket); else newest."""
    def mod_key(x: dict[str, Any]) -> str:
        return str(x.get("modified") or x.get("creation") or "")

    def is_ticket(x: dict[str, Any]) -> bool:
        return str(x.get("ci_record_type") or "").lower() in ("ticket", "tickets")

    tickets = [x for x in batch if is_ticket(x)]
    if tickets:
        tickets.sort(key=mod_key, reverse=True)
        return tickets[0]
    batch.sort(key=mod_key, reverse=True)
    return batch[0]


def _p360_enrich_lead_list_from_issues(
    lead_names: list[str],
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Snippet + best priority + newest Issue subject per Lead (custom_lead)."""
    issue_snippet_by_lead: dict[str, str] = {}
    issue_priority_by_lead: dict[str, str] = {}
    issue_subject_by_lead: dict[str, str] = {}
    if not lead_names or not frappe.get_meta("Issue").has_field("custom_lead"):
        return issue_snippet_by_lead, issue_priority_by_lead, issue_subject_by_lead
    issue_rows = frappe.get_all(
        "Issue",
        filters={"custom_lead": ["in", lead_names]},
        fields=[
            "custom_lead",
            "subject",
            "ci_ticket_notes",
            "ci_transcript",
            "description",
            "priority",
            "ci_priority_level",
            "creation",
        ],
        order_by="creation desc",
        limit_page_length=0,
    )
    for ir in issue_rows:
        lid = (ir.get("custom_lead") or "").strip()
        if not lid:
            continue
        if lid not in issue_subject_by_lead:
            sj = str(ir.get("subject") or "").strip()
            if sj:
                issue_subject_by_lead[lid] = sj
        if lid not in issue_snippet_by_lead:
            issue_snippet_by_lead[lid] = _p360_desc_snippet(
                ir.get("ci_ticket_notes") or ir.get("ci_transcript") or ir.get("description") or "",
                max_len=140,
            )
        candidate = _p360_best_priority_from_issue_row(ir)
        if not candidate:
            continue
        current = issue_priority_by_lead.get(lid, "")
        if _priority_rank(candidate) > _priority_rank(current):
            issue_priority_by_lead[lid] = candidate
    return issue_snippet_by_lead, issue_priority_by_lead, issue_subject_by_lead


def _p360_lead_list_query_fields(meta_lead: Any) -> list[str]:
    fields = [
        "name",
        "lead_name",
        "mobile_no",
        "phone",
        "modified",
        "ci_lead_notes",
        "ci_ai_summary",
        "transcript",
    ]
    if meta_lead.has_field("priority_score"):
        fields.append("priority_score")
    if meta_lead.has_field("call_id"):
        fields.append("call_id")
    if meta_lead.has_field("source"):
        fields.append("source")
    return fields


def _p360_lead_priority_from_row(row: dict[str, Any], issue_priority_by_lead: dict[str, str]) -> str:
    """List card priority: Issues first, then Lead custom fields when no Issue."""
    lid = str(row.get("name") or "").strip()
    ip = str(issue_priority_by_lead.get(lid) or "").strip()
    if ip:
        return ip
    # Lead-only / no linked Issue yet: JSON often maps priority into intent or notes — not ideal;
    # use priority_score bands when present.
    ps = row.get("priority_score")
    if ps is not None:
        try:
            x = float(ps)
            if x >= 0.67:
                return _("High")
            if x >= 0.34:
                return _("Medium")
            if x > 0:
                return _("Low")
        except (TypeError, ValueError):
            pass
    return ""


def _p360_lead_rows_to_list_payload(
    rows: list[dict[str, Any]],
    issue_snippet_by_lead: dict[str, str],
    issue_priority_by_lead: dict[str, str],
    snippet_field: str = "lead_notes",
    max_items: int = 30,
    issue_subject_by_lead: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """One list card per Lead document (no phone deduplication — same number may appear many times)."""
    out: list[dict[str, Any]] = []
    for row in rows:
        if len(out) >= max_items:
            break
        phone = (row.get("mobile_no") or row.get("phone") or "").strip()
        summary_raw = row.get("ci_lead_notes") or row.get("ci_ai_summary") or row.get("transcript") or ""
        if not str(summary_raw or "").strip():
            summary_raw = issue_snippet_by_lead.get(row.get("name") or "", "")
        lid = row.get("name") or ""
        item = {
            "name": row.get("name"),
            "lead_name": row.get("lead_name") or row.get("name"),
            "phone": phone,
            "modified": str(row.get("modified") or "")[:32],
            "priority": _p360_lead_priority_from_row(row, issue_priority_by_lead),
        }
        if issue_subject_by_lead is not None:
            item["issue_subject"] = str(issue_subject_by_lead.get(lid) or "").strip()
        item[snippet_field] = _p360_desc_snippet(summary_raw, max_len=140)
        out.append(item)
    return out


@frappe.whitelist()
def get_patient_360_leads() -> list[dict[str, Any]]:
    """
    Patient 360 “Leads” list: show normal CRM Leads plus ingest-tagged lead/leads.

    Previously only ``ci_record_type in (lead, leads)`` matched, so every Lead created from
    Desk (empty ``ci_record_type``) disappeared — an empty dashboard on a fresh Docker site.
    We exclude only ticket-style rows; unset/other values still appear.
    """
    meta_lead = frappe.get_meta("Lead")
    fields = _p360_lead_list_query_fields(meta_lead)
    list_kw: dict[str, Any] = {
        "fields": fields,
        "order_by": "modified desc",
    }
    if meta_lead.has_field("ci_record_type"):
        list_kw["limit_page_length"] = 500
        rows = frappe.get_all("Lead", **list_kw)
        rows = [
            r
            for r in rows
            if str(r.get("ci_record_type") or "").strip().lower() not in ("ticket", "tickets")
        ]
        rows = rows[:200]
    else:
        list_kw["limit_page_length"] = 200
        rows = frappe.get_all("Lead", **list_kw)
    rows = _p360_exclude_lead_rows_if_ticket_wins(rows)
    lead_names = [r.get("name") for r in rows if r.get("name")]
    issue_snippet_by_lead, issue_priority_by_lead, issue_subject_by_lead = _p360_enrich_lead_list_from_issues(
        lead_names
    )
    return _p360_lead_rows_to_list_payload(
        rows, issue_snippet_by_lead, issue_priority_by_lead, "lead_notes", 200, issue_subject_by_lead
    )


@frappe.whitelist()
def get_patient_360_leads_with_tickets() -> list[dict[str, Any]]:
    """
    Leads with recordType ticket (ci_record_type) for Patient 360 Tickets mode.
    If ci_record_type is missing on Lead, falls back to leads that have a linked Issue.
    """
    meta_lead = frappe.get_meta("Lead")
    if meta_lead.has_field("ci_record_type"):
        rows = frappe.get_all(
            "Lead",
            filters={"ci_record_type": ["in", ["ticket", "tickets"]]},
            fields=_p360_lead_list_query_fields(meta_lead),
            order_by="modified desc",
            limit_page_length=200,
        )
        lead_names = [r.get("name") for r in rows if r.get("name")]
        issue_snippet_by_lead, issue_priority_by_lead, issue_subject_by_lead = _p360_enrich_lead_list_from_issues(
            lead_names
        )
        return _p360_lead_rows_to_list_payload(
            rows, issue_snippet_by_lead, issue_priority_by_lead, "ticket_notes", 30, issue_subject_by_lead
        )

    if not frappe.get_meta("Issue").has_field("custom_lead"):
        return []

    raw = frappe.get_all(
        "Issue",
        filters={"custom_lead": ["!=", ""]},
        pluck="custom_lead",
        limit_page_length=0,
    )
    seen: set[str] = set()
    unique_ids: list[str] = []
    for lead_id in raw:
        if not lead_id or not isinstance(lead_id, str):
            continue
        lead_id = lead_id.strip()
        if not lead_id or lead_id in seen:
            continue
        if not frappe.db.exists("Lead", lead_id):
            continue
        seen.add(lead_id)
        unique_ids.append(lead_id)

    issue_note_by_lead: dict[str, str] = {}
    issue_priority_by_lead: dict[str, str] = {}
    issue_subject_by_lead: dict[str, str] = {}
    issue_rows = frappe.get_all(
        "Issue",
        filters={"custom_lead": ["in", unique_ids]},
        fields=[
            "custom_lead",
            "subject",
            "ci_ticket_notes",
            "ci_transcript",
            "description",
            "priority",
            "ci_priority_level",
            "creation",
        ],
        order_by="creation desc",
        limit_page_length=0,
    )
    for ir in issue_rows:
        lid = (ir.get("custom_lead") or "").strip()
        if not lid:
            continue
        if lid not in issue_subject_by_lead:
            sj = str(ir.get("subject") or "").strip()
            if sj:
                issue_subject_by_lead[lid] = sj
        if lid not in issue_note_by_lead:
            issue_note_by_lead[lid] = _p360_desc_snippet(
                ir.get("ci_ticket_notes") or ir.get("ci_transcript") or ir.get("description") or "",
                max_len=140,
            )
        candidate = _p360_best_priority_from_issue_row(ir)
        if not candidate:
            continue
        current = issue_priority_by_lead.get(lid, "")
        if _priority_rank(candidate) > _priority_rank(current):
            issue_priority_by_lead[lid] = candidate

    rows_legacy: list[dict[str, Any]] = []
    meta_for_cols = frappe.get_meta("Lead")
    legacy_cols = ["lead_name", "mobile_no", "phone", "modified", "ci_lead_notes", "ci_ai_summary", "transcript"]
    if meta_for_cols.has_field("priority_score"):
        legacy_cols.append("priority_score")
    for name in unique_ids:
        lead = frappe.db.get_value(
            "Lead",
            name,
            legacy_cols,
            as_dict=True,
        ) or {}
        row_d: dict[str, Any] = {
            "name": name,
            "lead_name": lead.get("lead_name") or name,
            "mobile_no": lead.get("mobile_no"),
            "phone": lead.get("phone"),
            "modified": lead.get("modified"),
            "ci_lead_notes": lead.get("ci_lead_notes"),
            "ci_ai_summary": lead.get("ci_ai_summary"),
            "transcript": lead.get("transcript"),
        }
        if meta_for_cols.has_field("priority_score"):
            row_d["priority_score"] = lead.get("priority_score")
        rows_legacy.append(row_d)

    rows_legacy.sort(key=lambda x: str(x.get("lead_name") or x.get("name") or "").lower())
    return _p360_lead_rows_to_list_payload(
        rows_legacy, issue_note_by_lead, issue_priority_by_lead, "ticket_notes", 30, issue_subject_by_lead
    )


@frappe.whitelist()
def get_patient_360_meta() -> dict[str, Any]:
    """Lead status options + users for Update Lead panel."""
    meta = frappe.get_meta("Lead")
    opts: list[str] = []
    sf = meta.get_field("status")
    if sf and sf.options:
        opts = [x.strip() for x in str(sf.options).split("\n") if x.strip()]
    users = frappe.get_all(
        "User",
        filters={"enabled": 1, "user_type": "System User"},
        fields=["name", "full_name"],
        order_by="full_name asc",
        limit_page_length=100,
    )
    return {"lead_statuses": opts, "users": users}


@frappe.whitelist()
def update_lead_quick(
    lead_name: str | None = None,
    status: str | None = None,
    lead_owner: str | None = None,
    remarks: str | None = None,
) -> dict[str, Any]:
    """Update Lead status / assignee; optional remark as timeline comment."""
    if not lead_name or not str(lead_name).strip():
        frappe.throw(_("lead_name is required"))
    lead_id = str(lead_name).strip()
    lead = frappe.get_doc("Lead", lead_id)
    if not frappe.has_permission("Lead", "write", lead):
        frappe.throw(_("Not permitted to update this Lead"), frappe.PermissionError)

    if status:
        lead.status = status
    meta = frappe.get_meta("Lead")
    if lead_owner is not None and meta.has_field("lead_owner"):
        lead.lead_owner = lead_owner or None

    lead.save()

    if remarks and str(remarks).strip():
        lead.add_comment("Comment", str(remarks).strip())

    frappe.db.commit()
    return {"ok": True, "name": lead.name}


@frappe.whitelist()
def get_patient_360_data(lead_name: str | None = None) -> dict[str, Any]:
    """
    Patient 360: Lead profile, custom AI fields, Issues, merged activity timeline.

    Args:
        lead_name: Lead document name (e.g. CRM-LEAD-2025-00001).
    """
    if not lead_name or not str(lead_name).strip():
        frappe.throw(_("lead_name is required"))

    lead_id = str(lead_name).strip()
    if not frappe.db.exists("Lead", lead_id):
        frappe.throw(_("Lead {0} not found").format(lead_id))

    if not frappe.get_meta("Issue").has_field("custom_lead"):
        frappe.throw(
            _("Issue.custom_lead is missing. Run `bench migrate` after installing call_intelligence."),
            frappe.ValidationError,
        )

    lead = frappe.get_doc("Lead", lead_id)
    meta_lead = frappe.get_meta("Lead")

    def gf(fieldname: str) -> Any:
        if meta_lead.has_field(fieldname):
            return lead.get(fieldname)
        return None

    def _s(fieldname: str) -> str:
        if not meta_lead.has_field(fieldname):
            return ""
        v = lead.get(fieldname)
        if v is None:
            return ""
        return str(v).strip()

    phone = (getattr(lead, "mobile_no", None) or getattr(lead, "phone", None) or "") or ""
    phone = str(phone).strip()
    if phone.lower() in ("nan", "none", "null", "-", "n/a", "na"):
        phone = ""
    email = _s("email_id")
    src = _s("source")

    transcript_stored = _s("transcript")
    sum_stored = _s("ci_ai_summary")
    notes_stored = _s("ci_lead_notes")

    call_time = gf("call_time")
    timestamp_display = format_datetime(call_time) if call_time else ""

    appt_raw = gf("appointment_date")
    upcoming_appointment = ""
    if appt_raw:
        try:
            upcoming_appointment = format_date(getdate(appt_raw))
        except Exception:
            upcoming_appointment = str(appt_raw).strip()

    creation_fmt = format_datetime(lead.creation) if lead.creation else ""

    dep_show = _s("ci_ai_department") or _s("intent")
    silent_call = not any(
        [
            transcript_stored,
            sum_stored,
            notes_stored,
            dep_show,
            timestamp_display,
            _s("ci_doctor"),
            _s("ci_ai_location"),
            _s("ci_services"),
            _s("ci_call_solution"),
            _s("ci_sentiment_summary"),
            _s("ci_action_required"),
        ]
    )

    summary_disp = sum_stored or _("No summary")
    transcript_disp = transcript_stored
    sentiment_disp = _s("sentiment") or _("UNKNOWN")
    outcome_disp = _s("outcome") or _("UNKNOWN")
    if notes_stored:
        lead_notes_disp = notes_stored
    elif silent_call:
        lead_notes_disp = _("No interaction recorded")
    else:
        lead_notes_disp = ""

    display_name = (lead.lead_name or lead.name or "").strip() or _("Unknown Caller")

    ai = {
        "sentiment": sentiment_disp,
        "sentiment_summary": _s("ci_sentiment_summary") or _("N/A"),
        "outcome": outcome_disp,
        "call_solution": _s("ci_call_solution") or _("N/A"),
        "transcript": transcript_stored,
        "action_required": _s("ci_action_required") or _("N/A"),
        "action_description": _s("ci_action_description") or _("N/A"),
        "summary": summary_disp,
        "lead_notes": lead_notes_disp,
    }

    meta_issue = frappe.get_meta("Issue")
    issue_fields = ["name", "subject", "status", "priority", "creation", "modified", "description"]
    for fn in (
        "ci_ticket_notes",
        "ci_transcript",
        "ci_action_required",
        "ci_action_description",
        "ci_customer_name",
        "ci_call_timestamp",
        "ci_call_classification",
        "ci_ticket_type",
        "booking_status",
        "ci_department",
        "ci_department_to_handle",
        "ci_doctor_name",
        "ci_follow_up_required",
        "ci_outcome",
        "ci_priority_level",
        "ci_call_solution",
        "ci_call_id",
    ):
        if meta_issue.has_field(fn):
            issue_fields.append(fn)

    rows = frappe.get_all(
        "Issue",
        filters={"custom_lead": lead_id},
        fields=issue_fields,
        order_by="creation desc, modified desc",
        limit_page_length=0,
    )

    issues: list[dict[str, Any]] = []
    timeline_raw: list[dict[str, Any]] = []

    for row in rows:
        raw_c = row.get("creation")
        c_fmt = format_datetime(raw_c) if raw_c else ""
        desc_raw = strip_html(str(row.get("description") or "")).strip()
        tnotes = str(row.get("ci_transcript") or row.get("ci_ticket_notes") or "").strip()
        display_notes = tnotes or desc_raw
        desc_snip = _p360_desc_snippet(row.get("description"))
        issues.append(
            {
                "name": row.get("name"),
                "subject": row.get("subject") or "",
                "status": row.get("status") or "",
                "priority": row.get("priority") or "",
                "priority_level": str(row.get("ci_priority_level") or "").strip(),
                "creation": c_fmt,
                "description": desc_raw,
                "description_preview": desc_snip,
                "ticket_notes": str(row.get("ci_ticket_notes") or "").strip(),
                "transcript": str(row.get("ci_transcript") or "").strip(),
                "action_required": str(row.get("ci_action_required") or "").strip(),
                "action_description": str(row.get("ci_action_description") or "").strip(),
                "customer_name": str(row.get("ci_customer_name") or "").strip(),
                "call_timestamp": format_datetime(row.get("ci_call_timestamp"))
                if row.get("ci_call_timestamp")
                else "",
                "call_classification": str(row.get("ci_call_classification") or "").strip(),
                "ticket_type": str(row.get("ci_ticket_type") or "").strip(),
                "booking_status": str(row.get("booking_status") or "").strip(),
                "department": str(row.get("ci_department") or "").strip(),
                "department_to_handle": str(row.get("ci_department_to_handle") or "").strip(),
                "doctor_name": str(row.get("ci_doctor_name") or "").strip(),
                "follow_up_required": str(row.get("ci_follow_up_required") or "").strip(),
                "outcome": str(row.get("ci_outcome") or "").strip(),
                "next_step": str(row.get("ci_call_solution") or "").strip(),
                "ci_call_id": str(row.get("ci_call_id") or "").strip(),
            }
        )
        timeline_raw.append(
            {
                "_ts": get_datetime(raw_c) if raw_c else None,
                "icon": "🎫",
                "title": row.get("subject") or _("Issue"),
                "timestamp": c_fmt,
                "description": desc_snip,
                "kind": "issue",
                "issue_name": row.get("name"),
            }
        )

    if call_time:
        ct_dt = get_datetime(call_time)
        timeline_raw.append(
            {
                "_ts": ct_dt,
                "icon": "📞",
                "title": _("Call"),
                "timestamp": format_datetime(call_time),
                "description": _p360_desc_snippet(transcript_stored or notes_stored, 280),
                "kind": "call",
                "issue_name": None,
            }
        )

    if lead.creation:
        try:
            lc_dt = get_datetime(lead.creation)
        except Exception:
            lc_dt = None
        timeline_raw.append(
            {
                "_ts": lc_dt,
                "icon": "📋",
                "title": _("Lead record"),
                "timestamp": creation_fmt,
                "description": _("Lead created in CRM"),
                "kind": "lead",
                "issue_name": None,
            }
        )

    issues = _dedupe_p360_issue_dicts(issues)
    # Single conversation card in Patient 360 (newest Issue after story dedupe).
    if issues:
        issues = [issues[0]]

    patient_type = _p360_patient_type_label(_s("ci_record_type"))
    sub_type = ""
    if issues:
        sub_type = str(issues[0].get("call_classification") or "").strip() or str(issues[0].get("ticket_type") or "").strip()
    if not sub_type:
        sub_type = _s("intent")
    if not sub_type:
        sub_type = _("General")

    tags = _p360_build_tags(
        _s("sentiment"),
        _s("outcome"),
        dep_show,
        _s("ci_services"),
        issues[0] if issues else None,
    )

    min_dt = datetime.datetime.min.replace(tzinfo=None)

    def _sort_key(x: dict[str, Any]) -> datetime.datetime:
        t = x.get("_ts")
        if isinstance(t, datetime.datetime):
            return t.replace(tzinfo=None) if t.tzinfo else t
        return min_dt

    timeline_raw.sort(key=_sort_key, reverse=True)
    activities: list[dict[str, Any]] = []
    for row in timeline_raw:
        activities.append({k: v for k, v in row.items() if k != "_ts"})

    # Read-only CRM rows: only include non-empty stored values (labels fixed; no inference).
    detail_specs: list[tuple[str, str]] = [
        ("status", _("CRM status")),
        ("source", _("Lead source")),
        ("lead_owner", _("Assignee")),
        ("email_id", _("Email")),
        ("call_id", _("Call ID")),
        ("creation", _("Created")),
    ]
    details: list[dict[str, str]] = []
    for fn, lb in detail_specs:
        if not meta_lead.has_field(fn):
            continue
        v = lead.get(fn)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        disp = format_datetime(v) if fn == "creation" else str(v).strip()
        details.append({"label": str(lb), "value": disp})
    if meta_lead.has_field("appointment_date"):
        v = lead.get("appointment_date")
        if v is not None and str(v).strip():
            try:
                details.append({"label": _("Appointment date"), "value": format_date(getdate(v))})
            except Exception:
                details.append({"label": _("Appointment date"), "value": str(v).strip()})

    details.append({"label": _("Department"), "value": dep_show or _("N/A")})
    details.append({"label": _("Doctor"), "value": _s("ci_doctor") or _("N/A")})
    details.append({"label": _("Location"), "value": _s("ci_ai_location") or _("N/A")})
    if _s("ci_services"):
        details.append({"label": _("Services"), "value": _s("ci_services")})
    if _s("ci_record_type"):
        details.append({"label": _("Record type (AI)"), "value": _s("ci_record_type")})
    if timestamp_display:
        details.append({"label": _("Call time (AI)"), "value": timestamp_display})

    gender = _s("gender")
    subtitle = gender

    return {
        "lead": {
            "lead_id": lead.name,
            "name": display_name,
            "phone": phone,
            "email": email,
            "call_id": _s("call_id"),
            "status": str(lead.status or ""),
            "lead_owner": _s("lead_owner"),
            "creation": creation_fmt,
            "source": src,
            "subtitle": subtitle,
            "record_type": _s("ci_record_type"),
            "department": dep_show or _("N/A"),
            "doctor": _s("ci_doctor") or _("N/A"),
            "location": _s("ci_ai_location") or _("N/A"),
            "services": _s("ci_services") or _("N/A"),
            "timestamp": timestamp_display,
            "upcoming_appointment": upcoming_appointment or _("N/A"),
            "booking_status": _s("booking_status") if meta_lead.has_field("booking_status") else "",
            "whatsapp_priority": _s("whatsapp_priority") if meta_lead.has_field("whatsapp_priority") else "",
            "silent_call": silent_call,
            "details": details,
            "ai": ai,
            "patient_type": patient_type,
            "sub_type": sub_type,
            "tags": tags,
        },
        "issues": issues,
        "activities": activities,
        "whatsapp_messages": list_whatsapp_communications_for_lead(lead_id, 80),
    }


def _issue_dedupe_fingerprint(row: dict[str, Any]) -> str:
    notes = str(row.get("ci_ticket_notes") or "").strip()
    transcript = str(row.get("ci_transcript") or "").strip()
    narrative = notes if len(notes) >= len(transcript) else transcript
    if not narrative:
        narrative = notes or transcript
    return "||".join(
        [
            str(row.get("custom_lead") or "").strip().lower(),
            str(row.get("ci_call_classification") or row.get("ci_ticket_type") or "").strip().lower(),
            narrative.strip().lower(),
            str(row.get("ci_action_description") or "").strip().lower(),
            str(row.get("ci_call_solution") or "").strip().lower(),
            str(row.get("ci_department_to_handle") or row.get("ci_department") or "").strip().lower(),
            str(row.get("ci_doctor_name") or "").strip().lower(),
            str(row.get("priority") or row.get("ci_priority_level") or "").strip().lower(),
            str(row.get("ci_outcome") or "").strip().lower(),
            str(row.get("ci_call_timestamp") or "").strip().lower(),
        ]
    )


@frappe.whitelist()
def cleanup_duplicate_issues_for_patient_360(dry_run: int | bool = 1) -> dict[str, Any]:
    """
    Remove duplicate Issue rows that cause repeated Patient 360 cards.
    Keeps newest by creation per dedupe key.
    """
    d = bool(int(dry_run)) if isinstance(dry_run, str | int) else bool(dry_run)
    meta_issue = frappe.get_meta("Issue")
    needed = [
        "custom_lead",
        "ci_call_id",
        "ci_ticket_notes",
        "ci_transcript",
        "ci_action_description",
        "ci_call_solution",
        "ci_call_classification",
        "ci_ticket_type",
        "ci_department",
        "ci_department_to_handle",
        "ci_doctor_name",
        "ci_priority_level",
        "ci_outcome",
        "ci_call_timestamp",
    ]
    fields = ["name", "creation", "modified", "priority"]
    for fn in needed:
        if meta_issue.has_field(fn):
            fields.append(fn)

    rows = frappe.get_all(
        "Issue",
        filters={"custom_lead": ["!=", ""]},
        fields=fields,
        order_by="creation desc, modified desc",
        limit_page_length=0,
    )
    keep_seen: set[str] = set()
    drop_names: list[str] = []
    for row in rows:
        lead_id = str(row.get("custom_lead") or "").strip()
        if not lead_id:
            continue
        cid = str(row.get("ci_call_id") or "").strip().lower()
        if cid:
            key = f"cid::{lead_id.lower()}::{cid}"
        else:
            key = f"fp::{_issue_dedupe_fingerprint(row)}"
        if key in keep_seen:
            drop_names.append(str(row.get("name")))
            continue
        keep_seen.add(key)

    if not d and drop_names:
        for nm in drop_names:
            frappe.delete_doc("Issue", nm, ignore_permissions=True, force=1)
        frappe.db.commit()

    return {
        "dry_run": d,
        "total_scanned": len(rows),
        "duplicate_count": len(drop_names),
        "duplicates": drop_names[:200],
        "deleted_count": 0 if d else len(drop_names),
    }


@frappe.whitelist()
def merge_duplicate_leads_by_phone(dry_run: int | bool = 1) -> dict[str, Any]:
    """
    One Lead per phone number: keep the most recently modified Lead, reassign Issues, delete duplicates.
    Stops the same contact appearing in both Leads and Tickets lists with different CRM IDs.
    """
    d = bool(int(dry_run)) if isinstance(dry_run, str | int) else bool(dry_run)
    meta = frappe.get_meta("Lead")
    fields = ["name", "mobile_no", "phone", "modified", "creation"]
    if meta.has_field("ci_record_type"):
        fields.append("ci_record_type")
    rows = frappe.get_all("Lead", fields=fields, order_by="modified desc", limit_page_length=0)
    by_key: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        ph = (r.get("mobile_no") or r.get("phone") or "").strip()
        key = _p360_normalize_phone_key(ph)
        if not key:
            continue
        by_key.setdefault(key, []).append(r)

    details: list[dict[str, Any]] = []
    removed = 0
    for key, batch in by_key.items():
        if len(batch) < 2:
            continue
        winner = _p360_winner_lead_batch(batch)
        wname = str(winner.get("name") or "").strip()
        if not wname:
            continue
        for loser in batch[1:]:
            lname = str(loser.get("name") or "").strip()
            if not lname or lname == wname:
                continue
            ic = frappe.db.count("Issue", {"custom_lead": lname})
            details.append(
                {
                    "phone_key": key,
                    "keep": wname,
                    "remove": lname,
                    "issues_reassigned": ic,
                }
            )
            if not d:
                for iname in frappe.get_all("Issue", filters={"custom_lead": lname}, pluck="name"):
                    frappe.db.set_value("Issue", iname, "custom_lead", wname)
                frappe.delete_doc("Lead", lname, ignore_permissions=True, force=1)
                removed += 1
    if not d and removed:
        frappe.db.commit()

    return {
        "dry_run": d,
        "duplicate_phone_groups": sum(1 for b in by_key.values() if len(b) >= 2),
        "leads_deleted": removed,
        "operations": details[:100],
    }


@frappe.whitelist()
def dedupe_lead_superseded_by_ticket_identity(dry_run: int | bool = 1) -> dict[str, Any]:
    """
    Delete lead-type Leads when a ticket-type Lead already exists for the same non-generic
    name (JSON ticket wins). Reassign Issues to the kept ticket Lead.
    """
    d = bool(int(dry_run)) if isinstance(dry_run, str | int) else bool(dry_run)
    meta = frappe.get_meta("Lead")
    if not meta.has_field("ci_record_type"):
        return {"dry_run": d, "leads_deleted": 0, "operations": []}

    tickets = frappe.get_all(
        "Lead",
        filters={"ci_record_type": ["in", ["ticket", "tickets"]]},
        fields=["name", "lead_name", "modified", "creation"],
        limit_page_length=0,
    )
    by_name: dict[str, list[dict[str, Any]]] = {}
    for r in tickets:
        nk = _p360_normalize_lead_name_key(r.get("lead_name"))
        if not nk:
            continue
        by_name.setdefault(nk, []).append(r)

    leads = frappe.get_all(
        "Lead",
        filters={"ci_record_type": ["in", ["lead", "leads"]]},
        fields=["name", "lead_name", "modified", "creation"],
        limit_page_length=0,
    )

    removed = 0
    ops: list[dict[str, Any]] = []
    for r in leads:
        nk = _p360_normalize_lead_name_key(r.get("lead_name"))
        if not nk or nk not in by_name:
            continue
        winner = _p360_winner_lead_batch(by_name[nk])
        wname = str(winner.get("name") or "").strip()
        lname = str(r.get("name") or "").strip()
        if not wname or lname == wname:
            continue
        ic = frappe.db.count("Issue", {"custom_lead": lname})
        ops.append({"remove": lname, "keep": wname, "name_key": nk, "issues_reassigned": ic})
        if not d:
            for iname in frappe.get_all("Issue", filters={"custom_lead": lname}, pluck="name"):
                frappe.db.set_value("Issue", iname, "custom_lead", wname)
            frappe.delete_doc("Lead", lname, ignore_permissions=True, force=1)
            removed += 1
    if not d and removed:
        frappe.db.commit()

    return {"dry_run": d, "leads_deleted": removed, "operations": ops[:100]}


@frappe.whitelist()
def remove_issues_linked_to_lead_recordtype_leads(dry_run: int | bool = 1) -> dict[str, Any]:
    """
    Delete Issues linked to Leads whose ci_record_type is lead (not ticket).
    Use after fixing ingest so lead-only contacts do not show a ticket in Patient 360.
    """
    d = bool(int(dry_run)) if isinstance(dry_run, str | int) else bool(dry_run)
    if not frappe.get_meta("Issue").has_field("custom_lead"):
        return {"dry_run": d, "deleted_count": 0, "sample": []}
    if not frappe.get_meta("Lead").has_field("ci_record_type"):
        return {"dry_run": d, "deleted_count": 0, "sample": []}

    rows = frappe.get_all(
        "Issue",
        filters={"custom_lead": ["!=", ""]},
        fields=["name", "custom_lead"],
        limit_page_length=0,
    )
    to_delete: list[str] = []
    for ir in rows:
        lid = str(ir.get("custom_lead") or "").strip()
        if not lid:
            continue
        crt = str(frappe.db.get_value("Lead", lid, "ci_record_type") or "").lower()
        if crt in ("lead", "leads"):
            to_delete.append(str(ir.get("name")))

    if not d and to_delete:
        for nm in to_delete:
            frappe.delete_doc("Issue", nm, ignore_permissions=True, force=1)
        frappe.db.commit()

    return {"dry_run": d, "deleted_count": len(to_delete), "sample": to_delete[:80]}


@frappe.whitelist()
def ingest_all_calls() -> dict[str, Any]:
    """
    Read all *.json from call-entity-extract/output (beside bench), create Lead + Issue per file.
    Reuses Lead when the same primary phone already exists (no duplicate Leads).
    Issues are linked to the Lead when ``custom_lead`` exists on Issue.
    """
    paths = _all_json_files()
    if not paths:
        frappe.throw(
            _(
                "No JSON files found. Place *.json under call-entity-extract/output or output/ next to your bench folder."
            )
        )

    if not frappe.get_meta("Issue").has_field("ci_call_id"):
        frappe.throw(
            _("Issue.ci_call_id is missing. Run `bench migrate` after installing call_intelligence."),
            frappe.ValidationError,
        )

    leads_created = 0
    leads_updated = 0
    issues_created = 0
    issues_skipped = 0
    issue_priority_by_lower = {
        n.lower(): n for n in frappe.get_all("Issue Priority", pluck="name")
    }

    for path in paths:
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            frappe.log_error(title="ingest_all_calls: skip file", message=path.name)
            continue

        if not isinstance(data, dict):
            continue

        _ingest_log_sparse_fields(data, path.name)

        cn = data.get("customer_name")
        if cn is None or (isinstance(cn, float) and math.isnan(cn)):
            customer_name = "Unknown Caller"
        else:
            cns = str(cn).strip()
            customer_name = cns if cns and cns.lower() not in ("nan", "none", "null") else "Unknown Caller"
        raw_phone = data.get("phone_number")
        phone = _normalize_phone_for_ingest(raw_phone)
        priority_raw = data.get("priority")
        lead_notes = data.get("LeadNotes") or ""
        call_classification = (data.get("call_classification") or "").strip()

        lead_id = _existing_lead_name_for_phone(phone) if phone else None

        if lead_id:
            lead = frappe.get_doc("Lead", lead_id)
            _populate_lead_from_call_json(lead, data, path, set_status_from_outcome=False)
            _apply_phone_to_lead(lead, raw_phone)
            lead.save(ignore_permissions=True)
            leads_updated += 1
        else:
            lead = frappe.get_doc(
                {
                    "doctype": "Lead",
                    "lead_name": customer_name,
                }
            )
            company = _default_company()
            if company and hasattr(lead, "company"):
                lead.company = company
            _apply_phone_to_lead(lead, raw_phone)
            _populate_lead_from_call_json(lead, data, path, set_status_from_outcome=True)
            lead.insert(ignore_permissions=True)
            lead_id = lead.name
            leads_created += 1

        call_id_cid = _json_call_id(data, path)
        if call_id_cid and frappe.db.exists("Issue", {"ci_call_id": str(call_id_cid).strip()}):
            issues_skipped += 1
            continue

        if not phone:
            issues_skipped += 1
            continue

        issue = frappe.new_doc("Issue")
        issue.subject = _issue_subject_from_structured_data(data)
        issue.description = _issue_description_from_call_data(data)
        _populate_issue_from_call_dict(issue, data)

        _apply_ci_priority_level_to_issue(issue, data)
        if not getattr(issue, "priority", None):
            resolved_priority = _resolve_issue_priority(
                str(priority_raw) if priority_raw is not None else None,
                issue_priority_by_lower,
            )
            if resolved_priority and hasattr(issue, "priority"):
                issue.priority = resolved_priority

        if lead_id and frappe.get_meta("Issue").has_field("custom_lead"):
            issue.custom_lead = lead_id
        if lead_id and hasattr(issue, "lead"):
            issue.lead = lead_id

        issue.insert(ignore_permissions=True)
        issues_created += 1

    frappe.db.commit()

    return {
        "leads_created": leads_created,
        "leads_updated": leads_updated,
        "issues_created": issues_created,
        "issues_skipped_duplicate": issues_skipped,
    }


def _set_if_has_field(doc, fieldname: str, value: Any) -> None:
    """Avoid errors if custom fields are not yet migrated."""
    if value is None:
        return
    if frappe.get_meta(doc.doctype).has_field(fieldname):
        doc.set(fieldname, value)


def _clear_if_has_field(doc, fieldname: str) -> None:
    """Clear custom field when JSON no longer supplies a value (re-ingest fixes stale data)."""
    if not frappe.get_meta(doc.doctype).has_field(fieldname):
        return
    doc.set(fieldname, None)


def _normalize_sentiment(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    key = raw.lower()
    mapping = {
        "positive": "Positive",
        "neutral": "Neutral",
        "negative": "Negative",
    }
    if key in mapping:
        return mapping[key]
    if raw in ALLOWED_SENTIMENT:
        return raw
    # e.g. NEUTRAL from extract pipeline
    title = raw.title()
    if title in ALLOWED_SENTIMENT:
        return title
    return None


def _normalize_outcome(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip().upper()
    if raw in ALLOWED_OUTCOME:
        return raw
    return None


def _default_company() -> str | None:
    company = frappe.defaults.get_defaults().get("company")
    if company:
        return company
    if frappe.db.exists("DocType", "Global Defaults"):
        company = frappe.db.get_single_value("Global Defaults", "default_company")
        if company:
            return company
    return frappe.db.get_value("Company", {}, "name")


@frappe.whitelist()
def create_lead_from_ai(
    name: str,
    phone: str | None = None,
    transcript: str | None = None,
    sentiment: str | None = None,
    outcome: str | None = None,
    call_id: str | None = None,
    call_time: str | None = None,
    intent: str | None = None,
    priority_score: float | None = None,
    appointment_date: str | None = None,
    chatwoot_conversation_id: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """
    Create a CRM Lead populated from AI / call pipeline.

    Required:
        name: maps to lead_name
    Optional:
        phone -> mobile_no (and phone if column exists)
        transcript, sentiment, outcome, call_id, call_time, intent, priority_score,
        appointment_date, chatwoot_conversation_id, source (Lead source)
    """
    if not name or not str(name).strip():
        frappe.throw(_("name is required"))

    lead = frappe.new_doc("Lead")
    lead.lead_name = str(name).strip()

    company = _default_company()
    if company and hasattr(lead, "company"):
        lead.company = company

    if phone:
        p = str(phone).strip()
        if hasattr(lead, "mobile_no"):
            lead.mobile_no = p
        if hasattr(lead, "phone"):
            lead.phone = p

    if source and hasattr(lead, "source"):
        lead.source = source
    elif hasattr(lead, "source") and not lead.source:
        lead.source = "Call Intelligence"

    norm_sent = _normalize_sentiment(sentiment)
    _set_if_has_field(lead, "sentiment", norm_sent)

    norm_out = _normalize_outcome(outcome)
    _set_if_has_field(lead, "outcome", norm_out)
    _set_if_has_field(lead, "lead_status", _lead_status_from_outcome_ci(norm_out))

    _set_if_has_field(lead, "transcript", transcript)
    _set_if_has_field(lead, "call_id", str(call_id).strip() if call_id else None)
    ct = _normalize_call_time(call_time)
    _set_if_has_field(lead, "call_time", ct)
    _set_if_has_field(lead, "call_timestamp", ct)
    _set_if_has_field(lead, "intent", str(intent).strip() if intent else None)
    if priority_score is not None:
        try:
            _set_if_has_field(lead, "priority_score", float(priority_score))
        except (TypeError, ValueError):
            pass
    _set_if_has_field(lead, "appointment_date", appointment_date)
    _set_if_has_field(
        lead,
        "chatwoot_conversation_id",
        str(chatwoot_conversation_id).strip() if chatwoot_conversation_id else None,
    )

    lead.insert(ignore_permissions=True)
    frappe.db.commit()

    return {"name": lead.name, "lead_name": lead.lead_name}


@frappe.whitelist()
def create_issue(
    lead_id: str,
    subject: str | None = None,
    issue_type: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    """
    Create an Issue linked to a Lead (custom_lead).

    Args:
        lead_id: Lead name
        subject: defaults from Lead
        issue_type: maps to custom field ci_ticket_type (Appointment Booking | Follow-up | Other)
        description: optional body
    """
    if not lead_id or not frappe.db.exists("Lead", lead_id):
        frappe.throw(_("Valid lead_id is required"))

    if not frappe.get_meta("Issue").has_field("custom_lead"):
        frappe.throw(
            _("Issue.custom_lead is missing. Run `bench migrate` after installing call_intelligence."),
            frappe.ValidationError,
        )

    lead = frappe.get_doc("Lead", lead_id)

    issue = frappe.new_doc("Issue")
    issue.subject = (subject or "").strip() or _("Follow-up: {0}").format(lead.lead_name or lead_id)
    _set_if_has_field(issue, "custom_lead", lead_id)
    if hasattr(issue, "lead"):
        issue.lead = lead_id

    itype = issue_type or "Other"
    _set_if_has_field(issue, "ci_ticket_type", itype)

    if description:
        issue.description = description
    elif getattr(lead, "transcript", None):
        issue.description = lead.transcript

    # Optional: map customer when Lead is linked to Customer (future)
    if hasattr(issue, "customer") and getattr(lead, "customer", None):
        issue.customer = lead.customer

    issue.insert(ignore_permissions=True)
    frappe.db.commit()

    return {"name": issue.name, "subject": issue.subject}


@frappe.whitelist()
def create_issue_from_call_intelligence(
    phone_number: str | None = None,
    call_id: str | None = None,
    transcript: str | None = None,
    sentiment: str | None = None,
    outcome: str | None = None,
    timestamp: str | None = None,
    call_time: str | None = None,
    payload: dict | str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Create a Support Issue from structured call intelligence (same JSON shape as Lead / extract pipeline).

    Pass either ``payload`` (full dict / JSON string) or legacy keyword args. Full payload keys are mapped
    into Issue custom fields; ``description`` is built from ``ticket_notes`` / ``LeadNotes`` (else transcript
    excerpt); full text also in ``ci_ticket_notes`` / ``ci_transcript``.

    Required:
        phone_number (in payload or arg): 10+ digits after normalization.

    Optional:
        call_id: de-duplication against ``ci_call_id`` when set.

    Does not set ``custom_lead`` or ``lead``; Issues stay independent of CRM Leads unless linked manually.
    """
    data: dict[str, Any] = {}
    if payload is not None:
        pl = json.loads(payload) if isinstance(payload, str) else payload
        if not isinstance(pl, dict):
            frappe.throw(_("payload must be a JSON object"))
        data.update(pl)
    if phone_number is not None:
        data["phone_number"] = phone_number
    if call_id is not None:
        data["call_id"] = call_id
    if transcript is not None:
        data["transcript"] = transcript
    if sentiment is not None:
        data["sentiment"] = sentiment
    if outcome is not None:
        data["outcome"] = outcome
    if timestamp is not None:
        data["timestamp"] = timestamp
    if call_time is not None:
        data["call_time"] = call_time
    for k, v in kwargs.items():
        if k not in data and v is not None:
            data[k] = v

    _ingest_log_sparse_fields(data)

    cleaned = _normalize_phone_for_ingest(data.get("phone_number"))
    if not cleaned:
        frappe.throw(
            _("phone_number is required and must contain at least 10 digits (same validation as call ingest).")
        )

    cid = _str_clean(data.get("call_id")) or _str_clean(data.get("filename"))
    if cid and frappe.get_meta("Issue").has_field("ci_call_id"):
        existing = frappe.db.get_value("Issue", {"ci_call_id": cid}, ["name", "subject"], as_dict=True)
        if existing:
            return {
                "name": existing.name,
                "subject": existing.subject,
                "skipped": True,
                "reason": "duplicate_call_id",
            }

    if not frappe.get_meta("Issue").has_field("ci_call_id"):
        frappe.throw(
            _("Issue.ci_call_id is missing. Run `bench migrate` after installing call_intelligence."),
            frappe.ValidationError,
        )

    issue = frappe.new_doc("Issue")
    issue.subject = _issue_subject_from_structured_data(data)
    issue.description = _issue_description_from_call_data(data)
    _populate_issue_from_call_dict(issue, data)
    _apply_ci_priority_level_to_issue(issue, data)

    issue.insert(ignore_permissions=True)
    frappe.db.commit()

    return {
        "name": issue.name,
        "subject": issue.subject,
        "skipped": False,
    }


@frappe.whitelist()
def repair_issues_from_call_json_files(
    directory_path: str | None = None,
    dry_run: int | bool = 1,
) -> dict[str, Any]:
    """
    Re-apply Issue subject, description, and Call Intelligence custom fields from JSON files.
    Matches Issues by ``ci_call_id`` (JSON ``call_id`` or filename stem).

    Use after fixing mapping or to refresh Issues created with old defaults.
    Set ``dry_run`` to 0 to write changes.
    """
    d = bool(int(dry_run)) if isinstance(dry_run, str | int) else bool(dry_run)
    paths: list[Path]
    if directory_path and str(directory_path).strip():
        root = Path(str(directory_path).strip()).expanduser().resolve()
        if not root.is_dir():
            frappe.throw(_("Directory not found: {0}").format(directory_path))
        paths = sorted(root.glob("*.json"))
    else:
        paths = _all_json_files()
    if not paths:
        frappe.throw(_("No JSON files found for repair."))

    scanned = len(paths)
    matched = 0
    updated = 0
    skipped_no_issue = 0
    skipped_bad = 0

    for path in paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            skipped_bad += 1
            continue
        if not isinstance(data, dict):
            skipped_bad += 1
            continue

        cid = _str_clean(data.get("call_id")) or _json_call_id(data, path)
        if not cid:
            skipped_bad += 1
            continue
        iname = frappe.db.get_value("Issue", {"ci_call_id": str(cid).strip()}, "name")
        if not iname:
            skipped_no_issue += 1
            continue
        matched += 1
        if d:
            continue
        _ingest_log_sparse_fields(data, path.name)
        issue = frappe.get_doc("Issue", iname)
        issue.subject = _issue_subject_from_structured_data(data)
        issue.description = _issue_description_from_call_data(data)
        _populate_issue_from_call_dict(issue, data)
        _apply_ci_priority_level_to_issue(issue, data)
        ip_raw = data.get("priority")
        if not getattr(issue, "priority", None):
            by_lower = {n.lower(): n for n in frappe.get_all("Issue Priority", pluck="name")}
            rp = _resolve_issue_priority(str(ip_raw) if ip_raw is not None else None, by_lower)
            if rp and hasattr(issue, "priority"):
                issue.priority = rp
        issue.save(ignore_permissions=True)
        updated += 1

    if not d and updated:
        frappe.db.commit()

    return {
        "dry_run": d,
        "files_scanned": scanned,
        "issues_matched": matched,
        "issues_updated": updated if not d else 0,
        "would_update": matched if d else 0,
        "skipped_no_matching_issue": skipped_no_issue,
        "skipped_bad_json": skipped_bad,
    }


@frappe.whitelist()
def create_issue_from_call_extract(payload: dict | str | None = None) -> dict[str, Any]:
    """
    Same payload shape as call-entity-extract / ``create_lead_from_call_extract`` — maps all keys into Issue fields.
    """
    if payload is None:
        frappe.throw(_("payload is required"))
    return create_issue_from_call_intelligence(payload=payload)


@frappe.whitelist()
def create_lead_from_call_extract(payload: dict | str | None = None) -> dict[str, Any]:
    """
    Map a call-entity-extract style JSON blob into create_lead_from_ai.

    Accepts dict or JSON string (from client / integration).
    """
    import json

    if payload is None:
        frappe.throw(_("payload is required"))
    if isinstance(payload, str):
        payload = json.loads(payload)

    if not isinstance(payload, dict):
        frappe.throw(_("payload must be a JSON object"))

    name = (
        payload.get("customer_name")
        or payload.get("name")
        or payload.get("caller_name")
        or (payload.get("phone_number") or "").split(",")[0].strip()
        or "Unknown Caller"
    )

    phone = payload.get("phone_number")
    if phone and "," in str(phone):
        phone = str(phone).split(",")[0].strip()

    sentiment = payload.get("sentiment_label") or payload.get("sentiment")
    outcome = payload.get("outcome")
    transcript = payload.get("transcript")
    call_id = payload.get("call_id") or payload.get("filename")
    call_time = payload.get("timestamp") or payload.get("call_time")
    intent = payload.get("call_classification") or payload.get("intent")

    kw = {
        "name": name,
        "phone": phone,
        "transcript": transcript,
        "sentiment": sentiment,
        "outcome": outcome,
        "call_id": call_id,
        "call_time": call_time,
        "intent": intent,
        "source": payload.get("hospital_name") or "Call Intelligence",
    }

    result = create_lead_from_ai(**kw)
    ld = frappe.get_doc("Lead", result["name"])
    _populate_lead_from_call_json(ld, payload, None, set_status_from_outcome=False)
    ld.save(ignore_permissions=True)
    frappe.db.commit()
    return result


def create_or_update_lead(payload: dict[str, Any]) -> str:
    """
    Create or update a Lead by normalized ``phone_number``.
    Maps transcript, sentiment, outcome, call timestamp, LeadNotes (and related keys via
    ``_populate_lead_from_call_json``).
    """
    _ingest_log_sparse_fields(payload)

    raw_phone = payload.get("phone_number")
    cleaned = _normalize_phone_for_ingest(raw_phone)
    if not cleaned:
        frappe.throw(_("phone_number must contain at least 10 digits"))

    existing = _existing_lead_name_for_phone(cleaned)
    cn = payload.get("customer_name")
    if cn is None or (isinstance(cn, float) and math.isnan(cn)):
        customer_name = "Unknown Caller"
    else:
        cns = str(cn).strip()
        customer_name = cns if cns and cns.lower() not in ("nan", "none", "null") else "Unknown Caller"

    if existing:
        lead = frappe.get_doc("Lead", existing)
        _populate_lead_from_call_json(lead, payload, None, set_status_from_outcome=False)
        _apply_phone_to_lead(lead, raw_phone)
        lead.save(ignore_permissions=True)
        return lead.name

    lead = frappe.new_doc("Lead")
    lead.lead_name = customer_name
    company = _default_company()
    if company and hasattr(lead, "company"):
        lead.company = company
    _apply_phone_to_lead(lead, raw_phone)
    _populate_lead_from_call_json(lead, payload, None, set_status_from_outcome=True)
    lead.insert(ignore_permissions=True)
    return lead.name


def insert_lead_from_call_intelligence_payload(payload: dict[str, Any]) -> str:
    """
    Create a **new** Lead from call-intelligence shaped ``payload`` (no phone deduplication).

    Use when each inbound event should appear as its own Lead (e.g. Medplum Encounter
    notifications on status transitions).
    """
    _ingest_log_sparse_fields(payload)

    raw_phone = payload.get("phone_number")
    cleaned = _normalize_phone_for_ingest(raw_phone)
    if not cleaned:
        frappe.throw(_("phone_number must contain at least 10 digits"))

    cn = payload.get("customer_name")
    if cn is None or (isinstance(cn, float) and math.isnan(cn)):
        customer_name = "Unknown Caller"
    else:
        cns = str(cn).strip()
        customer_name = cns if cns and cns.lower() not in ("nan", "none", "null") else "Unknown Caller"

    lead = frappe.new_doc("Lead")
    lead.lead_name = customer_name
    company = _default_company()
    if company and hasattr(lead, "company"):
        lead.company = company
    _apply_phone_to_lead(lead, raw_phone)
    _populate_lead_from_call_json(lead, payload, None, set_status_from_outcome=True)
    lead.insert(ignore_permissions=True)
    return lead.name


def create_issue_for_call_record(payload: dict[str, Any]) -> str:
    """
    Create an Issue from call intelligence JSON; skip insert when ``ci_call_id`` already exists.
    Does not link to Lead (``custom_lead`` / ``lead``) — linking is done by ``ingest_calls_from_directory``.
    """
    _ingest_log_sparse_fields(payload)

    cid = _str_clean(payload.get("call_id"))
    if not cid:
        fn = payload.get("filename")
        if fn and str(fn).strip():
            cid = Path(str(fn)).stem
    if not cid:
        frappe.throw(_("call_id or filename is required for ticket"))

    if not frappe.get_meta("Issue").has_field("ci_call_id"):
        frappe.throw(
            _("Issue.ci_call_id is missing. Run `bench migrate` after installing call_intelligence."),
            frappe.ValidationError,
        )

    issue_priority_by_lower = {n.lower(): n for n in frappe.get_all("Issue Priority", pluck="name")}

    existing = frappe.db.get_value("Issue", {"ci_call_id": cid}, "name")
    if existing:
        # Re-sync existing Issue from latest JSON so old/incomplete rows get fully populated.
        issue = frappe.get_doc("Issue", existing)
        issue.subject = _issue_subject_from_structured_data(payload)
        issue.description = _issue_description_from_call_data(payload)
        _populate_issue_from_call_dict(issue, payload)
        _apply_ci_priority_level_to_issue(issue, payload)
        if not getattr(issue, "priority", None):
            rp = _resolve_issue_priority(
                str(payload.get("priority")) if payload.get("priority") is not None else None,
                issue_priority_by_lower,
            )
            if rp and hasattr(issue, "priority"):
                issue.priority = rp
        issue.save(ignore_permissions=True)
        return existing

    issue = frappe.new_doc("Issue")
    issue.subject = _issue_subject_from_structured_data(payload)
    issue.description = _issue_description_from_call_data(payload)
    _populate_issue_from_call_dict(issue, payload)
    _apply_ci_priority_level_to_issue(issue, payload)
    if not getattr(issue, "priority", None):
        rp = _resolve_issue_priority(
            str(payload.get("priority")) if payload.get("priority") is not None else None,
            issue_priority_by_lower,
        )
        if rp and hasattr(issue, "priority"):
            issue.priority = rp

    issue.insert(ignore_permissions=True)
    return issue.name


def _coerce_medplum_fhir_root(raw: dict[str, Any]) -> dict[str, Any]:
    """Unwrap common Medplum / subscription wrappers to a FHIR resource root."""
    if not isinstance(raw, dict):
        return {}
    rt = str(raw.get("resourceType") or "")
    if rt in ("Bundle", "Encounter", "Patient"):
        return raw
    inner = raw.get("resource")
    if isinstance(inner, dict) and str(inner.get("resourceType") or ""):
        return inner
    inputs = raw.get("input")
    if isinstance(inputs, list) and inputs:
        first = inputs[0]
        if isinstance(first, dict):
            r2 = first.get("resource")
            if isinstance(r2, dict) and str(r2.get("resourceType") or ""):
                return r2
    return raw


def _medplum_qualification_summary(qual: dict[str, Any]) -> str:
    parts: list[str] = []
    if qual.get("diagnosis"):
        parts.append(_("Diagnosis: {0}").format(qual.get("diagnosis")))
    fu = qual.get("follow_up")
    if isinstance(fu, dict):
        parts.append(
            _("Follow-up: required={0} days={1}").format(
                bool(fu.get("required")), fu.get("days") or ""
            )
        )
    if qual.get("patient_id"):
        parts.append(_("FHIR Patient: {0}").format(qual.get("patient_id")))
    if qual.get("insurance_eligibility_status"):
        parts.append(_("Insurance: {0}").format(qual.get("insurance_eligibility_status")))
    return "\n".join(parts) if parts else _("Medplum FHIR ingest")


def _parse_send_followup_flag(
    send_followup_whatsapp: bool | int | str | None, *, default: bool
) -> bool:
    if send_followup_whatsapp is None:
        return default
    if isinstance(send_followup_whatsapp, bool):
        return send_followup_whatsapp
    if isinstance(send_followup_whatsapp, (int, float)):
        return bool(send_followup_whatsapp)
    return str(send_followup_whatsapp).strip().lower() not in ("0", "false", "no", "")


def _medplum_raw_to_lead_response(raw_medplum: dict[str, Any], *, send_followup: bool) -> dict[str, Any]:
    """Shared: FHIR Encounter/Bundle JSON → **new** Lead each time (+ optional WhatsApp follow-up)."""
    from call_intelligence.medplum_fhir import (
        build_qualification_payload_from_fhir,
        encounter_status_code,
        extract_encounter_and_bundle,
        ref_to_id,
        soften_payload_with_defaults,
    )

    fhir_root = _coerce_medplum_fhir_root(raw_medplum)
    encounter, bundle = extract_encounter_and_bundle(fhir_root)

    if not encounter:
        frappe.throw(
            _("Medplum payload must include an Encounter (e.g. Bundle containing Encounter).")
        )

    qual, _ok = build_qualification_payload_from_fhir(encounter, bundle)

    subj = encounter.get("subject")
    pref = None
    if isinstance(subj, dict):
        pref = subj.get("reference")
    elif isinstance(subj, str):
        pref = subj
    pid_hint = ref_to_id(pref) or str(qual.get("patient_id") or "").strip() or None
    qual = soften_payload_with_defaults(qual, pid_hint, skip_follow_up=False)

    phone = str(qual.get("phone") or "").strip()
    if not qual.get("patient_name"):
        qual["patient_name"] = (f"Patient {pid_hint}" if pid_hint else _("Unknown Caller"))

    enc_id = str(encounter.get("id") or "").strip()
    enc_st = encounter_status_code(encounter)
    meta = encounter.get("meta") if isinstance(encounter.get("meta"), dict) else {}
    ver = str(meta.get("versionId") or meta.get("lastUpdated") or "").strip()
    call_key = "|".join(x for x in (enc_id, enc_st, ver) if x)
    if not call_key:
        call_key = frappe.generate_hash(length=12)
    call_id_medplum = f"medplum-Encounter:{call_key}"[:140]

    summary_base = _medplum_qualification_summary(qual)
    status_line = _("Encounter {0} · FHIR status={1}").format(enc_id or "—", enc_st or "—")
    summary_full = f"{summary_base}\n{status_line}" if summary_base else status_line

    lead_payload: dict[str, Any] = {
        "recordType": "lead",
        "phone_number": phone,
        "customer_name": str(qual.get("patient_name") or "").strip() or _("Unknown Caller"),
        "summary": summary_full,
        "outcome": "PENDING",
        "call_id": call_id_medplum,
        "source": "Medplum Encounter",
    }

    lead_name = insert_lead_from_call_intelligence_payload(lead_payload)
    frappe.db.commit()

    wa_out: dict[str, Any] | None = None
    if send_followup:
        from call_intelligence.whatsapp_integration import (
            is_cloud_configured,
            send_lead_whatsapp_followup_flow,
        )

        if is_cloud_configured():
            try:
                wa_out = send_lead_whatsapp_followup_flow(lead_name)
            except Exception:
                frappe.log_error(
                    title="medplum_encounter_ingest whatsapp",
                    message=frappe.get_traceback(),
                )
                wa_out = {"ok": False, "error": "whatsapp_failed"}
        else:
            wa_out = {"ok": False, "skipped": True, "reason": "whatsapp_cloud_not_configured"}

    return {
        "ok": True,
        "lead": lead_name,
        "qualification": qual,
        "whatsapp": wa_out,
        "encounter_id": enc_id or None,
        "encounter_status": enc_st or None,
    }


def _medplum_payload_debug(raw: dict[str, Any]) -> dict[str, Any]:
    """Lightweight shape summary when Medplum payload differs from curl tests."""
    out: dict[str, Any] = {"resourceType": raw.get("resourceType")}
    ent = raw.get("entry")
    if isinstance(ent, list):
        types: list[str] = []
        for e in ent[:24]:
            if isinstance(e, dict) and isinstance(e.get("resource"), dict):
                types.append(str(e["resource"].get("resourceType") or ""))
        out["entry_resource_types"] = types
    return out


@frappe.whitelist(allow_guest=True)
def medplum_encounter_webhook() -> dict[str, Any]:
    """
    **Medplum rest-hook → Cloudflare → Frappe (Guest).** Creates a Lead from Encounter FHIR JSON.

    - **POST** ``application/fhir+json`` body: bare ``Bundle`` / ``Encounter``, or wrapper with
      ``resource`` / subscription shapes handled by :func:`_coerce_medplum_fhir_root`.
    - Optional wrapper: ``{"raw_medplum": {...}, "send_followup_whatsapp": false}``.
    - **GET** returns a small health hint (for manual checks).

    Each successful POST creates a **new** Lead from the Encounter (same patient phone can appear
    on many rows). Prefer a Medplum Subscription whose criteria only fires when
    ``Encounter.status`` changes (e.g. FHIRPath via Medplum's subscription criteria extensions),
    so each status transition yields one Lead.

    Optional site config ``call_intelligence_medplum_webhook_secret``: if set, require header
    ``X-Medplum-Webhook-Secret`` (or query param ``secret``) to match.

    **URL:** ``/api/method/call_intelligence.api.medplum_encounter_webhook``

    Point Medplum (or Cloudflare rewrite) at that path. WhatsApp follow-up defaults to **off**
    for this endpoint; pass ``send_followup_whatsapp: true`` in JSON to enable.
    """
    if frappe.request.method == "GET":
        expected = (frappe.conf.get("call_intelligence_medplum_webhook_secret") or "").strip()
        return {
            "ok": True,
            "hint": "POST application/fhir+json with Encounter or Bundle",
            "secret_required": bool(expected),
        }

    if frappe.request.method != "POST":
        frappe.throw(_("Method Not Allowed"), frappe.ValidationError)

    expected = (frappe.conf.get("call_intelligence_medplum_webhook_secret") or "").strip()
    if expected:
        got = (
            (frappe.get_request_header("X-Medplum-Webhook-Secret") or "").strip()
            or (frappe.form_dict.get("secret") or "").strip()
        )
        if got != expected:
            frappe.throw(_("Unauthorized"), frappe.AuthenticationError)

    payload = frappe.request.get_json(silent=True)
    if payload is None:
        raw_txt = (frappe.request.get_data(as_text=True) or "").strip()
        if raw_txt:
            try:
                payload = json.loads(raw_txt)
            except json.JSONDecodeError:
                frappe.throw(_("Body must be valid JSON"))
        else:
            payload = {}
    if not isinstance(payload, dict):
        frappe.throw(_("Expected JSON object body"))

    if payload.get("raw_medplum") is not None:
        raw_medplum = payload["raw_medplum"]
        send_flag = payload.get("send_followup_whatsapp")
    elif payload.get("payload") is not None:
        raw_medplum = payload["payload"]
        send_flag = payload.get("send_followup_whatsapp")
    else:
        # Typical Medplum rest-hook: entire body is FHIR (Bundle / Encounter / …)
        raw_medplum = payload
        send_flag = payload.get("send_followup_whatsapp")

    if not isinstance(raw_medplum, dict):
        frappe.throw(_("Medplum FHIR body must be a JSON object"))

    send_followup = _parse_send_followup_flag(send_flag, default=False)

    try:
        return _medplum_raw_to_lead_response(raw_medplum, send_followup=send_followup)
    except Exception as e:
        dbg = _medplum_payload_debug(raw_medplum)
        frappe.log_error(
            title="medplum_encounter_webhook_failed",
            message=f"{e!s}\n{dbg!r}",
        )
        return {"ok": False, "error": str(e), "debug": dbg}


@frappe.whitelist()
def ingest_medplum_gateway_event(
    raw_medplum: dict | str | None = None,
    send_followup_whatsapp: bool | int | str | None = None,
) -> dict[str, Any]:
    """
    Ingest for **server-to-server** callers using Frappe **API Key + Secret** (not Guest).

    Parses Encounter/Bundle from ``raw_medplum``, upserts a Lead, then optionally runs
    ``send_lead_whatsapp_followup_flow`` when WhatsApp Cloud is configured.
    """
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)

    if isinstance(raw_medplum, str):
        raw_medplum = json.loads(raw_medplum)
    if not isinstance(raw_medplum, dict):
        frappe.throw(_("raw_medplum must be a JSON object"))

    send_followup = _parse_send_followup_flag(send_followup_whatsapp, default=True)

    return _medplum_raw_to_lead_response(raw_medplum, send_followup=send_followup)


@frappe.whitelist()
def create_call_record(payload: dict | str | None = None) -> dict[str, Any]:
    """
    Unified ingest: ``recordType`` ``lead`` → upsert Lead by phone; ``ticket`` → create Issue (dedupe by ``call_id`` / ``ci_call_id``). Paths do not cross-link Lead and Issue.

    Payload may be a dict or JSON string. Does not dump full JSON into Issue ``description`` (short summary only; transcript in ``ci_transcript``).
    """
    if payload is None:
        frappe.throw(_("payload is required"))
    data = json.loads(payload) if isinstance(payload, str) else payload
    if not isinstance(data, dict):
        frappe.throw(_("payload must be a JSON object"))

    rt_raw = _str_clean(data.get("recordType"))
    rt = rt_raw.lower()
    if rt in ("lead", "leads"):
        rt_norm = "lead"
    elif rt in ("ticket", "tickets", "issue", "issues"):
        rt_norm = "ticket"
    else:
        frappe.throw(_("recordType must be 'lead' or 'ticket'"))

    if rt_norm == "lead":
        if not _normalize_phone_for_ingest(data.get("phone_number")):
            frappe.throw(_("phone_number is required for lead and must contain at least 10 digits"))
        lead_id = create_or_update_lead(data)
        frappe.db.commit()
        return {"status": "success", "record_created": "Lead", "id": lead_id}

    issue_id = create_issue_for_call_record(data)
    frappe.db.commit()
    return {"status": "success", "record_created": "Issue", "id": issue_id}


@frappe.whitelist()
def ingest_calls_from_directory(
    directory_path: str | None = None,
    min_leads: int = 30,
    min_tickets: int = 30,
    max_files: int = 5000,
) -> dict[str, Any]:
    """
    Import call JSON files from a specific directory.

    - ``recordType: lead`` → upsert Lead only (no Issue). Patient 360 Leads tab.
    - ``recordType: ticket`` → upsert Lead (ci_record_type=ticket) + Issue linked to Lead. Tickets tab.

    Stops when at least ``min_leads`` lead-type files and ``min_tickets`` ticket-type files have been
    processed successfully (same JSON can contribute to only one side per file).
    """
    raw_dir = (directory_path or "").strip() or str((Path.home() / "Desktop" / "call-output-json"))
    root = Path(raw_dir).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        frappe.throw(_("Directory not found: {0}").format(raw_dir))

    files = sorted(root.glob("*.json"))
    if not files:
        frappe.throw(_("No JSON files found under: {0}").format(str(root)))

    # High ceiling so we can reach min_lead + min_ticket counts across mixed folders.
    max_files = int(max_files or 5000)
    if max_files < 1:
        max_files = 5000
    min_leads = max(1, int(min_leads or 30))
    min_tickets = max(1, int(min_tickets or 30))

    imported_leads: set[str] = set()
    imported_tickets: set[str] = set()
    lead_files_ok = 0
    ticket_files_ok = 0
    scanned = 0
    skipped_bad_json = 0
    skipped_invalid_payload = 0

    for path in files:
        if scanned >= max_files:
            break
        if lead_files_ok >= min_leads and ticket_files_ok >= min_tickets:
            break

        scanned += 1
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            skipped_bad_json += 1
            continue

        if not isinstance(payload, dict):
            skipped_invalid_payload += 1
            continue

        # Lead requires valid normalized phone in our current model.
        if not _normalize_phone_for_ingest(payload.get("phone_number")):
            skipped_invalid_payload += 1
            continue

        rt = _normalize_p360_record_type_for_storage(
            payload.get("recordType") or payload.get("record_type")
        )
        # No recordType in JSON → treat as lead-only (no Issue), same as explicit lead.
        if not rt:
            rt = "lead"

        try:
            lead_id = create_or_update_lead(payload)
            imported_leads.add(lead_id)

            if rt == "lead":
                lead_files_ok += 1
                continue

            # ticket (and normalized synonyms already mapped to "ticket")
            issue_id = None
            try:
                issue_id = create_issue_for_call_record(payload)
            except Exception:
                issue_res = create_issue_from_call_extract(payload=payload)
                issue_id = issue_res.get("name") if isinstance(issue_res, dict) else None

            if issue_id and frappe.db.exists("Issue", issue_id):
                issue = frappe.get_doc("Issue", issue_id)
                if frappe.get_meta("Issue").has_field("custom_lead"):
                    issue.custom_lead = lead_id
                if frappe.get_meta("Issue").has_field("lead"):
                    issue.lead = lead_id
                issue.save(ignore_permissions=True)
                imported_tickets.add(issue_id)
                ticket_files_ok += 1
        except Exception:
            frappe.log_error(title="ingest_calls_from_directory: skipped file", message=str(path))
            continue

    frappe.db.commit()
    return {
        "directory": str(root),
        "files_scanned": scanned,
        "leads_imported": len(imported_leads),
        "tickets_imported": len(imported_tickets),
        "lead_files_processed": lead_files_ok,
        "ticket_files_processed": ticket_files_ok,
        "skipped_bad_json": skipped_bad_json,
        "skipped_invalid_payload": skipped_invalid_payload,
        "targets": {"min_leads": min_leads, "min_tickets": min_tickets},
    }


@frappe.whitelist()
def get_whatsapp_communications(lead_name: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
    """
    WhatsApp thread for a Lead: sent + received, merged by Lead/Issue reference and matching phone_no.

    Each row includes ``direction`` (incoming|outgoing) and ``mapping_unknown`` when not linked to a reference.
    """
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)
    if not lead_name or not str(lead_name).strip():
        frappe.throw(_("lead_name is required"))
    return list_whatsapp_communications_for_lead(str(lead_name).strip(), max(1, min(int(limit or 25), 100)))


@frappe.whitelist()
def get_whatsapp_integration_status() -> dict[str, Any]:
    """UI: show test mode and whether Twilio is configured (no secrets returned)."""
    if frappe.session.user == "Guest":
        frappe.throw(_("Login required"), frappe.AuthenticationError)
    return {
        "test_mode": get_whatsapp_test_mode(),
        "admin_number": get_admin_destination_number(),
        "twilio_configured": is_twilio_configured(),
        "cloud_configured": is_cloud_configured(),
    }


@frappe.whitelist()
def create_demo_patient() -> dict[str, Any]:
    """
    Demo: create or refresh Lead 'Demo Patient' with phone = admin/test number
    (matches inbound webhook normalization).
    """
    from call_intelligence.demo_whatsapp_flow import create_demo_patient_impl

    return create_demo_patient_impl()


@frappe.whitelist()
def send_demo_whatsapp_message(lead_name: str | None = None) -> dict[str, Any]:
    """Demo: send structured Confirm/Cancel/Reschedule prompt for the demo lead only."""
    from call_intelligence.demo_whatsapp_flow import send_demo_whatsapp_message_impl

    return send_demo_whatsapp_message_impl(lead_name)


@frappe.whitelist(allow_guest=True)
def whatsapp_webhook() -> Any:
    """
    Inbound WhatsApp — **Twilio** (form POST) **or Meta WhatsApp Cloud** (GET verify + JSON POST).

    - **Meta** uses the same flow as :func:`whatsapp_cloud_webhook`: GET ``hub.challenge`` as
      ``text/plain``, POST JSON with ``object`` / ``entry``. Set
      ``call_intelligence_whatsapp_cloud_verify_token`` to match Meta’s Verify token (e.g. ``hello@123``).
    - **Twilio**: ``POST`` x-www-form-urlencoded ``Body``, ``From``, …

    Configure either provider to: ``https://<site>/api/method/call_intelligence.api.whatsapp_webhook``

    CSRF: Twilio server-to-server POSTs typically have no session CSRF token; Frappe skips
    validation when no csrf_token is set on the session. If you see Invalid Request, use
    site config only in dev: bench --site <site> set-config ignore_csrf 1
    """
    # Meta WhatsApp Cloud: GET verification + JSON POST (delegate to dedicated handler)
    if frappe.request.method == "GET":
        q = frappe.form_dict
        if str(q.get("hub.mode") or "") != "" or q.get("hub.challenge") is not None:
            return whatsapp_cloud_webhook()
        return {"ok": True, "hint": "POST with Twilio form Body, From — or use Meta GET with hub.mode"}

    if frappe.request.method == "POST":
        data = frappe.request.get_json(silent=True) or {}
        if isinstance(data, dict) and (
            data.get("object") == "whatsapp_business_account" or data.get("entry")
        ):
            return whatsapp_cloud_webhook()

        form = frappe.local.form_dict
        if not form:
            return {"ok": False, "reason": "empty_form"}

        try:
            result = process_inbound_twilio_webhook(dict(form))
            frappe.db.commit()
            return result
        except Exception as e:
            frappe.log_error(title="whatsapp_webhook", message=frappe.get_traceback())
            return {"ok": False, "error": str(e)}

    return {"ok": True, "hint": "GET (Meta verify) or POST"}
