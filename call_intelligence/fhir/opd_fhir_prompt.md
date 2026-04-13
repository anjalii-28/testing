# OPD case sheet — lead follow-up qualification (FHIR-first)

You receive **structured text** extracted from a FHIR R4 bundle (Composition sections, Conditions, Observations, and resolved Follow-up Plan references).

## Mandatory FHIR rules (do not contradict)

These rules **override** any narrative or “clinical judgement”. If they apply, you **must** set the JSON fields as specified. **Do not** set `follow_up_required` to `false` when a rule below applies.

1. **Appointment in Follow-up Plan**  
   If the structured extract shows that the Composition **Follow-up Plan** section references resolve to an **`Appointment`** resource, you **must** set:
   - `follow_up_required` = `true`
   - `follow_up_type` = `appointment`  
   Do not change this based on medical interpretation of the visit narrative.

2. **ServiceRequest**  
   If the bundle contains a **`ServiceRequest`** resource that appears in the Follow-up Plan references (or is listed among resolved follow-up resources), you **must** set:
   - `follow_up_required` = `true`  
   Use `follow_up_type` = `appointment` unless the resource clearly indicates a procedural order only; then you may use `procedure`.

3. **No contradiction**  
   Do not use wording like “based on clinical judgement”, “at your discretion”, or “infer from narrative alone” to ignore the above. When the FHIR extract lists Appointments or ServiceRequests in the follow-up context, the output **must** reflect `follow_up_required` = `true` as above.

## Task (when rules above do not force an answer)

If none of the mandatory rules apply, decide from the structured extract whether a **follow-up lead** is appropriate (return visit, procedure, lab recall, or documented advisory follow-up).

## Output format

Respond with **only** one JSON object (no markdown, no commentary) with exactly these keys:

- `follow_up_required` (boolean)
- `follow_up_type` (string): one of `appointment`, `procedure`, `advisory`
- `urgency` (string): one of `high`, `medium`, `low` (use observation severity, timing, or documentation in the extract)
- `summary` (string): short text for CRM. If `follow_up_required` is `false`, state briefly that no follow-up resource or plan was documented in the extract.

If the extract is insufficient for a discretionary decision, set `follow_up_required` to `false` and explain in `summary` — **unless** a mandatory rule above already forced `true`.
