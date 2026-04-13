/**
 * Lead List — same subset as Patient 360 "Leads" tab + URL param fix.
 *
 * `?lead_name=CRM-LEAD-xxx` is wrongly applied as field `lead_name` (person title),
 * not document id. Strip that. Do **not** replace it with `name` = id — that hides
 * every other lead; only remove the bad filter and keep the P360 subset.
 *
 * Default filters: `ci_record_type` in (lead, leads) — matches
 * `get_patient_360_leads` in api.py (when the field exists).
 */

frappe.listview_settings["Lead"] = frappe.listview_settings["Lead"] || {};

if (frappe.meta.has_field("Lead", "ci_record_type")) {
	frappe.listview_settings["Lead"].filters = [["ci_record_type", "in", ["lead", "leads"]]];
}

/** Doc id accidentally used as `name` / `lead_name` filter (empty or one-row list). */
function is_mistaken_lead_id_filter(f) {
	if (!f || f.length < 4) return false;
	const field = f[1];
	if (!["name", "lead_name"].includes(field)) return false;
	if (!["=", "like", "Like"].includes(String(f[2]))) return false;
	return /^CRM-LEAD-/i.test(String(f[3] || "").trim());
}

frappe.listview_settings["Lead"].onload = function (listview) {
	const params = new URLSearchParams(window.location.search);
	const raw = params.get("lead_name");
	const had_bad_url_param = raw && /^CRM-LEAD-/i.test(String(raw).trim());
	if (had_bad_url_param) {
		const path = window.location.pathname || "";
		const u = new URL(window.location.href);
		u.searchParams.delete("lead_name");
		window.history.replaceState({}, "", path + (u.search || ""));
		if (frappe.route_options) {
			delete frappe.route_options.lead_name;
		}
	}

	const mistaken = listview.filter_area.get().filter(is_mistaken_lead_id_filter);
	const fields_to_clear = [...new Set(mistaken.map((f) => f[1]))];

	let chain = Promise.resolve();
	for (const fieldname of fields_to_clear) {
		chain = chain.then(() => listview.filter_area.remove(fieldname));
	}

	return chain.then(() => {
		const has_crt = listview.filter_area
			.get()
			.some((f) => f[1] === "ci_record_type");
		if (frappe.meta.has_field("Lead", "ci_record_type") && !has_crt) {
			return listview.filter_area.add([
				[listview.doctype, "ci_record_type", "in", ["lead", "leads"]],
			]);
		}
		if (fields_to_clear.length || had_bad_url_param) {
			return listview.refresh();
		}
	});
};
