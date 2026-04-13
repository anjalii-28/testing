/**
 * Lead List — Patient 360 row button.
 *
 * Adds a "Patient 360" button to each row in the Lead list view so users can
 * jump directly to the Patient 360 dashboard filtered to that lead.
 *
 * NOTE: This file was not included in the original repository export.
 * Implement the body below based on your Frappe version's list-view API.
 *
 * Frappe v14/v15 list-view button API:
 *   frappe.listview_settings['Lead'] = {
 *       button: {
 *           show(doc) { return true; },
 *           get_label() { return __('Patient 360'); },
 *           get_description(doc) { return __('Open Patient 360 for this Lead'); },
 *           action(doc) {
 *               frappe.route_options = { lead_name: doc.name };
 *               frappe.set_route('patient-360');
 *           },
 *       },
 *   };
 */
frappe.listview_settings['Lead'] = frappe.listview_settings['Lead'] || {};

// Uncomment and adapt the block above once you confirm the Frappe version API.
