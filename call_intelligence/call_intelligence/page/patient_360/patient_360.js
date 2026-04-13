frappe.pages['patient-360'].on_page_load = function (wrapper) {
	const page = frappe.ui.make_app_page({
		parent: wrapper,
		title: __('Patient 360'),
		single_column: true,
	});

	const C = {
		primary: '#2563eb',
		accent: '#2563eb',
		bg: '#ffffff',
		border: '#E5E7EB',
		text: '#1f2937',
		muted: '#6B7280',
		card: '#FFFFFF',
		successBg: '#D1FAE5',
		successFg: '#065F46',
		warnBg: '#FEF3C7',
		warnFg: '#92400E',
		dangerBg: '#FEE2E2',
		dangerFg: '#991B1B',
		infoBg: '#DBEAFE',
		infoFg: '#1E40AF',
		neutralBg: '#F3F4F6',
		neutralFg: '#4B5563',
		sentP: '#D1FAE5',
		sentPc: '#065F46',
		sentN: '#FEE2E2',
		sentNc: '#991B1B',
		sentU: '#E5E7EB',
		sentUc: '#4B5563',
	};

	const state = {
		leadId: null,
		payload: null,
		selectedIssueName: null,
		meta: { lead_statuses: [], users: [] },
		/** `lead` = all CRM leads; `ticket` = leads that have ≥1 linked Issue. */
		recordMode: 'lead',
	};

	function get_resolved_mode_from_entry() {
		const m = (frappe.utils.get_url_arg('mode') || '').toLowerCase().trim();
		return m === 'ticket' ? 'ticket' : 'lead';
	}

	function get_resolved_lead_name_from_entry() {
		let raw = frappe.utils.get_url_arg('lead_name') || '';
		if (!raw && frappe.route_options && frappe.route_options.lead_name != null) {
			raw = frappe.route_options.lead_name;
		}
		if (raw == null || raw === '') {
			return '';
		}
		if (typeof raw !== 'string') {
			raw = String(raw);
		}
		raw = raw.trim();
		if (raw.startsWith('"') && raw.endsWith('"')) {
			try {
				return JSON.parse(raw);
			} catch (e) {
				/* ignore */
			}
		}
		return raw;
	}

	function esc(t) {
		return frappe.utils.escape_html(t == null ? '' : String(t));
	}

	function disp(v) {
		if (v == null) return '—';
		const s = String(v).trim();
		return s ? esc(s) : '—';
	}

	function phoneDisp(v) {
		if (v == null) return esc(__('N/A'));
		const s = String(v).trim();
		if (!s || s.toLowerCase() === 'nan') return esc(__('N/A'));
		return esc(s);
	}

	function ctxVal(v) {
		if (v == null || String(v).trim() === '') return esc(__('N/A'));
		return esc(String(v).trim());
	}

	function naStr(v) {
		if (v == null || String(v).trim() === '') return __('N/A');
		return String(v).trim();
	}

	function clip(s, n) {
		const t = (s || '').trim();
		if (t.length <= n) {
			return t;
		}
		return t.slice(0, n - 1) + '…';
	}

	function pill(html, bg, fg) {
		return `<span class="d360-pill" style="background:${bg};color:${fg}">${html}</span>`;
	}

	function priorityMini(p) {
		const s = (p || '').toLowerCase();
		if (s.includes('high') || s.includes('urgent')) {
			return pill(disp(p), C.dangerBg, C.dangerFg);
		}
		if (s.includes('medium') || s.includes('normal')) {
			return pill(disp(p), C.warnBg, C.warnFg);
		}
		return pill(disp(p), C.successBg, C.successFg);
	}

	function sentimentMini(s) {
		const x = (s || '').toLowerCase();
		if (x.includes('positive')) {
			return pill(esc(s), C.sentP, C.sentPc);
		}
		if (x.includes('negative')) {
			return pill(esc(s), C.sentN, C.sentNc);
		}
		return pill(esc(s || '—'), C.sentU, C.sentUc);
	}

	function outcomeMini(o) {
		const x = (o || '').toUpperCase();
		if (x === 'BOOKED') {
			return pill(esc(o), C.successBg, C.successFg);
		}
		if (x === 'NOT') {
			return pill(esc(o), C.dangerBg, C.dangerFg);
		}
		if (x === 'PENDING') {
			return pill(esc(o), C.warnBg, C.warnFg);
		}
		return pill(esc(o || '—'), C.neutralBg, C.neutralFg);
	}

	function set_header_lead_id(lead) {
		const $e = page.body.find('.d360-hdr-id');
		if (!lead) {
			$e.text('—');
		} else {
			$e.text(lead.lead_id || '—');
		}
	}

	function section(title, bodyHtml) {
		return `
		<div class="d360-sec">
			<div class="d360-sec-h">${esc(title)}</div>
			<div class="d360-sec-b">${bodyHtml}</div>
		</div>`;
	}

	function kv(label, val) {
		return `<div class="d360-kv"><span class="d360-k">${esc(label)}</span><span class="d360-v">${val}</span></div>`;
	}

	function render_left(lead) {
		const $m = page.body.find('.d360-mount-left');
		if (!lead) {
			$m.html(section(__('Patient'), `<p class="d360-muted">${__('Select a Lead above.')}</p>`));
			return;
		}
		const ai = lead.ai || {};
		const docLink = lead.lead_id
			? `<a href="#" class="d360-link d360-view-lead">${__('View full lead')}</a>`
			: '';

		const s1 = section(
			__('Patient info'),
			`
			<div class="d360-name">${esc(naStr(lead.name))}</div>
			${kv(__('Phone'), phoneDisp(lead.phone))}
			${kv(__('Lead ID'), esc(lead.lead_id || ''))}
			<div class="d360-sec-foot">${docLink}</div>`
		);

		const lastSvc = naStr(lead.timestamp) !== __('N/A') ? naStr(lead.timestamp) : __('No prior service');
		const doctorShow = naStr(lead.doctor);
		const ptype =
			lead.patient_type != null && String(lead.patient_type).trim() !== ''
				? String(lead.patient_type).trim()
				: __('Outpatient');
		const tagList = Array.isArray(lead.tags) ? lead.tags : [];
		let chipsHtml = '';
		if (tagList.length) {
			chipsHtml = tagList.map((t) => `<span class="d360-chip">${esc(String(t))}</span>`).join('');
		} else if (ai.sentiment) {
			chipsHtml = `<span class="d360-chip d360-chip-ai">${esc(naStr(ai.sentiment))}</span>`;
		}
		const s2 = section(
			__('Health profile'),
			`
			${kv(__('Patient type'), ctxVal(ptype))}
			${kv(__('Last service'), esc(lastSvc))}
			${kv(__('Doctor'), ctxVal(doctorShow))}
			<div class="d360-k">${esc(__('Tags'))}</div>
			<div class="d360-chips">${chipsHtml || `<span class="d360-muted">${esc(__('None'))}</span>`}</div>`
		);

		const appt =
			lead.upcoming_appointment && String(lead.upcoming_appointment).trim() && lead.upcoming_appointment !== __('N/A')
				? esc(String(lead.upcoming_appointment))
				: esc(__('None scheduled'));
		const s3 = section(__('Upcoming appointments'), `<p class="d360-appt">${appt}</p>`);

		$m.html(s1 + s2 + s3);

		$m.find('.d360-view-lead').on('click', (e) => {
			e.preventDefault();
			if (state.leadId) {
				frappe.set_route('Form', 'Lead', state.leadId);
			}
		});
	}

	function render_lead_details_tab(lead, activities, issues) {
		activities = activities || [];
		issues = issues || [];
		const $m = page.body.find('.d360-mount-tab-lead');
		if (!lead) {
			$m.html(`<p class="d360-muted">${__('No Lead selected.')}</p>`);
			return;
		}
		const ai = lead.ai || {};
		const desc = clip(ai.summary || ai.lead_notes || '', 480);
		let subType =
			lead.sub_type != null && String(lead.sub_type).trim() !== '' ? String(lead.sub_type).trim() : '';
		if (issues.length && state.selectedIssueName) {
			const iss = issues.find((i) => i.name === state.selectedIssueName);
			if (iss) {
				const st = String(iss.call_classification || iss.ticket_type || '').trim();
				if (st) {
					subType = st;
				}
			}
		}
		const leftCol = `
			<div class="d360-grid-col">
				${kv(__('Service type'), ctxVal(naStr(lead.department)))}
				${kv(__('Sub type'), ctxVal(subType || __('General')))}
				${kv(__('Hospital'), ctxVal(naStr(lead.location)))}
				${kv(__('Lead source'), ctxVal(naStr(lead.source)))}
			</div>`;
		const rightCol = `
			<div class="d360-grid-col">
				${kv(__('Doctor'), ctxVal(naStr(lead.doctor)))}
				<div class="d360-kv"><span class="d360-k">${esc(__('Documents'))}</span><span class="d360-v"><a href="#" class="d360-link d360-doc-link">${esc(__('View documents'))}</a></span></div>
				<div class="d360-kv d360-kv-block"><span class="d360-k">${esc(__('Description'))}</span><span class="d360-v d360-desc">${disp(desc)}</span></div>
			</div>`;

		const book = `
			<div class="d360-book-inline">
				<select class="form-control input-sm d360-book-sel">
					<option value="">${esc(__('Service…'))}</option>
					<option value="c">${esc(__('Consultation'))}</option>
					<option value="f">${esc(__('Follow-up'))}</option>
					<option value="l">${esc(__('Lab'))}</option>
				</select>
				<button type="button" class="btn btn-xs btn-primary d360-book-go">${esc(__('Book'))}</button>
			</div>`;

		const histItems =
			activities.length > 0
				? activities
						.map(
							(a) => `
			<div class="d360-hist-i">
				<div class="d360-hist-dot"></div>
				<div class="d360-hist-body">
					<div class="d360-hist-d">${esc(a.timestamp || '')}</div>
					<div class="d360-hist-t">${esc(a.title || '')}</div>
					<div class="d360-hist-r">${esc(clip(a.description || '', 320))}</div>
				</div>
			</div>`
						)
						.join('')
				: `<p class="d360-muted">${esc(__('No timeline entries yet.'))}</p>`;

		$m.html(`
			<div class="d360-two-col">${leftCol}${rightCol}</div>
			<div class="d360-book-row">${book}</div>
			<div class="d360-subh">${esc(__('Lead history'))}</div>
			<div class="d360-hist">${histItems}</div>`);

		$m.find('.d360-doc-link').on('click', (e) => {
			e.preventDefault();
			frappe.show_alert({ message: __('No document store linked from this page.'), indicator: 'blue' });
		});
		$m.find('.d360-book-go').on('click', () => {
			if (!$m.find('.d360-book-sel').val()) {
				frappe.show_alert({ message: __('Select a service.'), indicator: 'orange' });
				return;
			}
			frappe.show_alert({ message: __('Booking is not wired to a workflow yet.'), indicator: 'blue' });
		});
	}

	function render_placeholder($mount, msg) {
		$mount.html(`<p class="d360-muted d360-pad-sm">${esc(msg)}</p>`);
	}

	function render_tickets_compact(issues) {
		const $m = page.body.find('.d360-mount-tab-tickets');
		const list = issues || [];
		if (!list.length) {
			$m.html(`<p class="d360-muted d360-pad-sm">${__('No tickets for this Lead.')}</p>`);
			return;
		}
		const sel = state.selectedIssueName;
		$m.html(
			list
				.map((row) => {
					const active = sel && row.name === sel;
					const prev = clip(row.description_preview || row.ticket_notes || row.description || '', 120);
					return `
				<div class="d360-ticket-row d360-issue-pick${active ? ' is-sel' : ''}" data-issue-name="${esc(row.name || '')}">
					<div class="d360-tr-main">
						<span class="d360-tr-title">${esc(row.subject || '—')}</span>
						${priorityMini(row.priority)}
						<span class="d360-tr-time">${disp(row.creation)}</span>
					</div>
					<div class="d360-tr-desc">${esc(prev)}</div>
				</div>`;
				})
				.join('')
		);

		$m.find('.d360-issue-pick').on('click', function () {
			const name = $(this).data('issue-name');
			if (name) {
				state.selectedIssueName = name;
				render_tickets_compact(list);
				const pl = state.payload;
				if (pl && pl.lead) {
					render_lead_details_tab(pl.lead, pl.activities || [], pl.issues || []);
					render_ai_compact(pl.lead, pl.lead.ai || {});
				}
			}
		});
	}

	function render_activity_dense(activities) {
		const $m = page.body.find('.d360-mount-tab-activity');
		const list = activities || [];
		if (!list.length) {
			render_placeholder($m, __('No activity history yet.'));
			return;
		}
		$m.html(
			list
				.map(
					(a) => `
			<div class="d360-act-row">
				<div class="d360-act-t">${esc(a.timestamp)}</div>
				<div class="d360-act-body">
					<div class="d360-act-title">${a.icon || '•'} ${esc(a.title)}</div>
					<div class="d360-act-desc">${esc(clip(a.description, 200))}</div>
				</div>
			</div>`
				)
				.join('')
		);
	}

	function render_update_lead_form(lead) {
		const $m = page.body.find('.d360-mount-update');
		if (!lead) {
			$m.html(`<p class="d360-muted">${__('Select a Lead.')}</p>`);
			return;
		}
		const st = state.meta.lead_statuses || [];
		const users = state.meta.users || [];

		let statusOpts = st.map((s) => `<option value="${esc(s)}" ${s === lead.status ? 'selected' : ''}>${esc(s)}</option>`).join('');
		if (!statusOpts) {
			statusOpts = `<option value="${esc(lead.status || '')}">${disp(lead.status)}</option>`;
		}

		let userOpts = `<option value="">${esc(__('Assign to…'))}</option>`;
		userOpts += users
			.map((u) => {
				const sel = u.name === lead.lead_owner ? ' selected' : '';
				const lab = u.full_name || u.name;
				return `<option value="${esc(u.name)}"${sel}>${esc(lab)}</option>`;
			})
			.join('');

		$m.html(`
			<div class="d360-rh">${esc(__('Update Lead'))}</div>
			<div class="d360-form-compact">
				<label class="d360-fl">${esc(__('Priority'))}</label>
				<select class="form-control input-sm d360-f-priority">
					<option>${esc(__('Medium'))}</option>
					<option>${esc(__('High'))}</option>
					<option>${esc(__('Low'))}</option>
				</select>
				<label class="d360-fl">${esc(__('Status'))}</label>
				<select class="form-control input-sm d360-f-status">${statusOpts}</select>
				<label class="d360-fl">${esc(__('Assign to'))}</label>
				<select class="form-control input-sm d360-f-owner">${userOpts}</select>
				<label class="d360-fl">${esc(__('Due date'))}</label>
				<input type="date" class="form-control input-sm d360-f-due" />
				<label class="d360-fl">${esc(__('Remarks'))}</label>
				<textarea class="form-control d360-f-rem" rows="2" placeholder="${esc(__('Optional note'))}"></textarea>
				<button type="button" class="btn btn-xs btn-primary d360-f-save">${esc(__('Update Lead'))}</button>
				<p class="d360-form-note">${esc(__('Due date and priority are not saved by the server yet.'))}</p>
			</div>`);

		$m.find('.d360-f-save').on('click', () => {
			const status = $m.find('.d360-f-status').val();
			const lead_owner = $m.find('.d360-f-owner').val();
			const remarks = $m.find('.d360-f-rem').val();
			frappe.call({
				method: 'call_intelligence.api.update_lead_quick',
				args: {
					lead_name: state.leadId,
					status: status || undefined,
					lead_owner: lead_owner,
					remarks: remarks || undefined,
				},
				freeze: true,
				callback(r) {
					if (!r.exc) {
						frappe.show_alert({ message: __('Lead updated'), indicator: 'green' });
						fetch_patient(state.leadId);
					}
				},
			});
		});
	}

	function render_ai_compact(lead, ai) {
		ai = ai || {};
		const $m = page.body.find('.d360-mount-ai');
		if (!lead) {
			$m.html('');
			return;
		}
		const trId = 'd360-tr-' + frappe.utils.get_random(8);
		const tr = (ai.transcript || '').trim();
		const sum = clip(ai.summary || '', 280);
		const trBlock = tr
			? `<details class="d360-tr-det" id="${trId}"><summary>${esc(__('Transcript'))}</summary><pre class="d360-pre">${esc(tr)}</pre></details>`
			: `<p class="d360-muted" style="margin:4px 0 0;">${esc(__('No transcript.'))}</p>`;

		$m.html(`
			<div class="d360-rh">${esc(__('AI insights'))}</div>
			<div class="d360-ai-row">
				<span class="d360-k">${esc(__('Sentiment'))}</span> ${sentimentMini(ai.sentiment)}
			</div>
			<div class="d360-ai-row">
				<span class="d360-k">${esc(__('Outcome'))}</span> ${outcomeMini(ai.outcome)}
			</div>
			<div class="d360-ai-sum"><span class="d360-k">${esc(__('Summary'))}</span><div class="d360-ai-sum-t">${disp(sum)}</div></div>
			${trBlock}`);
	}

	function bind_tabs() {
		page.body.find('.d360-tab').on('click', function () {
			const tab = $(this).data('tab');
			page.body.find('.d360-tab').removeClass('is-on');
			$(this).addClass('is-on');
			page.body.find('.d360-tab-pane').hide();
			page.body.find(`.d360-tab-pane[data-pane="${tab}"]`).show();
		});
	}

	function set_tab(tab) {
		page.body.find(`.d360-tab[data-tab="${tab}"]`).trigger('click');
	}

	function apply_dashboard(payload) {
		state.payload = payload;
		if (!payload || !payload.lead) {
			state.leadId = null;
			state.selectedIssueName = null;
			set_header_lead_id(null);
			render_left(null);
			render_lead_details_tab(null, [], []);
			render_placeholder(page.body.find('.d360-mount-tab-associated'), __('No associated leads.'));
			render_tickets_compact([]);
			render_placeholder(page.body.find('.d360-mount-tab-trans'), __('No transactions.'));
			render_placeholder(page.body.find('.d360-mount-tab-comm'), __('No communications logged.'));
			render_activity_dense([]);
			render_update_lead_form(null);
			render_ai_compact(null, {});
			set_tab('lead');
			return;
		}
		const L = payload.lead;
		state.leadId = L.lead_id;
		const issues = payload.issues || [];
		if (!state.selectedIssueName || !issues.some((i) => i.name === state.selectedIssueName)) {
			state.selectedIssueName = issues.length ? issues[0].name : null;
		}
		set_header_lead_id(L);
		render_left(L);
		render_lead_details_tab(L, payload.activities || [], payload.issues || []);
		render_placeholder(page.body.find('.d360-mount-tab-associated'), __('No associated leads linked to this record.'));
		render_tickets_compact(issues);
		render_placeholder(page.body.find('.d360-mount-tab-trans'), __('Transactions module not connected.'));
		render_placeholder(page.body.find('.d360-mount-tab-comm'), __('Communication log not connected.'));
		render_activity_dense(payload.activities);
		render_update_lead_form(L);
		render_ai_compact(L, L.ai);
		set_tab('lead');
	}

	function set_loading(on) {
		page.body.find('.d360-loading').toggle(!!on);
	}

	function fetch_patient(leadName) {
		if (!leadName) {
			apply_dashboard(null);
			return;
		}
		set_loading(true);
		frappe.call({
			method: 'call_intelligence.api.get_patient_360_data',
			args: { lead_name: leadName },
			callback(r) {
				set_loading(false);
				apply_dashboard(r.message);
			},
			error() {
				set_loading(false);
				apply_dashboard(null);
				frappe.show_alert({ message: __('Could not load patient data'), indicator: 'red' });
			},
		});
	}

	function sync_url_params(leadName) {
		if (!window.history || !window.history.replaceState) {
			return;
		}
		const path = window.location.pathname || '';
		const params = new URLSearchParams();
		if (leadName) {
			params.set('lead_name', leadName);
		}
		if (state.recordMode === 'ticket') {
			params.set('mode', 'ticket');
		}
		const q = params.toString();
		window.history.replaceState({}, '', q ? `${path}?${q}` : path);
	}

	function navigate_to_lead(leadName) {
		if (!leadName) {
			return;
		}
		frappe.route_options = frappe.route_options || {};
		frappe.route_options.lead_name = leadName;
		sync_url_params(leadName);
		fetch_patient(leadName);
	}

	function set_record_mode(mode) {
		state.recordMode = mode === 'ticket' ? 'ticket' : 'lead';
		page.body.find('.d360-mode-btn').removeClass('is-on');
		page.body.find(`.d360-mode-btn[data-mode="${state.recordMode}"]`).addClass('is-on');
		page.body.find('.d360-mode-hint').text(
			state.recordMode === 'ticket'
				? __('Patients with at least one ticket (Issue).')
				: __('All patients (CRM Leads).')
		);
	}

	function fill_lead_select(rows, preferredName) {
		const $sel = page.body.find('.p360-lead-select');
		$sel.empty();
		$sel.append(`<option value="">${esc(__('Select patient…'))}</option>`);
		(rows || []).forEach((row) => {
			const label = row.lead_name || row.name;
			$sel.append(`<option value="${esc(row.name)}">${esc(label)}</option>`);
		});
		let pick = (preferredName || '').trim();
		if (pick && !(rows || []).some((row) => row.name === pick)) {
			$sel.append(`<option value="${esc(pick)}">${esc(pick)}</option>`);
		}
		if (!pick && rows && rows.length) {
			pick = rows[0].name;
		}
		$sel.off('change').on('change', function () {
			const v = $(this).val();
			if (v) {
				navigate_to_lead(v);
			} else {
				state.selectedIssueName = null;
				frappe.route_options = frappe.route_options || {};
				delete frappe.route_options.lead_name;
				sync_url_params('');
				apply_dashboard(null);
			}
		});
		return pick;
	}

	function load_lead_directory(urlLeadHint, preferredLeadId) {
		const method =
			state.recordMode === 'ticket'
				? 'call_intelligence.api.get_patient_360_leads_with_tickets'
				: 'call_intelligence.api.get_patient_360_leads';
		set_loading(true);
		frappe.call({
			method,
			callback(r) {
				set_loading(false);
				const rows = r.message || [];
				const preferred = (preferredLeadId || urlLeadHint || '').trim();
				const pick = fill_lead_select(rows, preferred);
				const $sel = page.body.find('.p360-lead-select');
				if (pick) {
					$sel.val(pick);
					frappe.route_options = frappe.route_options || {};
					frappe.route_options.lead_name = pick;
					sync_url_params(pick);
					fetch_patient(pick);
				} else {
					$sel.val('');
					apply_dashboard(null);
				}
			},
			error() {
				set_loading(false);
				page.body.find('.p360-lead-select').html(`<option value="">${esc(__('Could not load list'))}</option>`);
				if (urlLeadHint) {
					fetch_patient(urlLeadHint);
				} else {
					apply_dashboard(null);
				}
			},
		});
	}

	function on_create_ticket() {
		if (!state.leadId) {
			frappe.show_alert({ message: __('Select a Lead first.'), indicator: 'orange' });
			return;
		}
		frappe.call({
			method: 'call_intelligence.api.create_issue',
			args: { lead_id: state.leadId },
			freeze: true,
			freeze_message: __('Creating…'),
			callback(r) {
				if (!r.exc && r.message && r.message.name) {
					frappe.show_alert({ message: __('Ticket created'), indicator: 'green' });
					fetch_patient(state.leadId);
					set_tab('tickets');
				}
			},
		});
	}

	function on_create_lead() {
		if (frappe.new_doc) {
			frappe.new_doc('Lead');
		} else {
			frappe.set_route('List', 'Lead');
			frappe.show_alert({ message: __('Create a new Lead from the list.'), indicator: 'blue' });
		}
	}

	function on_view_leads() {
		frappe.set_route('List', 'Lead');
	}

	function on_view_tickets() {
		frappe.set_route('List', 'Issue');
	}

	const css = `
<style id="d360-dense-crm-styles">
.d360-root { background:${C.bg}; min-height:calc(100vh - 52px); width:100%; max-width:none; font-size:16px; color:${C.text}; box-sizing:border-box; }
.d360-mode-strip {
	display:flex; align-items:center; flex-wrap:wrap; gap:8px 14px;
	padding:6px 10px; margin-bottom:6px;
	background:${C.card}; border:1px solid ${C.border}; border-radius:3px;
	width:100%;
}
.d360-mode-btns { display:flex; gap:4px; }
.d360-mode-btn {
	border:1px solid ${C.border}; background:#fff; color:${C.muted}; padding:6px 14px; font-size:14px; font-weight:600;
	border-radius:3px; cursor:pointer;
}
.d360-mode-btn:hover { border-color:${C.accent}; color:${C.primary}; }
.d360-mode-btn.is-on { background:${C.primary}; color:#fff; border-color:${C.primary}; }
.d360-mode-hint { font-size:13px; color:${C.muted}; flex:1; min-width:200px; }
.d360-hdr {
	display:flex; align-items:center; flex-wrap:wrap; gap:6px 10px;
	padding:4px 10px; margin-bottom:6px;
	background:${C.card}; border:1px solid ${C.border};
	border-radius:3px; box-shadow:0 1px 1px rgba(0,0,0,.04);
	width:100%; max-width:none;
}
.d360-hdr-l { display:flex; align-items:center; gap:8px; flex:1 1 220px; min-width:0; }
.d360-hdr-c { flex:2 1 280px; display:flex; justify-content:center; }
.d360-hdr-r { display:flex; align-items:center; gap:8px; flex:1 1 220px; justify-content:flex-end; flex-wrap:wrap; }
.d360-back { padding:4px 10px !important; font-size:14px !important; line-height:1.35 !important; }
.d360-hdr-title { font-weight:800; color:${C.primary}; font-size:16px; }
.d360-hdr-sep { color:${C.border}; }
.d360-hdr-id { font-size:14px; font-weight:600; color:${C.muted}; font-family:ui-monospace,monospace; }
.d360-hdr .form-control { padding:6px 12px !important; height:auto !important; font-size:15px !important; min-width:240px; width:100%; max-width:min(520px, 96vw); border-radius:3px; }
.d360-hdr .btn { padding:6px 12px !important; font-size:14px !important; line-height:1.35 !important; border-radius:3px; }
.d360-cols { display:flex; gap:6px; align-items:stretch; width:100%; max-width:none; margin:0; flex-wrap:wrap; }
.d360-col-l { flex:0 0 260px; width:260px; max-width:100%; display:flex; flex-direction:column; gap:6px; }
.d360-col-c { flex:1 1 520px; min-width:min(100%, 400px); border:1px solid ${C.border}; background:${C.card}; border-radius:3px; overflow:hidden; }
.d360-col-r { flex:0 0 280px; width:280px; max-width:100%; display:flex; flex-direction:column; gap:6px; }
.d360-sec { border:1px solid ${C.border}; background:${C.card}; border-radius:3px; padding:10px 12px; }
.d360-sec-h { font-size:12px; font-weight:800; text-transform:uppercase; letter-spacing:.04em; color:${C.primary}; margin-bottom:8px; border-bottom:1px solid ${C.border}; padding-bottom:6px; }
.d360-sec-b { font-size:15px; }
.d360-sec-foot { margin-top:8px; }
.d360-name { font-weight:800; font-size:20px; margin-bottom:8px; color:${C.text}; line-height:1.2; }
.d360-kv { display:flex; justify-content:space-between; gap:10px; margin-bottom:6px; font-size:15px; align-items:baseline; }
.d360-kv-block { flex-direction:column; align-items:stretch; }
.d360-k { color:${C.muted}; flex:0 0 44%; font-size:14px; }
.d360-v { font-weight:600; text-align:right; word-break:break-word; font-size:15px; }
.d360-kv-block .d360-v { text-align:left; margin-top:4px; }
.d360-desc { font-weight:400; color:${C.text}; line-height:1.45; font-size:15px; }
.d360-muted { color:${C.muted}; font-size:14px; margin:0; }
.d360-link { color:${C.accent}; font-size:14px; font-weight:600; text-decoration:none; }
.d360-link:hover { text-decoration:underline; }
.d360-chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:6px; }
.d360-chip { font-size:13px; padding:4px 10px; border-radius:3px; background:${C.neutralBg}; color:${C.neutralFg}; border:1px solid ${C.border}; }
.d360-chip-ai { background:${C.infoBg}; color:${C.infoFg}; }
.d360-appt { margin:0; font-size:15px; font-weight:600; }
.d360-tabbar { display:flex; flex-wrap:nowrap; gap:0; overflow-x:auto; border-bottom:1px solid ${C.border}; background:#FAFAFA; -webkit-overflow-scrolling:touch; }
.d360-tab {
	border:none; background:transparent; padding:8px 12px; font-size:14px; font-weight:700; color:${C.muted};
	cursor:pointer; white-space:nowrap; border-bottom:2px solid transparent; margin-bottom:-1px;
}
.d360-tab:hover { color:${C.primary}; background:rgba(59,130,246,.06); }
.d360-tab.is-on { color:${C.primary}; border-bottom-color:${C.accent}; background:${C.card}; }
.d360-tab-pane { padding:12px 14px; display:none; min-height:140px; }
.d360-tab-pane[data-pane="lead"] { display:block; }
.d360-two-col { display:grid; grid-template-columns:1fr 1fr; gap:12px 24px; }
@media (max-width:900px){ .d360-two-col { grid-template-columns:1fr; } }
.d360-grid-col { display:flex; flex-direction:column; gap:4px; }
.d360-book-row { margin-top:10px; padding-top:10px; border-top:1px solid ${C.border}; }
.d360-book-inline { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
.d360-book-inline .form-control { flex:1; min-width:180px; max-width:320px; padding:6px 10px !important; font-size:15px !important; height:auto !important; }
.d360-book-inline .btn { font-size:14px !important; padding:6px 14px !important; }
.d360-subh { font-size:12px; font-weight:800; color:${C.primary}; text-transform:uppercase; margin:12px 0 8px; }
.d360-hist { border-left:2px solid ${C.border}; margin-left:8px; padding-left:12px; }
.d360-hist-i { position:relative; padding-bottom:10px; }
.d360-hist-dot { position:absolute; left:-17px; top:5px; width:10px; height:10px; border-radius:50%; background:${C.accent}; border:2px solid ${C.card}; }
.d360-hist-d { font-size:13px; color:${C.muted}; }
.d360-hist-t { font-size:15px; font-weight:700; }
.d360-hist-r { font-size:14px; color:${C.muted}; line-height:1.4; margin-top:2px; }
.d360-ticket-row { border:1px solid ${C.border}; border-radius:3px; padding:8px 10px; margin-bottom:6px; cursor:pointer; background:${C.card}; }
.d360-ticket-row:hover { background:#F9FAFB; }
.d360-ticket-row.is-sel { border-color:${C.accent}; box-shadow:0 0 0 1px ${C.accent}; background:#EFF6FF; }
.d360-tr-main { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
.d360-tr-title { font-weight:700; font-size:15px; flex:1 1 180px; min-width:0; }
.d360-tr-time { font-size:13px; color:${C.muted}; margin-left:auto; }
.d360-tr-desc { font-size:14px; color:${C.muted}; margin-top:6px; line-height:1.4; }
.d360-pill { display:inline-block; padding:2px 10px; border-radius:3px; font-size:12px; font-weight:700; }
.d360-act-row { display:flex; gap:10px; align-items:flex-start; font-size:14px; padding:6px 0; border-bottom:1px solid ${C.border}; }
.d360-act-row:last-child { border-bottom:none; }
.d360-act-t { flex:0 0 96px; color:${C.muted}; font-size:13px; line-height:1.4; }
.d360-act-body { flex:1; min-width:0; }
.d360-act-title { font-weight:700; color:${C.text}; font-size:15px; }
.d360-act-desc { color:${C.muted}; line-height:1.4; margin-top:3px; font-size:14px; }
.d360-rh { font-size:12px; font-weight:800; text-transform:uppercase; color:${C.primary}; margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid ${C.border}; }
.d360-mount-update, .d360-mount-ai { border:1px solid ${C.border}; background:${C.card}; border-radius:3px; padding:10px 12px; }
.d360-form-compact .d360-fl { display:block; font-size:12px; color:${C.muted}; margin:8px 0 3px; text-transform:uppercase; font-weight:600; }
.d360-form-compact .form-control { margin-bottom:0; padding:6px 10px !important; font-size:15px !important; border-radius:3px; }
.d360-form-compact textarea.form-control { min-height:56px; }
.d360-f-save { margin-top:10px; width:100%; font-size:14px !important; padding:8px !important; }
.d360-form-note { font-size:12px; color:${C.muted}; margin:8px 0 0; line-height:1.35; }
.d360-ai-row { margin-bottom:6px; font-size:15px; display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
.d360-ai-sum { margin-top:8px; }
.d360-ai-sum-t { font-size:15px; line-height:1.45; margin-top:4px; color:${C.text}; }
.d360-tr-det summary { cursor:pointer; font-size:14px; font-weight:700; color:${C.accent}; padding:6px 0; }
.d360-pre { margin:6px 0 0; padding:8px 10px; background:#F9FAFB; border:1px solid ${C.border}; border-radius:3px; font-size:13px; white-space:pre-wrap; word-break:break-word; max-height:200px; overflow:auto; }
.d360-pad-sm { padding:6px 0; }
</style>`;

	const shell = `
${css}
<div class="d360-root" style="padding:6px 8px 12px;">
	<div class="d360-mode-strip">
		<div class="d360-mode-btns">
			<button type="button" class="d360-mode-btn is-on" data-mode="lead">${esc(__('Leads'))}</button>
			<button type="button" class="d360-mode-btn" data-mode="ticket">${esc(__('Tickets'))}</button>
		</div>
		<span class="d360-mode-hint">${esc(__('All patients (CRM Leads).'))}</span>
	</div>
	<div class="d360-hdr">
		<div class="d360-hdr-l">
			<button type="button" class="btn btn-default btn-sm d360-back">${esc(__('Back'))}</button>
			<span class="d360-hdr-sep">|</span>
			<span class="d360-hdr-title">${esc(__('Patient 360'))}</span>
			<span class="d360-hdr-sep">|</span>
			<span class="d360-hdr-id">—</span>
		</div>
		<div class="d360-hdr-c">
			<select class="form-control input-sm p360-lead-select"><option value="">${esc(__('Loading…'))}</option></select>
		</div>
		<div class="d360-hdr-r">
			<span class="d360-loading text-muted" style="display:none;font-size:14px;">${esc(__('Loading…'))}</span>
			<button type="button" class="btn btn-default btn-sm d360-btn-view-leads">${esc(__('View Leads'))}</button>
			<button type="button" class="btn btn-default btn-sm d360-btn-view-tickets">${esc(__('View Tickets'))}</button>
			<button type="button" class="btn btn-default btn-sm d360-btn-ticket">${esc(__('Create Ticket'))}</button>
			<button type="button" class="btn btn-primary btn-sm d360-btn-newlead">${esc(__('Create Lead'))}</button>
		</div>
	</div>
	<div class="d360-cols">
		<div class="d360-col-l d360-mount-left"></div>
		<div class="d360-col-c">
			<div class="d360-tabbar">
				<button type="button" class="d360-tab is-on" data-tab="lead">${esc(__('Lead details'))}</button>
				<button type="button" class="d360-tab" data-tab="associated">${esc(__('Associated leads'))}</button>
				<button type="button" class="d360-tab" data-tab="tickets">${esc(__('Tickets'))}</button>
				<button type="button" class="d360-tab" data-tab="trans">${esc(__('Transactions'))}</button>
				<button type="button" class="d360-tab" data-tab="comm">${esc(__('Communication'))}</button>
				<button type="button" class="d360-tab" data-tab="activity">${esc(__('Activity history'))}</button>
			</div>
			<div class="d360-tab-pane" data-pane="lead"><div class="d360-mount-tab-lead"></div></div>
			<div class="d360-tab-pane" data-pane="associated" style="display:none;"><div class="d360-mount-tab-associated"></div></div>
			<div class="d360-tab-pane" data-pane="tickets" style="display:none;"><div class="d360-mount-tab-tickets"></div></div>
			<div class="d360-tab-pane" data-pane="trans" style="display:none;"><div class="d360-mount-tab-trans"></div></div>
			<div class="d360-tab-pane" data-pane="comm" style="display:none;"><div class="d360-mount-tab-comm"></div></div>
			<div class="d360-tab-pane" data-pane="activity" style="display:none;"><div class="d360-mount-tab-activity"></div></div>
		</div>
		<div class="d360-col-r">
			<div class="d360-mount-update"></div>
			<div class="d360-mount-ai"></div>
		</div>
	</div>
</div>`;

	state.recordMode = get_resolved_mode_from_entry();

	$(page.body).empty().append(shell);
	$(wrapper).css({ width: '100%', maxWidth: 'none' });
	const $pb = $(wrapper).closest('.page-body');
	if ($pb.length) {
		$pb.css({ maxWidth: '100%' });
	}

	set_record_mode(state.recordMode);
	bind_tabs();

	page.body.find('.d360-mode-btn').on('click', function () {
		const m = $(this).data('mode');
		if (m === state.recordMode) {
			return;
		}
		state.recordMode = m;
		set_record_mode(m);
		sync_url_params(state.leadId || '');
		load_lead_directory(null, state.leadId || '');
	});

	page.body.find('.d360-back').on('click', () => window.history.back());
	page.body.find('.d360-btn-view-leads').on('click', on_view_leads);
	page.body.find('.d360-btn-view-tickets').on('click', on_view_tickets);
	page.body.find('.d360-btn-ticket').on('click', on_create_ticket);
	page.body.find('.d360-btn-newlead').on('click', on_create_lead);

	frappe.call({
		method: 'call_intelligence.api.get_patient_360_meta',
		callback(r) {
			if (r.message) {
				state.meta = r.message;
			}
			if (state.payload && state.payload.lead) {
				render_update_lead_form(state.payload.lead);
			}
		},
	});

	const urlLead = get_resolved_lead_name_from_entry();
	load_lead_directory(urlLead, null);
};
