frappe.pages["patient-360-dashboard"].on_page_load = function (wrapper) {
	function runPatient360Dashboard() {
	if (frappe.session.user === "Guest") {
		frappe.msgprint({
			title: __("Login required"),
			message: __("Please log in to use Patient 360 Dashboard."),
			indicator: "orange",
		});
		return;
	}
	const page = frappe.ui.make_app_page({
		parent: wrapper,
		title: __("Patient 360 Dashboard"),
		single_column: true,
	});

	const pch = call_intelligence.patient_chat;

	const state = {
		mode: "lead",
		list: [],
		selectedName: "",
		detail: null,
		wa: [],
		waOptimistic: [],
		loadingWA: false,
		loadingList: false,
		loadingDetail: false,
		waPollTimer: null,
		leadListPollTimer: null,
	};

	function esc(v) {
		return frappe.utils.escape_html(v == null ? "" : String(v));
	}

	function text(v, fallback = "—") {
		const s = v == null ? "" : String(v).trim();
		return s || fallback;
	}

	function priorityBadgeClass(p) {
		const s = text(p, "").toLowerCase();
		if (s.includes("high") || s.includes("urgent") || s.includes("critical")) return "pch-badge--pri-high";
		if (s.includes("medium")) return "pch-badge--pri-med";
		if (s.includes("low")) return "pch-badge--pri-low";
		return "";
	}

	/**
	 * Compact context bar: TL;DR + Status by default; full fields behind Expand.
	 * `extra.priorityLabel` matches header badges when available.
	 */
	function renderCallSummaryCard(lead, issues, extra) {
		const ai = (lead && lead.ai) || {};
		const iss0 = (issues && issues[0]) || {};
		const suggestedTeam = text(
			iss0.department_to_handle || lead.department,
			"—"
		);
		const summary = text(ai.summary, "—");
		const actionRequired = text(ai.action_required, "—");
		const actionDesc = text(ai.action_description, "—");
		const nextStep = text(ai.call_solution, "—");
		const crmStatus = text(lead.status, "—");
		const booking = lead.booking_status != null && String(lead.booking_status).trim() !== ""
			? text(lead.booking_status, "—")
			: "";
		const statusLine = booking ? `${crmStatus} · ${__("Booking")}: ${booking}` : crmStatus;
		const pri =
			(extra && String(extra.priorityLabel || "").trim()) ||
			text(lead.whatsapp_priority, "");
		const statusCompactParts = [];
		if (crmStatus && crmStatus !== "—") statusCompactParts.push(crmStatus);
		if (booking && booking !== "—") statusCompactParts.push(`${__("Booking")}: ${booking}`);
		if (pri && pri !== "—") statusCompactParts.push(`${__("Priority")}: ${pri}`);
		const statusCompact =
			statusCompactParts.length > 0 ? statusCompactParts.join(" · ") : statusLine;

		function row(dt, dd) {
			return `<div class="pch-call-summary-row"><span class="pch-call-summary-dt">${esc(dt)}</span><span class="pch-call-summary-dd">${esc(dd)}</span></div>`;
		}

		return `
			<div class="pch-call-summary-card pch-call-summary-card--compact is-collapsed" data-ci-summary-expanded="0">
				<div class="pch-call-summary-head-row">
					<span class="pch-call-summary-head">${esc(__("Call summary"))}</span>
					<button type="button" class="btn btn-xs btn-default pch-call-summary-toggle" aria-expanded="false">${esc(__("Expand"))}</button>
				</div>
				<div class="pch-call-summary-preview">
					${row(__("TL;DR"), summary)}
					${row(__("Status"), statusCompact)}
				</div>
				<div class="pch-call-summary-extra" hidden>
					${row(__("Action required"), actionRequired)}
					${row(__("Action"), actionDesc)}
					${row(__("Next step"), nextStep)}
					${row(__("Suggested team"), suggestedTeam)}
					${row(__("Status"), statusLine)}
				</div>
			</div>
		`;
	}

	function renderAiInsightCard(lead) {
		const ins = (lead && lead.lq_insight) || {};
		const rationale = text(ins.rationale, "");
		const scoreStr =
			ins.score != null && ins.score !== "" ? String(ins.score) : "";
		const hasData =
			(rationale && rationale !== "—") ||
			scoreStr ||
			ins.score === 0 ||
			ins.qualified === true ||
			ins.qualified === false;
		if (!hasData) {
			return "";
		}
		const qualLabel =
			ins.qualified === true
				? __("Yes")
				: ins.qualified === false
					? __("No")
					: "—";
		const dx = text(ins.diagnosis, "");
		const insSt = text(ins.insurance_status, "");
		function row(dt, dd) {
			return `<div class="pch-call-summary-row"><span class="pch-call-summary-dt">${esc(dt)}</span><span class="pch-call-summary-dd">${esc(dd)}</span></div>`;
		}
		return `
			<div class="pch-ai-insight-card pch-call-summary-card pch-call-summary-card--compact is-collapsed" data-ci-ai-insight-expanded="0">
				<div class="pch-call-summary-head-row">
					<span class="pch-call-summary-head">${esc(__("AI Insight"))}</span>
					<button type="button" class="btn btn-xs btn-default pch-ai-insight-toggle" aria-expanded="false">${esc(__("Expand"))}</button>
				</div>
				<div class="pch-call-summary-preview">
					${row(__("Qualified"), qualLabel)}
					${scoreStr || ins.score === 0 ? row(__("Score"), scoreStr || "0") : ""}
					${dx && dx !== "—" ? row(__("Diagnosis (LQ)"), dx) : ""}
				</div>
				<div class="pch-ai-insight-rationale pch-call-summary-extra" hidden>
					${rationale ? `<div class="pch-ai-insight-rationale-text">${esc(rationale)}</div>` : ""}
					${insSt && insSt !== "—" ? row(__("Insurance (LQ)"), insSt) : ""}
				</div>
			</div>
		`;
	}

	function renderCallSummaryPopover(lead, issues, extra) {
		const ai = (lead && lead.ai) || {};
		const iss0 = (issues && issues[0]) || {};
		const suggestedTeam = text(iss0.department_to_handle || lead.department, "—");

		const summary = text(ai.summary, "—");
		const actionRequired = text(ai.action_required, "—");
		const nextStep = text(ai.call_solution, "—");

		const crmStatus = text(lead.status, "—");
		const booking =
			lead.booking_status != null && String(lead.booking_status).trim() !== ""
				? text(lead.booking_status, "—")
				: "";

		const pri = (extra && String(extra.priorityLabel || "").trim()) || text(lead.whatsapp_priority, "");
		const statusCompactParts = [];
		if (crmStatus && crmStatus !== "—") statusCompactParts.push(crmStatus);
		if (booking && booking !== "—") statusCompactParts.push(`${__("Booking")}: ${booking}`);
		if (pri && pri !== "—") statusCompactParts.push(`${__("Priority")}: ${pri}`);
		const statusCompact = statusCompactParts.length > 0 ? statusCompactParts.join(" · ") : crmStatus;

		function row(dt, dd) {
			return `<div class="pch-call-summary-row"><span class="pch-call-summary-dt">${esc(dt)}</span><span class="pch-call-summary-dd">${esc(dd)}</span></div>`;
		}

		return `
			<div class="pch-inline-popover-title">${esc(__("Call summary"))}</div>
			<div class="pch-inline-popover-body">
				${row(__("TL;DR"), summary)}
				${row(__("Status"), statusCompact)}
				<div class="pch-inline-popover-divider"></div>
				${row(__("Action required"), actionRequired)}
				${row(__("Next step"), nextStep)}
				${suggestedTeam && suggestedTeam !== "—" ? row(__("Suggested team"), suggestedTeam) : ""}
			</div>
		`;
	}

	function renderAiInsightPopover(lead) {
		const ins = (lead && lead.lq_insight) || {};
		const rationale = text(ins.rationale, "");
		const scoreStr = ins.score != null && ins.score !== "" ? String(ins.score) : "";

		const hasData =
			(rationale && rationale !== "—") ||
			scoreStr ||
			ins.score === 0 ||
			ins.qualified === true ||
			ins.qualified === false;

		if (!hasData) {
			return `
				<div class="pch-inline-popover-title">${esc(__("AI Insight"))}</div>
				<div class="pch-inline-popover-body">
					<div class="pch-inline-popover-empty">${esc(__("No AI insight yet."))}</div>
				</div>
			`;
		}

		const qualLabel =
			ins.qualified === true ? __("Yes") : ins.qualified === false ? __("No") : "—";
		const dx = text(ins.diagnosis, "");
		const insSt = text(ins.insurance_status, "");

		function row(dt, dd) {
			return `<div class="pch-call-summary-row"><span class="pch-call-summary-dt">${esc(dt)}</span><span class="pch-call-summary-dd">${esc(dd)}</span></div>`;
		}

		return `
			<div class="pch-inline-popover-title">${esc(__("AI Insight"))}</div>
			<div class="pch-inline-popover-body">
				${row(__("Qualified"), qualLabel)}
				${scoreStr || ins.score === 0 ? row(__("Score"), scoreStr || "0") : ""}
				${dx && dx !== "—" ? row(__("Diagnosis (LQ)"), dx) : ""}
				${insSt && insSt !== "—" ? row(__("Insurance (LQ)"), insSt) : ""}
				${rationale && rationale !== "—" ? `<div class="pch-inline-popover-divider"></div>${row(__("Rationale"), rationale)}` : ""}
			</div>
		`;
	}

	function clearWhatsAppPoll() {
		if (state.waPollTimer) {
			clearInterval(state.waPollTimer);
			state.waPollTimer = null;
		}
	}

	function patchWhatsAppThreadDom() {
		const $thread = page.body.find(".pch-chat-thread");
		if (!$thread.length) return;
		const lead = state.detail && state.detail.lead;
		const patientLabel = lead ? text(lead.name, __("Patient")) : __("Patient");
		$thread.html(
			pch.renderThread(state.wa, {
				patientName: patientLabel,
				optimistic: state.waOptimistic,
			})
		);
		pch.scrollThreadToBottom($thread, true);
	}

	function loadWhatsAppQuiet(leadId) {
		if (!leadId) return;
		frappe.call({
			method: "call_intelligence.api.get_whatsapp_communications",
			args: { lead_name: leadId, limit: 80 },
			callback(r) {
				state.wa = (r.message || []).slice(0, 80);
				state.waOptimistic = [];
				patchWhatsAppThreadDom();
			},
		});
	}

	function scheduleWhatsAppPoll(leadId) {
		clearWhatsAppPoll();
		if (!leadId) return;
		state.waPollTimer = setInterval(() => {
			if (state.selectedName !== leadId) {
				clearWhatsAppPoll();
				return;
			}
			loadWhatsAppQuiet(leadId);
		}, 2500);
	}

	function route_to_form(doctype, name) {
		if (!name) return;
		frappe.set_route("Form", doctype, name);
	}

	function renderShell() {
		const html = `
			<div class="ci-root-container">
				<div class="p360dash-root pch-page">
					<div class="p360dash-top">
						<div class="p360dash-title">${esc(__("Patient CRM Dashboard"))}</div>
						<div class="p360dash-toggle">
							<button class="btn btn-sm btn-primary p360dash-toggle-btn" data-mode="lead">${esc(__("Leads"))}</button>
							<button class="btn btn-sm btn-default p360dash-toggle-btn" data-mode="ticket">${esc(__("Tickets"))}</button>
						</div>
					</div>
					<div class="p360dash-main ci-main-layout">
						<div class="pch-conv-panel ci-left-panel">
							<div class="pch-conv-head">
								<span class="p360dash-left-label">${esc(__("Lead List"))}</span>
							</div>
							<div class="pch-conv-list p360dash-list"></div>
						</div>
						<div class="pch-chat-panel chat-container ci-right-panel">
							<div class="p360dash-detail"></div>
						</div>
					</div>
				</div>
			</div>
		`;

		$(page.body).empty().append(html);
	}

	function setToggleUI() {
		const mode = state.mode;
		page.body.find(".p360dash-toggle-btn").each(function () {
			const $btn = $(this);
			const isOn = $btn.data("mode") === mode;
			$btn.toggleClass("btn-primary", isOn).toggleClass("btn-default", !isOn);
		});
		page.body.find(".p360dash-left-label").text(mode === "ticket" ? __("Ticket Patients") : __("Lead List"));
	}

	function listMethod() {
		return state.mode === "ticket"
			? "call_intelligence.api.get_patient_360_leads_with_tickets"
			: "call_intelligence.api.get_patient_360_leads";
	}

	function renderList() {
		const $list = page.body.find(".p360dash-list");
		if (state.loadingList) {
			$list.html(`<div class="pch-thread-empty">${esc(__("Loading list..."))}</div>`);
			return;
		}
		if (!state.list.length) {
			$list.html(`<div class="pch-thread-empty">${esc(__("No records found."))}</div>`);
			return;
		}

		$list.html(pch.renderConversationList(state.list, state.selectedName, state.mode));

		$list.find(".pch-conv-item").on("click", function () {
			const leadName = $(this).data("name");
			if (!leadName || leadName === state.selectedName) return;
			state.selectedName = leadName;
			renderList();
			loadDetail(leadName);
		});
	}

	function renderDetail() {
		const $detail = page.body.find(".p360dash-detail");
		if (state.loadingDetail) {
			$detail.html(`<div class="pch-thread-empty">${esc(__("Loading details..."))}</div>`);
			return;
		}
		if (!state.detail || !state.detail.lead) {
			$detail.html(`<div class="pch-thread-empty">${esc(__("Select a conversation from the left."))}</div>`);
			return;
		}

		const lead = state.detail.lead || {};
		const ai = lead.ai || {};
		const issues = (state.detail.issues || []).slice(0, 1);
		const sentiment = text(ai.sentiment, "UNKNOWN");
		const outcome = text(ai.outcome, "UNKNOWN");
		const booking = text(lead.booking_status, "—");
		const waPri = text(lead.whatsapp_priority, "—");
		const listRow = state.list.find((x) => x.name === state.selectedName);
		const priFromList = listRow ? text(listRow.priority, "") : "";
		const priSource = priFromList && priFromList !== "—" ? priFromList : waPri;
		const priClass = priorityBadgeClass(priSource);

		const waHtml = state.loadingWA
			? `<div class="pch-thread-empty">${esc(__("Loading WhatsApp..."))}</div>`
			: pch.renderThread(state.wa, {
					patientName: text(lead.name, __("Patient")),
					optimistic: state.waOptimistic,
			  });

		$detail.html(`
			<div class="pch-chat-toolbar">
				<button type="button" class="btn btn-sm btn-default p360dash-open-lead">${esc(__("Open Lead"))}</button>
				<button type="button" class="btn btn-sm btn-default p360dash-open-p360">${esc(__("Open Patient 360 Page"))}</button>
				<button type="button" class="btn btn-sm btn-default p360dash-lq-agent">${esc(__("Run Lead Qualification"))}</button>
				<button type="button" class="btn btn-sm btn-default p360dash-demo-wa">${esc(__("Send Demo WhatsApp"))}</button>
				<button type="button" class="btn btn-sm btn-danger p360dash-delete-lead">${esc(__("Delete Patient"))}</button>
			</div>
			<div class="pch-chat-header">
				<div class="pch-chat-header-main">
					<div class="pch-chat-header-avatar">${esc(pch.initials(lead.name))}</div>
					<div>
						<div class="pch-chat-title">${esc(text(lead.name))}</div>
						<div class="pch-chat-sub">${esc(text(lead.phone, __("No phone")))}</div>
					</div>
				</div>
				<div class="pch-badges">
					<span class="pch-badge">${esc(__("Booking"))}: ${esc(booking)}</span>
					<span class="pch-inline-info">
						<button
							type="button"
							class="pch-badge pch-inline-info-btn pch-inline-info-btn--call-summary"
							data-popover="call-summary"
							aria-expanded="false"
						>
							${esc(__("Call summary"))}
						</button>
						<div class="pch-inline-popover pch-inline-popover--call-summary" hidden>
							${renderCallSummaryPopover(lead, state.detail.issues || [], { priorityLabel: priSource })}
						</div>
					</span>
					<span class="pch-inline-info">
						<button
							type="button"
							class="pch-badge pch-inline-info-btn pch-inline-info-btn--ai-insight"
							data-popover="ai-insight"
							aria-expanded="false"
						>
							${esc(__("AI Insight"))}
						</button>
						<div class="pch-inline-popover pch-inline-popover--ai-insight" hidden>
							${renderAiInsightPopover(lead)}
						</div>
					</span>
					<span class="pch-badge ${esc(priClass)}">${esc(__("Priority"))}: ${esc(priSource)}</span>
					<span class="pch-badge">${esc(__("Sentiment"))}: ${esc(sentiment)}</span>
					<span class="pch-badge">${esc(__("Outcome"))}: ${esc(outcome)}</span>
					<button type="button" class="btn btn-xs btn-default pch-resolve">${esc(__("Resolve"))}</button>
				</div>
			</div>
			<div class="pch-chat-main pch-chat-main--thread">
				<div class="pch-chat-messages-wrap ci-chat-container">
					<div class="pch-chat-thread"></div>
				</div>
			</div>
			<div class="pch-composer">
				${pch.renderComposerInnerHtml()}
			</div>
		`);

		$detail.find(".pch-chat-thread").html(waHtml);
		pch.scrollThreadToBottom($detail.find(".pch-chat-thread"), false);
		pch.setupPatientComposer($detail);

		function closeInlinePopovers() {
			$detail.find(".pch-inline-popover").attr("hidden", true);
			$detail.find(".pch-inline-info-btn").attr("aria-expanded", "false");
		}

		function toggleInlinePopover(which) {
			const $pop = $detail.find(`.pch-inline-popover--${which}`);
			const $btn = $detail.find(`.pch-inline-info-btn--${which}`);
			const isOpen = $pop.length && $pop.attr("hidden") == null;
			closeInlinePopovers();
			if (!isOpen && $pop.length) {
				$pop.removeAttr("hidden");
				$btn.attr("aria-expanded", "true");
			}
		}

		closeInlinePopovers();
		$detail.find(".pch-inline-info-btn--call-summary").on("click", function (e) {
			e.preventDefault();
			e.stopPropagation();
			toggleInlinePopover("call-summary");
		});
		$detail.find(".pch-inline-info-btn--ai-insight").on("click", function (e) {
			e.preventDefault();
			e.stopPropagation();
			toggleInlinePopover("ai-insight");
		});

		$(page.body).off("click.pch-inline-popover").on("click.pch-inline-popover", function (e) {
			if ($(e.target).closest(".pch-inline-info").length) return;
			closeInlinePopovers();
		});

		$detail.find(".pch-ai-insight-toggle").on("click", function () {
			const $btn = $(this);
			const $card = $btn.closest(".pch-ai-insight-card");
			const $extra = $card.find(".pch-call-summary-extra");
			const expanded = $card.hasClass("is-expanded");
			if (expanded) {
				$card.removeClass("is-expanded").addClass("is-collapsed");
				$btn.attr("aria-expanded", "false").text(__("Expand"));
				$extra.attr("hidden", true);
			} else {
				$card.addClass("is-expanded").removeClass("is-collapsed");
				$btn.attr("aria-expanded", "true").text(__("Collapse"));
				$extra.removeAttr("hidden");
			}
		});

		$detail.find(".pch-call-summary-toggle").on("click", function () {
			const $btn = $(this);
			const $card = $btn.closest(".pch-call-summary-card");
			const $extra = $card.find(".pch-call-summary-extra");
			const expanded = $card.hasClass("is-expanded");
			if (expanded) {
				$card.removeClass("is-expanded").addClass("is-collapsed");
				$btn.attr("aria-expanded", "false").text(__("Expand"));
				$extra.attr("hidden", true);
				$card.attr("data-ci-summary-expanded", "0");
			} else {
				$card.addClass("is-expanded").removeClass("is-collapsed");
				$btn.attr("aria-expanded", "true").text(__("Collapse"));
				$extra.removeAttr("hidden");
				$card.attr("data-ci-summary-expanded", "1");
			}
		});

		$detail.find(".p360dash-open-lead").on("click", () => route_to_form("Lead", lead.lead_id));
		$detail.find(".p360dash-open-p360").on("click", () =>
			frappe.set_route("patient-360", { lead_name: lead.lead_id })
		);
		$detail.find(".p360dash-lq-agent").on("click", function () {
			const $btn = $(this);
			const lid = state.selectedName;
			if (!lid) {
				frappe.show_alert({ message: __("Select a lead first."), indicator: "orange" });
				return;
			}
			$btn.prop("disabled", true);
			frappe.call({
				method: "call_intelligence.api.run_lead_qualification",
				args: { lead_name: lid },
				freeze: true,
				freeze_message: __("Running lead qualification…"),
				callback(r) {
					const m = r.message || {};
					const res = m.result || {};
					if (res.qualified) {
						frappe.show_alert({
							message: __("Lead Qualified (Score: {0})", [String(res.score)]),
							indicator: "green",
						});
					} else {
						frappe.show_alert({ message: __("Not Qualified"), indicator: "orange" });
					}
					if (res.rationale) {
						frappe.msgprint({
							title: __("Rationale"),
							message: esc(res.rationale),
							indicator: "blue",
						});
					}
					loadWhatsAppQuiet(lid);
					loadDetail(lid);
				},
				error() {
					frappe.show_alert({ message: __("Lead qualification failed."), indicator: "red" });
				},
				always() {
					$btn.prop("disabled", false);
				},
			});
		});
		$detail.find(".pch-resolve").on("click", () => {
			frappe.show_alert({ message: __("Marked for follow-up (demo)."), indicator: "blue" });
		});

		$detail.find(".p360dash-demo-wa").on("click", function () {
			const $btn = $(this);
			$btn.prop("disabled", true);
			frappe.call({
				method: "call_intelligence.api.create_demo_patient",
				freeze: true,
				freeze_message: __("Creating demo lead…"),
				callback(r) {
					const leadName = (r.message || {}).lead_name;
					const demoPhone = String((r.message || {}).phone || "").trim();
					if (!leadName) {
						frappe.show_alert({ message: __("Demo lead could not be created."), indicator: "red" });
						$btn.prop("disabled", false);
						return;
					}
					frappe.call({
						method: "call_intelligence.api.send_demo_whatsapp_message",
						args: { lead_name: leadName },
						freeze: true,
						freeze_message: __("Sending demo WhatsApp…"),
						callback(r2) {
							const m = r2.message || {};
							if (m.ok) {
								state.selectedName = leadName;
								const row = {
									name: leadName,
									lead_name: __("Demo Patient"),
									phone: demoPhone,
									priority: "",
								};
								const ix = state.list.findIndex((x) => x.name === leadName);
								if (ix >= 0) {
									state.list[ix] = { ...state.list[ix], ...row };
								} else {
									state.list.unshift(row);
								}
								renderList();
								frappe.show_alert({ message: __("Demo WhatsApp sent."), indicator: "green" });
								loadDetail(leadName);
							} else {
								frappe.show_alert({
									message: __("Demo message could not be sent. Check Error Log."),
									indicator: "red",
								});
							}
						},
						error() {
							frappe.show_alert({ message: __("Demo send failed."), indicator: "red" });
						},
						always() {
							$btn.prop("disabled", false);
						},
					});
				},
				error() {
					frappe.show_alert({ message: __("Could not create demo lead."), indicator: "red" });
					$btn.prop("disabled", false);
				},
			});
		});

		$detail.find(".p360dash-delete-lead").on("click", function () {
			const $btn = $(this);
			const lid = state.selectedName;
			if (!lid) {
				frappe.show_alert({ message: __("Select a lead first."), indicator: "orange" });
				return;
			}

			const patientLabel = text(lead.name, __("Patient"));
			const phoneLabel = text(lead.phone, "").trim();
			const confirmMsg = phoneLabel
				? __("Delete {0} ({1})? This cannot be undone.", [patientLabel, phoneLabel])
				: __("Delete {0}? This cannot be undone.", [patientLabel]);

			// Simple native confirm to avoid extra dependencies.
			if (!window.confirm(confirmMsg)) return;

			$btn.prop("disabled", true);
			frappe.call({
				method: "call_intelligence.api.delete_patient_360_lead",
				args: { lead_name: lid },
				freeze: true,
				freeze_message: __("Deleting…"),
				callback(r) {
					const m = r.message || {};
					if (m.ok) {
						frappe.show_alert({ message: __("Patient deleted."), indicator: "green" });
						state.selectedName = "";
						state.detail = null;
						renderDetail();
						loadList();
					} else {
						frappe.show_alert({
							message: m.error || __("Could not delete patient."),
							indicator: "red",
						});
					}
				},
				error() {
					frappe.show_alert({ message: __("Could not delete patient."), indicator: "red" });
				},
				always() {
					$btn.prop("disabled", false);
				},
			});
		});

		$detail.find(".pch-composer-send").on("click", function () {
			const activeTab = String($detail.find(".pch-composer-tab.is-on").data("tab") || "reply");
			if (activeTab === "note") {
				frappe.show_alert({
					message: __("Private notes are not sent to WhatsApp. Switch to Reply."),
					indicator: "orange",
				});
				return;
			}
			const err = pch.composerValidationError($detail);
			if (err) {
				frappe.show_alert({ message: err, indicator: "orange" });
				return;
			}
			const payload = pch.readComposerPayload($detail);
			const $btn = $(this);

			function sendArgsFromPayload(p) {
				const args = {
					phone: text(lead.phone, ""),
					reference_doctype: "Lead",
					reference_name: text(lead.lead_id, ""),
					msg_type: p.msg_type,
				};
				if (p.msg_type === "text") {
					args.message = p.message;
				} else {
					args.media_url = p.media_url;
					if (p.message) {
						args.caption = p.message;
						args.message = p.message;
					} else {
						args.message = "";
					}
					if (p.msg_type === "document" && p.filename) {
						args.filename = p.filename;
					}
				}
				return args;
			}

			function afterSendResponse(res) {
				state.waOptimistic = [];
				if (res.ok) {
					let sentMsg =
						res.note ||
						(res.fallback === "template"
							? __("Template message sent (session text was not allowed).")
							: __("Message sent to WhatsApp."));
					// In test mode we already show a short "delivered to admin/test" message.
					// Do NOT append the long routing_hint ("Test mode is ON: Meta sends to...").
					if (res.routing_hint && res.routing !== "test_mode_admin") {
						sentMsg = sentMsg + " " + String(res.routing_hint);
					} else if (res.destination && res.routing === "test_mode_admin") {
						sentMsg =
							sentMsg +
							" " +
							__("Delivered to admin/test number {0} (not the Lead phone on this card).", [
								String(res.destination),
							]);
					}
					frappe.show_alert({ message: sentMsg, indicator: "green" });
					const lid = state.selectedName;
					pch.resetComposer($detail);
					loadWhatsAppQuiet(lid);
				} else {
					const hint = res.error_hint || (res.response && res.response.error_hint) || "";
					frappe.show_alert({
						message: hint
							? __("Could not send: {0}", [hint])
							: __("Message could not be sent. Check Error Log."),
						indicator: "red",
					});
					patchWhatsAppThreadDom();
				}
			}

			$btn.prop("disabled", true);

			if (payload.pendingFile) {
				state.waOptimistic.push(pch.buildOptimisticSend(payload));
				patchWhatsAppThreadDom();
				pch.setComposerBusy($detail, true);
				pch
					.uploadFilePublic(payload.pendingFile)
					.then((fileUrlPath) => {
						const fullUrl = pch.toPublicFileUrl(fileUrlPath);
						const mediaPayload = {
							msg_type: payload.pendingKind === "image" ? "image" : "document",
							message: payload.message,
							media_url: fullUrl,
							filename: payload.filename,
							pendingFile: null,
							pendingKind: null,
							pendingPreviewUrl: null,
						};
						frappe.call({
							method: "call_intelligence.api.send_whatsapp_message",
							args: sendArgsFromPayload(mediaPayload),
							callback(r) {
								afterSendResponse(r.message || {});
							},
							error() {
								state.waOptimistic = [];
								frappe.show_alert({
									message: __("Could not send WhatsApp message."),
									indicator: "red",
								});
								patchWhatsAppThreadDom();
							},
							always() {
								pch.setComposerBusy($detail, false);
								$btn.prop("disabled", false);
								pch.syncComposerSendState($detail);
							},
						});
					})
					.catch((e) => {
						state.waOptimistic = [];
						patchWhatsAppThreadDom();
						frappe.show_alert({
							message: String((e && e.message) || e || __("Upload failed")),
							indicator: "red",
						});
						pch.setComposerBusy($detail, false);
						$btn.prop("disabled", false);
						pch.syncComposerSendState($detail);
					});
				return;
			}

			state.waOptimistic.push(pch.buildOptimisticSend(payload));
			patchWhatsAppThreadDom();

			frappe.call({
				method: "call_intelligence.api.send_whatsapp_message",
				args: sendArgsFromPayload(payload),
				callback(r) {
					afterSendResponse(r.message || {});
				},
				error() {
					state.waOptimistic = [];
					frappe.show_alert({ message: __("Could not send WhatsApp message."), indicator: "red" });
					patchWhatsAppThreadDom();
				},
				always() {
					$btn.prop("disabled", false);
					pch.syncComposerSendState($detail);
				},
			});
		});

		$detail.find(".pch-composer-tab").on("click", function () {
			$detail.find(".pch-composer-tab").removeClass("is-on");
			$(this).addClass("is-on");
			pch.syncComposerSendState($detail);
		});
	}

	function priorityRank(v) {
		const s = text(v, "").toLowerCase();
		if (s.includes("urgent") || s.includes("critical") || s.includes("emergency")) return 3;
		if (s.includes("high")) return 3;
		if (s.includes("medium")) return 2;
		if (s.includes("low")) return 1;
		return 0;
	}

	function bestPriorityFromIssue(i) {
		const p = String(i.priority || "").trim();
		const pl = String(i.priority_level || "").trim();
		let best = "";
		[p, pl].forEach((c) => {
			if (c && priorityRank(c) > priorityRank(best)) {
				best = c;
			}
		});
		if (best) {
			return best;
		}
		return pl || p;
	}

	function derivePriorityFromIssues(issues) {
		let best = "";
		(issues || []).forEach((i) => {
			const cand = bestPriorityFromIssue(i);
			if (cand && priorityRank(cand) > priorityRank(best)) {
				best = cand;
			}
		});
		return best;
	}

	function loadDetail(leadName) {
		clearWhatsAppPoll();
		state.loadingDetail = true;
		state.wa = [];
		state.waOptimistic = [];
		renderDetail();
		frappe.call({
			method: "call_intelligence.api.get_patient_360_data",
			args: { lead_name: leadName },
			callback(r) {
				state.detail = r.message || null;
				if (state.detail && state.detail.issues && leadName) {
					const derived = derivePriorityFromIssues(state.detail.issues);
					if (derived) {
						const row = state.list.find((x) => x.name === leadName);
						if (row && (!row.priority || priorityRank(derived) > priorityRank(row.priority))) {
							row.priority = derived;
							renderList();
						}
					}
				}
				state.loadingDetail = false;
				state.wa = Array.isArray(state.detail && state.detail.whatsapp_messages)
					? (state.detail.whatsapp_messages || []).slice(0, 80)
					: [];
				state.loadingWA = false;
				renderDetail();
				if (state.detail && state.detail.lead) {
					const lid = leadName;
					scheduleWhatsAppPoll(lid);
				}
			},
			error() {
				state.detail = null;
				state.loadingDetail = false;
				state.wa = [];
				state.waOptimistic = [];
				renderDetail();
				frappe.show_alert({ message: __("Could not load patient details"), indicator: "red" });
			},
		});
	}

	function loadList() {
		clearWhatsAppPoll();
		state.loadingList = true;
		state.detail = null;
		state.selectedName = "";
		state.waOptimistic = [];
		renderList();
		renderDetail();

		frappe.call({
			method: listMethod(),
			cache: false,
			callback(r) {
				state.list = r.message || [];
				state.loadingList = false;
				renderList();
				if (state.list.length) {
					state.selectedName = state.list[0].name;
					renderList();
					loadDetail(state.selectedName);
				}
			},
			error() {
				state.loadingList = false;
				state.list = [];
				renderList();
				frappe.show_alert({ message: __("Could not load list"), indicator: "red" });
			},
		});
	}

	/**
	 * Fetch latest leads (API: creation desc — new leads at top). Updates state.list.
	 * Selection: never switch away from the lead the user is viewing if that lead still exists.
	 * Auto-select first row + loadDetail only when nothing is selected.
	 */
	function refreshLeadListCore() {
		if (state.loadingList || state.mode !== "lead") {
			return;
		}
		const prev = String(state.selectedName || "").trim();
		frappe.call({
			method: listMethod(),
			cache: false,
			callback(r) {
				const incoming = Array.isArray(r.message) ? r.message : [];
				state.list = incoming;
				const names = new Set(incoming.map((x) => x.name));

				if (prev && names.has(prev)) {
					state.selectedName = prev;
					renderList();
					return;
				}

				if (!prev && incoming.length) {
					state.selectedName = incoming[0].name;
					renderList();
					loadDetail(state.selectedName);
					return;
				}

				if (!prev && !incoming.length) {
					state.selectedName = "";
					state.detail = null;
					renderDetail();
					return;
				}

				if (prev && !names.has(prev)) {
					state.selectedName = "";
					state.detail = null;
					renderList();
					renderDetail();
				}
			},
		});
	}

	const refreshLeadList =
		frappe.utils && typeof frappe.utils.debounce === "function"
			? frappe.utils.debounce(refreshLeadListCore, 450)
			: refreshLeadListCore;

	function clearLeadListPoll() {
		if (state.leadListPollTimer) {
			clearInterval(state.leadListPollTimer);
			state.leadListPollTimer = null;
		}
	}

	function scheduleLeadListPoll() {
		clearLeadListPoll();
		state.leadListPollTimer = setInterval(() => {
			if (state.mode === "lead" && !state.loadingList) {
				refreshLeadList();
			}
		}, 8000);
	}

	function bindEvents() {
		page.body.find(".p360dash-toggle-btn").on("click", function () {
			const nextMode = $(this).data("mode");
			if (!nextMode || nextMode === state.mode) return;
			state.mode = nextMode;
			setToggleUI();
			loadList();
		});
	}

	renderShell();
	setToggleUI();
	bindEvents();
	loadList();
	scheduleLeadListPoll();

	if (frappe.realtime && typeof frappe.realtime.on === "function") {
		frappe.realtime.on("new_lead_created", function () {
			if (state.mode === "lead") {
				refreshLeadList();
			}
		});
		frappe.realtime.on("lead_qualification_updated", function () {
			if (state.mode === "lead") {
				refreshLeadList();
			}
		});
		frappe.realtime.on("lead_whatsapp_updated", function (data) {
			const ln = data && data.lead_name ? String(data.lead_name).trim() : "";
			if (!ln) {
				return;
			}
			// Always refresh the thread for the lead that just received WhatsApp.
			// Some updates can arrive before `state.selectedName` is in sync.
			if (ln !== state.selectedName) {
				state.selectedName = ln;
				renderList();
			}
			loadWhatsAppQuiet(ln);
			loadDetail(ln);
			if (state.mode === "lead") {
				refreshLeadList();
			}
		});
	}

	$(document)
		.off("visibilitychange.p360wa")
		.on("visibilitychange.p360wa", function () {
			if (document.visibilityState !== "visible") return;
			const lid = state.selectedName;
			if (lid) loadWhatsAppQuiet(lid);
		});
	}

	if (window.call_intelligence && window.call_intelligence.patient_chat) {
		runPatient360Dashboard();
	} else {
		frappe.require("/assets/call_intelligence/js/patient_chat.js", runPatient360Dashboard);
	}
};
