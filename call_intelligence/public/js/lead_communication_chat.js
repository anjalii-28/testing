/**
 * Lead form — WhatsApp-style chat for Communication rows (reference Lead).
 * Fetches last 50 messages (creation desc → reversed to ASC), scrollable thread, chat header.
 */
(function () {
	"use strict";

	const PAGE_SIZE = 50;

	frappe.provide("call_intelligence.lead_comm_chat");

	function esc(t) {
		return frappe.utils.escape_html(t == null ? "" : String(t));
	}

	function strip_html(html) {
		if (!html) return "";
		const d = document.createElement("div");
		d.innerHTML = String(html);
		return (d.textContent || d.innerText || "").trim();
	}

	function msg_body(row) {
		const raw = row.content || row.subject || "";
		return strip_html(raw) || "—";
	}

	function format_ts(raw) {
		if (!raw) return "";
		try {
			return frappe.datetime.str_to_user ? frappe.datetime.str_to_user(raw) : String(raw).slice(0, 19);
		} catch (e) {
			return String(raw).slice(0, 19);
		}
	}

	function lead_display_name(frm) {
		const n = frm.doc.lead_name || frm.doc.name;
		return (n && String(n).trim()) || frm.doc.name || "";
	}

	function lead_phone_display(frm) {
		const p =
			frm.doc.mobile_no ||
			frm.doc.phone ||
			frm.doc.phone_number ||
			(frm.doc.whatsapp_no !== undefined ? frm.doc.whatsapp_no : "");
		const s = p != null ? String(p).trim() : "";
		return s || "—";
	}

	function ai_label(row) {
		const sub = String(row.subject || "").toLowerCase();
		const ctype = String(row.communication_type || "").toLowerCase();
		if (sub.includes("ai") || sub.includes("generated") || ctype.includes("bot")) {
			return __("AI");
		}
		return "";
	}

	/** Sent = outgoing (us → patient), Received = incoming (patient → us) */
	function bubble_html(row) {
		const sor = String(row.sent_or_received || "");
		const is_out = sor === "Sent";
		const rowCls = is_out ? "lcc-wa-row lcc-wa-row--out" : "lcc-wa-row lcc-wa-row--in";
		const al = ai_label(row);
		const badge = al ? `<div class="lcc-wa-badges">${esc(al)}</div>` : "";
		return `
			<div class="${rowCls}" data-name="${esc(row.name)}">
				<div class="lcc-wa-bubble">
					${badge}
					<div class="lcc-wa-text">${esc(msg_body(row))}</div>
					<div class="lcc-wa-meta">
						<span class="lcc-wa-time">${esc(format_ts(row.creation))}</span>
					</div>
				</div>
			</div>
		`;
	}

	function scroll_to_bottom($stream, smooth) {
		const el = $stream && $stream.length ? $stream[0] : null;
		if (!el) return;
		const top = el.scrollHeight;
		if (smooth && typeof el.scrollTo === "function") {
			try {
				el.scrollTo({ top: top, behavior: "smooth" });
				return;
			} catch (e) {
				/* ignore */
			}
		}
		el.scrollTop = top;
	}

	function update_chat_header(frm) {
		if (!frm._lcc_$panel || !frm._lcc_$panel.length) return;
		frm._lcc_$panel.find(".lcc-wa-header-name").text(lead_display_name(frm));
		frm._lcc_$panel.find(".lcc-wa-header-phone").text(lead_phone_display(frm));
	}

	function clear_timers(frm) {
		if (frm._lcc_interval) {
			clearInterval(frm._lcc_interval);
			frm._lcc_interval = null;
		}
		if (frm._lcc_rt_handler) {
			frappe.realtime.off("doc_update", frm._lcc_rt_handler);
			frm._lcc_rt_handler = null;
		}
	}

	function fetch_batch(frm, opts) {
		const args = {
			doctype: "Communication",
			filters: [
				["reference_doctype", "=", "Lead"],
				["reference_name", "=", frm.doc.name],
			],
			fields: [
				"name",
				"creation",
				"subject",
				"content",
				"sent_or_received",
				"communication_medium",
				"communication_type",
				"read_receipt",
			],
			order_by: "creation desc",
			limit_page_length: PAGE_SIZE,
		};
		if (opts && opts.before_creation) {
			args.filters.push(["creation", "<", opts.before_creation]);
		}
		return frappe.call({ method: "frappe.client.get_list", args: args });
	}

	function render_messages(frm, rows, prepend, smoothScroll) {
		const $stream = frm._lcc_$stream;
		if (!$stream) return;
		if (!rows.length && !prepend) return;
		const html = rows.map(bubble_html).join("");
		if (prepend) {
			const h = $stream[0].scrollHeight;
			$stream.prepend(html);
			$stream[0].scrollTop = $stream[0].scrollHeight - h;
		} else {
			$stream.html(html);
			requestAnimationFrame(function () {
				scroll_to_bottom($stream, !!smoothScroll);
			});
		}
	}

	function load(frm, options) {
		if (!frm._lcc_$stream || frm.is_new()) return;
		update_chat_header(frm);
		const silent = options && options.silent;
		if (!silent) {
			frm._lcc_$stream.html(`<div class="lcc-wa-muted">${__("Loading…")}</div>`);
		}
		fetch_batch(frm, {})
			.then((r) => {
				/* Last PAGE_SIZE messages, chronological ASC */
				const rows = (r.message || []).slice().reverse();
				frm._lcc_rows = rows;
				frm._lcc_oldest_ts =
					rows.length && rows[0].creation ? rows[0].creation : null;
				frm._lcc_has_more = (r.message || []).length >= PAGE_SIZE;
				if (!rows.length) {
					frm._lcc_$stream.html(
						`<div class="lcc-wa-muted">${__("No messages yet.")}</div>`
					);
					frm._lcc_$loadOlder.prop("disabled", true);
					return;
				}
				render_messages(frm, rows, false, silent);
				frm._lcc_$loadOlder.prop("disabled", !frm._lcc_has_more);
			})
			.catch(() => {
				if (frm._lcc_$stream) {
					frm._lcc_$stream.html(
						`<div class="lcc-wa-muted">${__("Could not load messages.")}</div>`
					);
				}
			});
	}

	function load_older(frm) {
		if (!frm._lcc_oldest_ts || !frm._lcc_$stream) return;
		fetch_batch(frm, { before_creation: frm._lcc_oldest_ts }).then((r) => {
			const batch = (r.message || []).slice().reverse();
			if (!batch.length) {
				frm._lcc_has_more = false;
				frm._lcc_$loadOlder.prop("disabled", true);
				frappe.show_alert({ message: __("No older messages."), indicator: "blue" });
				return;
			}
			frm._lcc_oldest_ts = batch[0].creation;
			frm._lcc_has_more = (r.message || []).length >= PAGE_SIZE;
			frm._lcc_$loadOlder.prop("disabled", !frm._lcc_has_more);
			render_messages(frm, batch, true);
		});
	}

	function bind_realtime(frm) {
		if (frm._lcc_rt_handler) {
			frappe.realtime.off("doc_update", frm._lcc_rt_handler);
		}
		frm._lcc_rt_handler = function (data) {
			if (!data || data.doctype !== "Communication" || !data.name) return;
			frappe.call({
				method: "frappe.client.get_value",
				args: {
					doctype: "Communication",
					filters: { name: data.name },
					fieldname: ["reference_doctype", "reference_name"],
				},
				callback(r) {
					const v = r.message;
					if (
						v &&
						v.reference_doctype === "Lead" &&
						v.reference_name === frm.doc.name
					) {
						load(frm, { silent: true });
					}
				},
			});
		};
		frappe.realtime.on("doc_update", frm._lcc_rt_handler);
	}

	function bind_poll(frm) {
		if (frm._lcc_interval) {
			clearInterval(frm._lcc_interval);
		}
		frm._lcc_interval = setInterval(function () {
			if (frappe.get_route_str && frappe.get_route_str() !== `Form/Lead/${frm.doc.name}`) {
				return;
			}
			load(frm, { silent: true });
		}, 45000);
	}

	function mount(frm) {
		if (frm._lcc_mounted) return;
		const title = esc(lead_display_name(frm));
		const phone = esc(lead_phone_display(frm));
		const $wrap = $(`
			<div class="lcc-wa-panel">
				<div class="lcc-wa-header">
					<div class="lcc-wa-header-main">
						<div class="lcc-wa-header-name">${title}</div>
						<div class="lcc-wa-header-phone">${phone}</div>
					</div>
					<button type="button" class="btn btn-xs btn-default lcc-wa-refresh">${esc(__("Refresh"))}</button>
				</div>
				<div class="lcc-wa-toolbar">
					<button type="button" class="btn btn-xs btn-default lcc-wa-load-older" disabled>${esc(
						__("Load older")
					)}</button>
				</div>
				<div class="lcc-wa-stream"></div>
			</div>
		`);
		const $layout = frm.wrapper.find(".form-layout").first();
		if ($layout.length) {
			$layout.after($wrap);
		} else {
			frm.wrapper.append($wrap);
		}
		frm._lcc_mounted = true;
		frm._lcc_$panel = $wrap;
		frm._lcc_$stream = $wrap.find(".lcc-wa-stream");
		frm._lcc_$loadOlder = $wrap.find(".lcc-wa-load-older");
		$wrap.find(".lcc-wa-refresh").on("click", function () {
			load(frm);
		});
		frm._lcc_$loadOlder.on("click", function () {
			load_older(frm);
		});
	}

	function teardown(frm) {
		clear_timers(frm);
		frm.wrapper.find(".lcc-wa-panel").remove();
		frm._lcc_mounted = false;
		frm._lcc_$panel = null;
		frm._lcc_$stream = null;
		frm._lcc_$loadOlder = null;
		frm._lcc_lead_name = null;
	}

	function open_whatsapp_dialog(frm) {
		if (frm.is_new()) {
			frappe.msgprint(__("Save the Lead first."));
			return;
		}
		const title = esc(lead_display_name(frm));
		const phone = esc(lead_phone_display(frm));
		const d = new frappe.ui.Dialog({
			title: __("Chat"),
			size: "large",
			fields: [{ fieldtype: "HTML", fieldname: "h" }],
		});
		const $h = d.fields_dict.h.$wrapper;
		$h.empty();
		$h.append(`
			<div class="lcc-wa-panel">
				<div class="lcc-wa-header">
					<div class="lcc-wa-header-main">
						<div class="lcc-wa-header-name">${title}</div>
						<div class="lcc-wa-header-phone">${phone}</div>
					</div>
				</div>
				<div class="lcc-wa-stream lcc-wa-stream--dialog"></div>
			</div>
		`);
		const $sd = $h.find(".lcc-wa-stream--dialog");
		frappe.call({
			method: "frappe.client.get_list",
			args: {
				doctype: "Communication",
				filters: [
					["reference_doctype", "=", "Lead"],
					["reference_name", "=", frm.doc.name],
				],
				fields: [
					"name",
					"creation",
					"subject",
					"content",
					"sent_or_received",
					"communication_medium",
					"communication_type",
					"read_receipt",
				],
				order_by: "creation desc",
				limit_page_length: PAGE_SIZE,
			},
			callback(r) {
				const rows = (r.message || []).slice().reverse();
				$sd.html(
					rows.map(bubble_html).join("") || `<div class="lcc-wa-muted">${__("No messages.")}</div>`
				);
				requestAnimationFrame(function () {
					scroll_to_bottom($sd, false);
				});
			},
		});
		d.show();
	}

	call_intelligence.lead_comm_chat.setup = function (frm) {
		if (frm.is_new()) {
			teardown(frm);
			return;
		}
		const switched =
			frm._lcc_lead_name != null && frm._lcc_lead_name !== frm.doc.name;
		if (switched) {
			teardown(frm);
		}
		frm._lcc_lead_name = frm.doc.name;
		mount(frm);
		load(frm);
		bind_realtime(frm);
		bind_poll(frm);
	};

	frappe.ui.form.on("Lead", {
		refresh(frm) {
			call_intelligence.lead_comm_chat.setup(frm);
			if (!frm._lcc_whatsapp_dialog_btn) {
				frm._lcc_whatsapp_dialog_btn = true;
				frm.add_custom_button(
					__("Open chat"),
					function () {
						open_whatsapp_dialog(frm);
					},
					__("View")
				);
			}
		},
	});
})();
