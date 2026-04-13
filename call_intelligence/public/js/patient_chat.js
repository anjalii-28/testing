/**
 * Patient 360 Dashboard — Chatwoot-style thread, lead list, and composer.
 * Loaded via hooks.py `page_js` for `patient-360-dashboard` and via `frappe.require` fallback.
 * Exposes `window.call_intelligence.patient_chat` (required by `patient_360_dashboard.js`).
 */
(function () {
	"use strict";

	function esc(t) {
		return frappe.utils.escape_html(t == null ? "" : String(t));
	}

	function initials(name) {
		const s = String(name || "").trim();
		if (!s) return "?";
		const parts = s.split(/\s+/).filter(Boolean);
		if (parts.length >= 2) {
			return (parts[0][0] + parts[1][0]).toUpperCase();
		}
		return s.slice(0, 2).toUpperCase();
	}

	function msgText(m) {
		if (!m) return "";
		return String(m.content || m.text || m.message || "").trim();
	}

	function isOutgoing(m) {
		const d = String(m.direction || m.type || "").toLowerCase();
		if (d === "outgoing" || d === "out") return true;
		if (d === "incoming" || d === "in") return false;
		return !!m._optimistic;
	}

	function formatWhen(raw) {
		if (!raw) return "";
		try {
			const d = frappe.datetime.str_to_obj ? frappe.datetime.str_to_obj(raw) : new Date(raw);
			if (isNaN(d.getTime())) return String(raw).slice(0, 16);
			return frappe.datetime.str_to_user
				? frappe.datetime.str_to_user(raw)
				: d.toLocaleString();
		} catch (e) {
			return String(raw).slice(0, 19);
		}
	}

	function renderConversationList(list, selectedName, mode) {
		const sk = mode === "ticket" ? "ticket_notes" : "lead_notes";
		const parts = [];
		for (let i = 0; i < list.length; i++) {
			const row = list[i] || {};
			const name = row.name || "";
			const label = row.lead_name || name || __("Unknown");
			const phone = row.phone || "";
			const snippet = row[sk] || row.lead_notes || row.ticket_notes || "";
			const active = name && name === selectedName ? " is-active" : "";
			const av = initials(label);
			const t = formatWhen(row.modified);
			parts.push(
				`<div class="pch-conv-item${active}" data-name="${esc(name)}">
					<div class="pch-conv-avatar">${esc(av)}</div>
					<div class="pch-conv-body">
						<div class="pch-conv-topline">
							<span class="pch-conv-name">${esc(label)}</span>
							<span class="pch-conv-time">${esc(t)}</span>
						</div>
						<div class="pch-conv-phone">${esc(phone || "—")}</div>
						<div class="pch-conv-preview">${esc(snippet || "—")}</div>
					</div>
				</div>`
			);
		}
		return parts.join("");
	}

	function renderMessageBubble(m, patientName) {
		const out = isOutgoing(m);
		const text = msgText(m);
		const when = formatWhen(m.creation || m.timestamp);
		const rowClass = out ? "pch-msg-row pch-msg-row--out" : "pch-msg-row pch-msg-row--in";
		const bubbleClass = out ? "pch-msg-bubble pch-msg-bubble--out" : "pch-msg-bubble pch-msg-bubble--in";
		const avClass = out ? "pch-msg-avatar pch-msg-avatar--out" : "pch-msg-avatar pch-msg-avatar--in";
		const avLetter = out ? initials(frappe.session.user_fullname || "Me") : initials(patientName);
		let inner = `<div class="pch-msg-text">${esc(text) || " "}</div>`;
		const mt = String(m.msg_type || "").toLowerCase();
		const media = m.media_url || m.file_url || "";
		if (media && mt === "image") {
			inner += `<div class="pch-msg-text" style="margin-top:8px"><img src="${esc(
				media
			)}" alt="" style="max-width:100%;border-radius:8px"/></div>`;
		} else if (media && (mt === "document" || mt === "file")) {
			inner += `<div class="pch-msg-meta"><a href="${esc(media)}" target="_blank" rel="noopener">${esc(
				__("Attachment")
			)}</a></div>`;
		}
		if (m.mapping_unknown) {
			return `<div class="pch-msg-row pch-msg-row--system">
				<div class="pch-msg-bubble pch-msg-bubble--system">${esc(
					text || __("Message could not be linked to this lead.")
				)}</div>
			</div>`;
		}
		const bubbleExtra = m._optimistic ? " pch-msg-bubble--sending" : "";
		const meta =
			when || m._optimistic
				? `<div class="pch-msg-meta"><span>${esc(when)}</span>${
						m._optimistic
							? `<span class="pch-msg-status--sent">${esc(__("Sending…"))}</span>`
							: ""
				  }</div>`
				: "";
		return `<div class="${rowClass}">
			<div class="${avClass}">${esc(avLetter)}</div>
			<div class="pch-msg-stack">
				<div class="${bubbleClass}${bubbleExtra}">${inner}${meta}</div>
			</div>
		</div>`;
	}

	function renderThread(messages, opts) {
		const patientName = (opts && opts.patientName) || __("Patient");
		const optimistic = (opts && opts.optimistic) || [];
		const rows = []
			.concat(Array.isArray(messages) ? messages : [])
			.concat(Array.isArray(optimistic) ? optimistic : []);
		if (!rows.length) {
			return `<div class="pch-thread-empty">${esc(__("No WhatsApp messages yet."))}</div>`;
		}
		const html = [];
		for (let i = 0; i < rows.length; i++) {
			html.push(renderMessageBubble(rows[i], patientName));
		}
		return html.join("");
	}

	function scrollThreadToBottom($thread, smooth) {
		const el = $thread && $thread.length ? $thread[0] : null;
		if (!el) return;
		const go = function () {
			el.scrollTop = el.scrollHeight;
		};
		if (smooth) {
			window.requestAnimationFrame(go);
		} else {
			go();
		}
	}

	function renderComposerInnerHtml() {
		return `
			<div class="pch-composer-inner">
				<div class="pch-composer-tabs">
					<button type="button" class="pch-composer-tab is-on" data-tab="reply">${esc(__("Reply"))}</button>
					<button type="button" class="pch-composer-tab" data-tab="note">${esc(__("Private note"))}</button>
				</div>
				<div class="pch-composer-file-preview" style="display:none"></div>
				<div class="pch-composer-input-row">
					<button type="button" class="btn btn-xs btn-default pch-composer-attach" title="${esc(
						__("Attach file")
					)}">📎</button>
					<input type="file" class="pch-composer-file-input" accept="image/*,application/pdf,.pdf,.doc,.docx" style="display:none" />
					<div class="pch-composer-input-wrap">
						<textarea class="pch-composer-input" rows="2" placeholder="${esc(
							__("Type a message…")
						)}"></textarea>
					</div>
					<div class="pch-composer-actions">
						<button type="button" class="btn btn-primary btn-sm pch-composer-send">${esc(__("Send"))}</button>
					</div>
				</div>
			</div>`;
	}

	function clearPendingFile($detail) {
		const prevUrl = $detail.data("pch-pending-preview-url");
		if (prevUrl && String(prevUrl).startsWith("blob:")) {
			try {
				URL.revokeObjectURL(prevUrl);
			} catch (e) {
				/* ignore */
			}
		}
		$detail.removeData("pch-pending-file");
		$detail.removeData("pch-pending-kind");
		$detail.removeData("pch-pending-filename");
		$detail.removeData("pch-pending-preview-url");
		$detail.find(".pch-composer-file-input").val("");
		$detail.find(".pch-composer-file-preview").empty().hide();
	}

	function setupPatientComposer($detail) {
		const $inner = $detail.find(".pch-composer-inner");
		const $file = $detail.find(".pch-composer-file-input");
		$detail.find(".pch-composer-attach").off("click.pch").on("click.pch", function (e) {
			e.preventDefault();
			$file.trigger("click");
		});
		$file.off("change.pch").on("change.pch", function () {
			const f = this.files && this.files[0];
			clearPendingFile($detail);
			if (!f) {
				patient_chat.syncComposerSendState($detail);
				return;
			}
			$detail.data("pch-pending-file", f);
			const isImg = f.type && f.type.indexOf("image/") === 0;
			$detail.data("pch-pending-kind", isImg ? "image" : "document");
			$detail.data("pch-pending-filename", f.name);
			const $pv = $detail.find(".pch-composer-file-preview");
			if (isImg) {
				const url = URL.createObjectURL(f);
				$detail.data("pch-pending-preview-url", url);
				$pv.html(
					`<div class="pch-composer-file-preview-inner">
						<img class="pch-composer-file-thumb" src="${esc(url)}" alt=""/>
						<span class="pch-composer-file-name">${esc(f.name)}</span>
					</div>`
				);
			} else {
				$pv.html(
					`<div class="pch-composer-file-preview-inner">
						<span class="pch-composer-file-name">${esc(f.name)}</span>
					</div>`
				);
			}
			$pv.show();
			patient_chat.syncComposerSendState($detail);
		});
		$detail.find(".pch-composer-input").off("input.pch").on("input.pch", function () {
			patient_chat.syncComposerSendState($detail);
		});
	}

	function composerValidationError($detail) {
		const tab = String($detail.find(".pch-composer-tab.is-on").data("tab") || "reply");
		if (tab === "note") {
			return null;
		}
		const msg = String($detail.find(".pch-composer-input").val() || "").trim();
		const pending = $detail.data("pch-pending-file");
		if (!msg && !pending) {
			return __("Enter a message or attach a file.");
		}
		return null;
	}

	function readComposerPayload($detail) {
		const msg = String($detail.find(".pch-composer-input").val() || "").trim();
		const pendingFile = $detail.data("pch-pending-file") || null;
		const pendingKind = $detail.data("pch-pending-kind") || null;
		const pendingPreviewUrl = $detail.data("pch-pending-preview-url") || null;
		const filename = $detail.data("pch-pending-filename") || "";
		let msg_type = "text";
		if (pendingFile) {
			msg_type = pendingKind === "image" ? "image" : "document";
		}
		return {
			message: msg,
			msg_type: msg_type,
			pendingFile: pendingFile,
			pendingKind: pendingKind,
			pendingPreviewUrl: pendingPreviewUrl,
			filename: filename,
		};
	}

	function resetComposer($detail) {
		$detail.find(".pch-composer-input").val("");
		clearPendingFile($detail);
		patient_chat.syncComposerSendState($detail);
	}

	function buildOptimisticSend(payload) {
		const msg = payload.message || "";
		return {
			direction: "outgoing",
			content: msg,
			creation: frappe.datetime.get_datetime_as_string
				? frappe.datetime.get_datetime_as_string()
				: new Date().toISOString(),
			_optimistic: true,
			msg_type: payload.msg_type || "text",
			media_url: payload.pendingPreviewUrl || "",
		};
	}

	function setComposerBusy($detail, busy) {
		const $inner = $detail.find(".pch-composer-inner");
		$inner.toggleClass("pch-composer-inner--busy", !!busy);
	}

	function syncComposerSendState($detail) {
		const tab = String($detail.find(".pch-composer-tab.is-on").data("tab") || "reply");
		const msg = String($detail.find(".pch-composer-input").val() || "").trim();
		const pending = $detail.data("pch-pending-file");
		const ok = tab === "reply" && (!!msg.trim() || !!pending);
		$detail.find(".pch-composer-send").prop("disabled", !ok);
	}

	function toPublicFileUrl(path) {
		if (!path) return "";
		const p = String(path);
		if (p.indexOf("http://") === 0 || p.indexOf("https://") === 0) {
			return p;
		}
		const prefix = p.indexOf("/") === 0 ? p : "/" + p;
		return window.location.origin + prefix;
	}

	function uploadFilePublic(file) {
		return new Promise(function (resolve, reject) {
			if (!file) {
				reject(new Error(__("No file")));
				return;
			}
			const xhr = new XMLHttpRequest();
			const fd = new FormData();
			fd.append("file", file, file.name);
			fd.append("is_private", "0");
			fd.append("folder", "Home");
			xhr.open("POST", "/api/method/upload_file", true);
			xhr.setRequestHeader("Accept", "application/json");
			xhr.setRequestHeader("X-Frappe-CSRF-Token", frappe.csrf_token || "");
			xhr.onload = function () {
				if (xhr.status !== 200) {
					reject(new Error(__("Upload failed")));
					return;
				}
				try {
					const r = JSON.parse(xhr.responseText);
					const doc = r.message;
					const url = doc && (doc.file_url || doc.file_name);
					if (!url) {
						reject(new Error(__("Upload failed")));
						return;
					}
					resolve(url);
				} catch (e) {
					reject(e);
				}
			};
			xhr.onerror = function () {
				reject(new Error(__("Upload failed")));
			};
			xhr.send(fd);
		});
	}

	const patient_chat = {
		initials: initials,
		renderConversationList: renderConversationList,
		renderThread: renderThread,
		scrollThreadToBottom: scrollThreadToBottom,
		renderComposerInnerHtml: renderComposerInnerHtml,
		setupPatientComposer: setupPatientComposer,
		composerValidationError: composerValidationError,
		readComposerPayload: readComposerPayload,
		resetComposer: resetComposer,
		buildOptimisticSend: buildOptimisticSend,
		setComposerBusy: setComposerBusy,
		syncComposerSendState: syncComposerSendState,
		toPublicFileUrl: toPublicFileUrl,
		uploadFilePublic: uploadFilePublic,
	};

	window.call_intelligence = window.call_intelligence || {};
	window.call_intelligence.patient_chat = patient_chat;

	// Legacy name used in comments / older code paths
	window.PatientChat = window.PatientChat || {};
	window.PatientChat.renderThread = function (container, messages) {
		if (container) {
			container.innerHTML = renderThread(messages, {});
		}
	};
})();
