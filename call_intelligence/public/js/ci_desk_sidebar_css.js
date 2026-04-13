// Loads after desk bundles so sidebar rules win over desk.bundle.css (active-sidebar strip).
(function () {
	function inject() {
		var id = "ci-desk-sidebar-override-css";
		if (document.getElementById(id)) {
			return;
		}
		var v =
			typeof window._version_number !== "undefined"
				? window._version_number
				: String(Date.now());
		var l = document.createElement("link");
		l.id = id;
		l.rel = "stylesheet";
		l.href =
			"/assets/call_intelligence/css/desk_sidebar_override.css?v=" + encodeURIComponent(v);
		document.head.appendChild(l);
	}
	if (document.readyState === "loading") {
		document.addEventListener("DOMContentLoaded", inject);
	} else {
		inject();
	}
})();
