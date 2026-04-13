// Frappe's TypeSectionBreak.make() returns immediately when nested_items is empty, so flat
// "MAIN" / "CRM" / "SYSTEM" rows from Workspace Sidebar never render. Delegate to TypeLink for those.
(function () {
	function patch() {
		var SB = frappe.ui.sidebar_item && frappe.ui.sidebar_item.TypeSectionBreak;
		if (!SB || !SB.prototype || SB.prototype.__ciFlatSectionPatched) {
			return;
		}
		var orig = SB.prototype.make;
		SB.prototype.make = function () {
			var nested = this.item.nested_items || [];
			if (nested.length === 0) {
				frappe.ui.sidebar_item.TypeLink.prototype.make.call(this);
				return;
			}
			return orig.call(this);
		};
		SB.prototype.__ciFlatSectionPatched = true;
	}

	if (
		typeof frappe !== "undefined" &&
		frappe.ui &&
		frappe.ui.sidebar_item &&
		frappe.ui.sidebar_item.TypeSectionBreak
	) {
		patch();
	}
})();
