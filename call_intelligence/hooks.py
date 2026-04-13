"""
Frappe hooks entrypoint.

Canonical hook definitions live in `call_intelligence/hooks.py` (inner Python
package). This top-level `hooks.py` re-exports them for tooling that imports
`hooks` from the app root; Frappe loads `call_intelligence.hooks` from the same
inner module.
"""

from call_intelligence.hooks import *  # noqa: F403
