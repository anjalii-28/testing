"""Call Intelligence Frappe app — top-level package."""

import os

__version__ = "1.0.0"

# App code lives under `call_intelligence/call_intelligence/` while Frappe expects
# `import call_intelligence` (app root = this directory). Merge the nested package
# path so submodules like `call_intelligence.webhooks` resolve without moving files.
_root_dir = os.path.dirname(os.path.abspath(__file__))
_nested_dir = os.path.join(_root_dir, "call_intelligence")
__path__ = [_root_dir]
if os.path.isdir(_nested_dir) and os.path.isfile(os.path.join(_nested_dir, "__init__.py")):
    __path__.append(_nested_dir)
