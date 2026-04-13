# Call Intelligence Frappe App

This is a custom Frappe app. It does **not** include Frappe or ERPNext.

## Prerequisites

- Frappe / ERPNext installed (**version 15**)

## Installation

```bash
bench get-app https://github.com/anjalii-28/testing
bench --site <site_name> install-app call_intelligence
bench migrate
bench build --app call_intelligence
```

## Notes

- UI (workspace, dashboards, custom fields, client scripts) is provided via **fixtures** in `call_intelligence/fixtures/`.
- **No database dump** is included or required for the app package.
- After changing Desk fixtures, re-export with:  
  `bench --site <site_name> export-fixtures --app call_intelligence`
