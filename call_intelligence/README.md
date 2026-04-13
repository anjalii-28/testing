# Call Intelligence (Frappe app)

Desk UI (Workspace, Workspace Sidebar, Dashboard, Dashboard Chart, Number Card) and Custom Field / Client Script definitions are shipped as **fixtures** and load on `bench migrate`. A minimal demo Lead is created by `setup/demo_data.py` on first successful migrate when a Company exists.

## Install on a bench

Requires **ERPNext** (Leads, Issues, Communication).

```bash
# From bench root, with your site created and erpnext installed:
bench get-app /path/to/this/call_intelligence/repo
bench --site <site> install-app call_intelligence
bench --site <site> migrate
bench build
```

Re-export fixtures after Desk changes:

```bash
bench --site <site> export-fixtures --app call_intelligence
```

## frappe_docker

Use the official [frappe_docker](https://github.com/frappe/frappe_docker) stack. Mount or copy this folder to `apps/call_intelligence`, then run `install-app` and `migrate` inside the backend container as usual. No database snapshot is required.
