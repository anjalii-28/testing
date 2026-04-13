# Call Intelligence

Frappe / ERPNext custom app (v15). **Docker and full setup:** see the [repository root `README.md`](../README.md).

- **Fixtures** live in `call_intelligence/fixtures/` (imported on `bench migrate`).
- **ERPNext** is required (`required_apps = ["erpnext"]`).

```bash
bench get-app /path/to/call_intelligence
bench --site <site> install-app call_intelligence
bench --site <site> migrate
bench build --app call_intelligence
```
