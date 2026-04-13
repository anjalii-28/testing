# Call Intelligence (Frappe custom app) + Docker (ERPNext v15)

This repository contains **only** the **Call Intelligence** Frappe app and a **self-contained Docker Compose** stack pinned to **`frappe/erpnext:version-15`** (not v16).

- **No** bundled database dump, **no** full bench, **no** `sites/` in git.
- Desk UI (**Workspace**, **Dashboard**, charts, number cards) and **Custom Field** / **Client Script** rows ship as **JSON fixtures** under `call_intelligence/call_intelligence/fixtures/` and load on `bench migrate`.

## Prerequisites

- Docker Desktop (or Docker Engine) + Docker Compose v2
- ~4 GB RAM free for containers  
- **Apple Silicon:** Compose sets `platform: linux/amd64` for compatibility.

## One-time setup

```bash
git clone https://github.com/anjalii-28/testing.git
cd testing/docker
docker compose up -d
```

Wait until all services are healthy (first run can take **several minutes**). The **`create-site`** service creates the Frappe site named **`frontend`**, installs **ERPNext** and **call_intelligence**, runs **migrate**, and builds app assets.

Then open:

- **http://localhost:8080**

Login:

| Field    | Value          |
|----------|----------------|
| User     | `Administrator` |
| Password | `admin`         |

Site name in the browser is **`frontend`** (set in nginx as `FRAPPE_SITE_NAME_HEADER`).

### If you need the shell (optional)

```bash
cd docker
docker compose exec backend bash
# inside container, bench root is /home/frappe/frappe-bench
bench --site frontend list-apps
```

You normally **do not** need `bench get-app` — the app is **bind-mounted** from `../call_intelligence`. Re-run migrate after pulling app changes:

```bash
docker compose exec backend bench --site frontend migrate
docker compose exec backend bench build --app call_intelligence
```

## Version pin (why v15)

The Compose file uses **`frappe/erpnext:version-15`** only. Pulling `latest` or v16 images can break this app; do not change the image tag unless you port and test the app.

## Troubleshooting

| Issue | What to try |
|-------|-------------|
| Blank page / 502 | Wait 2–5 min; `docker compose logs create-site` then `docker compose logs backend`. |
| Port in use | Change `8080:8080` in `docker/docker-compose.yml` to another host port. |
| Reset everything | `docker compose down -v` (deletes DB and sites volumes), then `docker compose up -d` again. |

## Repo layout

| Path | Purpose |
|------|---------|
| `call_intelligence/` | Installable Frappe app (`setup.py`, package `call_intelligence/`, `fixtures/`, etc.) |
| `docker/docker-compose.yml` | MariaDB, Redis, backend, workers, scheduler, websocket, nginx **frontend**, **v15** images |
| `docker/bootstrap.sh` | Creates site `frontend`, installs apps, migrate, build |

## License

See `call_intelligence/license.txt`.
