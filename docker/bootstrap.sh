#!/usr/bin/env bash
# One-shot: create site "frontend", install call_intelligence, migrate, build assets.
set -euo pipefail
cd /home/frappe/frappe-bench

wait-for-it -t 120 db:3306
wait-for-it -t 120 redis-cache:6379
wait-for-it -t 120 redis-queue:6379

start=$(date +%s)
until [[ -n $(grep -hs ^ sites/common_site_config.json | jq -r ".db_host // empty") ]] \
  && [[ -n $(grep -hs ^ sites/common_site_config.json | jq -r ".redis_cache // empty") ]] \
  && [[ -n $(grep -hs ^ sites/common_site_config.json | jq -r ".redis_queue // empty") ]]; do
  echo "Waiting for sites/common_site_config.json ..."
  sleep 5
  if (( $(date +%s) - start > 180 )); then
    echo "Timeout waiting for bench configurator."
    exit 1
  fi
done

if [[ -d sites/frontend ]]; then
  echo "Site 'frontend' already exists — ensuring app is installed and migrated."
  bench --site frontend list-apps 2>/dev/null | grep -q call_intelligence \
    || bench --site frontend install-app call_intelligence
  bench --site frontend migrate
  bench build --app call_intelligence || true
  exit 0
fi

bench new-site frontend \
  --mariadb-user-host-login-scope='%' \
  --admin-password=admin \
  --db-root-username=root \
  --db-root-password=admin \
  --install-app erpnext \
  --set-default

bench --site frontend install-app call_intelligence
bench --site frontend migrate
bench build --app call_intelligence

echo "Bootstrap complete — open http://localhost:8080 (site: frontend, user: Administrator, password: admin)"
