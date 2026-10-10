#!/usr/bin/env bash
# Rebuild, redeploy, and verify the SCENT e2e stack.
# Usage: e2e/e2e.sh [skip-build]   (skip-build = redeploy existing zip, skip mvn)
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
# JDK 25 is required. Honour an explicit JAVA_HOME, else discover one, then verify.
if [ -z "${JAVA_HOME:-}" ]; then
  JAVA_HOME="$(/usr/libexec/java_home -v 25 2>/dev/null || true)"
fi
if [ -z "${JAVA_HOME:-}" ] || ! "$JAVA_HOME/bin/javac" -version 2>&1 | grep -q 'javac 25'; then
  echo "error: JDK 25 required; set JAVA_HOME to a JDK 25 install" >&2
  exit 1
fi
export JAVA_HOME

if [ "${1:-}" != "skip-build" ]; then
  echo "== mvn package (includes unit tests)"
  (cd "$ROOT" && mvn -o clean package)
  echo "== refresh e2e/plugin from zip"
  rm -rf plugin && mkdir -p plugin/scent
  unzip -q "$ROOT"/target/scent-trino-plugin-0.1.0-SNAPSHOT.zip -d plugin
  mv plugin/scent-trino-plugin-0.1.0-SNAPSHOT/* plugin/scent/
  rmdir plugin/scent-trino-plugin-0.1.0-SNAPSHOT
fi

echo "== docker build + up"
docker compose build 2>&1 | tail -1
docker compose up -d --force-recreate

echo "== verifying shared cache volume mount"
worker_ids=$(docker compose ps -q trino-worker)
[ -n "$worker_ids" ] || { echo "FAIL: no worker container found"; exit 1; }
shared_cache_source=""
while IFS= read -r worker_id; do
  [ -n "$worker_id" ] || continue
  mount_info=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/tmp/scent-cache"}}{{.Type}} {{.Source}}{{end}}{{end}}' "$worker_id")
  echo "worker $worker_id cache mount: $mount_info"
  [ "${mount_info%% *}" = "bind" ] || { echo "FAIL: /tmp/scent-cache is not a bind mount"; exit 1; }
  cache_source=${mount_info#* }
  if [ -z "$shared_cache_source" ]; then
    shared_cache_source="$cache_source"
  elif [ "$cache_source" != "$shared_cache_source" ]; then
    echo "FAIL: workers are mounted to different cache directories"
    exit 1
  fi
done <<EOF
$worker_ids
EOF
echo "all workers share host cache directory: $shared_cache_source"

echo "== waiting for coordinator"
ready=0
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8080/v1/info 2>/dev/null | grep -q '"starting":false'; then ready=1; break; fi
  sleep 5
done
[ "$ready" = 1 ] || { echo "FAIL: coordinator not ready"; exit 1; }

echo "== ensuring join tables exist in postgres"
docker compose exec -T postgres psql -U test -d test -v ON_ERROR_STOP=1 < postgres-init.sql

echo "== waiting for worker (first query)"
worker=0
for _ in $(seq 1 30); do
  if python3 run-query.py "SELECT count(*) FROM scent.public.metrics" >/dev/null 2>&1; then worker=1; break; fi
  sleep 5
done
[ "$worker" = 1 ] || { echo "FAIL: worker did not come up"; exit 1; }

pass=0; fail=0
check() { # desc expected actual
  if [ "$2" = "$3" ]; then echo "PASS: $1"; pass=$((pass+1)); else echo "FAIL: $1 (want '$2' got '$3')"; fail=$((fail+1)); fi
}
scalar() { python3 run-query.py "$1" 2>/dev/null | sed -n '2p'; }

echo "== verification"
check "passthrough count(*)=300"  300     "$(scalar 'SELECT count(*) FROM scent.public.metrics')"

check "populate us-west partition" 100 "$(scalar "SELECT count(*) FROM scent.public.metrics WHERE region='us-west'")"
west_parquet_files=$(docker compose exec -T trino-worker sh -c "find /tmp/scent-cache -type f -name '*us-west*.parquet' -print | grep -c . || true" | tr -d '\r')
[ "${west_parquet_files:-0}" -ge 1 ] && west_parquet_files=1 || west_parquet_files=0
check "us-west Parquet slice created" 1 "$west_parquet_files"

masked_query="SELECT region,
  day,
  CASE WHEN value IS NULL THEN NULL ELSE 0.0 END AS masked_value
FROM scent.public.metrics
WHERE region = 'us-west'
LIMIT 10"
masked_result=$(
  python3 run-query.py "$masked_query" |
    awk -F '\t' '
      NR == 1 { next }
      substr($0, 1, 3) == "-- " { next }
      {
        rows++
        if ($3 == "" || $3 + 0) invalid++
      }
      END { print (rows == 10 && invalid == 0) ? 1 : 0 }
    '
)
check "mask query returns ten zero values" 1 "$masked_result"

join_query="SELECT m.region,
  m.day,
  r.market,
  r.manager,
  m.value,
  t.target,
  m.value - t.target AS variance
FROM scent.public.metrics AS m
JOIN scent.public.regions AS r ON r.region = m.region
JOIN scent.public.daily_targets AS t ON t.region = m.region AND t.day = m.day
WHERE m.region = 'us-east'
ORDER BY m.day
LIMIT 5"
check "3-table join returns five rows" 5 "$(python3 run-query.py "$join_query" | awk 'NR > 1 && $0 !~ /^-- / {count++} END {print count+0}')"

parquet_files=$(docker compose exec -T trino-worker sh -c "find /tmp/scent-cache -type f -name '*.parquet' -print | grep -c . || true" | tr -d '\r')
[ "${parquet_files:-0}" -ge 1 ] && parquet_files=1 || parquet_files=0
check "join creates Parquet slice" 1 "$parquet_files"

check "populate sum us-east"      7425.0  "$(scalar "SELECT sum(value) FROM scent.public.metrics WHERE region='us-east'")"
pub=$(docker compose logs trino-worker 2>/dev/null | grep -c 'published slice' || true); [ "$pub" -ge 1 ] && pub=1; check "slice published" 1 "$pub"

check "hit: sum us-east again"    7425.0  "$(scalar "SELECT sum(value) FROM scent.public.metrics WHERE region='us-east'")"
hits=$(docker compose logs trino-worker 2>/dev/null | grep -c 'cache hit' || true); [ "$hits" -ge 1 ] && hits=1; check "cache hit logged" 1 "$hits"

check "TopN day limit 3 rows"     3       "$(python3 run-query.py "SELECT day FROM scent.public.metrics WHERE region='us-east' ORDER BY day LIMIT 3" 2>/dev/null | grep -c '^2024-01-0')"
check "0-col count(*)=100"        100     "$(scalar "SELECT count(*) FROM scent.public.metrics WHERE region='us-east'")"

check "no-partition passthrough"  300     "$(scalar 'SELECT count(*) FROM scent.public.metrics')"
pt=$(docker compose logs trino-coordinator 2>/dev/null | grep -c 'getSplits passthrough'); [ "$pt" -ge 1 ] && pt=1; check "passthrough logged" 1 "$pt"

echo "== Parquet files in the shared cache mount"
find ./cache -type f -name '*.parquet' -exec ls -lh {} \;
while IFS= read -r worker_id; do
  [ -n "$worker_id" ] || continue
  echo "worker $worker_id:"
  docker exec "$worker_id" sh -c 'find /tmp/scent-cache -type f -name "*.parquet" -exec ls -lh {} \;'
done <<EOF
$worker_ids
EOF

echo
echo "== $pass passed, $fail failed"
exit $([ "$fail" = 0 ] && echo 0 || echo 1)
