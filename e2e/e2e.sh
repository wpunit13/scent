#!/usr/bin/env bash
# Rebuild, redeploy, and verify the SCENT e2e stack.
# Usage: e2e/e2e.sh [skip-build]   (skip-build = redeploy existing zip, skip mvn)
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
export JAVA_HOME="${JAVA_HOME:-/Library/Java/JavaVirtualMachines/temurin-25.jdk/Contents/Home}"

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

echo "== waiting for coordinator"
ready=0
for _ in $(seq 1 60); do
  if curl -sf http://localhost:8080/v1/info 2>/dev/null | grep -q '"starting":false'; then ready=1; break; fi
  sleep 5
done
[ "$ready" = 1 ] || { echo "FAIL: coordinator not ready"; exit 1; }

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
check "populate sum us-east"      7425.0  "$(scalar "SELECT sum(value) FROM scent.public.metrics WHERE region='us-east'")"
pub=$(docker compose logs trino-worker 2>/dev/null | grep -c 'published slice'); check "1 slice published" 1 "$pub"

check "hit: sum us-east again"    7425.0  "$(scalar "SELECT sum(value) FROM scent.public.metrics WHERE region='us-east'")"
hits=$(docker compose logs trino-worker 2>/dev/null | grep -c 'cache hit'); check "cache hit logged" 1 "$hits"

check "TopN day limit 3 rows"     3       "$(python3 run-query.py "SELECT day FROM scent.public.metrics WHERE region='us-east' ORDER BY day LIMIT 3" 2>/dev/null | grep -c '^2024-01-0')"
check "0-col count(*)=100"        100     "$(scalar "SELECT count(*) FROM scent.public.metrics WHERE region='us-east'")"

check "no-partition passthrough"  300     "$(scalar 'SELECT count(*) FROM scent.public.metrics')"
pt=$(docker compose logs trino-coordinator 2>/dev/null | grep -c 'getSplits passthrough'); [ "$pt" -ge 1 ] && pt=1; check "passthrough logged" 1 "$pt"

echo
echo "== $pass passed, $fail failed"
exit $([ "$fail" = 0 ] && echo 0 || echo 1)