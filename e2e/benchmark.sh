#!/usr/bin/env bash
# Report cache hit rate and source-read count for a repeated query.
# Assumes the e2e stack is up and the slice is warmed by one run below.
set -euo pipefail
cd "$(dirname "$0")"

SQL="SELECT sum(value) FROM scent.public.metrics WHERE region='us-east'"
N="${1:-10}"

# Warm the slice once, then take a baseline so only the measured runs count.
python3 run-query.py "$SQL" > /dev/null
base_miss=$(docker compose logs trino-worker 2>/dev/null | grep -c 'cache miss' || true)
base_hit=$(docker compose logs trino-worker 2>/dev/null | grep -c 'cache hit' || true)

for _ in $(seq 1 "$N"); do
  python3 run-query.py "$SQL" > /dev/null
done

miss=$(docker compose logs trino-worker 2>/dev/null | grep -c 'cache miss' || true)
hit=$(docker compose logs trino-worker 2>/dev/null | grep -c 'cache hit' || true)

new_miss=$((miss - base_miss))
new_hit=$((hit - base_hit))
total=$((new_miss + new_hit))
rate=0
if [ "$total" -gt 0 ]; then
  rate=$(awk "BEGIN{printf \"%.0f\", 100*$new_hit/$total}")
fi

echo "source reads: $new_miss"
echo "cache hits:   $new_hit"
echo "hit rate:     ${rate}%"
