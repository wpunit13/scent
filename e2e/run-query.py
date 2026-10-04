#!/usr/bin/env python3
"""Run a Trino query via /v1/statement from the host, following nextUri. Prints rows or error."""
import json
import sys
import urllib.error
import urllib.request

COORDINATOR = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8080"
SQL = sys.argv[1]

url = COORDINATOR + "/v1/statement"
rows = []
cols = None
first = True
while url:
    data = SQL.encode() if first else None
    first = False
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET",
                                  headers={"X-Trino-User": "e2e"})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.load(resp)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            payload = json.loads(raw)
        except ValueError:
            print(f"HTTP {e.code}: {raw[:2000]}", file=sys.stderr)
            sys.exit(2)
    if cols is None and payload.get("columns"):
        cols = [c["name"] for c in payload["columns"]]
    if payload.get("data"):
        rows.extend(payload["data"])
    error = payload.get("error")
    url = payload.get("nextUri")
    if error:
        print(f"QUERY FAILED: {error.get('message')}", file=sys.stderr)
        print(f"errorType={error.get('errorType')} code={error.get('errorCode')}", file=sys.stderr)
        sys.exit(1)

if cols is not None:
    print("\t".join(cols))
for row in rows:
    print("\t".join(str(v) for v in row))
print(f"-- {len(rows)} rows, query OK")