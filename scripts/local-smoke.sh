#!/usr/bin/env bash
# End-to-end smoke test of the single-user local mode: start `harness local`
# on a scratch data directory, check auth, run a diagnosis through
# `harness chat`, take a backup while it runs, and restore it.
# usage: scripts/local-smoke.sh [path/to/harness]
set -euo pipefail
bin="${1:-./harness}"
work="$(mktemp -d)"
data="$work/data"
addr="127.0.0.1:${SMOKE_PORT:-18199}"
pid=""
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

HARNESS_HTTP_ADDR="$addr" "$bin" local -data "$data" >"$work/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 50); do curl -sf "http://$addr/healthz" >/dev/null 2>&1 && break; sleep 0.2; done
curl -sf "http://$addr/healthz" >/dev/null || { cat "$work/server.log"; echo "server did not start"; exit 1; }

token="$(cat "$data/token")"
code="$(curl -s -o /dev/null -w '%{http_code}' "http://$addr/v1/whoami")"
[ "$code" = 401 ] || { echo "unauthenticated request got $code, want 401"; exit 1; }
curl -sf -H "Authorization: Bearer $token" "http://$addr/v1/whoami" | grep -q local-user

out="$(printf 'diagnose high latency service=checkout\n' | "$bin" chat -server "http://$addr" -data "$data")"
echo "$out" | grep -q 'COMPLETED' || { echo "$out"; echo "chat run did not complete"; exit 1; }

if "$bin" local -data "$data" -addr 127.0.0.1:0 >/dev/null 2>&1; then
  echo "a second instance on the same data directory must be refused"; exit 1
fi
"$bin" local backup -data "$data" -out "$work/backup.tar.gz" >/dev/null
if tar -tzf "$work/backup.tar.gz" | grep -qE '(^|/)(token|token\.json|local\.env)$'; then
  echo "backup must not contain credentials"; exit 1
fi

kill "$pid"; wait "$pid" 2>/dev/null || true; pid=""
"$bin" local restore -data "$data" "$work/backup.tar.gz" >/dev/null
"$bin" local doctor -data "$data" -addr "$addr" -offline >/dev/null
echo "local smoke test passed"
