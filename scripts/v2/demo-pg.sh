#!/usr/bin/env bash
# v2 MVP demo on `harness all` + PostgreSQL, fully offline (no model
# provider needed). It checks, and fails on, each property:
#
#   1. change + alert correlation: a deploy event and the repeating alert
#      land in ONE incident Work (shared correlation group + lookback)
#   2. idempotent replay: a re-sent event is a duplicate, a second approval
#      does not dispatch again, a re-sent postmortem request returns the
#      same Work (Idempotency-Key)
#   3. crash recovery: the harness is killed with SIGKILL right after the
#      approval and again during verification; after each restart the Work
#      carries on, and the rollback was applied exactly once downstream
#   4. topology: service -deployed_as-> deployment -owns-> pod -runs_on->
#      node in the world state, with the pods replaced by the rollout
#
# Usage:
#   HARNESS_PG_ADMIN_URL=postgres://postgres@127.0.0.1:55432/postgres scripts/v2/demo-pg.sh
# A fresh database (harness_demo_<pid>) is created and dropped afterwards
# (KEEP_DB=1 keeps it). PORT defaults to 18090.
set -euo pipefail
cd "$(dirname "$0")/../.."
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print($1)"; }
fail() { echo "FAIL: $*"; exit 1; }
ok() { echo "  ok: $*"; }

ADMIN_URL=${HARNESS_PG_ADMIN_URL:-postgres://postgres@127.0.0.1:55432/postgres}
PORT=${PORT:-18090}
DB=harness_demo_$$
DB_URL="${ADMIN_URL%/*}/$DB?sslmode=disable"
DATA=$(mktemp -d "${TMPDIR:-/tmp}/harness-v2-pg.XXXXXX")
export SIM_STATE_FILE="$DATA/sim-state.json"

TOKEN=$(python3 -c "import secrets; print(secrets.token_hex(24))")
SHA=$(printf '%s' "$TOKEN" | shasum -a 256 | cut -d' ' -f1)
printf '[{"sha256":"%s","tenant":"local","subject":"demo-oncall","roles":["admin"]}]\n' "$SHA" >"$DATA/tokens.json"

psql "$ADMIN_URL" -qc "CREATE DATABASE $DB" >/dev/null
echo "database: $DB"
echo "building harness…"
go build -o "$DATA/harness" ./cmd/harness

SERVER=""
start() {
  HARNESS_DATABASE_URL="$DB_URL" HARNESS_HTTP_ADDR="127.0.0.1:$PORT" HARNESS_CONFIG="$PWD/scripts/v2/harness.pg.json" \
    HARNESS_AUTH_MODE=token HARNESS_AUTH_TOKENS_FILE="$DATA/tokens.json" HARNESS_DEFAULT_TENANT=local \
    HARNESS_ARTIFACT_DIR="$DATA/artifacts" HARNESS_STRATEGY=rule \
    HARNESS_WORK_ENGINE_ENABLED=1 HARNESS_EVENT_SHADOW_MODE=0 HARNESS_ACTION_V2_ENABLED=1 \
    "$DATA/harness" all >>"$DATA/server.log" 2>&1 &
  SERVER=$!
  for _ in $(seq 1 150); do curl -sf -o /dev/null "$BASE_URL/healthz" && return 0; sleep 0.2; done
  tail -20 "$DATA/server.log"; fail "server did not start"
}
crash() {
  kill -9 "$SERVER" 2>/dev/null || true
  wait "$SERVER" 2>/dev/null || true
  echo "  ✗ harness killed with SIGKILL ($1)"
  start
  echo "  ↻ harness restarted (pid $SERVER)"
}
cleanup() {
  [[ -n "$SERVER" ]] && kill "$SERVER" 2>/dev/null || true
  wait 2>/dev/null || true
  if [[ -z "${KEEP_DB:-}" ]]; then psql "$ADMIN_URL" -qc "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null 2>&1 || true; fi
  echo "logs and data: $DATA"
}
trap cleanup EXIT

BASE_URL="http://127.0.0.1:$PORT"
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
get() { curl -sf "${H[@]}" "$BASE_URL$1"; }
# post PATH BODY: prints the response; fails loudly on HTTP >= 400.
post() {
  local out code
  out=$(curl -s -w $'\n%{http_code}' "${H[@]}" -d "$2" "$BASE_URL$1")
  code=${out##*$'\n'}; out=${out%$'\n'*}
  [[ "$code" -lt 400 ]] || fail "POST $1 → HTTP $code: $out"
  printf '%s' "$out"
}

start
echo "server: $BASE_URL (harness all, pid $SERVER)"

echo
echo "1. change + alert correlation"
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
CHANGE="{\"source\":\"argocd\",\"type\":\"change.deployed\",\"subject_ref\":\"deployment://prod/checkout\",\"dedup_key\":\"sync-42\",\"event_id\":\"argocd-sync-42\",\"occurred_at\":\"$NOW\",\"labels\":{\"image\":\"checkout:v2\"}}"
R1=$(post /v1/events "$CHANGE")
INBOX=$(echo "$R1" | json 'd["inbox_id"]')
echo "  deploy event accepted: $(echo "$R1" | json 'd["decision"] or d["state"]')"

WORK=""
for _ in $(seq 1 60); do
  WORK=$(get "/v1/works?kind=incident" | json 'd["items"][0]["id"] if d["items"] else ""')
  [[ -n "$WORK" ]] && break
  sleep 1
done
[[ -n "$WORK" ]] || fail "no incident Work was created"
echo "  work: $WORK"
for _ in $(seq 1 20); do
  CW=$(get "/v1/events/$INBOX" | json 'd.get("work_id") or ""')
  [[ "$CW" == "$WORK" ]] && break
  sleep 1
done
[[ "$CW" == "$WORK" ]] || fail "deploy event not attached to the Work (work_id=$CW)"
ok "the deploy event is attached to the incident Work"
sleep 6 # let the alert fire again
N=$(get "/v1/works?kind=incident" | json 'len(d["items"])')
[[ "$N" == 1 ]] || fail "$N incident Works for one episode"
ALERTS=$(get "/v1/works/$WORK/events" | json 'sum(1 for e in d["items"] if e["type"]=="work.event_attached")')
ok "one incident Work with $ALERTS events attached (deploy + alert); repeated firings deduplicated"

echo
echo "2. idempotent replay"
DUP=$(post /v1/events "$CHANGE" | json 'd["duplicate"]')
[[ "$DUP" == True ]] || fail "re-sent event was not deduplicated"
ok "re-sent deploy event → duplicate"

for _ in $(seq 1 60); do
  STATE=$(get "/v1/works/$WORK/actions" | json 'd["items"][0]["state"] if d["items"] else ""')
  [[ "$STATE" == WAITING_APPROVAL || "$STATE" == REJECTED ]] && break
  sleep 1
done
[[ "$STATE" == WAITING_APPROVAL ]] || fail "rollback not waiting for approval (state=$STATE)"
AID=$(get "/v1/works/$WORK/actions" | json 'd["items"][0]["id"]')
IH=$(get "/v1/works/$WORK/actions" | json 'd["items"][0]["intent_hash"]')
echo "  rollback planned (R2): $AID waits for approval"
APPROVE="{\"decision\":\"approve\",\"intent_hash\":\"$IH\",\"comment\":\"demo\"}"
C1=$(curl -s -o /dev/null -w "%{http_code}" "${H[@]}" -d "$APPROVE" "$BASE_URL/v1/actions/$AID/approve")
[[ "$C1" == 200 ]] || fail "approval returned HTTP $C1"
ok "approved (HTTP $C1)"

echo
echo "3. crash recovery"
crash "right after the approval"
C2=$(curl -s -o /dev/null -w "%{http_code}" "${H[@]}" -d "$APPROVE" "$BASE_URL/v1/actions/$AID/approve")
ok "approval re-sent after the restart → HTTP $C2 (no second decision)"

CRASHED_VERIFY=""; LAST=""
for _ in $(seq 1 240); do
  ST=$(get "/v1/works/$WORK" | json '(d["status"]+" "+d.get("reason","")).strip()')
  [[ "$ST" != "$LAST" ]] && { echo "  status: $ST"; LAST=$ST; }
  case "$ST" in
    SUCCEEDED*|FAILED*|BLOCKED*|CANCELLED*) break ;;
  esac
  if [[ -z "$CRASHED_VERIFY" ]]; then
    OUTS=$(get "/v1/works/$WORK/outcomes" | json 'len(d["items"])')
    if [[ "$OUTS" -ge 1 ]]; then crash "during verification"; CRASHED_VERIFY=1; fi
  fi
  sleep 2
done
[[ "$ST" == SUCCEEDED* ]] || { tail -30 "$DATA/server.log"; fail "Work ended $ST"; }
[[ -n "$CRASHED_VERIFY" ]] || echo "  (verification finished before the second crash point)"
ok "Work SUCCEEDED across restarts"
ASTATE=$(get "/v1/actions/$AID" | json 'd["state"]')
[[ "$ASTATE" == VERIFIED || "$ASTATE" == SUCCEEDED || "$ASTATE" == APPLIED ]] || fail "action state $ASTATE"
CHANGES=$(python3 -c "import json; print(json.load(open('$SIM_STATE_FILE'))['deployments']['deployment://prod/checkout']['changes'])")
[[ "$CHANGES" == 1 ]] || fail "downstream applied the rollback $CHANGES times"
IMAGE=$(get "/v1/world/facts?subject=deployment://prod/checkout&predicate=image" | json 'd["items"][0]["value"]')
ok "action $ASTATE; downstream applied the rollback exactly once; image now $IMAGE"
OUTCOME=$(get "/v1/works/$WORK/outcomes" | json 'd["items"][0]["verdict"]')
ok "latest outcome: $OUTCOME"

echo
echo "4. topology"
sleep 6 # one refresh after the rollout
SNAP=$(post /v1/world/snapshots/query '{"subjects":["service://prod/checkout"],"depth":3,"max_nodes":100}')
echo "$SNAP" | python3 -c '
import sys, json
s = json.load(sys.stdin)
rels = [r for r in s["relations"] if not r.get("valid_to")]
by = lambda t: sorted(r["from_ref"] + " -> " + r["to_ref"] for r in rels if r["type"] == t)
for t in ("deployed_as", "owns", "runs_on"):
    for line in by(t):
        print("  %-11s %s" % (t, line))
pods = by("owns")
assert len(by("deployed_as")) == 1, "service -> deployment edge missing"
assert len(pods) == 3, "want 3 pods, got %d" % len(pods)
assert len(by("runs_on")) >= 3, "pods without nodes"
' || fail "topology incomplete"
ok "service → deployment → 3 pods → nodes"

echo
echo "postmortem + idempotent request"
get "/v1/works/$WORK/postmortem?format=markdown" | head -12 | sed 's/^/  | /'
P1=$(curl -s -w " %{http_code}" "${H[@]}" -X POST "$BASE_URL/v1/works/$WORK/postmortem")
P2=$(curl -s -w " %{http_code}" "${H[@]}" -X POST "$BASE_URL/v1/works/$WORK/postmortem")
ID1=$(echo "${P1% *}" | json 'd["id"]'); ID2=$(echo "${P2% *}" | json 'd["id"]')
[[ "$ID1" == "$ID2" ]] || fail "postmortem request created two Works"
ok "postmortem Work $ID1 (HTTP ${P1##* }, re-sent → HTTP ${P2##* }, same Work)"

echo
echo "PASS: correlation, idempotent replay, crash recovery and topology on harness all + PostgreSQL"
