#!/usr/bin/env bash
# v2 control-plane demo, fully offline (no model provider needed):
#
#   simulated alert → incident Work (correlated, deduplicated)
#   → deployment.rollback planned (R2: waits for approval, pinned to the
#     target generation and an intent hash)
#   → approval bound to the intent hash → dispatch (ledger first, idempotent)
#   → error rate verified by the metric source until stable
#   → Work SUCCEEDED with a SATISFIED outcome
#
# Usage: scripts/v2/demo.sh            (builds and starts harness-local)
#        BASE_URL=... TOKEN=... scripts/v2/demo.sh   (use a running server with
#        HARNESS_WORK_ENGINE_ENABLED=1 HARNESS_EVENT_SHADOW_MODE=0
#        HARNESS_ACTION_V2_ENABLED=1 and the sre-sim work_extension)
set -euo pipefail
cd "$(dirname "$0")/../.."
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print($1)"; }

PORT=${PORT:-18080}
if [[ -z "${BASE_URL:-}" ]]; then
  DATA=$(mktemp -d "${TMPDIR:-/tmp}/harness-v2-demo.XXXXXX")
  TOKEN=$(python3 -c "import secrets; print(secrets.token_hex(24))")
  SHA=$(printf '%s' "$TOKEN" | shasum -a 256 | cut -d' ' -f1)
  printf '[{"sha256":"%s","tenant":"local","subject":"demo-oncall","roles":["admin"]}]\n' "$SHA" >"$DATA/tokens.json"
  echo "building harness-local…"
  go build -o "$DATA/harness-local" ./cmd/harness-local
  HARNESS_LOCAL_DATA="$DATA" HARNESS_AUTH_TOKENS_FILE="$DATA/tokens.json" HARNESS_HTTP_ADDR="127.0.0.1:$PORT" \
    HARNESS_LOCAL_CONFIG="$PWD/scripts/v2/harness.local.json" \
    HARNESS_WORK_ENGINE_ENABLED=1 HARNESS_EVENT_SHADOW_MODE=0 HARNESS_ACTION_V2_ENABLED=1 \
    "$DATA/harness-local" >"$DATA/server.log" 2>&1 &
  SERVER=$!
  trap 'kill $SERVER 2>/dev/null || true; echo "logs and data: $DATA"' EXIT
  BASE_URL="http://127.0.0.1:$PORT"
fi
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

for _ in $(seq 1 100); do curl -sf -o /dev/null "$BASE_URL/healthz" && break; sleep 0.2; done
curl -sf -o /dev/null "$BASE_URL/healthz" || { echo "server did not start"; exit 1; }
echo "server: $BASE_URL"

echo "waiting for the simulated alert to open an incident Work…"
WORK=""
for _ in $(seq 1 60); do
  WORK=$(curl -sf "${H[@]}" "$BASE_URL/v1/works?kind=incident" | json 'd["items"][0]["id"] if d["items"] else ""')
  [[ -n "$WORK" ]] && break
  sleep 1
done
[[ -n "$WORK" ]] || { echo "no Work was created"; exit 1; }
echo "work: $WORK"

echo "waiting for the rollback plan…"
for _ in $(seq 1 60); do
  STATE=$(curl -sf "${H[@]}" "$BASE_URL/v1/works/$WORK/actions" | json 'd["items"][0]["state"] if d["items"] else ""')
  [[ "$STATE" == WAITING_APPROVAL || "$STATE" == REJECTED ]] && break
  sleep 1
done
ACT=$(curl -sf "${H[@]}" "$BASE_URL/v1/works/$WORK/actions" | json 'd["items"][0]')
echo "$ACT" | python3 -c "import sys,ast; a=ast.literal_eval(sys.stdin.read()); print('plan: %s %s args=%s risk=%s pinned_generation=%s\nintent_hash=%s\nstate=%s %s' % (a['action_name'], a['target_ref'], a['args'], a['risk'], a.get('target_version'), a['intent_hash'], a['state'], a.get('reason','')))"
AID=$(echo "$ACT" | python3 -c "import sys,ast; print(ast.literal_eval(sys.stdin.read())['id'])")
IH=$(echo "$ACT" | python3 -c "import sys,ast; print(ast.literal_eval(sys.stdin.read())['intent_hash'])")

echo "approving with a wrong intent hash (must be refused):"
curl -s -o /dev/null -w "  → HTTP %{http_code}\n" "${H[@]}" -d '{"decision":"approve","intent_hash":"sha256:0000"}' "$BASE_URL/v1/actions/$AID/approve"
echo "approving with the intent hash shown above:"
curl -s -o /dev/null -w "  → HTTP %{http_code}\n" "${H[@]}" -d "{\"decision\":\"approve\",\"intent_hash\":\"$IH\",\"comment\":\"demo\"}" "$BASE_URL/v1/actions/$AID/approve"

echo "verifying the error rate (stable for 30s)…"
for _ in $(seq 1 180); do
  ST=$(curl -sf "${H[@]}" "$BASE_URL/v1/works/$WORK" | json 'd["status"]+" "+d.get("reason","")')
  printf '\r  %-80s' "$ST"
  [[ "$ST" == SUCCEEDED* || "$ST" == FAILED* || "$ST" == BLOCKED* || "$ST" == CANCELLED* ]] && break
  sleep 2
done
echo
echo "timeline:"
curl -sf "${H[@]}" "$BASE_URL/v1/works/$WORK/events" | json '"\n".join("  %3d %-24s %s" % (e["work_seq"], e["type"], e["actor_type"]) for e in d["items"])'
echo "latest outcome:"
curl -sf "${H[@]}" "$BASE_URL/v1/works/$WORK/outcomes" | json '"  %s (%s)" % (d["items"][0]["verdict"], ", ".join(o["id"]+"="+o["verdict"] for o in d["items"][0]["objectives"])) if d["items"] else "  none"'
echo "world (deployment image now):"
curl -sf "${H[@]}" "$BASE_URL/v1/world/facts?subject=deployment://prod/checkout&predicate=image" | json '"  "+", ".join(f["value"]+" ("+f["source"]+")" for f in d["items"])'
[[ "$ST" == SUCCEEDED* ]]
