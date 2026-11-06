#!/usr/bin/env bash
# Drives one remediation run against a running harness:
# create → wait for approval → approve (bound to action_hash + expected_version) → stream events → result.
# Usage: BASE_URL=http://127.0.0.1:8080 scripts/demo.sh ["goal text"]
set -euo pipefail
BASE_URL=${BASE_URL:-http://127.0.0.1:8080}
GOAL=${1:-"remediate high latency service=checkout"}
# Dev auth mode uses identity headers; with HARNESS_AUTH_MODE=token set TOKEN (and APPROVER_TOKEN for approvals).
if [[ -n "${TOKEN:-}" ]]; then
  HDR=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
  AHDR=(-H "Authorization: Bearer ${APPROVER_TOKEN:-$TOKEN}" -H "Content-Type: application/json")
else
  HDR=(-H "X-Tenant-ID: ${TENANT:-tenant-a}" -H "X-Actor-ID: ${ACTOR:-alice}" -H "Content-Type: application/json")
  AHDR=("${HDR[@]}")
fi
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print($1)"; }

if ! curl -sf -o /dev/null "$BASE_URL/healthz"; then
  echo "harness is not reachable at $BASE_URL — start it first, e.g.:" >&2
  echo "  HARNESS_DATABASE_URL=postgres://... go run ./cmd/harness all" >&2
  exit 1
fi

RUN=$(curl -sf "${HDR[@]}" -H "Idempotency-Key: demo-$(date +%s%N)" \
  -d "{\"goal\":\"$GOAL\"}" "$BASE_URL/v1/runs" | json 'd["run_id"]')
echo "run: $RUN"

for _ in $(seq 1 50); do
  STATUS=$(curl -sf "${HDR[@]}" "$BASE_URL/v1/runs/$RUN" | json 'd["display_status"]')
  [[ "$STATUS" == WAITING_APPROVAL || "$STATUS" == COMPLETED || "$STATUS" == FAILED ]] && break
  sleep 0.2
done
echo "status: $STATUS"

if [[ "$STATUS" == WAITING_APPROVAL ]]; then
  read -r AID AH RISK TOOL < <(curl -sf "${HDR[@]}" "$BASE_URL/v1/runs/$RUN/approvals" |
    json '" ".join(str(a[k]) for a in [[x for x in d["approvals"] if x["status"]=="PENDING"][0]] for k in ("id","action_hash","risk_class","tool_name"))')
  VER=$(curl -sf "${HDR[@]}" "$BASE_URL/v1/runs/$RUN" | json 'd["run"]["version"]')
  echo "approving $TOOL ($RISK) action_hash=${AH:0:20}… at version $VER"
  curl -s -o /dev/null -w "approve → HTTP %{http_code}\n" "${AHDR[@]}" -H "Idempotency-Key: approve-$AID" \
    -d "{\"type\":\"approve_tool\",\"expected_version\":$VER,\"payload\":{\"approval_request_id\":\"$AID\",\"action_hash\":\"$AH\"}}" \
    "$BASE_URL/v1/runs/$RUN/commands"
fi

echo "events:"
curl -sN --max-time 15 "${HDR[@]}" "$BASE_URL/v1/runs/$RUN/events/stream" | sed -n 's/^event: /  /p'
curl -sf "${HDR[@]}" "$BASE_URL/v1/runs/$RUN" | json '"final: " + d["display_status"] + "\n" + (d.get("result") or {}).get("answer","")'
