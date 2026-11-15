#!/usr/bin/env bash
set -euo pipefail
root="$(git rev-parse --show-toplevel)"
tmp="$root/.tmp/security-rolling"
rm -rf "$tmp"
mkdir -p "$tmp/old" "$tmp/new"
trap 'rm -rf "$tmp"' EXIT
git archive HEAD | tar -x -C "$tmp/old"
tar -C "$root" --exclude=.git --exclude=.tmp -cf - . | tar -C "$tmp/new" -xf -
(cd "$tmp/old" && go build -o "$tmp/harness-old" ./cmd/harness)
(cd "$tmp/new" && go build -o "$tmp/harness-new" ./cmd/harness)
: "${HARNESS_PG_TEST_URL:?HARNESS_PG_TEST_URL is required}"
export HARNESS_DATABASE_URL="$HARNESS_PG_TEST_URL"
export HARNESS_SECURITY_MODE=legacy
"$tmp/harness-new" migrate
"$tmp/harness-old" validate-config --tier staging
"$tmp/harness-new" validate-config --tier staging
HARNESS_SECURITY_MODE=shadow "$tmp/harness-new" validate-config --tier staging
HARNESS_SECURITY_MODE=enforce "$tmp/harness-new" validate-config --tier staging
