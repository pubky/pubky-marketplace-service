#!/usr/bin/env bash
# Read-only sweep for the listing lifecycle check: lists every listing the
# service would still sell or take payment on (scripts/listing-record-sweep.sql)
# and probes its seller record on the homeserver, exactly as the service reads
# it (GET {HOMESERVER_URL}/pub/pubky.app/marketplace/v1/listings/{id} with a
# pubky-host header). Run it before deploying the check: a sellable listing
# whose record is missing starts refusing checkout and payment as
# listing_removed.
#
# Usage:
#   DATABASE_URL=postgres://... HOMESERVER_URL=https://... \
#     scripts/listing-record-sweep.sh [--all]
#
# Prints one tab-separated line per finding: verdict, aggregate_id,
# seller_pubky, listing_id, sale_format, service state. Verdicts:
#   missing      the homeserver answers 404 (refused as listing_removed)
#   removed      the record says removed (refused as listing_removed)
#   unreachable  no usable answer (refused with UPSTREAM_UNAVAILABLE while it lasts)
#   paused, ended, not_active  new commitments refused; payments continue
#   active       on sale (printed only with --all)
# A summary goes to stderr. Exit status: 0 when nothing is missing, removed
# or unreachable, 2 when something is, 1 on a usage or database error.
#
# The database session is read-only (default_transaction_read_only); the
# homeserver gets one GET per listing, SWEEP_DELAY_SECONDS apart (default 0.1).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SHOW_ALL=0
if [ "${1:-}" = "--all" ]; then
  SHOW_ALL=1
elif [ "$#" -gt 0 ]; then
  echo "usage: $0 [--all]" >&2
  exit 1
fi
: "${DATABASE_URL:?DATABASE_URL must be set}"
: "${HOMESERVER_URL:?HOMESERVER_URL must be set}"
for tool in psql curl jq; do
  command -v "$tool" >/dev/null || { echo "listing-record-sweep: $tool is required" >&2; exit 1; }
done
BASE="${HOMESERVER_URL%/}"
DELAY="${SWEEP_DELAY_SECONDS:-0.1}"

rows="$(PGOPTIONS='-c default_transaction_read_only=on' \
  psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -At -F $'\t' \
  -f "$ROOT/scripts/listing-record-sweep.sql")" || {
  echo "listing-record-sweep: the listing query failed" >&2
  exit 1
}

body="$(mktemp)"
trap 'rm -f "$body"' EXIT
declare -A counts=()
total=0
while IFS=$'\t' read -r aggregate_id seller listing_id sale_format state; do
  [ -n "$aggregate_id" ] || continue
  total=$((total + 1))
  status="$(curl -sS -o "$body" -w '%{http_code}' --max-time 10 \
    -H "pubky-host: ${seller}" \
    "${BASE}/pub/pubky.app/marketplace/v1/listings/${listing_id}" 2>/dev/null || echo 000)"
  case "$status" in
    404) verdict=missing ;;
    2??)
      record_state="$(jq -r 'if type == "object" then (.state // "active") else "invalid" end' "$body" 2>/dev/null || echo invalid)"
      case "$record_state" in
        active | paused | ended | removed) verdict="$record_state" ;;
        invalid) verdict=unreachable ;;
        *) verdict=not_active ;;
      esac
      ;;
    *) verdict=unreachable ;;
  esac
  counts[$verdict]=$(( ${counts[$verdict]:-0} + 1 ))
  if [ "$verdict" != active ] || [ "$SHOW_ALL" = 1 ]; then
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$verdict" "$aggregate_id" "$seller" "$listing_id" "$sale_format" "$state"
  fi
  sleep "$DELAY"
done <<< "$rows"

summary="listing-record-sweep: ${total} sellable listings"
for verdict in active paused ended not_active removed missing unreachable; do
  summary+=", ${verdict}=${counts[$verdict]:-0}"
done
echo "$summary" >&2
if [ "${counts[missing]:-0}" -gt 0 ] || [ "${counts[removed]:-0}" -gt 0 ] || [ "${counts[unreachable]:-0}" -gt 0 ]; then
  exit 2
fi
