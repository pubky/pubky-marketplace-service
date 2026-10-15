#!/usr/bin/env bash
# Read-only sweep of the listings that cannot convert to per-variant stock yet
# (scripts/variant-conversion-sweep.sql). Run it after deploying migration 0070,
# and again once holds have settled.
#
# Usage:
#   DATABASE_URL=postgres://... scripts/variant-conversion-sweep.sh [--sync-many]
#
# Prints one tab-separated line per blocked listing: aggregate_id,
# seller_pubky, listing_id, reason, enabled_variants, reserved_quantity,
# sold_quantity, attempts, first_blocked_at, last_attempt_at. Reasons:
#   open_holds             units are reserved; converts at a sync after they settle
#   unattributable_sales   units were sold that the paid orders' lines do not
#                          attribute to the record's variants; stays on its
#                          shared total until the seller republishes the record
#                          with those variants or the sales are resolved
# With --sync-many it prints instead the body for POST /v1/listings/sync-many
# (the first 100 blocked listings) so the operator can re-attempt them:
#   scripts/variant-conversion-sweep.sh --sync-many | curl -sS -X POST \
#     -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' \
#     --data @- "$SERVICE_URL/v1/listings/sync-many"
# Each result carries `variant_conversion` while the listing is still blocked.
# A summary goes to stderr. Exit status: 0 when nothing is blocked, 2 when
# something is, 1 on a usage or database error.
#
# The database session is read-only (default_transaction_read_only).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE=rows
if [ "${1:-}" = "--sync-many" ]; then
  MODE=sync-many
elif [ "$#" -gt 0 ]; then
  echo "usage: $0 [--sync-many]" >&2
  exit 1
fi
: "${DATABASE_URL:?DATABASE_URL must be set}"
command -v psql >/dev/null || { echo "variant-conversion-sweep: psql is required" >&2; exit 1; }
if [ "$MODE" = sync-many ]; then
  command -v jq >/dev/null || { echo "variant-conversion-sweep: jq is required" >&2; exit 1; }
fi

rows="$(PGOPTIONS='-c default_transaction_read_only=on' \
  psql "$DATABASE_URL" -X -q -v ON_ERROR_STOP=1 -At -F $'\t' \
  -f "$ROOT/scripts/variant-conversion-sweep.sql")" || {
  echo "variant-conversion-sweep: the query failed" >&2
  exit 1
}

total=0
if [ -n "$rows" ]; then
  total="$(printf '%s\n' "$rows" | wc -l)"
fi
if [ "$MODE" = sync-many ]; then
  printf '%s\n' "$rows" | head -n 100 | jq -R -s '
    {listings: [split("\n")[] | select(length > 0) | split("\t")
      | {seller_pubky: .[1], listing_id: .[2]}]}'
else
  [ -z "$rows" ] || printf '%s\n' "$rows"
fi
echo "variant-conversion-sweep: ${total} listings cannot convert to per-variant stock yet" >&2
[ "$total" -eq 0 ] || exit 2
