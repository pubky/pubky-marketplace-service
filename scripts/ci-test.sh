#!/usr/bin/env bash
# Workspace tests with database-per-test parallelism.
#
# #[sqlx::test] creates one database per test. TEST_MIGRATOR wraps the
# cluster-global role window (0032..=0037) in a single transaction holding
# EXCLUSIVE locks on pg_catalog.pg_authid and pg_catalog.pg_auth_members
# (cluster-wide, database = 0 in pg_locks) so parallel migrations cannot race
# 0032's CONNECTION LIMIT check. Integration binaries whose test bodies mutate
# cluster-global roles (fixed CREATE/ALTER ROLE, shared login passwords, or
# unguarded catalog replays) run afterwards, one test at a time.
#
# Cargo compiles test binaries in parallel across CPUs and executes each test
# binary sequentially. Inside each binary, sqlx::test shares one 20-connection
# MASTER_POOL across per-test pools of up to 5 connections each, so 4 threads
# (4 * 5 = 20) prevents cross-test pool starvation on lock-barrier tests while
# saturating 4-vCPU CI runners under Postgres max_connections = 100.
#
# Usage: scripts/ci-test.sh [package...]
# No arguments tests the whole workspace (CI and PREPUSH_FULL=1).
# Package names limit the run to those packages; the integration binaries run
# only when marketplace-service is named.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

THREADS="${CI_TEST_THREADS:-${PREPUSH_TEST_THREADS:-4}}"

# Integration-test binaries that depend on cluster-global PostgreSQL state:
# - role-mutating and raw-migration binaries (CREATE/ALTER ROLE, shared login
#   passwords, or catalog replays outside TEST_MIGRATOR);
# - connector outbox binaries calling `connectors::outbox::consume_batch`,
#   which checks cluster-global `pg_snapshot_xmin(pg_current_snapshot())` and
#   would see open write transactions in sibling #[sqlx::test] databases.
SERIAL=(
  refusal_audit_test
  migration_0033_test
  migration_0035_test
  migration_0037_test
  migration_0070_test
  migration_0083_test
  migration_0087_test
  migration_0089_test
  migration_0090_test
  migration_0093_test
  migration_0100_test
  rolling_overlap_test
  manual_review_resolve_test
  migrator_tolerance_test
  store_seller_test
  connectors_address_share_test
  connectors_api_test
  connectors_inbound_test
  connectors_ledger_test
  connectors_product_sync_test
  connectors_refund_reversal_test
  connectors_shopify_money_test
  connectors_shopify_test
  connectors_woocommerce_flow_test
)

is_serial() {
  local name="$1" item
  for item in "${SERIAL[@]}"; do
    if [ "$item" = "$name" ]; then
      return 0
    fi
  done
  return 1
}

scope=(--workspace)
service=1
if [ "$#" -gt 0 ]; then
  scope=()
  service=0
  for pkg in "$@"; do
    scope+=(-p "$pkg")
    if [ "$pkg" = marketplace-service ]; then
      service=1
    fi
  done
fi

echo "ci-test: lib and bins (${scope[*]}), ${THREADS} threads"
cargo test "${scope[@]}" --lib --bins --no-fail-fast -- --test-threads="$THREADS"

if [ "$service" != 1 ]; then
  echo "ci-test: integration tests skipped (marketplace-service not selected)"
  exit 0
fi

parallel_args=()
for file in crates/service/tests/*.rs; do
  name="$(basename "$file" .rs)"
  if is_serial "$name"; then
    continue
  fi
  parallel_args+=(--test "$name")
done

echo "ci-test: parallel integration tests ($(( ${#parallel_args[@]} / 2 )) binaries, ${THREADS} threads)"
cargo test -p marketplace-service "${parallel_args[@]}" --no-fail-fast -- --test-threads="$THREADS"

serial_args=()
for name in "${SERIAL[@]}"; do
  serial_args+=(--test "$name")
done

echo "ci-test: serial cluster-state tests (${#SERIAL[@]} binaries, 1 thread)"
echo "  ${SERIAL[*]}"
cargo test -p marketplace-service "${serial_args[@]}" --no-fail-fast -- --test-threads=1
