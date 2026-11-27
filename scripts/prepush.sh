#!/usr/bin/env bash
# Pre-push gate. Last line on success: PREPUSH OK <sha> <seconds> <mode>
#
# fast (default): cargo fmt --check, plus cargo check --all-targets for the
#   packages changed since the merge base (scripts/prepush-packages.sh). A
#   change to a workspace-wide file checks the whole workspace; a change to no
#   package (docs, CI, scripts) runs fmt only. No Docker or Postgres required.
# full (PREPUSH_FULL=1): cargo fmt --check, workspace clippy, and the full
#   workspace test suite via scripts/ci-test.sh (using DATABASE_URL when set,
#   otherwise starting a local scram-sha-256 Postgres 16 container in Docker).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Match .github/workflows/ci.yml when rustup has 1.89.0 installed, without
# failing on environments using a system or newer Rust toolchain.
if [ -z "${RUSTUP_TOOLCHAIN:-}" ] && command -v rustup >/dev/null 2>&1 \
  && rustup toolchain list 2>/dev/null | grep -q '^1\.89\.0'; then
  export RUSTUP_TOOLCHAIN=1.89.0
fi

start=$(date +%s)
# shellcheck source=heavy-lock.sh
source "$ROOT/scripts/heavy-lock.sh"
# shellcheck source=prepush-stamp.sh
source "$ROOT/scripts/prepush-stamp.sh"
prepush_mode_init || exit 1
echo "prepush: mode ${PREPUSH_MODE}"

# Hook stdin lists the refs being pushed. A push whose every commit message
# contains [skip ci] does not run the gate.
if [ ! -t 0 ]; then
  skip=1
  saw=0
  while read -r _local_ref local_sha _remote_ref remote_sha; do
    [ -n "${local_sha:-}" ] || continue
    saw=1
    if [ "$local_sha" = "0000000000000000000000000000000000000000" ]; then
      continue
    fi
    if [ "$remote_sha" = "0000000000000000000000000000000000000000" ]; then
      range="$local_sha"
    else
      range="${remote_sha}..${local_sha}"
    fi
    # grep -q under pipefail exits 141 when git log is still writing,
    # and the gate then skips every push. Read the subjects first.
    subjects="$(git log --format=%s "$range" || true)"
    if printf '%s\n' "$subjects" | grep -v '\[skip ci\]' >/dev/null; then
      skip=0
    fi
  done
  if [ "$saw" = 1 ] && [ "$skip" = 1 ]; then
    echo "prepush: every commit has [skip ci]; gate not run"
    exit 0
  fi
fi

sha="$(git rev-parse HEAD)"
if prepush_reuse "$sha"; then
  exit 0
fi

if [ -n "${PREPUSH_CARGO_TARGET:-}" ]; then
  export CARGO_TARGET_DIR="$PREPUSH_CARGO_TARGET"
fi

packages=()
if [ "$PREPUSH_MODE" = full ]; then
  packages=(--workspace)
else
  base=""
  if [ -n "${PREPUSH_BASE:-}" ]; then
    base="$PREPUSH_BASE"
  elif git rev-parse --verify --quiet origin/main >/dev/null; then
    base="$(git merge-base HEAD origin/main)"
  fi

  if [ -z "$base" ]; then
    packages=(--workspace)
  else
    # shellcheck source=prepush-packages.sh
    source "$ROOT/scripts/prepush-packages.sh"
    selected="$(prepush_changed_packages "$base" \
      Cargo.toml Cargo.lock rust-toolchain rust-toolchain.toml '.cargo/*' \
      clippy.toml .clippy.toml rustfmt.toml .rustfmt.toml \
      'contracts/*' scripts/ci-test.sh)" || {
      echo "prepush: cannot diff against base ${base}" >&2
      exit 1
    }
    while IFS= read -r line; do
      [ -n "$line" ] && packages+=("$line")
    done <<< "$selected"
  fi
fi

if [ "${#packages[@]}" -eq 0 ]; then
  echo "prepush: packages (none changed)"
else
  echo "prepush: packages ${packages[*]}"
fi

echo "prepush: cargo fmt"
cargo fmt --check

if [ "${#packages[@]}" -eq 0 ]; then
  echo "prepush: cargo check skipped (no package changed)"
  prepush_stamp "$sha"
  seconds="$(( $(date +%s) - start ))"
  echo "PREPUSH OK ${sha} ${seconds} ${PREPUSH_MODE}"
  exit 0
fi

scope=()
if [ "${packages[0]}" = --workspace ]; then
  scope=(--workspace)
else
  for pkg in "${packages[@]}"; do
    scope+=(-p "$pkg")
  done
fi

if [ "$PREPUSH_MODE" = fast ]; then
  echo "prepush: cargo check"
  cargo check "${scope[@]}" --all-targets
  prepush_stamp "$sha"
  seconds="$(( $(date +%s) - start ))"
  echo "PREPUSH OK ${sha} ${seconds} ${PREPUSH_MODE}"
  exit 0
fi

ensure_postgres() {
  if [ -n "${DATABASE_URL:-}" ]; then
    return 0
  fi

  # One container per worktree. A shared name can race across concurrent
  # worktrees. This gate does not remove containers.
  local worktree_slug name owner_label port sum owner ready encryption host_all
  worktree_slug="$(printf '%s' "$(basename "$ROOT")" | tr -c 'A-Za-z0-9_.-' '-')"
  name="${PREPUSH_PG_CONTAINER:-prepush-ms-pg-${worktree_slug}}"
  owner_label="pubky.prepush.worktree"
  if [ -n "${PREPUSH_PG_PORT:-}" ]; then
    port="$PREPUSH_PG_PORT"
  else
    sum="$(printf '%s' "$ROOT" | cksum | awk '{print $1}')"
    port="$((56000 + sum % 1000))"
  fi

  if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    echo "prepush: set DATABASE_URL or start docker for PREPUSH_FULL=1 Postgres tests" >&2
    exit 1
  fi

  if docker ps -a --format '{{.Names}}' | grep -qx "$name"; then
    owner="$(docker inspect -f "{{index .Config.Labels \"${owner_label}\"}}" "$name")"
    if [ "$owner" != "$ROOT" ]; then
      echo "prepush: container ${name} exists and this gate did not create it" >&2
      exit 1
    fi
    if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
      docker start "$name" >/dev/null
    fi
    port="$(docker inspect -f '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$name")"
  else
    docker run -d --name "$name" \
      --label "${owner_label}=${ROOT}" \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=postgres \
      -e POSTGRES_DB=postgres \
      -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
      -p "127.0.0.1:${port}:5432" \
      postgres:16 >/dev/null
  fi

  export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:${port}/postgres"

  ready=0
  for _ in $(seq 1 60); do
    if docker exec "$name" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [ "$ready" != 1 ]; then
    echo "prepush: Postgres did not become ready" >&2
    exit 1
  fi

  encryption="$(docker exec "$name" psql -h 127.0.0.1 -U postgres -d postgres -tAc 'SHOW password_encryption' | tr -d '[:space:]')"
  host_all="$(docker exec "$name" psql -h 127.0.0.1 -U postgres -d postgres -tAc "SELECT auth_method FROM pg_hba_file_rules WHERE type = 'host' AND address = 'all'" | tr -d '[:space:]')"
  if [ "$encryption" != "scram-sha-256" ] || [ "$host_all" != "scram-sha-256" ]; then
    echo "prepush: Postgres is not scram-sha-256 like CI (encryption=${encryption} host_all=${host_all})" >&2
    exit 1
  fi
}

ensure_postgres

echo "prepush: cargo clippy"
run_heavy cargo cargo clippy --workspace --all-targets -- -D warnings

echo "prepush: cargo test"
run_heavy cargo bash scripts/ci-test.sh

prepush_stamp "$sha"
seconds="$(( $(date +%s) - start ))"
echo "PREPUSH OK ${sha} ${seconds} ${PREPUSH_MODE}"
