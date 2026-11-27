#!/usr/bin/env bash
# Proves the fast and full modes of scripts/prepush.sh run the right steps,
# and that scripts/prepush-packages.sh maps a change to its packages.
# Runs the real scripts in a throwaway workspace. cargo and docker are
# stand-ins on PATH that log their arguments; metadata goes to the real cargo,
# and nothing is built or started.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-mode-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
unset CARGO_TARGET_DIR
unset PREPUSH_CARGO_TARGET
unset PREPUSH_FULL
unset DATABASE_URL

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

REAL_CARGO="$(command -v cargo)"
mkdir -p "$WORK/bin"
cat > "$WORK/bin/cargo" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = metadata ]; then
  exec "$REAL_CARGO" "$@"
fi
printf '%s\n' "$*" >> "$FAKE_LOG/cargo"
EOF
cat > "$WORK/bin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_LOG/docker"
case "$*" in
  *password_encryption*|*pg_hba_file_rules*) echo scram-sha-256 ;;
esac
exit 0
EOF
mkdir -p "$WORK/broken"
printf '#!/usr/bin/env bash\nexit 101\n' > "$WORK/broken/cargo"
chmod +x "$WORK/bin/cargo" "$WORK/bin/docker" "$WORK/broken/cargo"
export REAL_CARGO

# A real two-crate workspace: the service depends on the domain by path, as
# in this repository.
manifest() {
  mkdir -p "$1/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n' "$2" > "$1/Cargo.toml"
  if [ -n "${3:-}" ]; then
    printf '\n[dependencies]\n%s = { path = "%s" }\n' "$3" "$4" >> "$1/Cargo.toml"
  fi
  printf '// lib\n' > "$1/src/lib.rs"
  printf '// extra\n' > "$1/src/extra.rs"
}

git init -q "$WORK/ws"
cd "$WORK/ws"
git config user.email test@example.invalid
git config user.name test
printf '[workspace]\nresolver = "2"\nmembers = ["crates/domain", "crates/service"]\n' > Cargo.toml
manifest crates/domain marketplace-domain
manifest crates/service marketplace-service marketplace-domain ../domain
"$REAL_CARGO" generate-lockfile --offline --quiet
mkdir -p crates/service/tests docs contracts scripts
printf '// t\n' > crates/service/tests/orders_test.rs
printf '// t\n' > crates/service/tests/refusal_audit_test.rs
printf 'doc\n' > docs/readme.md
printf '{}\n' > contracts/endpoints.json
cp "$DIR/prepush.sh" "$DIR/prepush-stamp.sh" "$DIR/prepush-packages.sh" \
  "$DIR/heavy-lock.sh" "$DIR/ci-test.sh" scripts/
printf '/target\n' > .gitignore
git add -A
git commit -qm base
BASE="$(git rev-parse HEAD)"

# shellcheck source=prepush-packages.sh
source "$DIR/prepush-packages.sh"
pkgs() {
  prepush_changed_packages "$BASE" Cargo.toml Cargo.lock 'contracts/*' | tr '\n' ' ' | sed 's/ $//'
}
reset_ws() {
  git reset -q --hard "$BASE"
  git clean -qfd
}
both="marketplace-domain marketplace-service"

echo "test: packages: no change selects nothing"
[ -z "$(pkgs)" ] || fail "clean tree: $(pkgs)"

echo "test: packages: a domain change also selects its dependent marketplace-service"
printf '// x\n' >> crates/domain/src/lib.rs
git commit -qam domain
[ "$(pkgs)" = "$both" ] || fail "domain commit: $(pkgs)"
reset_ws

echo "test: packages: a service change selects only the service"
printf '// x\n' >> crates/service/src/lib.rs
git commit -qam service
[ "$(pkgs)" = marketplace-service ] || fail "service commit: $(pkgs)"
reset_ws

echo "test: packages: staged, unstaged and untracked files count"
printf '// x\n' >> crates/domain/src/lib.rs
git add crates/domain/src/lib.rs
[ "$(pkgs)" = "$both" ] || fail "staged: $(pkgs)"
reset_ws
printf '// x\n' >> crates/service/src/lib.rs
[ "$(pkgs)" = marketplace-service ] || fail "unstaged: $(pkgs)"
reset_ws
printf '// new\n' > crates/service/tests/new_test.rs
[ "$(pkgs)" = marketplace-service ] || fail "untracked: $(pkgs)"
reset_ws

echo "test: packages: deletes and both sides of a rename count"
git rm -q crates/service/src/extra.rs
[ "$(pkgs)" = marketplace-service ] || fail "delete: $(pkgs)"
reset_ws
git mv crates/service/src/extra.rs crates/domain/src/moved.rs
[ "$(pkgs)" = "$both" ] || fail "rename: $(pkgs)"
reset_ws

echo "test: packages: a file outside every package selects nothing"
printf 'more\n' >> docs/readme.md
printf 'echo\n' >> scripts/prepush.sh
[ -z "$(pkgs)" ] || fail "docs and scripts: $(pkgs)"
reset_ws

echo "test: packages: a workspace-wide file selects --workspace"
for file in Cargo.toml Cargo.lock contracts/endpoints.json; do
  printf '\n' >> "$file"
  [ "$(pkgs)" = --workspace ] || fail "$file: $(pkgs)"
  reset_ws
done

echo "test: packages: a package manifest without a readable name selects --workspace"
printf '[package]\nversion = "0.1.0"\n' > crates/domain/Cargo.toml
[ "$(pkgs)" = --workspace ] || fail "nameless manifest: $(pkgs)"
reset_ws

echo "test: packages: an unreadable package graph selects --workspace"
printf '// x\n' >> crates/service/src/lib.rs
got="$(PATH="$WORK/broken:$PATH" pkgs 2>/dev/null)"
[ "$got" = --workspace ] || fail "failing cargo metadata: $got"
reset_ws

echo "test: packages: widening is transitive and counts dev-dependencies"
(
  git init -q "$WORK/chain"
  cd "$WORK/chain"
  printf '[workspace]\nresolver = "2"\nmembers = ["a", "b", "c", "d", "e"]\n' > Cargo.toml
  for spec in "a:" "b:a" "c:b" "e:"; do
    name="${spec%%:*}"
    dep="${spec#*:}"
    mkdir -p "$name/src"
    printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\n' "$name" > "$name/Cargo.toml"
    [ -z "$dep" ] || printf '\n[dependencies]\n%s = { path = "../%s" }\n' "$dep" "$dep" >> "$name/Cargo.toml"
    printf '// lib\n' > "$name/src/lib.rs"
  done
  mkdir -p d/src
  printf '[package]\nname = "d"\nversion = "0.1.0"\nedition = "2021"\n\n[dev-dependencies]\na = { path = "../a" }\n' > d/Cargo.toml
  printf '// lib\n' > d/src/lib.rs
  "$REAL_CARGO" generate-lockfile --offline --quiet
  got="$(prepush_with_dependents a | tr '\n' ' ' | sed 's/ $//')"
  [ "$got" = "a b c d" ] || fail "chain from a: $got"
  got="$(prepush_with_dependents c e | tr '\n' ' ' | sed 's/ $//')"
  [ "$got" = "c e" ] || fail "leaves: $got"
)

echo "test: packages: a base git cannot read fails"
if prepush_changed_packages 0000000000000000000000000000000000000001 >/dev/null 2>&1; then
  fail "unknown base returned 0"
fi

echo "test: packages: this repository's graph widens marketplace-domain to marketplace-service"
got="$(cd "$DIR/.." && prepush_with_dependents marketplace-domain | tr '\n' ' ' | sed 's/ $//')"
[ "$got" = "$both" ] || fail "repository graph: $got"

gate() {
  rm -rf "$WORK/log"
  mkdir -p "$WORK/log"
  local st=0
  FAKE_LOG="$WORK/log" PATH="$WORK/bin:$PATH" PREPUSH_BASE="$BASE" \
    HEAVY_LOCK_DIR="$WORK/locks" \
    bash scripts/prepush.sh </dev/null >"$WORK/out" 2>&1 || st=$?
  printf '%s\n' "$st"
}
cargo_log() { cat "$WORK/log/cargo" 2>/dev/null || true; }
expect_ok() {
  [ "$1" = 0 ] || fail "$2: status $1: $(cat "$WORK/out")"
  tail -1 "$WORK/out" | grep -q "^PREPUSH OK $(git rev-parse HEAD) [0-9]* $3$" \
    || fail "$2: last line: $(tail -1 "$WORK/out")"
}
commit_change() {
  reset_ws
  printf '// %s %s\n' "$RANDOM" "$(date +%s)" >> "$1"
  git commit -qam "change $1"
}

echo "test: gate: fast runs fmt and cargo check for widened packages without Docker"
commit_change crates/domain/src/lib.rs
st="$(gate)"
expect_ok "$st" "domain fast" fast
cargo_log | grep -qx 'fmt --check' || fail "domain fast: no fmt: $(cargo_log)"
cargo_log | grep -qx 'check -p marketplace-domain -p marketplace-service --all-targets' \
  || fail "domain fast: check: $(cargo_log)"
if cargo_log | grep -qE '^(clippy|test) '; then
  fail "domain fast ran clippy or tests: $(cargo_log)"
fi
[ ! -e "$WORK/log/docker" ] || fail "domain fast started Docker: $(cat "$WORK/log/docker")"

echo "test: gate: fast runs cargo check only for marketplace-service on service change"
commit_change crates/service/src/lib.rs
st="$(gate)"
expect_ok "$st" "service fast" fast
cargo_log | grep -qx 'fmt --check' || fail "service fast: no fmt: $(cargo_log)"
cargo_log | grep -qx 'check -p marketplace-service --all-targets' \
  || fail "service fast: check: $(cargo_log)"
[ ! -e "$WORK/log/docker" ] || fail "service fast started Docker: $(cat "$WORK/log/docker")"

echo "test: gate: fast runs fmt only when no package changed"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast" fast
[ "$(cargo_log)" = 'fmt --check' ] || fail "docs fast: cargo calls: $(cargo_log)"
[ ! -e "$WORK/log/docker" ] || fail "docs fast started Docker: $(cat "$WORK/log/docker")"

echo "test: gate: fast checks the workspace for a Cargo.lock change"
commit_change Cargo.lock
st="$(gate)"
expect_ok "$st" "lock fast" fast
cargo_log | grep -qx 'fmt --check' || fail "lock fast: no fmt: $(cargo_log)"
cargo_log | grep -qx 'check --workspace --all-targets' \
  || fail "lock fast: check: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=1 runs fmt, clippy, and parallel ci-test.sh via Docker when DATABASE_URL is unset"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast before full" fast
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "docs full" full
grep -q 'reused' "$WORK/out" && fail "full reused the fast stamp: $(cat "$WORK/out")"
cargo_log | grep -qx 'fmt --check' || fail "docs full: no fmt: $(cargo_log)"
cargo_log | grep -qx 'clippy --workspace --all-targets -- -D warnings' \
  || fail "docs full: clippy: $(cargo_log)"
cargo_log | grep -q '^test --workspace --lib --bins --no-fail-fast -- --test-threads=4$' \
  || fail "docs full: lib tests: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --test orders_test --no-fail-fast -- --test-threads=4$' \
  || fail "docs full: parallel integration binaries: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --test refusal_audit_test ' \
  || fail "docs full: serial binaries: $(cargo_log)"
[ -e "$WORK/log/docker" ] || fail "docs full did not start Docker when DATABASE_URL unset"

echo "test: gate: PREPUSH_FULL=1 reuses DATABASE_URL without invoking Docker"
commit_change docs/readme.md
st="$(PREPUSH_FULL=1 DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres' gate)"
expect_ok "$st" "full with DATABASE_URL" full
[ ! -e "$WORK/log/docker" ] || fail "full with DATABASE_URL called Docker: $(cat "$WORK/log/docker")"

echo "test: gate: a fast run after a full pass reuses it"
st="$(gate)"
expect_ok "$st" "fast after full" "fast reused:full .*"
[ -z "$(cargo_log)" ] || fail "fast after full ran cargo: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=yes is refused before any step"
commit_change docs/readme.md
st="$(PREPUSH_FULL=yes gate)"
[ "$st" = 1 ] || fail "PREPUSH_FULL=yes: status $st"
grep -q 'PREPUSH_FULL must be 1, 0 or unset' "$WORK/out" || fail "PREPUSH_FULL=yes: $(cat "$WORK/out")"
[ -z "$(cargo_log)" ] || fail "PREPUSH_FULL=yes ran cargo: $(cargo_log)"

echo "ALL OK"
