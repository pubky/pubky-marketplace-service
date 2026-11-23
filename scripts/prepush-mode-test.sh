#!/usr/bin/env bash
# Proves the fast and full modes of scripts/prepush.sh run the right steps,
# and that scripts/prepush-packages.sh maps a change to its packages.
# Runs the real scripts in a throwaway workspace. cargo and docker are
# stand-ins on PATH that log their arguments; nothing is built or started.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/prepush-mode-test.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
# A caller's CARGO_TARGET_DIR would make the gate seed its private target by
# copying that directory; a caller's override would replace every case's target.
unset CARGO_TARGET_DIR
unset PREPUSH_CARGO_TARGET
unset PREPUSH_FULL

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

REAL_CARGO="$(command -v cargo)"
mkdir -p "$WORK/bin"
# metadata goes to the real cargo, so package widening reads a real graph.
cat > "$WORK/bin/cargo" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = metadata ]; then
  if [ "${FAKE_METADATA_NO_DEPS_FAIL:-0}" = 1 ]; then
    for arg in "$@"; do
      [ "$arg" != --no-deps ] || exit 101
    done
  fi
  exec "$REAL_CARGO" "$@"
fi
printf '%s\n' "$*" >> "$FAKE_LOG/cargo"
if [ "${1:-}" = clean ]; then
  if [ "${FAKE_CLEAN_FAIL:-0}" = 1 ]; then
    echo 'fake cargo: clean failed' >&2
    exit 101
  fi
  exec "$REAL_CARGO" "$@"
fi
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

# The real gate. Each case commits one change on top of the base, so the
# selection is that change alone and no stamp is reused.
gate() {
  rm -rf "$WORK/log"
  mkdir -p "$WORK/log"
  local st=0
  FAKE_LOG="$WORK/log" PATH="$WORK/bin:$PATH" PREPUSH_BASE="$BASE" \
    PREPUSH_CARGO_TARGET="${PREPUSH_CARGO_TARGET:-$WORK/target}" HEAVY_LOCK_DIR="$WORK/locks" \
    HEAVY_DISK_VOLUMES="" bash scripts/prepush.sh </dev/null >"$WORK/out" 2>&1 || st=$?
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
expected_clean='clean -p marketplace-domain -p marketplace-service'
actual_members="$("$REAL_CARGO" metadata --no-deps --format-version 1 --locked | jq -r '.packages[].name')"
[ "$actual_members" = $'marketplace-domain\nmarketplace-service' ] \
  || fail "workspace members changed; update the literal clean expectation: $actual_members"

echo "test: gate: a seeded target cleans every workspace member before the gate"
commit_change crates/domain/src/lib.rs
mkdir -p "$WORK/shared-target/debug/.fingerprint/marketplace-domain-deadbeef" \
  "$WORK/shared-target/debug/deps"
printf 'registry artifact\n' > "$WORK/shared-target/artifact"
printf 'fingerprint\n' > "$WORK/shared-target/debug/.fingerprint/marketplace-domain-deadbeef/lib-marketplace-domain.json"
printf 'member\n' > "$WORK/shared-target/debug/deps/libmarketplace_domain-deadbeef.rlib"
printf 'member\n' > "$WORK/shared-target/debug/deps/marketplace_service-deadbeef.d"
printf 'registry\n' > "$WORK/shared-target/debug/deps/libserde-deadbeef.rlib"
st="$(CARGO_TARGET_DIR="$WORK/shared-target" PREPUSH_CARGO_TARGET="$WORK/seeded-target" gate)"
expect_ok "$st" "seeded target" fast
[ -f "$WORK/seeded-target/artifact" ] || fail "seeded target: shared artifact was not copied"
[ ! -e "$WORK/seeded-target/debug/.fingerprint/marketplace-domain-deadbeef" ] \
  || fail "seeded target: domain fingerprint was not cleaned"
[ ! -e "$WORK/seeded-target/debug/deps/libmarketplace_domain-deadbeef.rlib" ] \
  || fail "seeded target: domain rlib was not cleaned"
[ ! -e "$WORK/seeded-target/debug/deps/marketplace_service-deadbeef.d" ] \
  || fail "seeded target: service dep-info was not cleaned"
[ -f "$WORK/seeded-target/debug/deps/libserde-deadbeef.rlib" ] \
  || fail "seeded target: registry artifact was cleaned"
cargo_log | grep -Fqx "$expected_clean" || fail "seeded target: expected $expected_clean: $(cargo_log)"
[ "$(cargo_log | sed -n '1p')" = "$expected_clean" ] \
  || fail "seeded target: member clean did not precede fmt/clippy/test: $(cargo_log)"
[ -f "$WORK/seeded-target/.prepush-members-clean" ] || fail "seeded target: no clean marker"
[ "$(cat "$WORK/seeded-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "seeded target: marker has the wrong root"

echo "test: gate: an existing unmarked target survives a failed clean"
mkdir -p "$WORK/uncleanable-target"
printf 'keep\n' > "$WORK/uncleanable-target/sentinel"
commit_change crates/domain/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/uncleanable-target" FAKE_CLEAN_FAIL=1 gate)"
[ "$st" -ne 0 ] || fail "failed existing-target clean returned zero: $(cat "$WORK/out")"
[ -d "$WORK/uncleanable-target" ] || fail "failed existing-target clean deleted the directory"
[ -f "$WORK/uncleanable-target/sentinel" ] || fail "failed existing-target clean deleted the sentinel"
grep -Fqx "prepush: cannot clean workspace members in $WORK/uncleanable-target; delete it or set PREPUSH_CARGO_TARGET to a new path" "$WORK/out" \
  || fail "failed existing-target clean: wrong error: $(cat "$WORK/out")"
grep -q '^fake cargo: clean failed$' "$WORK/out" \
  || fail "failed existing-target clean hid cargo stderr: $(cat "$WORK/out")"

echo "test: gate: a marker copied from the shared target does not skip the seeded clean"
mkdir -p "$WORK/marked-shared-target"
printf '%s\n' "$PWD" > "$WORK/marked-shared-target/.prepush-members-clean"
commit_change crates/service/src/lib.rs
st="$(CARGO_TARGET_DIR="$WORK/marked-shared-target" PREPUSH_CARGO_TARGET="$WORK/copied-marker-target" gate)"
expect_ok "$st" "copied marker" fast
[ "$(cargo_log | grep -Fxc "$expected_clean" || true)" = 1 ] \
  || fail "copied marker skipped the seeded clean: $(cargo_log)"
[ "$(cat "$WORK/copied-marker-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "copied marker: marker has the wrong root"

echo "test: gate: a brand-new target is marked without a clean"
commit_change crates/service/src/lib.rs
st="$(gate)"
expect_ok "$st" "brand-new target" fast
if cargo_log | grep -q '^clean '; then
  fail "brand-new target was cleaned: $(cargo_log)"
fi
[ "$(cat "$WORK/target/.prepush-members-clean")" = "$PWD" ] \
  || fail "brand-new target: marker has the wrong root"

echo "test: gate: a marker from another root causes one clean and is updated"
mkdir -p "$WORK/other-root-target"
printf '/another/worktree\n' > "$WORK/other-root-target/.prepush-members-clean"
commit_change crates/domain/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/other-root-target" gate)"
expect_ok "$st" "other-root marker" fast
[ "$(cargo_log | grep -Fxc "$expected_clean" || true)" = 1 ] \
  || fail "other-root marker: expected one clean: $(cargo_log)"
[ "$(cat "$WORK/other-root-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "other-root marker was not updated"

echo "test: gate: an unmarked target is cleaned once"
mkdir -p "$WORK/legacy-target"
commit_change crates/domain/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/legacy-target" gate)"
expect_ok "$st" "unmarked target" fast
[ "$(cargo_log | grep -Fxc "$expected_clean" || true)" = 1 ] \
  || fail "unmarked target: expected one member clean: $(cargo_log)"
[ "$(cat "$WORK/legacy-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "unmarked target: marker has the wrong root"
commit_change crates/service/src/lib.rs
st="$(PREPUSH_CARGO_TARGET="$WORK/legacy-target" gate)"
expect_ok "$st" "marked target" fast
if cargo_log | grep -q '^clean '; then
  fail "marked target cleaned again: $(cargo_log)"
fi

echo "test: gate: a seeded target with a failed clean falls back to an empty target"
mkdir -p "$WORK/failing-shared-target"
printf 'discard\n' > "$WORK/failing-shared-target/artifact"
commit_change crates/domain/src/lib.rs
st="$(CARGO_TARGET_DIR="$WORK/failing-shared-target" PREPUSH_CARGO_TARGET="$WORK/failing-seeded-target" \
  FAKE_CLEAN_FAIL=1 gate)"
expect_ok "$st" "seeded clean failure" fast
[ -d "$WORK/failing-seeded-target" ] || fail "seeded clean failure: no fallback directory"
[ ! -e "$WORK/failing-seeded-target/artifact" ] || fail "seeded clean failure retained the seeded artifact"
[ "$(cat "$WORK/failing-seeded-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "seeded clean failure: fallback marker has the wrong root"
grep -q '^prepush: could not clean workspace members; using an empty target$' "$WORK/out" \
  || fail "seeded clean failure: no fallback explanation: $(cat "$WORK/out")"
grep -q '^fake cargo: clean failed$' "$WORK/out" \
  || fail "seeded clean failure hid cargo stderr: $(cat "$WORK/out")"

echo "test: gate: a member metadata failure falls back to an empty target"
commit_change crates/domain/src/lib.rs
st="$(CARGO_TARGET_DIR="$WORK/shared-target" PREPUSH_CARGO_TARGET="$WORK/failed-target" \
  FAKE_METADATA_NO_DEPS_FAIL=1 gate)"
expect_ok "$st" "metadata failure" fast
[ ! -e "$WORK/failed-target/artifact" ] || fail "metadata failure retained the seeded artifact"
[ "$(cat "$WORK/failed-target/.prepush-members-clean")" = "$PWD" ] \
  || fail "metadata failure: fallback marker has the wrong root"
grep -q '^prepush: could not clean workspace members; using an empty target$' "$WORK/out" \
  || fail "metadata failure: no fallback explanation: $(cat "$WORK/out")"

echo "test: gate: fast widens a domain change to the service and its integration binaries"
commit_change crates/domain/src/lib.rs
st="$(gate)"
expect_ok "$st" "domain fast" fast
cargo_log | grep -qx 'fmt --check' || fail "domain fast: no fmt: $(cargo_log)"
cargo_log | grep -qx 'clippy -p marketplace-domain -p marketplace-service --all-targets -- -D warnings' \
  || fail "domain fast: clippy: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-domain -p marketplace-service --jobs 1 --lib --bins ' \
  || fail "domain fast: lib tests: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --test orders_test ' \
  || fail "domain fast: integration binaries: $(cargo_log)"
if cargo_log | grep -q -- '--workspace'; then
  fail "domain fast used --workspace: $(cargo_log)"
fi

echo "test: gate: fast runs the integration binaries for a service change"
commit_change crates/service/src/lib.rs
st="$(gate)"
expect_ok "$st" "service fast" fast
cargo_log | grep -qx 'clippy -p marketplace-service --all-targets -- -D warnings' \
  || fail "service fast: clippy: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --lib --bins ' \
  || fail "service fast: lib tests: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --test orders_test ' \
  || fail "service fast: parallel binaries: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --test refusal_audit_test --test ' \
  || fail "service fast: serial binaries: $(cargo_log)"
if cargo_log | grep -q -- '--workspace'; then
  fail "service fast used --workspace: $(cargo_log)"
fi

echo "test: gate: fast runs fmt only when no package changed"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast" fast
[ "$(cargo_log)" = 'fmt --check' ] || fail "docs fast: cargo calls: $(cargo_log)"
[ ! -e "$WORK/log/docker" ] || fail "docs fast started Postgres: $(cat "$WORK/log/docker")"

echo "test: gate: fast runs the workspace for a Cargo.lock change"
commit_change Cargo.lock
st="$(gate)"
expect_ok "$st" "lock fast" fast
cargo_log | grep -qx 'clippy --workspace --all-targets -- -D warnings' \
  || fail "lock fast: clippy: $(cargo_log)"
cargo_log | grep -q '^test --workspace --jobs 1 --lib --bins ' \
  || fail "lock fast: tests: $(cargo_log)"

echo "test: gate: PREPUSH_FULL=1 runs the whole workspace for a docs-only change"
commit_change docs/readme.md
st="$(gate)"
expect_ok "$st" "docs fast before full" fast
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "docs full" full
grep -q 'reused' "$WORK/out" && fail "full reused the fast stamp: $(cat "$WORK/out")"
cargo_log | grep -qx 'clippy --workspace --all-targets -- -D warnings' \
  || fail "docs full: clippy: $(cargo_log)"
cargo_log | grep -q '^test --workspace --jobs 1 --lib --bins ' \
  || fail "docs full: lib tests: $(cargo_log)"
cargo_log | grep -q '^test -p marketplace-service --jobs 1 --test orders_test ' \
  || fail "docs full: integration binaries: $(cargo_log)"

echo "test: gate: a fast run after a full pass reuses it"
commit_change docs/readme.md
st="$(PREPUSH_FULL=1 gate)"
expect_ok "$st" "full before fast" full
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
