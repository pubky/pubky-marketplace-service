#!/usr/bin/env bash
# Brings the recording rig up and runs the WooCommerce tests against the live
# Docker store, writing the captured fixtures (exchanges/ and deliveries/).
#
#   stack/tools/record.sh            # all recorded tests
#   stack/tools/record.sh flow_test  # one test binary: flow_test | provider_test
#   KEEP=1 keeps the current store; FILTER=name runs the tests matching it
#
# Needs Docker (run as a user that can `sudo docker`, or set WOO_DOCKER).
#
# Layout while recording:
#   :8443  capture proxy  (what the connector talks to; writes exchanges/)
#   :8444  the stack's nginx TLS proxy in front of WordPress
#   :18443 receiver       (wc-auth callbacks and webhook deliveries)
set -euo pipefail
STACK="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURES="$(cd "$STACK/.." && pwd)"
SERVICE="$(cd "$FIXTURES/../../.." && pwd)"
DOCKER="${WOO_DOCKER:-sudo docker}"
RECV=/tmp/woo-recv
cd "$STACK"

[ -f certs/ca.pem ] || ./certs/gen.sh
mkdir -p .local
[ -f .local/admin-password ] || openssl rand -hex 12 > .local/admin-password

echo WOO_PROXY_PORT=8444 > .env
if [ "${KEEP:-0}" != 1 ]; then
  # A clean store, so the webhook ids and orders in the recording are reproducible.
  $DOCKER compose --profile tools down -v
  rm -f .local/admin-key
fi
$DOCKER compose up -d --wait
echo "waiting for WordPress..."
for _ in $(seq 1 60); do
  curl -s --cacert certs/ca.pem -o /dev/null https://localhost:8444/ && break
  sleep 2
done
if ! $DOCKER compose --profile tools run --rm -T cli wp --path=/var/www/html core is-installed 2>/dev/null; then
  $DOCKER compose --profile tools run --rm -T -e "ADMIN_PASSWORD=$(cat .local/admin-password)" cli bash /seed/seed.sh
fi
if [ ! -f .local/admin-key ]; then
  $DOCKER compose --profile tools run --rm -T -v "$STACK/tools/make_admin_key.php:/tmp/make_admin_key.php:ro" cli \
    wp --path=/var/www/html eval-file /tmp/make_admin_key.php \
    | sed -n 's/^KEY://p;s/^SECRET://p' > .local/admin-key
fi

rm -rf "$RECV"
pkill -f tools/receiver.py 2>/dev/null || true
pkill -f tools/capture_proxy.py 2>/dev/null || true
nohup python3 tools/receiver.py "$RECV" 18443 > .local/receiver.log 2>&1 &
mkdir -p "$FIXTURES/exchanges"
nohup python3 tools/capture_proxy.py "$FIXTURES/exchanges" 8443 8444 > .local/capture.log 2>&1 &
sleep 2

cd "$SERVICE"
export WOO_LIVE=1 WOO_RECORD=1 WOO_RECEIVER_DIR="$RECV"
# #[sqlx::test] needs a PostgreSQL (see docker-compose.yml at the repository root).
export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/postgres}"
tests=(connectors_woocommerce_flow_test connectors_woocommerce_provider_test)
case "${1:-}" in
  flow_test) tests=(connectors_woocommerce_flow_test) ;;
  provider_test) tests=(connectors_woocommerce_provider_test) ;;
esac
status=0
for name in "${tests[@]}"; do
  cargo test -p marketplace-service --test "$name" -- ${FILTER:-} --test-threads=1 --nocapture || status=1
done
pkill -f tools/receiver.py || true
pkill -f tools/capture_proxy.py || true
python3 "$STACK/tools/externalize_blobs.py" "$FIXTURES/exchanges"
rm -f "$STACK/.env"
exit "$status"
