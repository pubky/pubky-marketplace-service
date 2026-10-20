#!/bin/sh
# Runs proxy.conf.template on a local nginx with the country read from the X-Test-Country request
# header (the GeoIP lookup itself is exercised by the image build and the live checks) in front of
# a stand-in service that reports the headers it receives.
# Usage: sh deploy/railway-edge/tests/test.sh      (needs nginx with the realip module, python3, curl)
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
edge="$(cd "$here/.." && pwd)"
work="$(mktemp -d)"
EDGE_PORT=18080
UP_PORT=18081
nginx_pid=""
up_pid=""
fail=0
total=0
cleanup() {
    [ -n "$nginx_pid" ] && kill "$nginx_pid" 2>/dev/null || true
    [ -n "$up_pid" ] && kill "$up_pid" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

check() {
    total=$((total + 1))
    if [ "$2" = "$3" ]; then
        printf 'ok   %s\n' "$1"
    else
        printf 'FAIL %s\n       want: %s\n       got:  %s\n' "$1" "$3" "$2"
        fail=$((fail + 1))
    fi
}

start_edge() {
    cidr="$1"
    [ -n "$nginx_pid" ] && { kill "$nginx_pid" 2>/dev/null || true; wait "$nginx_pid" 2>/dev/null || true; }
    PORT=$EDGE_PORT UPSTREAM_ORIGIN="http://127.0.0.1:$UP_PORT" RESOLVER=127.0.0.1 TRUSTED_EDGE_CIDR="$cidr" \
        envsubst '${PORT} ${UPSTREAM_ORIGIN} ${RESOLVER} ${TRUSTED_EDGE_CIDR}' \
        < "$edge/proxy.conf.template" > "$work/proxy.conf"
    cp "$edge/g1-block.conf" "$work/g1-block.conf"
    sed -i.bak "s#/etc/nginx/edge/g1-block.conf#$work/g1-block.conf#" "$work/proxy.conf"
    cat > "$work/nginx.conf" <<CONF
worker_processes 1;
pid $work/nginx.pid;
error_log $work/error.log;
events { worker_connections 64; }
http {
    client_body_temp_path $work/body;
    proxy_temp_path $work/proxy;
    fastcgi_temp_path $work/fcgi;
    uwsgi_temp_path $work/uwsgi;
    scgi_temp_path $work/scgi;
    map \$http_x_test_country \$geoip2_country_code { default \$http_x_test_country; }
    include $work/proxy.conf;
}
CONF
    nginx -t -c "$work/nginx.conf" >/dev/null 2>&1 || { nginx -t -c "$work/nginx.conf"; exit 1; }
    nginx -c "$work/nginx.conf" -g 'daemon off;' >/dev/null 2>&1 &
    nginx_pid=$!
    sleep 1
}

python3 "$here/upstream.py" "$UP_PORT" &
up_pid=$!
sleep 1

seen() {
    curl -s "http://127.0.0.1:$EDGE_PORT/v0/ping?x=1" "$@" | python3 -c '
import json, sys
h = json.load(sys.stdin)["headers"]
want = sys.argv[1]
print(",".join(v for k, v in h if k == want) or "-")' "$HDR"
}

start_edge 100.64.0.0/10

HDR=x-client-region
check "country passed through" "$(seen -H 'X-Test-Country: DE')" "DE"
check "US passed through" "$(seen -H 'X-Test-Country: US')" "US"
check "client X-Client-Region replaced by the edge country" "$(seen -H 'X-Test-Country: DE' -H 'X-Client-Region: CH')" "DE"
check "duplicate client headers replaced" "$(seen -H 'X-Test-Country: GB' -H 'X-Client-Region: CH' -H 'X-Client-Region: US')" "GB"
check "lower-case client header replaced" "$(seen -H 'X-Test-Country: FR' -H 'x-client-region: US')" "FR"
check "unknown country sends no header" "$(seen)" "-"
check "client header dropped when the country is unknown" "$(seen -H 'X-Client-Region: CH')" "-"

HDR=x-forwarded-proto
check "client X-Forwarded-Proto replaced" "$(seen -H 'X-Forwarded-Proto: http')" "https"
HDR=x-real-ip
check "untrusted peer: X-Real-IP is the socket address" "$(seen -H 'X-Real-IP: 1.2.3.4')" "127.0.0.1"
HDR=x-forwarded-for
check "untrusted peer: client X-Forwarded-For dropped" "$(seen -H 'X-Forwarded-For: 5.6.7.8' -H 'X-Real-IP: 1.2.3.4')" "127.0.0.1"

code="$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Test-Country: IR' "http://127.0.0.1:$EDGE_PORT/v0/ping")"
check "sanctioned country (IR) answers 403" "$code" "403"
for c in CU KP SY RU BY; do
    code="$(curl -s -o /dev/null -w '%{http_code}' -H "X-Test-Country: $c" "http://127.0.0.1:$EDGE_PORT/v0/ping")"
    check "sanctioned country ($c) answers 403" "$code" "403"
done
code="$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Test-Country: DE' "http://127.0.0.1:$EDGE_PORT/v0/ping")"
check "EU country is not blocked by G1" "$code" "200"
code="$(curl -s -o /dev/null -w '%{http_code}' -H 'X-Test-Country: IR' "http://127.0.0.1:$EDGE_PORT/_edge/health")"
check "G1 applies to the health path too" "$code" "403"
code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$EDGE_PORT/_edge/health")"
check "health answers 200" "$code" "200"

start_edge 127.0.0.0/8
HDR=x-real-ip
check "trusted edge: X-Real-IP is the client address the edge reported" "$(seen -H 'X-Real-IP: 1.2.3.4')" "1.2.3.4"
check "trusted edge: duplicate X-Real-IP, nginx takes the first (Railway overwrites it; checked live)" "$(seen -H 'X-Real-IP: 1.2.3.4' -H 'X-Real-IP: 9.9.9.9')" "1.2.3.4"
HDR=x-forwarded-for
check "trusted edge: forwarded single client address" "$(seen -H 'X-Forwarded-For: 5.6.7.8' -H 'X-Real-IP: 1.2.3.4')" "1.2.3.4"

mkdir -p "$work/bin"
cat > "$work/bin/mmdblookup" <<'FAKE'
#!/bin/sh
while [ $# -gt 0 ]; do [ "$1" = "--ip" ] && ip="$2"; shift; done
cc="$(sed -n "s/^$ip \([A-Z][A-Z]\)$/\1/p" "$FAKE_DB")"
printf '\n  "%s" <utf8_string>\n\n' "$cc"
FAKE
chmod +x "$work/bin/mmdblookup"
awk '/^PROBES$/{p=0} p&&/^[0-9]/{print} /<<PROBES/{p=1}' "$edge/geoip-selftest.sh" > "$work/good.db"
sed 's/^77.88.55.60 RU$/77.88.55.60 US/' "$work/good.db" > "$work/bad.db"
check "self-test accepts a database that maps every probe correctly" "$(PATH="$work/bin:$PATH" FAKE_DB="$work/good.db" sh "$edge/geoip-selftest.sh" /dev/null >/dev/null 2>&1 && echo 0 || echo 1)" "0"
check "self-test rejects a database that maps a G1 address elsewhere" "$(PATH="$work/bin:$PATH" FAKE_DB="$work/bad.db" sh "$edge/geoip-selftest.sh" /dev/null >/dev/null 2>&1 && echo 0 || echo 1)" "1"

printf '\n%s checks, %s failed\n' "$total" "$fail"
[ "$fail" -eq 0 ]
