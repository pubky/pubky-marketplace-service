#!/bin/sh
set -eu

: "${UPSTREAM_ORIGIN:?UPSTREAM_ORIGIN is required, e.g. http://marketplace-service.railway.internal:8080}"
export PORT="${PORT:-8080}"
export TRUSTED_EDGE_CIDR="${TRUSTED_EDGE_CIDR:-100.64.0.0/10}"

if [ -z "${RESOLVER:-}" ]; then
    RESOLVER="$(awk '$1 == "nameserver" { printf "%s%s", sep, ($2 ~ /:/ ? "[" $2 "]" : $2); sep = " " }' /etc/resolv.conf)"
fi
: "${RESOLVER:?no nameserver found in /etc/resolv.conf}"
export RESOLVER

mkdir -p /tmp/nginx-body /tmp/nginx-proxy /tmp/nginx-fastcgi /tmp/nginx-uwsgi /tmp/nginx-scgi
vars='${PORT} ${UPSTREAM_ORIGIN} ${RESOLVER} ${TRUSTED_EDGE_CIDR}'
envsubst "$vars" < /etc/nginx/edge/proxy.conf.template > /etc/nginx/edge/proxy.conf
envsubst "$vars" < /etc/nginx/edge/nginx.conf.template > /etc/nginx/edge/nginx.conf

nginx -t -c /etc/nginx/edge/nginx.conf
echo "edge: geoip database month $(cat /usr/share/GeoIP/country.mmdb.month)"
exec nginx -g 'daemon off;' -c /etc/nginx/edge/nginx.conf
