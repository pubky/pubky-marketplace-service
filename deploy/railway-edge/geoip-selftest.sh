#!/bin/sh
# Build-time check: the country database must map the addresses below to the stated countries.
# These are the mappings the edge acts on: the G1 block list, the EU/GB digital hold, and the US allow case.
set -eu
db="${1:?usage: geoip-selftest.sh <country.mmdb>}"
failed=0
while read -r ip want; do
    got="$(mmdblookup --file "$db" --ip "$ip" country iso_code | sed -n 's/^ *"\([A-Z][A-Z]\)".*/\1/p')"
    if [ "$got" != "$want" ]; then
        echo "geoip self-test: $ip expected $want, got '${got}'" >&2
        failed=1
    fi
done <<PROBES
8.8.8.8 US
5.9.0.1 DE
62.210.0.1 FR
193.0.14.129 NL
195.20.0.1 PL
81.2.69.142 GB
77.88.55.60 RU
5.160.139.1 IR
152.206.0.1 CU
175.45.176.1 KP
82.137.200.1 SY
213.184.231.1 BY
PROBES
[ "$failed" = 0 ] || exit 1
echo "geoip self-test: all mappings match"
