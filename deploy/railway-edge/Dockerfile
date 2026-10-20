FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        nginx libnginx-mod-http-geoip2 libmaxminddb0 mmdb-bin gettext-base curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# DB-IP Lite country database (CC BY 4.0, https://db-ip.com), current month with the previous month
# as fallback. The build stops unless the file answers a known address.
RUN set -eu; \
    mkdir -p /usr/share/GeoIP; \
    for month in "$(date -u +%Y-%m)" "$(date -u -d '-1 month' +%Y-%m)"; do \
        if curl -fsSL --retry 3 "https://download.db-ip.com/free/dbip-country-lite-${month}.mmdb.gz" -o /tmp/country.mmdb.gz; then \
            gunzip -c /tmp/country.mmdb.gz > /usr/share/GeoIP/country.mmdb; \
            echo "${month}" > /usr/share/GeoIP/country.mmdb.month; \
            break; \
        fi; \
    done; \
    rm -f /tmp/country.mmdb.gz; \
    test -s /usr/share/GeoIP/country.mmdb; \
    mmdblookup --file /usr/share/GeoIP/country.mmdb --ip 8.8.8.8 country iso_code | grep -q '"US"'; \
    mmdblookup --file /usr/share/GeoIP/country.mmdb --ip 5.9.0.1 country iso_code | grep -q '"DE"'

COPY nginx.conf.template /etc/nginx/edge/nginx.conf.template
COPY proxy.conf.template /etc/nginx/edge/proxy.conf.template
COPY g1-block.conf /etc/nginx/edge/g1-block.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
