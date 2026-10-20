# Railway edge: trusted `X-Client-Region` for marketplace-service

The GCP deployment gets the client's country from the edge: nginx (shop-migration `docs/geo-block/client-region-header.conf`)
or the load balancer sets `X-Client-Region`, replacing anything the client sent, and the service port is reachable only from
that edge (shop-migration #42). The service itself checks no secret: `digital_region.rs` trusts the header named by
`CLIENT_COUNTRY_HEADER` (default `x-client-region`), so the trust is the network path.

Railway has no country header, and its edge passes a client-sent `X-Client-Region` straight through (measured on the John Prod
project: a request carrying `X-Client-Region: CH` reached the container with `CH`). This directory is the Railway equivalent of
the GCP edge, with no service code change:

```text
browser ──> marketplace-edge (public Railway domain, nginx + GeoIP) ──private network──> marketplace-service (no public domain)
```

- nginx resolves the country from the client address and sets `X-Client-Region` from it. `proxy_set_header` replaces every
  client copy (any casing, any count); an unknown country sends no header, so the service's digital hold refuses digital
  lines (`buyer_region_unknown`) instead of trusting a client value.
- The client address comes from `X-Real-IP`, which the Railway edge overwrites with the connecting address, and is trusted
  only from `100.64.0.0/10`, the address space the edge connects from. The proxy forwards that address as `X-Real-IP` and as a
  single-value `X-Forwarded-For`, so `ADDRESS_SEARCH_CLIENT_IP_HEADER` and `LISTING_AVAILABILITY_CLIENT_IP_HEADER` keep working
  with either header name.
- The same rule as the GCP edge blocks CU, IR, KP, SY, RU and BY with `403` (`g1-block.conf`, copied from shop-migration
  `docs/geo-block`).
- The service must have no public domain. With one, a client can reach the service directly and send the header itself.

## Variables (marketplace-edge)

| Name | Value |
| --- | --- |
| `UPSTREAM_ORIGIN` | `http://marketplace-service.railway.internal:<service PORT>` |
| `PORT` | listen port, also the public domain's target port |
| `TRUSTED_EDGE_CIDR` | optional, default `100.64.0.0/10` |
| `RESOLVER` | optional, default the nameserver in `/etc/resolv.conf` |

Point everything that used the service's public origin at the edge's: the Shop's `MARKETPLACE_SERVICE_URL` and
`PUBKY_RUNTIME_MARKETPLACE_URL`, and the service's `PUBLIC_SERVICE_ORIGIN` (the grant-session proof audience and the PayPal IPN
`notify_url`) and `CONNECTOR_PUBLIC_BASE_URL`.

## Deploy

`railway up` needs a directory that is not inside a git checkout:

```sh
rm -rf /tmp/edge-deploy && cp -R deploy/railway-edge /tmp/edge-deploy && rm -rf /tmp/edge-deploy/tests
cd /tmp/edge-deploy && railway up -s <marketplace-edge service id> -e <environment id> -p <project id> --ci
```

The image downloads the DB-IP Lite country database when it is built (current month, previous month as fallback) and the
build fails unless the file answers 8.8.8.8 with `US` and 5.9.0.1 with `DE`. Redeploy about monthly to refresh it.
The database is by [DB-IP](https://db-ip.com), licensed CC BY 4.0. It is not MaxMind's, so borderline addresses can resolve
differently from the GCP edge.

## Tests

`sh deploy/railway-edge/tests/test.sh` runs the same `proxy.conf.template` on a local nginx (realip module required) with the
country read from an `X-Test-Country` header, in front of a stand-in service that reports the headers it receives: 19 checks
(country passed through, client-sent `X-Client-Region` single, duplicate and lower-case replaced, nothing sent for an unknown
country, `X-Real-IP` honoured only from the trusted range, G1 `403`s). Removing the `X-Client-Region` line fails 6 of them.
The GeoIP lookup itself is covered by the image build check and the live checks below.

Live checks (no payment): from a US source and an EU source, request through the edge to an echo upstream and read the
headers it receives, with and without a spoofed `X-Client-Region`.

## Rollback

Point the Shop and service variables back at a service origin that has a public domain, redeploy, and delete the edge
service. Removing the service's public domain is the step that cannot be undone exactly: a new Railway domain gets a new name.
