# WooCommerce fixtures

Everything under `exchanges/` and `deliveries/` was captured from a real
WooCommerce running in Docker on 15 October 2026, not written by hand. CI
replays it; nothing here needs Docker.

| Component | Version |
|---|---|
| WooCommerce | **11.2.1** |
| WordPress | **7.1.3** (WooCommerce 11.2.1 declares "requires WordPress 7.0"; it will not install on 6.9) |
| PHP | 8.3.35 (`wordpress:apache`, Apache 2.4.68) |
| MariaDB | 11.8.9 |
| TLS proxy | nginx 1.31.7, certificate from a local CA |
| Shipment tracking | Advanced Shipment Tracking for WooCommerce 4.0.4 (writes `_wc_shipment_tracking_items`) |
| Store | currency CHF, Swiss VAT 8.1% switched on with prices entered including tax and the shipping tax class set to Standard (pushed orders must still total what the buyer paid), WooCommerce's own sample products (created row by row from `sample-data/sample_products.csv`) plus a variable product *Little Hodler Socks* (S, M, L, in CHF, per-variation stock 5, 3, 0, two images, category Socks, a Material attribute that does not pick a variant) and a simple *Pubky Test Item* (CHF 12.50, 10 in stock, category Accessories, Material Cotton) |

## What is captured

* `exchanges/<scenario>.json`: every request the connector sent to the store in
  one scenario and the store's answer (status, a few headers, body). Large
  bodies (WordPress's REST index is over a megabyte) are stored once in
  `exchanges/_blobs/` and referenced as `{"$blob": "<id>"}`.
* `deliveries/<scenario>.<name>.json`: what WooCommerce sent outward, byte for
  byte: the `wc-auth` key callback (`*.callback`, with the key pair replaced
  by fixed values, which the replay never compares) and webhook deliveries
  (headers and raw body, with the throwaway webhook secret of the recording
  run, so a test can check a real signature).

Scenarios (`p_` = provider only, `flow_` = through the service and PostgreSQL):

| Scenario | What it shows |
|---|---|
| `p_connect` | probe, wc-auth callback, auth-mode check and write check (nothing is created by connecting), webhook registration at the connection's own URL, a second registration replacing that connection's earlier webhooks, disconnect |
| `p_webhook_isolation` | two connections of one store and a failed registration: replacing, a failed registration and disconnecting one connection never delete the other's webhooks |
| `p_webhook_pages` | the webhook list read across pages of two |
| `p_refund_pages` | an order's refunds read across pages of two: a repeated refund finds its marker on the last page and writes nothing |
| `p_duplicate_retry` | a duplicate order whose cancel fails once is cancelled by the next lookup; the order search read across pages of one |
| `p_stripped_header` | the proxy drops `Authorization`: Basic gets 401, the query string works |
| `p_bad_keys` | a key pair the store does not know: both ways answer 401 |
| `p_read_only` | a `scope=read` key: WooCommerce's 401 "does not have write permissions" |
| `p_refused` | plain permalinks (`/wp-json/` redirects to itself, `/?rest_route=/` works), WooCommerce deactivated (no `wc/v3` namespace) |
| `p_rotate_other_store` | valid keys for a connection whose store answers at another address: `wrong_store`, and no key is sent |
| `p_home_mismatch` | the store answers at an address other than its WordPress address |
| `p_catalog` | the full catalog in pages of seven, variations, stock, images |
| `p_orders` | order create (with the opted-in address), find by id, cancel (and again), refunds mirrored as the difference from what the store shows refunded (a later larger record adds only the rest, a smaller one nothing, one above the payment is capped, a merchant's own refund counts), each with `api_refund: false` |
| `flow_a` | connect, catalog, selection, mapping, paid order pushed, stock poll, stock webhook, completed-with-tracking webhook, ping, bad signature, disconnect |
| `flow_b` | a cancel and a trash in the store raise one alert and refund nobody |
| `flow_c` | a Pubky cancel cancels the store order; its webhook echo raises no alert |
| `flow_d` | a Pubky refund is recorded in the store; its webhook echo raises no alert |
| `flow_e` | pasted keys: wrong shape, wrong keys, another seller's connection, right keys, repeat |
| `flow_f` | WooCommerce inactive: six refused connects leave no draft behind (the limit is five), a restart reuses its draft |
| `flow_i` | replace credentials on a connected store: wrong shape, unknown keys, a read-only key and another seller change nothing; a new pair swaps keys, secret and webhooks; a store needing attention is recovered; a disconnected one is refused |
| `flow_h` | completed without tracking, tracking added later, tracking changed (the newest item counts), a repeat |
| `flow_j` | a second seller approving keys for a connected store is refused (`shop_in_use`) and creates and deletes no webhook |
| `flow_k` | a disconnect while new keys are being verified goes through at once; the keys are not stored; no webhook is left |
| `flow_l` | a disconnect between the credential swap and the webhook registration: the late registration removes what it created |
| `flow_m` | two replacements of the keys: the slow one writes nothing (`credentials_changed`) and the store's webhooks carry the winner's secret |
| `flow_n` | a variable product on a listing with per-variant rows: the stock poll lands each variation on its own variant row (judged against that row, not the listing's total), and a single mapped variation of the keyed listing still addresses its own row, with no alert |
| `flow_g` | the Shop's `PUT /credentials` form with `consumer_key` and `consumer_secret` |

## How a replay works

`tests/woocommerce_support/replay.rs` is a small HTTP server that answers each
request with the recorded answer for the same method, path, query and JSON
body. Repeated requests consume the recorded answers in order, so a catalog
read before and after an order gets the stock WooCommerce reported at each
point. It also checks how the request authenticated (Basic header, query
string, none). Differences between the recording and a replay are normalised on
both sides and nowhere else:

* the origin (`https://localhost:8443` becomes the replay server);
* the webhook secret in a registration body (random per connection);
* the connection id in a webhook's delivery URL, learned from the requests that
  carry it and applied to every later answer (a webhook list names it);
* UUIDs and RFC 3339 instants in request bodies and queries (the Pubky order
  id and clock of the test); a recorded UUID in an answer is replaced by the one
  the request carried;
* the signature and `X-WC-Webhook-Source` of a replayed webhook delivery are
  made for this run, since the recorded signature belongs to the recording
  run's secret (`a_real_webhook_signature_verifies_and_a_changed_body_does_not`
  checks a real WooCommerce signature against its recorded secret).

The replay server speaks plain HTTP on loopback, through
`WooOptions::local_testing`. That constructor exists for tests only: nothing
reads it from the environment, and the default options refuse plain HTTP and
any address that is not a public domain name. The live run goes over HTTPS and
trusts the local CA.

## Recording again

Needs Docker and a PostgreSQL for the `#[sqlx::test]` databases
(`DATABASE_URL`, default `postgres://postgres:postgres@localhost:5432/postgres`,
`max_connections` above 100). On a VM where containers cannot reach each other,
`sudo iptables -P FORWARD ACCEPT` (and the `iptables-legacy` one) fixes it.

```sh
crates/service/tests/fixtures/woocommerce/stack/tools/record.sh
```

It generates the local CA, builds a fresh store (WordPress, WooCommerce, the
sample catalogue, the tracking plugin), starts a receiver on `:18443` for
callbacks and webhooks and a capture proxy on `:8443` in front of the stack's
nginx, and runs the same test files with `WOO_LIVE=1 WOO_RECORD=1`. Those
tests drive the store as a merchant would: approving `wc-auth` in WP admin,
changing stock, adding tracking, completing, cancelling and deleting orders,
turning permalinks, WooCommerce and the `Authorization` header off and on.
`KEEP=1` keeps the current store; `FILTER=name` runs matching tests.

## What a pilot store needs (found while recording)

* WordPress 7.0 or newer for WooCommerce 11.x; PHP 7.4 or newer.
* HTTPS with a certificate a public CA signed. `wc-auth` refuses a `callback_url`
  that is not HTTPS, and the connector refuses plain-HTTP sites.
* "Post name" (or any pretty) permalinks. With plain permalinks WordPress
  answers `/wp-json/` by redirecting to itself and `wc-auth` has no endpoint.
* The store address the seller enters must be WordPress's own (`home`); the
  webhooks name the store by it.
* The `Authorization` header reaching PHP, or accept the query-string fallback
  (recorded in `p_stripped_header`). In the fixture stack, WordPress's own
  `.htaccess` rule forwards the header; nginx dropping it reproduces the
  failure.
* WooCommerce's webhook delivery runs on Action Scheduler, which runs on
  WP-Cron. A store with cron disabled delivers late or never, so the connector
  also polls stock every 15 minutes. The stack disables cron and runs
  `wp action-scheduler run` to deliver.
* `callback_url` and `delivery_url` must be reachable from the store; WooCommerce
  refuses private hosts and unusual ports unless a plugin allows them (the
  stack's `mu-plugins/pubky-test-network.php`, test only).
