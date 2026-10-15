# Shopify test fixtures

There is no Shopify store in CI. The Shopify provider is tested against a local
fake (`tests/common/shopify_fake.rs`) that checks everything the provider sends
against Shopify's published Admin GraphQL schema for API version 2026-10.

* `schema-2026-10.pruned.graphql` is the part of the published schema the
  provider's documents use, generated from
  <https://shopify.dev/admin-graphql-direct-proxy/2026-10> by
  `scripts/shopify-schema-prune.py`. It is the real schema, pruned.
* `responses/*.json` are **spec-derived**: written from the schema to have the
  shape the documents select. They are not captures from a store. Replace them
  with real captures after the first dogfood run against a development store:
  `shop_overview`, `products_page_1`, `products_page_2`,
  `product_variants_page`, `inventory_item`.
* The fake's stateful behaviour (order tags and search, the search-index lag,
  `@idempotent` keys, THROTTLED and 429 answers, webhook subscriptions) is
  likewise derived from Shopify's documentation, not captured.
* Webhook payloads in the tests are written from Shopify's documented webhook
  payload shapes (REST-style JSON bodies with numeric ids), not captured.
