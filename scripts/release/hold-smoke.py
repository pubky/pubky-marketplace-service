#!/usr/bin/env python3
"""Staging hold-smoke for marketplace-service cutover.

Canonical copy: pubky/pubky-marketplace-service
`scripts/release/hold-smoke.py`. The release skill points here; do not keep
a second live copy under .evidence/.

Exercises checkout.create with the deployed Shop v0.6.25 wire shape
(shop-v0.6.25 @ 5e7fc0e6433e8535ca0444607e7864d79ff4f73c, checkout wire
unchanged since v0.6.22): the v0.6.17 keys
plus the line's `variant_id`, which the Shop sends for every line whose
listing variant it resolved. Shipped checkouts carry sealed delivery
addresses as the current Shop sends them (Shop master deb3c7f7,
`sealDeliveryAddressForSellers`): one RFC 9180 HPKE envelope per shipped
seller in `delivery_address_envelopes`, which is what a service running
REQUIRE_ENCRYPTED_DELIVERY_ADDRESS=true accepts. Hold semantics are
Option B:

  (0) a plaintext `delivery_address` is refused 422
      `encrypted_delivery_required` and creates no order (proves the flag
      is on; a service with the flag off fails this case on purpose, since
      production runs with it on);
  (a) shipping listing + a sealed envelope for the listing's seller, for
      each of three address classes (free-text region "California", ISO
      suffix "CA", empty region for PT). The service stores the envelope
      unopened and holds no plaintext address, so each order must have a
      NULL `delivery_address`, the 2096 sent bytes and the sent key id;
  (b) pickup-only (no address of any kind);
Every listing comes from the dedicated fixture seller (FIXTURE_SELLER, env
HOLD_SMOKE_FIXTURE_SELLER); no other seller's listing is picked or modified.
  (c) two unheld checkout.create 200s, then bind exclusivity (first
      POST /v0/orders/{id}/payment-method holds; loser 409 HOLDING_COPY);
  (d) hold TTL equals live FIAT_PAYMENT_WINDOW_SECONDS (exact-key read);
  (e) one digital case per delivery kind (file, link, text, email, message)
      on a fixture listing that already has that delivery set: the digital
      checkout (no address, `delivery_email` only for the email kind), the
      bind hold on the listing's stock, nothing released or logged while the
      payment is pending (buyer download and the email read answer
      `not_paid`), and the cancel that releases the hold. Staging cannot
      confirm a real payment, so the confirmed-payment release is covered by
      the service's integration tests (`digital_orders_test`,
      `digital_manual_test`), not here. Select kinds with `--digital-kinds`
      (default all five; an empty value runs none).

Fixture lifecycle: the fixtures are never left standing. The staging homeserver
this seller writes to is also indexed by the production Nexus, so a fixture
that outlives a run is a test listing live on the production index. Each run
sweeps the fixture seller's homeserver (deleting every listing, media file
and delivery ciphertext it holds), publishes the fixtures, runs the cases, and
in a `finally` sweeps again, waits until each deleted listing answers 404 on
the staging and production Nexus and the service, and then runs the guard:
no Nexus may list an active fixture. A run that cannot prove that is STOP,
whatever its cases did, and a run killed past `finally` (SIGKILL, power loss)
is cleaned by the next run's opening sweep or by
`node hold-smoke-fixtures.mjs sweep` and checked by `... guard`.
Environment: HOLD_SMOKE_SEAT_FILE, HOLD_SMOKE_FIXTURE_STATE, PUBKY_SDK_DIR (see
hold-smoke-fixtures.mjs) and, when digital kinds are smoked,
HOLD_SMOKE_DIGITAL_FIXTURE_CMD: a command that takes `publish` or `teardown`,
reads the kinds from HOLD_SMOKE_DIGITAL_FIXTURE_KINDS, and publishes (or tears
down) one digital listing per kind on the fixture seller with its delivery
set (the Shop's scripts/release/production-digital-fixture.mjs, wrapped).

Any HTTP 4xx other than the expected bind 409 is STOP. Never production.
Every created order is cancelled and verified cancelled at the end.

    MARKETPLACE_URL=https://staging-api.pubky.app
    RAILWAY_PROJECT_ID=c991d768-4a3c-42ea-b5ed-eaa22d4916ed
    RAILWAY_ENVIRONMENT=c67a6435-bb23-453b-9169-764bfa0312e1
    RAILWAY_DATABASE_SERVICE=Postgres
    python3 scripts/release/hold-smoke.py

    python3 scripts/release/hold-smoke.py --self-check

Sealing runs `hold-smoke-seal.mjs` under node with the same `hpke-js`
packages and pins the Shop uses. Install them once (the default HPKE_CWD):

    mkdir -p /Volumes/t7/vibes-dev/.deps/hold-smoke-hpke && cd "$_" && npm init -y \\
      && npm install --save-exact @hpke/core@1.9.0 @hpke/chacha20poly1305@1.8.0 \\
         @hpke/dhkem-x25519@1.8.0

Staging sellers publish no delivery key we hold and the service never opens
an envelope, so each address is sealed to a fresh throwaway recipient key.
`--self-check` proves the framing by opening a Shop-made envelope
(`shop-delivery-envelope.json`) and by round-tripping its own.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

HOLDING_COPY = (
    "Another buyer's payment is holding this item. If it isn't completed in time, the item restocks."
)
TTL_SLACK_SECONDS = 45.0
TTL_OVER_SECONDS = 5.0
FIAT_WINDOW_DEFAULT = 600

SHOP_TAG = "shop-v0.6.25"
SHOP_SHA = "5e7fc0e6433e8535ca0444607e7864d79ff4f73c"
FIXTURE_NAME = "shop-v0.6.25-checkout.create.json"

STAGING_URL = "https://staging-api.pubky.app"
STAGING_PROJECT = "c991d768-4a3c-42ea-b5ed-eaa22d4916ed"
STAGING_ENVIRONMENT = "c67a6435-bb23-453b-9169-764bfa0312e1"
STAGING_SERVICE = "3e98e363-f9a7-4711-b668-d714acee6e32"
PRODUCTION_PROJECT = "75faa4fe-466c-4277-977f-1d8e4e31df8c"
# Every listing the smoke holds, buys or cancels belongs to this dedicated staging seller
# ("Hold-smoke fixtures (staging)"): a shipping, a pickup-only and one digital "SMOKE FIXTURE, do not buy"
# listing per kind. Other sellers' listings are never picked or modified.
FIXTURE_SELLER = os.environ.get(
    "HOLD_SMOKE_FIXTURE_SELLER", "ka7q4cmabp6amo13bye9q6iwhb4pxuwr9fo37h5ygx4uk9e63tjy"
)
FIXTURE_HINT = (
    "the smoke publishes its own fixtures on the fixture seller at the start of a run "
    "(see Fixture lifecycle in the header); check the publish output above"
)
FIXTURES_SCRIPT = Path(__file__).with_name("hold-smoke-fixtures.mjs")
FIXTURE_ENV = ("HOLD_SMOKE_SEAT_FILE", "HOLD_SMOKE_FIXTURE_STATE", "PUBKY_SDK_DIR")
DIGITAL_FIXTURE_ENV = "HOLD_SMOKE_DIGITAL_FIXTURE_CMD"
PUBLISH_TIMEOUT_SECONDS = 1800
SWEEP_TIMEOUT_SECONDS = 900
GUARD_TIMEOUT_SECONDS = 180
PRODUCTION_HOST_MARKERS = (
    "marketplace-service-production-ce23",
    "marketplace-edge-production",
    "shop.pubky.app",
    "-ce23.up.railway.app",
)

NODE = "/Users/johncarvalho/.nvm/versions/node/v22.14.0/bin/node"
PUBKY_CWD = "/Users/johncarvalho/work/.deps/pubky-app-ci-green"
HPKE_CWD = os.environ.get("HPKE_CWD", "/Volumes/t7/vibes-dev/.deps/hold-smoke-hpke")
SEAL_HELPER = Path(__file__).with_name("hold-smoke-seal.mjs")
ENVELOPE_FIXTURE_NAME = "shop-delivery-envelope.json"
ENVELOPE_FIXTURE_SHOP_SHA = "deb3c7f71b8b915bc960b3a799bf2af286062ff0"
DELIVERY_ENVELOPE_BYTES = 2096
DELIVERY_ENVELOPE_CHARS = 2795
DELIVERY_KEY_ID_CHARS = 32
ENCRYPTED_REQUIRED_REASON = "encrypted_delivery_required"

# The address the Shop seals (its snake_case wire form). Region values are the
# three classes the 2026-09-22 hold-smoke missed (NY/pickup-only); under
# sealed delivery they vary the sealed content, which the service never reads.
SHIPPING_ADDRESSES: dict[str, dict[str, str]] = {
    "california_free_text": {
        "name": "Alice Buyer",
        "line1": "1 Market Street",
        "line2": "",
        "city": "San Francisco",
        "region": "California",
        "postal_code": "94105",
        "country_code": "US",
    },
    "ca_iso_suffix": {
        "name": "Alice Buyer",
        "line1": "1 Market Street",
        "line2": "",
        "city": "San Francisco",
        "region": "CA",
        "postal_code": "94105",
        "country_code": "US",
    },
    "pt_empty_region": {
        "name": "Alice Buyer",
        "line1": "Rua Augusta 1",
        "line2": "",
        "city": "Lisboa",
        "region": "",
        "postal_code": "1100-053",
        "country_code": "PT",
    },
}

ENVELOPE_KEYS = {
    "version",
    "command_id",
    "aggregate_id",
    "expected_revision",
    "issued_at",
    "kind",
    "payload",
}
PLAINTEXT_SHIPPING_PAYLOAD_KEYS = {"lines", "delivery_address"}
SEALED_SHIPPING_PAYLOAD_KEYS = {"lines", "delivery_address_envelopes"}
ENVELOPE_ENTRY_KEYS = {"seller_pubky", "key_id", "envelope"}
PICKUP_PAYLOAD_KEYS = {"lines"}
ADDRESS_KEYS = {"name", "line1", "line2", "city", "region", "postal_code", "country_code"}
LINE_KEYS = {"listing_aggregate_id", "expected_revision", "quantity", "variant_id", "fulfillment"}
# The variant id production Shop sent for a single-variant listing (order
# c7e700de, 2026-09-23). `variant_options` rides only a variant that has
# options, which the smoke listings do not.
SHOP_VARIANT_ID = "variant_1"

DIGITAL_KINDS = ("file", "link", "text", "email", "message")
# The address the email-kind case checks out with. The service seals it and
# the unpaid-checkout purge deletes it after BUYER_EMAIL_UNPAID_RETENTION_DAYS.
DIGITAL_SMOKE_EMAIL = "hold-smoke.digital@example.com"
DIGITAL_PAYLOAD_KEYS = {"lines"}


def refuse_production(url: str, project: str) -> None:
    lowered = url.lower()
    if project == PRODUCTION_PROJECT:
        raise SystemExit("HOLD_SMOKE=STOP production project id is forbidden")
    if "ce23" in lowered or any(marker in lowered for marker in PRODUCTION_HOST_MARKERS):
        raise SystemExit(f"HOLD_SMOKE=STOP production host is forbidden: {url}")
    if project != STAGING_PROJECT:
        raise SystemExit(f"HOLD_SMOKE=STOP expected staging project {STAGING_PROJECT}, got {project}")
    if url.rstrip("/") != STAGING_URL:
        raise SystemExit(f"HOLD_SMOKE=STOP expected {STAGING_URL}, got {url}")


def issued_at() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def bind_body(method: str) -> dict[str, str]:
    """Shop payment-method bind: `{ "method": "paypal" | "bitcoin" }`."""
    return {"method": method}


def ttl_matches_window(ttl: float, window: int, *, slack: float = TTL_SLACK_SECONDS) -> bool:
    return (window - slack) <= ttl <= (window + TTL_OVER_SECONDS)


def checkout_body(
    listing: dict[str, Any],
    command_id: str,
    *,
    fulfillment: str,
    address: dict[str, str] | None,
    envelopes: list[dict[str, str]] | None = None,
    delivery_email: str | None = None,
) -> dict[str, Any]:
    """Shop checkout.create after toSnakeCaseWire.

    A shipped line carries either sealed `envelopes` (what the Shop sends) or
    a plaintext `address` (only the refusal case sends that), never both.
    """
    line: dict[str, Any] = {
        "listing_aggregate_id": listing["aggregate_id"],
        "expected_revision": listing["server_revision"],
        "quantity": 1,
        "variant_id": SHOP_VARIANT_ID,
        "fulfillment": fulfillment,
    }
    payload: dict[str, Any] = {"lines": [line]}
    if fulfillment == "shipping":
        if (address is None) == (envelopes is None):
            raise RuntimeError("shipping checkout carries exactly one of delivery_address or sealed envelopes")
        if address is not None:
            payload["delivery_address"] = dict(address)
        else:
            payload["delivery_address_envelopes"] = [dict(entry) for entry in envelopes or []]
    elif address is not None or envelopes is not None:
        raise RuntimeError(f"{fulfillment} checkout must omit delivery_address and envelopes")
    if delivery_email is not None:
        if fulfillment != "digital":
            raise RuntimeError("delivery_email rides only a digital checkout")
        payload["delivery_email"] = delivery_email
    return {
        "version": 1,
        "command_id": command_id,
        "aggregate_id": f"checkout:{command_id}",
        "expected_revision": 0,
        "issued_at": issued_at(),
        "kind": "checkout.create",
        "payload": payload,
    }


def cancel_body(order_id: str, revision: int, reason: str) -> dict[str, Any]:
    return {
        "version": 1,
        "command_id": str(uuid.uuid4()),
        "aggregate_id": f"order:{order_id}",
        "expected_revision": revision,
        "issued_at": issued_at(),
        "kind": "order.cancel_request",
        "payload": {"order_id": order_id, "reason": reason},
    }


def assert_envelopes(envelopes: Any, sellers: list[str] | None = None) -> None:
    """Shape of `delivery_address_envelopes`, as the service checks it.

    Never put an envelope in an error message; only counts and key positions.
    """
    if not isinstance(envelopes, list) or not envelopes:
        raise RuntimeError("delivery_address_envelopes must be a non-empty list")
    seen: list[str] = []
    for index, entry in enumerate(envelopes):
        if not isinstance(entry, dict) or set(entry) != ENVELOPE_ENTRY_KEYS:
            raise RuntimeError(f"envelope {index} keys must be {sorted(ENVELOPE_ENTRY_KEYS)}")
        key_id = entry["key_id"]
        if len(key_id) != DELIVERY_KEY_ID_CHARS or any(c not in "0123456789abcdef" for c in key_id):
            raise RuntimeError(f"envelope {index} key_id must be {DELIVERY_KEY_ID_CHARS} lowercase hex chars")
        sealed = entry["envelope"]
        if len(sealed) != DELIVERY_ENVELOPE_CHARS:
            raise RuntimeError(f"envelope {index} is {len(sealed)} chars, want {DELIVERY_ENVELOPE_CHARS}")
        decoded = decode_envelope(sealed)
        if len(decoded) != DELIVERY_ENVELOPE_BYTES:
            raise RuntimeError(f"envelope {index} decodes to {len(decoded)} bytes, want {DELIVERY_ENVELOPE_BYTES}")
        seen.append(entry["seller_pubky"])
    if len(set(seen)) != len(seen):
        raise RuntimeError("one envelope per seller; a seller repeats")
    if sellers is not None and sorted(seen) != sorted(sellers):
        raise RuntimeError("envelope sellers must equal the shipped sellers")


def decode_envelope(envelope: str) -> bytes:
    """Canonical unpadded base64url, or RuntimeError (the service refuses anything else)."""
    if not envelope or any(c not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for c in envelope):
        raise RuntimeError("envelope is not unpadded base64url")
    raw = base64.urlsafe_b64decode(envelope + "=" * (-len(envelope) % 4))
    if base64.urlsafe_b64encode(raw).rstrip(b"=").decode() != envelope:
        raise RuntimeError("envelope is not canonical base64url")
    return raw


def node_seal(mode: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Run hold-smoke-seal.mjs. The Shop's HPKE library lives under HPKE_CWD."""
    result = subprocess.run(
        [NODE, str(SEAL_HELPER), mode],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env={**os.environ, "HPKE_CWD": HPKE_CWD},
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"seal helper {mode} failed rc={result.returncode}: {result.stderr.strip()[-300:]}")
    return json.loads(result.stdout)


def seal_address(
    address: dict[str, str], *, command_id: str, buyer_pubky: str, sellers: list[str]
) -> list[dict[str, str]]:
    """One HPKE envelope per shipped seller, bound to this checkout command and buyer."""
    out = node_seal(
        "seal",
        {
            "address": address,
            "checkout_command_id": command_id,
            "buyer_pubky": buyer_pubky,
            "sellers": sellers,
        },
    )
    envelopes = out["envelopes"]
    assert_envelopes(envelopes, sellers)
    return envelopes


def build_checkout(
    listing: dict[str, Any], command_id: str, *, buyer_pubky: str, fulfillment: str, address: dict[str, str] | None
) -> dict[str, Any]:
    """What the Shop sends: a shipped checkout seals `address`; every other kind carries none."""
    if fulfillment != "shipping":
        return checkout_body(listing, command_id, fulfillment=fulfillment, address=None)
    if address is None:
        raise RuntimeError("shipping checkout requires an address to seal")
    envelopes = seal_address(
        address, command_id=command_id, buyer_pubky=buyer_pubky, sellers=[listing["seller_pubky"]]
    )
    return checkout_body(listing, command_id, fulfillment="shipping", address=None, envelopes=envelopes)


def assert_shape(body: dict[str, Any], *, shipping: bool, sealed: bool = True) -> None:
    missing = ENVELOPE_KEYS - body.keys()
    extra = body.keys() - ENVELOPE_KEYS
    if missing or extra:
        raise RuntimeError(f"envelope keys mismatch missing={missing} extra={extra}")
    if body["kind"] != "checkout.create":
        raise RuntimeError(f"kind {body['kind']!r}")
    payload = body["payload"]
    shipping_keys = SEALED_SHIPPING_PAYLOAD_KEYS if sealed else PLAINTEXT_SHIPPING_PAYLOAD_KEYS
    expected_payload = shipping_keys if shipping else PICKUP_PAYLOAD_KEYS
    if set(payload) != expected_payload:
        raise RuntimeError(f"payload keys {set(payload)} != {expected_payload}")
    line = payload["lines"][0]
    if set(line) != LINE_KEYS:
        raise RuntimeError(f"line keys {set(line)} != {LINE_KEYS}")
    if shipping:
        if sealed:
            assert_envelopes(payload["delivery_address_envelopes"])
        elif set(payload["delivery_address"]) != ADDRESS_KEYS:
            raise RuntimeError(f"address keys {set(payload['delivery_address'])}")
        if line["fulfillment"] != "shipping":
            raise RuntimeError("shipping line fulfillment")
    else:
        if "delivery_address" in payload or "delivery_address_envelopes" in payload:
            raise RuntimeError("pickup must omit delivery_address and envelopes")
        if line["fulfillment"] != "pickup":
            raise RuntimeError("pickup line fulfillment")
    dumped = json.dumps(body)
    if '"region": "NY"' in dumped:
        raise RuntimeError("hard-coded NY region is the class that hid the 422")


def assert_digital_shape(body: dict[str, Any], *, kind: str) -> None:
    """A digital checkout: no address; `delivery_email` exactly for the email kind."""
    missing = ENVELOPE_KEYS - body.keys()
    extra = body.keys() - ENVELOPE_KEYS
    if missing or extra:
        raise RuntimeError(f"envelope keys mismatch missing={missing} extra={extra}")
    if body["kind"] != "checkout.create":
        raise RuntimeError(f"kind {body['kind']!r}")
    payload = body["payload"]
    expected = DIGITAL_PAYLOAD_KEYS | ({"delivery_email"} if kind == "email" else set())
    if set(payload) != expected:
        raise RuntimeError(f"digital {kind} payload keys {set(payload)} != {expected}")
    line = payload["lines"][0]
    if set(line) != LINE_KEYS:
        raise RuntimeError(f"line keys {set(line)} != {LINE_KEYS}")
    if line["fulfillment"] != "digital":
        raise RuntimeError("digital line fulfillment")


def digital_listing_query(kind: str, exclude: set[str]) -> str:
    """Staging listings that sell `kind` and already have that delivery set.

    `digital_delivery_kind` is written only by `digital_delivery.set`, so a
    row carrying it has a current deliverable. Stock may be a number of
    copies or the unlimited cap; the case checks the hold against the stock
    it finds. `kind` comes from DIGITAL_KINDS only.
    """
    if kind not in DIGITAL_KINDS:
        raise RuntimeError(f"unknown digital kind {kind!r}")
    excluded = ""
    if exclude:
        ids = ",".join(f"'{quote(item)}'" for item in sorted(exclude))
        excluded = f"AND l.aggregate_id NOT IN ({ids}) "
    return (
        "SELECT l.aggregate_id || '|' || l.seller_pubky || '|' || l.server_revision::text "
        "|| '|' || l.available_quantity::text || '|' || l.fulfillment_methods::text "
        "|| '|' || CASE "
        "WHEN c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' THEN 'paypal' "
        "ELSE '' END "
        "FROM listings l "
        "LEFT JOIN seller_payment_configs c ON c.seller_pubky = l.seller_pubky "
        "WHERE l.deleted_at IS NULL "
        "AND l.sale_format='fixed_price' AND l.state='available' "
        "AND l.available_quantity>=1 AND l.reserved_quantity=0 "
        "AND l.fulfillment_methods::text LIKE '%digital%' "
        f"AND l.digital_delivery_kind='{kind}' "
        "AND c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' "
        f"{fixture_seller_clause()}"
        f"{excluded}"
        "ORDER BY l.updated_at DESC NULLS LAST LIMIT 5;"
    )


def fixture_seller_clause(seller: str | None = None) -> str:
    seller = FIXTURE_SELLER if seller is None else seller
    if not re.fullmatch(r"[ybndrfg8ejkmcpqxot1uwisza345h769]{52}", seller):
        raise RuntimeError("HOLD_SMOKE_FIXTURE_SELLER must be a 52-character z-base-32 pubky")
    return f"AND l.seller_pubky='{seller}' "


def refusal_reason(body: dict | str) -> str | None:
    if not isinstance(body, dict):
        return None
    error = body.get("error")
    return error.get("reason") if isinstance(error, dict) else None


def self_check() -> None:
    listing = {
        "aggregate_id": "listing:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "seller_pubky": z32_pubky(),
        "server_revision": 3,
        "fulfillment_methods": "{shipping}",
    }
    pickup_listing = {**listing, "fulfillment_methods": "{pickup}"}
    plaintext = checkout_body(
        listing,
        str(uuid.uuid4()),
        fulfillment="shipping",
        address=SHIPPING_ADDRESSES["california_free_text"],
    )
    assert_shape(plaintext, shipping=True, sealed=False)
    if "delivery_address_envelopes" in plaintext["payload"]:
        raise RuntimeError("the refusal payload must be plaintext only")
    buyer = z32_pubky()
    sealed_bodies = {}
    for name, address in SHIPPING_ADDRESSES.items():
        sealed_bodies[name] = build_checkout(
            listing, str(uuid.uuid4()), buyer_pubky=buyer, fulfillment="shipping", address=address
        )
        assert_shape(sealed_bodies[name], shipping=True)
        if "delivery_address" in sealed_bodies[name]["payload"]:
            raise RuntimeError(f"sealed shipping checkout leaked a plaintext address ({name})")
        entry = sealed_entry(sealed_bodies[name])
        if entry is None or entry["seller_pubky"] != listing["seller_pubky"]:
            raise RuntimeError(f"sealed envelope must be for the listing's seller ({name})")
        dumped = json.dumps(sealed_bodies[name])
        for value in address.values():
            if len(value) >= 8 and value in dumped:
                raise RuntimeError(f"sealed checkout carries address text in the clear ({name})")
        if address_class(address) != name:
            raise RuntimeError(f"address class lookup ({name})")
    first = sealed_entry(sealed_bodies["ca_iso_suffix"])
    second = sealed_entry(
        build_checkout(
            listing,
            str(uuid.uuid4()),
            buyer_pubky=buyer,
            fulfillment="shipping",
            address=SHIPPING_ADDRESSES["ca_iso_suffix"],
        )
    )
    if first is None or second is None:
        raise RuntimeError("sealed checkout carries no envelope")
    if first["envelope"] == second["envelope"] or first["key_id"] == second["key_id"]:
        raise RuntimeError("every seal must use fresh randomness and a fresh recipient key")
    roundtrip = node_seal(
        "roundtrip",
        {
            "address": SHIPPING_ADDRESSES["pt_empty_region"],
            "checkout_command_id": str(uuid.uuid4()),
            "buyer_pubky": buyer,
            "seller_pubky": listing["seller_pubky"],
        },
    )["address"]
    if roundtrip != SHIPPING_ADDRESSES["pt_empty_region"]:
        raise RuntimeError("sealed address did not round-trip through the helper")
    shop = json.loads(Path(__file__).with_name(ENVELOPE_FIXTURE_NAME).read_text())
    if shop["captured_from"]["shop_sha"] != ENVELOPE_FIXTURE_SHOP_SHA:
        raise RuntimeError("envelope fixture sha drift")
    shop_entry = {
        "seller_pubky": shop["binding"]["seller_pubky"],
        "key_id": shop["binding"]["key_id"],
        "envelope": shop["envelope"],
    }
    assert_envelopes([shop_entry], [shop["binding"]["seller_pubky"]])
    shop_open = {
        "envelope": shop["envelope"],
        "recipient_private_key_b64url": shop["recipient_private_key_b64url"],
        "binding": shop["binding"],
    }
    if node_seal("open", shop_open)["address"] != shop["address"]:
        raise RuntimeError("the helper cannot open the Shop's own envelope; the framing has drifted")
    try:
        node_seal("open", {**shop_open, "binding": {**shop["binding"], "checkout_command_id": str(uuid.uuid4())}})
    except RuntimeError:
        pass
    else:
        raise RuntimeError("an envelope opened under another checkout command id; the AAD is not bound")
    good_entry = {"seller_pubky": "s1", "key_id": "0" * DELIVERY_KEY_ID_CHARS, "envelope": shop["envelope"]}
    for bad_envelopes in (
        [],
        [{**good_entry, "envelope": shop["envelope"][:-1]}],
        [{**good_entry, "envelope": shop["envelope"][:-1] + "="}],
        [{**good_entry, "key_id": "XYZ"}],
        [{**good_entry, "key_id": "A" * DELIVERY_KEY_ID_CHARS}],
        [{**good_entry}, {**good_entry}],
        [{"seller_pubky": "s1", "key_id": good_entry["key_id"]}],
    ):
        try:
            assert_envelopes(bad_envelopes)
        except RuntimeError:
            continue
        raise RuntimeError("a malformed envelope list was accepted")
    try:
        assert_envelopes([good_entry], ["s2"])
    except RuntimeError:
        pass
    else:
        raise RuntimeError("envelopes for the wrong seller set were accepted")
    trailing_bits = shop["envelope"][:-1] + ("B" if shop["envelope"][-1] != "B" else "C")
    try:
        decode_envelope(trailing_bits)
    except RuntimeError:
        pass
    else:
        raise RuntimeError("an envelope with trailing bits set was accepted as canonical")
    chatter = 'Output format is unaligned.\nField separator is "|".\n'
    if sql_scalar(chatter + "address=true|0||\n") != "address=true|0||":
        raise RuntimeError("stored address marker must survive psql chatter")
    facts = parse_address_facts("address=true|2096|ab|0123456789abcdef0123456789abcdef", "order")
    if facts != {
        "plaintext_null": True,
        "envelope_bytes": 2096,
        "envelope_sha256": "ab",
        "key_id": "0123456789abcdef0123456789abcdef",
    }:
        raise RuntimeError("stored address facts parse")
    for bad_token in ("region=CA", "address=true|0|"):
        try:
            parse_address_facts(bad_token, "order")
        except RuntimeError:
            continue
        raise RuntimeError("a malformed stored address marker was accepted")
    if refusal_reason({"ok": False, "error": {"reason": ENCRYPTED_REQUIRED_REASON}}) != ENCRYPTED_REQUIRED_REASON:
        raise RuntimeError("encrypted_delivery_required reason parse")
    pickup = checkout_body(pickup_listing, str(uuid.uuid4()), fulfillment="pickup", address=None)
    assert_shape(pickup, shipping=False)
    if bind_body("paypal") != {"method": "paypal"}:
        raise RuntimeError("paypal bind body")
    if not ttl_matches_window(590.0, 600):
        raise RuntimeError("ttl slack around fiat default")
    if ttl_matches_window(500.0, 600):
        raise RuntimeError("ttl far below window must fail")
    if parse_fiat_window("") != FIAT_WINDOW_DEFAULT:
        raise RuntimeError("empty printenv must use fiat default 600")
    if parse_fiat_window("600") != 600:
        raise RuntimeError("exact-key 600")
    if parse_fiat_window("900") != 900:
        raise RuntimeError("exact-key 900")
    digital_listing = {**listing, "fulfillment_methods": "{digital}"}
    for kind in DIGITAL_KINDS:
        digital = checkout_body(
            digital_listing,
            str(uuid.uuid4()),
            fulfillment="digital",
            address=None,
            delivery_email=DIGITAL_SMOKE_EMAIL if kind == "email" else None,
        )
        assert_digital_shape(digital, kind=kind)
        if ("delivery_email" in digital["payload"]) != (kind == "email"):
            raise RuntimeError(f"delivery_email must ride only the email kind ({kind})")
        if kind not in digital_listing_query(kind, {"listing:a"}):
            raise RuntimeError(f"digital listing query misses {kind}")
        query = digital_listing_query(kind, set())
        if "l.deleted_at IS NULL" not in query:
            raise RuntimeError(f"digital listing query must skip tombstoned listings ({kind})")
        if "c.paypal_merchant_email IS NOT NULL" not in query:
            raise RuntimeError(f"digital listing query must require a seller payment rail ({kind})")
    for shipping_case in (True, False):
        if "l.deleted_at IS NULL" not in listing_pick_query(shipping=shipping_case)[0]:
            raise RuntimeError("shipping and pickup listing picks must skip tombstoned listings")
    seller_clause = f"AND l.seller_pubky='{FIXTURE_SELLER}'"
    pick_queries = [listing_pick_query(shipping=case)[0] for case in (True, False)]
    pick_queries += [digital_listing_query(kind, set()) for kind in DIGITAL_KINDS]
    if not all(seller_clause in query for query in pick_queries):
        raise RuntimeError("every listing pick must be limited to the fixture seller")
    if re.search(r"UPDATE\s+listings", Path(__file__).read_text().replace("UPDATE\\s+listings", "")):
        raise RuntimeError("the smoke must never rewrite a listing row")
    for bad_seller in ("", "ka7q4cma", f"{FIXTURE_SELLER[:51]}'", f"{FIXTURE_SELLER}' OR '1'='1", "l" * 52):
        try:
            fixture_seller_clause(bad_seller)
        except RuntimeError:
            continue
        raise RuntimeError(f"a malformed fixture seller was accepted: {bad_seller!r}")
    for bad in (
        lambda: checkout_body(
            digital_listing, str(uuid.uuid4()), fulfillment="digital", address=SHIPPING_ADDRESSES["ca_iso_suffix"]
        ),
        lambda: checkout_body(
            listing, str(uuid.uuid4()), fulfillment="shipping",
            address=SHIPPING_ADDRESSES["ca_iso_suffix"], delivery_email=DIGITAL_SMOKE_EMAIL,
        ),
        lambda: digital_listing_query("file' OR '1'='1", set()),
        lambda: checkout_body(
            listing, str(uuid.uuid4()), fulfillment="shipping",
            address=SHIPPING_ADDRESSES["ca_iso_suffix"], envelopes=[good_entry],
        ),
        lambda: checkout_body(listing, str(uuid.uuid4()), fulfillment="shipping", address=None),
        lambda: checkout_body(
            pickup_listing, str(uuid.uuid4()), fulfillment="pickup", address=None, envelopes=[good_entry]
        ),
    ):
        try:
            bad()
        except RuntimeError:
            continue
        raise RuntimeError("an invalid digital payload or kind was accepted")
    if refusal_reason({"ok": False, "error": {"reason": "not_paid"}}) != "not_paid":
        raise RuntimeError("refusal reason parse")
    if parse_digital_kinds("file, email") != ("file", "email") or parse_digital_kinds("") != ():
        raise RuntimeError("digital kinds parse")
    if parse_digital_kinds("all") != DIGITAL_KINDS:
        raise RuntimeError("digital kinds default")
    complete = {name: "x" for name in (*FIXTURE_ENV, DIGITAL_FIXTURE_ENV)}
    if fixture_missing_environment(DIGITAL_KINDS, complete) or fixture_missing_environment((), complete):
        raise RuntimeError("a complete fixture environment was refused")
    if fixture_missing_environment(DIGITAL_KINDS, {name: "x" for name in FIXTURE_ENV}) != [DIGITAL_FIXTURE_ENV]:
        raise RuntimeError("digital kinds without a digital fixture command were accepted")
    if fixture_missing_environment((), {name: "x" for name in FIXTURE_ENV}):
        raise RuntimeError("a run with no digital kinds must not need the digital fixture command")
    if sorted(fixture_missing_environment((), {})) != sorted(FIXTURE_ENV):
        raise RuntimeError("an empty fixture environment was accepted")
    if fixtures_argv("sweep")[2] != "sweep" or digital_fixture_argv("teardown", "zsh -c run.sh") != ["zsh", "-c", "run.sh", "teardown"]:
        raise RuntimeError("fixture command line")
    for bad in (lambda: fixtures_argv("status"), lambda: digital_fixture_argv("buy", "x"), lambda: digital_fixture_argv("publish", " ")):
        try:
            bad()
        except RuntimeError:
            continue
        raise RuntimeError("an unknown fixture mode or empty command was accepted")
    if fixture_environment()["HOLD_SMOKE_FIXTURE_SELLER"] != FIXTURE_SELLER:
        raise RuntimeError("the fixture steps must act on the smoke's fixture seller")
    failing = FixtureLifecycle(())
    failing.digital = lambda action: (_ for _ in ()).throw(RuntimeError("digital down"))  # type: ignore[method-assign]
    ran: list[str] = []
    failing.step = lambda label, argv, timeout: ran.append(label)  # type: ignore[method-assign]
    problems = failing.retire()
    if ran != ["sweep", "guard"] or len(problems) != 1 or not problems[0].startswith("digital teardown"):
        raise RuntimeError(f"retire must run sweep and guard after a failed digital teardown: {ran!r} {problems!r}")
    fixture_path = Path(__file__).with_name(FIXTURE_NAME)
    fixture = json.loads(fixture_path.read_text())
    if fixture["captured_from"]["shop_tag"] != SHOP_TAG:
        raise RuntimeError("fixture tag drift")
    if fixture["captured_from"]["shop_sha"] != SHOP_SHA:
        raise RuntimeError("fixture sha drift")
    print(
        json.dumps(
            {
                "shop_tag": SHOP_TAG,
                "shop_sha": SHOP_SHA,
                "sealed_envelope_chars": len(first["envelope"]),
                "sealed_envelope_bytes": DELIVERY_ENVELOPE_BYTES,
                "shop_envelope_sha": ENVELOPE_FIXTURE_SHOP_SHA,
                "shop_envelope_opened_by_helper": True,
                "pickup_has_address": "delivery_address" in pickup["payload"],
                "bind_paypal": bind_body("paypal"),
                "ttl_590_vs_600": ttl_matches_window(590.0, 600),
            },
            indent=2,
            sort_keys=True,
        )
    )
    print("HOLD_SMOKE=SELF_CHECK_PASS")


def sql(statement: str) -> str:
    project = os.environ["RAILWAY_PROJECT_ID"]
    environment = os.environ["RAILWAY_ENVIRONMENT"]
    db_service = os.environ.get("RAILWAY_DATABASE_SERVICE", "Postgres")
    refuse_production(os.environ["MARKETPLACE_URL"], project)
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/opt/postgresql@17/bin:" + env.get("PATH", "")
    env["PGCONNECT_TIMEOUT"] = "15"
    for key in ("RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_ID", "RAILWAY_SERVICE_ID"):
        env.pop(key, None)
    payload = (
        "\\set ON_ERROR_STOP on\n"
        "\\pset tuples_only on\n"
        "\\pset format unaligned\n"
        "\\pset fieldsep '|'\n"
        + statement
        + "\n\\q\n"
    )
    result = subprocess.run(
        ["railway", "connect", db_service, "--ssh", "-p", project, "-e", environment],
        input=payload,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    filtered = []
    for line in (result.stdout or "").splitlines():
        lower = line.lower()
        if "postgres://" in lower or "password" in lower:
            continue
        if "config as code" in lower or "migrate:" in lower or "existing files" in lower:
            continue
        if "ssh tunnel" in lower:
            continue
        filtered.append(line)
    if result.returncode != 0:
        err = "\n".join(
            ln
            for ln in (result.stderr or "").splitlines()
            if "postgres://" not in ln.lower() and "password" not in ln.lower()
        )
        raise RuntimeError(f"sql failed rc={result.returncode} err={err[:400]!r} out={filtered!r}")
    return "\n".join(filtered)


def railway_ssh(argv: list[str]) -> str:
    """Run one command on the staging marketplace-service instance.

    Used only for an exact-key `printenv FIAT_PAYMENT_WINDOW_SECONDS`. Never
    list or dump the rest of the environment.
    """
    project = os.environ["RAILWAY_PROJECT_ID"]
    environment = os.environ["RAILWAY_ENVIRONMENT"]
    refuse_production(os.environ["MARKETPLACE_URL"], project)
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:" + env.get("PATH", "")
    for key in ("RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT_ID", "RAILWAY_SERVICE_ID"):
        env.pop(key, None)
    result = subprocess.run(
        [
            "railway",
            "ssh",
            "-p",
            project,
            "-e",
            environment,
            "-s",
            STAGING_SERVICE,
            "--",
            *argv,
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    filtered = []
    for line in (result.stdout or "").splitlines():
        lower = line.lower()
        if "postgres://" in lower or "password" in lower:
            continue
        if "config as code" in lower or "migrate:" in lower:
            continue
        if "ssh tunnel" in lower or "connected to" in lower:
            continue
        filtered.append(line)
    if result.returncode != 0:
        # GNU printenv exits 1 when the named key is unset; that is the
        # empty-window case (code default 600), not an SSH failure.
        if (
            result.returncode == 1
            and len(argv) == 2
            and argv[0] == "printenv"
            and not filtered
        ):
            return ""
        err = "\n".join(
            ln
            for ln in (result.stderr or "").splitlines()
            if "postgres://" not in ln.lower() and "password" not in ln.lower()
        )
        raise RuntimeError(f"ssh failed rc={result.returncode} err={err[:400]!r} out={filtered!r}")
    return "\n".join(filtered)


def parse_digital_kinds(raw: str) -> tuple[str, ...]:
    """`--digital-kinds`: `all`, an empty value (none), or a comma list of kinds."""
    if raw.strip() == "all":
        return DIGITAL_KINDS
    kinds = tuple(item.strip() for item in raw.split(",") if item.strip())
    unknown = [item for item in kinds if item not in DIGITAL_KINDS]
    if unknown:
        raise SystemExit(f"HOLD_SMOKE=STOP unknown digital kinds {unknown}; choose from {list(DIGITAL_KINDS)}")
    return tuple(dict.fromkeys(kinds))


def parse_fiat_window(raw: str) -> int:
    """Parse an exact-key `printenv FIAT_PAYMENT_WINDOW_SECONDS` payload.

    Empty output means the process is on the service default (600). Staging
    IaC `preserve()`s the key and does not inject it when unset.
    """
    token = sql_scalar(raw) if "\n" in raw else raw.strip()
    if not token:
        return FIAT_WINDOW_DEFAULT
    try:
        window = int(token)
    except ValueError as error:
        raise RuntimeError(
            f"FIAT_PAYMENT_WINDOW_SECONDS not an integer ({len(token)} chars)"
        ) from error
    if window < 60:
        raise RuntimeError(f"FIAT_PAYMENT_WINDOW_SECONDS {window} is below the 60s floor")
    return window


def fiat_window_seconds() -> int:
    """Exact-key read of FIAT_PAYMENT_WINDOW_SECONDS from the running service."""
    return parse_fiat_window(railway_ssh(["printenv", "FIAT_PAYMENT_WINDOW_SECONDS"]))


def api(base: str, method: str, path: str, bearer: str | None, body: Any = None) -> tuple[int, dict | str]:
    headers: dict[str, str] = {}
    data = None
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode()
        headers["content-type"] = "application/json"
    request = Request(base + path, method=method, headers=headers, data=data)
    try:
        response = urlopen(request, timeout=30)
        raw = response.read()
        parsed = json.loads(raw) if raw else {}
        return response.status, parsed
    except HTTPError as error:
        raw = error.read()
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            parsed = raw.decode("utf-8", "replace")
        return error.code, parsed


def stop_on_unexpected_4xx(status: int, body: dict | str, *, allowed: set[int], case: str) -> None:
    if status in allowed:
        return
    if 400 <= status < 500:
        raise RuntimeError(f"STOP unexpected 4xx {status} case={case} body={body!r}")
    raise RuntimeError(f"STOP http {status} case={case} body={body!r}")


def z32_pubky() -> str:
    script = (
        "const {Keypair}=require('@synonymdev/pubky');"
        "const k=Keypair.random();"
        "process.stdout.write(k.publicKey.z32());"
    )
    result = subprocess.run(
        [NODE, "-e", script],
        cwd=PUBKY_CWD,
        capture_output=True,
        text=True,
        check=True,
    )
    token = result.stdout.strip()
    if len(token) != 52:
        raise RuntimeError(f"z32 length {len(token)}")
    return token


def quote(value: str) -> str:
    return value.replace("'", "''")


def insert_session(pubky: str) -> tuple[str, str]:
    bearer = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    session_id = str(uuid.uuid4())
    token_hash = hashlib.sha256(
        base64.urlsafe_b64decode(bearer + "=" * (-len(bearer) % 4))
    ).hexdigest()
    sql(
        "INSERT INTO auth_sessions "
        "(token_hash,session_id,pubky,capabilities,created_at,expires_at,last_used_at) VALUES "
        f"(decode('{token_hash}','hex'),'{session_id}','{quote(pubky)}',"
        "'/:rw',now(),now()+interval '15 minutes',now());"
    )
    return bearer, session_id


def parse_listing_rows(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        token = line.strip().strip('"')
        if token.count("|") < 4:
            continue
        parts = [p.strip().strip('"') for p in token.split("|")]
        if len(parts) < 5:
            continue
        bind_method = parts[5] if len(parts) > 5 else ""
        rows.append(
            {
                "aggregate_id": parts[0],
                "seller_pubky": parts[1],
                "server_revision": int(parts[2]),
                "available_quantity": int(parts[3]),
                "fulfillment_methods": parts[4],
                "bind_method": bind_method,
            }
        )
    return rows


def listing_pick_query(*, shipping: bool, exclude: set[str] | None = None) -> tuple[str, str]:
    if shipping:
        clause = "AND l.fulfillment_methods::text LIKE '%shipping%'"
        label = "shipping"
    else:
        clause = (
            "AND l.fulfillment_methods::text LIKE '%pickup%' "
            "AND l.fulfillment_methods::text NOT LIKE '%shipping%'"
        )
        label = "pickup-only"
    excluded = ""
    if exclude:
        ids = ",".join(f"'{quote(item)}'" for item in sorted(exclude))
        excluded = f"AND l.aggregate_id NOT IN ({ids}) "
    query = (
        "SELECT l.aggregate_id || '|' || l.seller_pubky || '|' || l.server_revision::text "
        "|| '|' || l.available_quantity::text || '|' || l.fulfillment_methods::text "
        "|| '|' || CASE "
        "WHEN c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' THEN 'paypal' "
        "ELSE '' END "
        "FROM listings l "
        "LEFT JOIN seller_payment_configs c ON c.seller_pubky = l.seller_pubky "
        "WHERE l.deleted_at IS NULL "
        "AND l.sale_format='fixed_price' AND l.state='available' AND l.available_quantity=1 "
        "AND l.reserved_quantity=0 "
        f"{fixture_seller_clause()}"
        f"{clause} {excluded}"
        "ORDER BY (CASE "
        "WHEN c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' THEN 0 "
        "ELSE 1 END), l.updated_at DESC NULLS LAST LIMIT 5;"
    )
    return query, label


def pick_listing(
    *, shipping: bool, exclude: set[str] | None = None, require_bind: bool = False
) -> dict[str, Any]:
    query, label = listing_pick_query(shipping=shipping, exclude=exclude)
    raw = sql(query)
    rows = parse_listing_rows(raw)
    if require_bind:
        rows = [row for row in rows if row.get("bind_method") == "paypal"]
    if not rows:
        suffix = " with paypal" if require_bind else ""
        raise RuntimeError(f"no qty-1 available {label} listing{suffix} from the fixture seller: {FIXTURE_HINT}\n{raw!r}")
    listing = rows[0]
    methods = listing["fulfillment_methods"]
    if shipping and "shipping" not in methods:
        raise RuntimeError(f"picked listing is not shipping: {methods!r}")
    if not shipping and ("pickup" not in methods or "shipping" in methods):
        raise RuntimeError(f"picked listing is not pickup-only: {methods!r}")
    return listing



def sql_scalar(raw: str) -> str:
    skip = {
        "output format is unaligned.",
        'field separator is "|".',
    }
    for line in raw.splitlines():
        token = line.strip().strip('"')
        if not token or token.lower() in skip:
            continue
        return token
    raise RuntimeError(f"scalar unparseable {raw!r}")


def stored_address_facts(order_id: str) -> dict[str, Any]:
    """What the order row keeps of the address, without reading any of it.

    A psql NULL prints as nothing, so each column is wrapped in a marker the
    parser can see. The envelope is compared by SHA-256, not echoed.
    """
    token = sql_scalar(
        sql(
            "SELECT 'address=' || (delivery_address IS NULL)::text "
            "|| '|' || coalesce(octet_length(delivery_address_envelope), 0)::text "
            "|| '|' || coalesce(encode(sha256(delivery_address_envelope), 'hex'), '') "
            "|| '|' || coalesce(delivery_key_id, '') "
            f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
        )
    )
    return parse_address_facts(token, order_id)


def parse_address_facts(token: str, order_id: str) -> dict[str, Any]:
    if not token.startswith("address="):
        raise RuntimeError(f"stored address marker missing for {order_id}")
    cols = token[len("address=") :].split("|")
    if len(cols) != 4:
        raise RuntimeError(f"stored address facts unparseable for {order_id}")
    return {
        "plaintext_null": cols[0] == "true",
        "envelope_bytes": int(cols[1]),
        "envelope_sha256": cols[2],
        "key_id": cols[3],
    }


def assert_stored_address(order_id: str, entry: dict[str, str] | None, *, case: str) -> dict[str, Any]:
    """The order keeps exactly the sent envelope and no plaintext (or nothing, for pickup and digital)."""
    facts = stored_address_facts(order_id)
    if not facts["plaintext_null"]:
        raise RuntimeError(f"{case}: order {order_id} stored a plaintext delivery address")
    if entry is None:
        if facts["envelope_bytes"] or facts["key_id"]:
            raise RuntimeError(f"{case}: order {order_id} stored an envelope it was never sent")
        return facts
    sent_sha = hashlib.sha256(decode_envelope(entry["envelope"])).hexdigest()
    if facts["envelope_bytes"] != DELIVERY_ENVELOPE_BYTES:
        raise RuntimeError(f"{case}: stored envelope is {facts['envelope_bytes']} bytes")
    if facts["envelope_sha256"] != sent_sha:
        raise RuntimeError(f"{case}: stored envelope differs from the one sent")
    if facts["key_id"] != entry["key_id"]:
        raise RuntimeError(f"{case}: stored key id differs from the one sent")
    return facts


def sealed_entry(body: dict[str, Any]) -> dict[str, str] | None:
    """The one envelope a single-seller checkout carries, or None when it carries none."""
    entries = body["payload"].get("delivery_address_envelopes")
    if entries is None:
        return None
    if len(entries) != 1:
        raise RuntimeError(f"single-seller checkout sent {len(entries)} envelopes")
    return entries[0]


def address_class(address: dict[str, str] | None) -> str | None:
    if address is None:
        return None
    for name, candidate in SHIPPING_ADDRESSES.items():
        if candidate == address:
            return name
    raise RuntimeError("address is not one of the smoke addresses")


def redacted_facts(facts: dict[str, Any]) -> dict[str, Any]:
    """Row facts with the envelope hash and key id cut to a prefix for the log."""
    return {
        "plaintext_null": facts["plaintext_null"],
        "envelope_bytes": facts["envelope_bytes"],
        "envelope_sha256_prefix": facts["envelope_sha256"][:8],
        "key_id_prefix": facts["key_id"][:6],
    }


def parse_hold_row(raw: str) -> dict[str, Any]:
    for line in raw.splitlines():
        token = line.strip().strip('"')
        if token.count("|") < 3:
            continue
        cols = [c.strip() for c in token.split("|")]
        return {
            "stock_held": cols[0] in {"t", "true"},
            "hold_source": None if cols[1] in {"", "null"} else cols[1],
            "hold_expires_at": None if cols[2] in {"", "null"} else cols[2],
            "state": cols[3],
        }
    raise RuntimeError(f"hold row unparseable {raw!r}")


def order_revision(order_id: str) -> int:
    return int(sql_scalar(sql(f"SELECT revision::text FROM orders WHERE id='{quote(order_id)}'::uuid;")))


def refresh_listing(aggregate_id: str) -> dict[str, Any]:
    raw = sql(
        "SELECT l.aggregate_id || '|' || l.seller_pubky || '|' || l.server_revision::text "
        "|| '|' || l.available_quantity::text || '|' || l.fulfillment_methods::text "
        "|| '|' || CASE "
        "WHEN c.paypal_merchant_email IS NOT NULL AND btrim(c.paypal_merchant_email) <> '' THEN 'paypal' "
        "ELSE '' END "
        "FROM listings l "
        "LEFT JOIN seller_payment_configs c ON c.seller_pubky = l.seller_pubky "
        f"WHERE l.aggregate_id='{quote(aggregate_id)}';"
    )
    rows = parse_listing_rows(raw)
    if not rows:
        raise RuntimeError(f"listing missing after restock {aggregate_id}")
    return rows[0]


def listing_stock(aggregate_id: str) -> tuple[int, int]:
    """(available, reserved) of a listing. A bind hold moves one copy from the
    first to the second; a released hold moves it back."""
    token = sql_scalar(
        sql(
            "SELECT 'stock=' || available_quantity::text || ',' || reserved_quantity::text "
            f"FROM listings WHERE aggregate_id='{quote(aggregate_id)}';"
        )
    )
    if not token.startswith("stock="):
        raise RuntimeError(f"stock marker missing {token!r}")
    available, reserved = token[len("stock=") :].split(",")
    return int(available), int(reserved)


def digital_order_facts(order_id: str) -> dict[str, Any]:
    """What the unpaid digital order stored: its fulfillment, the delivery kind
    its line snapshotted, whether it carries an address or a receipt, and how
    many pins, access rows and sealed delivery emails exist for it."""
    token = sql_scalar(
        sql(
            "SELECT 'fact=' || o.fulfillment || '|' || coalesce(o.lines->0->>'digital_kind','') "
            "|| '|' || (o.delivery_address IS NULL)::text || '|' || (o.receipt_id IS NULL)::text "
            f"|| '|' || (SELECT count(*) FROM order_digital_pins WHERE order_id=o.id)::text "
            f"|| '|' || (SELECT count(*) FROM order_digital_access WHERE order_id=o.id)::text "
            f"|| '|' || (SELECT count(*) FROM order_delivery_emails WHERE order_id=o.id)::text "
            f"FROM orders o WHERE o.id='{quote(order_id)}'::uuid;"
        )
    )
    if not token.startswith("fact="):
        raise RuntimeError(f"order facts marker missing {token!r}")
    cols = token[len("fact=") :].split("|")
    if len(cols) != 7:
        raise RuntimeError(f"order facts unparseable {token!r}")
    return {
        "fulfillment": cols[0],
        "digital_kind": cols[1],
        "no_address": cols[2] == "true",
        "no_receipt": cols[3] == "true",
        "pins": int(cols[4]),
        "access_rows": int(cols[5]),
        "email_rows": int(cols[6]),
    }


def pick_digital_listing(kind: str, exclude: set[str]) -> dict[str, Any]:
    raw = sql(digital_listing_query(kind, exclude))
    rows = parse_listing_rows(raw)
    if not rows:
        raise RuntimeError(
            f"no available {kind} fixture from the fixture seller with a PayPal rail; "
            f"{FIXTURE_HINT} (digital kinds: Shop scripts/release/production-digital-fixture.mjs publish) "
            f"or run with --digital-kinds without {kind}\n{raw!r}"
        )
    listing = rows[0]
    if "digital" not in listing["fulfillment_methods"]:
        raise RuntimeError(f"picked listing is not digital: {listing['fulfillment_methods']!r}")
    return listing


def fixture_environment() -> dict[str, str]:
    env = dict(os.environ)
    env["HOLD_SMOKE_FIXTURE_SELLER"] = FIXTURE_SELLER
    return env


def fixture_missing_environment(digital_kinds: tuple[str, ...], environ: dict[str, str] | None = None) -> list[str]:
    environ = os.environ if environ is None else environ
    missing = [name for name in FIXTURE_ENV if not environ.get(name)]
    if digital_kinds and not environ.get(DIGITAL_FIXTURE_ENV):
        missing.append(DIGITAL_FIXTURE_ENV)
    return missing


def fixtures_argv(mode: str) -> list[str]:
    if mode not in {"publish", "sweep", "guard"}:
        raise RuntimeError(f"unknown fixtures mode {mode!r}")
    return [NODE, str(FIXTURES_SCRIPT), mode]


def digital_fixture_argv(action: str, command: str) -> list[str]:
    if action not in {"publish", "teardown"}:
        raise RuntimeError(f"unknown digital fixture action {action!r}")
    argv = shlex.split(command)
    if not argv:
        raise RuntimeError(f"{DIGITAL_FIXTURE_ENV} is empty")
    return [*argv, action]


def run_step(label: str, argv: list[str], *, env: dict[str, str], timeout: int) -> None:
    print(f"fixture_step={label}", file=sys.stderr, flush=True)
    result = subprocess.run(argv, env=env, timeout=timeout, check=False, stdout=subprocess.PIPE, text=True)
    sys.stderr.write(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"fixture step {label} failed rc={result.returncode}")


class FixtureLifecycle:
    """The fixtures exist only for the length of one run (see Fixture lifecycle in the header)."""

    def __init__(self, digital_kinds: tuple[str, ...]) -> None:
        self.digital_kinds = digital_kinds
        self.env = fixture_environment()
        self.env["HOLD_SMOKE_DIGITAL_FIXTURE_KINDS"] = ",".join(digital_kinds)

    def check_environment(self) -> None:
        missing = fixture_missing_environment(self.digital_kinds, self.env)
        if missing:
            raise SystemExit(f"HOLD_SMOKE=STOP the fixture lifecycle needs {', '.join(missing)} (see the header)")

    def step(self, label: str, argv: list[str], timeout: int) -> None:
        run_step(label, argv, env=self.env, timeout=timeout)

    def digital(self, action: str) -> None:
        if self.digital_kinds:
            self.step(f"digital {action}", digital_fixture_argv(action, self.env[DIGITAL_FIXTURE_ENV]), PUBLISH_TIMEOUT_SECONDS)

    def publish(self) -> None:
        self.step("sweep before publish", fixtures_argv("sweep"), SWEEP_TIMEOUT_SECONDS)
        self.step("publish shipping and pickup", fixtures_argv("publish"), PUBLISH_TIMEOUT_SECONDS)
        self.digital("publish")

    def retire(self) -> list[str]:
        """Remove every fixture and prove it. Every step runs even when an earlier one failed."""
        problems: list[str] = []
        for label, action in (
            ("digital teardown", lambda: self.digital("teardown")),
            ("sweep", lambda: self.step("sweep", fixtures_argv("sweep"), SWEEP_TIMEOUT_SECONDS)),
            ("guard", lambda: self.step("guard", fixtures_argv("guard"), GUARD_TIMEOUT_SECONDS)),
        ):
            try:
                action()
            except (RuntimeError, subprocess.SubprocessError, OSError) as error:
                problems.append(f"{label}: {error}")
        return problems


def terminate_as_exit(signum: int, _frame: Any) -> None:
    raise SystemExit(128 + signum)


class Smoke:
    def __init__(self, digital_kinds: tuple[str, ...] = DIGITAL_KINDS) -> None:
        self.base = os.environ["MARKETPLACE_URL"].rstrip("/")
        self.project = os.environ["RAILWAY_PROJECT_ID"]
        refuse_production(self.base, self.project)
        environment = os.environ.get("RAILWAY_ENVIRONMENT", STAGING_ENVIRONMENT)
        os.environ["RAILWAY_ENVIRONMENT"] = environment
        if environment not in {STAGING_ENVIRONMENT, "production"}:
            raise SystemExit(f"HOLD_SMOKE=STOP unexpected RAILWAY_ENVIRONMENT {environment}")
        self.sessions: list[str] = []
        self.created_orders: list[dict[str, Any]] = []
        self.cases: list[dict[str, Any]] = []
        self.fiat_window: int | None = None
        self.digital_kinds = digital_kinds

    def load_fiat_window(self) -> int:
        if self.fiat_window is None:
            self.fiat_window = fiat_window_seconds()
        return self.fiat_window

    def command(self, token: str, body: dict[str, Any], *, allowed: set[int], case: str) -> tuple[int, dict | str]:
        status, resp = api(self.base, "POST", "/v1/commands", token, body)
        stop_on_unexpected_4xx(status, resp, allowed=allowed, case=case)
        return status, resp

    def cancel_order(self, token: str, order_id: str, reason: str) -> dict[str, Any]:
        revision = order_revision(order_id)
        status, resp = self.command(
            token,
            cancel_body(order_id, revision, reason),
            allowed={200, 409},
            case=f"cancel:{order_id}",
        )
        ok = isinstance(resp, dict) and resp.get("ok") is True
        if status == 409 and isinstance(resp, dict):
            message = ((resp.get("error") or {}).get("message") or "")
            if "no longer be cancelled" in message:
                cols_raw = sql(
                    "SELECT stock_held::text, coalesce(hold_source,''), "
                    "coalesce(hold_expires_at::text,''), state "
                    f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
                )
                hold = parse_hold_row(cols_raw)
                if hold["state"] == "cancelled" and not hold["stock_held"]:
                    return {"http": status, "state": hold["state"], "stock_held": False, "already_cancelled": True}
        if status != 200 or not ok:
            raise RuntimeError(f"cancel failed {status} {resp!r}")
        cols_raw = sql(
            "SELECT stock_held::text, coalesce(hold_source,''), "
            "coalesce(hold_expires_at::text,''), state "
            f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
        )
        hold = parse_hold_row(cols_raw)
        if hold["state"] != "cancelled":
            raise RuntimeError(f"order {order_id} state {hold['state']!r} after cancel")
        if hold["stock_held"]:
            raise RuntimeError(f"order {order_id} still stock_held after cancel")
        return {"http": status, "state": hold["state"], "stock_held": hold["stock_held"]}

    def order_hold(self, order_id: str) -> dict[str, Any]:
        hold_raw = sql(
            "SELECT stock_held::text, coalesce(hold_source,''), "
            "coalesce(hold_expires_at::text,''), state "
            f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
        )
        return parse_hold_row(hold_raw)

    def bind_payment(
        self, token: str, order_id: str, method: str, *, case: str
    ) -> tuple[int, dict | str]:
        body = bind_body(method)
        status, resp = api(
            self.base,
            "POST",
            f"/v0/orders/{order_id}/payment-method",
            token,
            body,
        )
        stop_on_unexpected_4xx(status, resp, allowed={200, 409}, case=case)
        return status, resp

    def two_buyer(
        self,
        listing: dict[str, Any],
        *,
        fulfillment: str,
        address: dict[str, str] | None,
        case: str,
    ) -> dict[str, Any]:
        method = listing.get("bind_method") or ""
        if method != "paypal":
            raise RuntimeError(f"{case}: listing has no paypal bind method {listing!r}")
        window = self.load_fiat_window()
        buyer_a = z32_pubky()
        buyer_b = z32_pubky()
        if buyer_a == listing["seller_pubky"] or buyer_b == listing["seller_pubky"]:
            raise RuntimeError("buyer collides with seller")
        token_a, sid_a = insert_session(buyer_a)
        token_b, sid_b = insert_session(buyer_b)
        self.sessions.extend([sid_a, sid_b])
        cmd_a = str(uuid.uuid4())
        cmd_b = str(uuid.uuid4())
        body_a = build_checkout(listing, cmd_a, buyer_pubky=buyer_a, fulfillment=fulfillment, address=address)
        body_b = build_checkout(listing, cmd_b, buyer_pubky=buyer_b, fulfillment=fulfillment, address=address)
        assert_shape(body_a, shipping=fulfillment == "shipping")
        assert_shape(body_b, shipping=fulfillment == "shipping")
        entry_a = sealed_entry(body_a)
        entry_b = sealed_entry(body_b)
        status_a, resp_a = self.command(token_a, body_a, allowed={200}, case=f"{case}.first_create")
        status_b, resp_b = self.command(token_b, body_b, allowed={200}, case=f"{case}.second_create")
        first_ok = isinstance(resp_a, dict) and resp_a.get("ok") is True
        second_ok = isinstance(resp_b, dict) and resp_b.get("ok") is True
        if not first_ok or not second_ok:
            raise RuntimeError(
                f"{case}: both checkouts must succeed unheld first={status_a} {resp_a!r} "
                f"second={status_b} {resp_b!r}"
            )
        order_a = resp_a["result"]["orders"][0]["id"]  # type: ignore[index]
        order_b = resp_b["result"]["orders"][0]["id"]  # type: ignore[index]
        self.created_orders.append({"id": order_a, "token": token_a, "case": case})
        self.created_orders.append({"id": order_b, "token": token_b, "case": case})
        hold_a = self.order_hold(order_a)
        hold_b = self.order_hold(order_b)
        if hold_a["stock_held"] or hold_b["stock_held"]:
            raise RuntimeError(f"{case}: checkout must not hold a={hold_a!r} b={hold_b!r}")
        if hold_a["hold_source"] or hold_b["hold_source"]:
            raise RuntimeError(f"{case}: checkout hold_source a={hold_a!r} b={hold_b!r}")
        bind_a_status, bind_a = self.bind_payment(token_a, order_a, method, case=f"{case}.first_bind")
        bind_b_status, bind_b = self.bind_payment(token_b, order_b, method, case=f"{case}.second_bind")
        if bind_a_status == 200 and bind_b_status == 200:
            raise RuntimeError(f"{case}: both binds succeeded")
        if bind_a_status == 200:
            winner_order, winner_token, winner_entry = order_a, token_a, entry_a
            loser_status, loser_body = bind_b_status, bind_b
        elif bind_b_status == 200:
            winner_order, winner_token, winner_entry = order_b, token_b, entry_b
            loser_status, loser_body = bind_a_status, bind_a
        else:
            raise RuntimeError(
                f"{case}: no bind winner first={bind_a_status} {bind_a!r} second={bind_b_status} {bind_b!r}"
            )
        loser_message = (loser_body.get("error") or {}).get("message") if isinstance(loser_body, dict) else None
        if loser_status != 409:
            raise RuntimeError(f"{case}: bind loser http {loser_status} body={loser_body!r}")
        if loser_message != HOLDING_COPY:
            raise RuntimeError(f"{case}: bind loser message {loser_message!r}")
        hold = self.order_hold(winner_order)
        if not hold["stock_held"] or hold["hold_source"] != "bind":
            raise RuntimeError(f"{case}: hold after bind {hold!r}")
        ttl = float(
            sql_scalar(
                sql(
                    "SELECT extract(epoch from (hold_expires_at - now())) "
                    f"FROM orders WHERE id='{quote(winner_order)}'::uuid;"
                )
            )
        )
        if not ttl_matches_window(ttl, window):
            raise RuntimeError(f"{case}: ttl {ttl} window={window}")
        stored = assert_stored_address(winner_order, winner_entry, case=case)
        cancel = self.cancel_order(winner_token, winner_order, f"hold-smoke {case} restock")
        loser_order = order_b if winner_order == order_a else order_a
        loser_token = token_b if winner_order == order_a else token_a
        self.cancel_order(loser_token, loser_order, f"hold-smoke {case} loser")
        listing_after = refresh_listing(listing["aggregate_id"])
        if listing_after["available_quantity"] != 1:
            raise RuntimeError(
                f"{case}: listing qty {listing_after['available_quantity']} after cancel"
            )
        return {
            "case": case,
            "listing": listing["aggregate_id"],
            "shop_tag": SHOP_TAG,
            "shop_sha": SHOP_SHA,
            "fulfillment": fulfillment,
            "address_class": address_class(address),
            "stored_address": redacted_facts(stored),
            "bind_method": method,
            "fiat_window_seconds": window,
            "first_http": status_a,
            "second_http": status_b,
            "first_bind_http": bind_a_status,
            "second_bind_http": bind_b_status,
            "loser_http": loser_status,
            "loser_message": loser_message,
            "winner_order_id": winner_order,
            "hold": hold,
            "ttl_seconds": ttl,
            "cancel": cancel,
            "listing_qty_after": listing_after["available_quantity"],
        }

    def one_buyer(
        self,
        listing: dict[str, Any],
        *,
        fulfillment: str,
        address: dict[str, str] | None,
        case: str,
    ) -> dict[str, Any]:
        buyer = z32_pubky()
        if buyer == listing["seller_pubky"]:
            raise RuntimeError("buyer collides with seller")
        token, sid = insert_session(buyer)
        self.sessions.append(sid)
        body = build_checkout(listing, str(uuid.uuid4()), buyer_pubky=buyer, fulfillment=fulfillment, address=address)
        assert_shape(body, shipping=fulfillment == "shipping")
        status, resp = self.command(token, body, allowed={200}, case=case)
        ok = isinstance(resp, dict) and resp.get("ok") is True
        if not ok:
            raise RuntimeError(f"{case}: not ok {status} {resp!r}")
        order_id = resp["result"]["orders"][0]["id"]  # type: ignore[index]
        self.created_orders.append({"id": order_id, "token": token, "case": case})
        hold = self.order_hold(order_id)
        if hold["stock_held"] or hold["hold_source"]:
            raise RuntimeError(f"{case}: checkout must not hold {hold!r}")
        stored = assert_stored_address(order_id, sealed_entry(body), case=case)
        cancel = self.cancel_order(token, order_id, f"hold-smoke {case} restock")
        listing_after = refresh_listing(listing["aggregate_id"])
        if listing_after["available_quantity"] != 1:
            raise RuntimeError(f"{case}: listing qty after cancel {listing_after['available_quantity']}")
        return {
            "case": case,
            "listing": listing["aggregate_id"],
            "shop_tag": SHOP_TAG,
            "shop_sha": SHOP_SHA,
            "fulfillment": fulfillment,
            "address_class": address_class(address),
            "stored_address": redacted_facts(stored),
            "http": status,
            "order_id": order_id,
            "hold": hold,
            "cancel": cancel,
            "listing_qty_after": listing_after["available_quantity"],
        }

    def plaintext_refusal(self, listing: dict[str, Any], *, case: str) -> dict[str, Any]:
        """A plaintext address under REQUIRE_ENCRYPTED_DELIVERY_ADDRESS=true is refused and leaves no order."""
        buyer = z32_pubky()
        if buyer == listing["seller_pubky"]:
            raise RuntimeError("buyer collides with seller")
        token, sid = insert_session(buyer)
        self.sessions.append(sid)
        address = SHIPPING_ADDRESSES["california_free_text"]
        body = checkout_body(listing, str(uuid.uuid4()), fulfillment="shipping", address=address)
        assert_shape(body, shipping=True, sealed=False)
        status, resp = api(self.base, "POST", "/v1/commands", token, body)
        if status == 200:
            if isinstance(resp, dict) and resp.get("ok") is True:
                orders = (resp.get("result") or {}).get("orders") or []
                for order in orders:
                    self.created_orders.append({"id": order["id"], "token": token, "case": case})
            raise RuntimeError(
                f"STOP {case}: the service accepted a plaintext delivery address, so "
                "REQUIRE_ENCRYPTED_DELIVERY_ADDRESS is not on for this service"
            )
        reason = refusal_reason(resp)
        if status != 422 or reason != ENCRYPTED_REQUIRED_REASON:
            raise RuntimeError(f"STOP {case}: want 422 {ENCRYPTED_REQUIRED_REASON}, got {status} reason={reason!r}")
        echoed = json.dumps(resp)
        for marker in (address["line1"], address["name"], address["postal_code"]):
            if marker in echoed:
                raise RuntimeError(f"STOP {case}: the refusal echoed part of the address")
        created = int(
            sql_scalar(sql(f"SELECT count(*)::text FROM orders WHERE buyer_pubky='{quote(buyer)}';"))
        )
        if created:
            raise RuntimeError(f"STOP {case}: a refused checkout left {created} order rows")
        return {"case": case, "listing": listing["aggregate_id"], "http": status, "reason": reason, "orders_created": created}

    def refused_read(
        self, token: str, path: str, *, reason: str, case: str, status_expected: int = 409
    ) -> dict[str, Any]:
        status, body = api(self.base, "GET", path, token)
        stop_on_unexpected_4xx(status, body, allowed={status_expected}, case=case)
        if status != status_expected or (reason and refusal_reason(body) != reason):
            raise RuntimeError(
                f"{case}: expected {status_expected} {reason!r}, got {status} {refusal_reason(body)!r}"
            )
        return {"http": status, "reason": refusal_reason(body)}

    def digital_case(self, kind: str, listing: dict[str, Any]) -> dict[str, Any]:
        """Hold and release for one digital delivery kind on an unpaid order.

        Checkout takes no hold; the bind takes it and the cancel gives it back.
        At every point nothing is released: the buyer's download and the
        seller's read of the buyer's email answer `not_paid`, and no pin,
        access row or receipt exists.
        """
        case = f"digital_{kind}"
        method = listing.get("bind_method") or ""
        if method != "paypal":
            raise RuntimeError(f"{case}: listing has no paypal bind method {listing!r}")
        window = self.load_fiat_window()
        aggregate_id = listing["aggregate_id"]
        stock_before = listing_stock(aggregate_id)
        available, reserved = stock_before
        buyer = z32_pubky()
        if buyer == listing["seller_pubky"]:
            raise RuntimeError("buyer collides with seller")
        token, sid = insert_session(buyer)
        self.sessions.append(sid)
        seller_token = None
        if kind == "email":
            seller_token, seller_sid = insert_session(listing["seller_pubky"])
            self.sessions.append(seller_sid)
        email = DIGITAL_SMOKE_EMAIL if kind == "email" else None
        body = checkout_body(
            listing, str(uuid.uuid4()), fulfillment="digital", address=None, delivery_email=email
        )
        assert_digital_shape(body, kind=kind)
        status, resp = self.command(token, body, allowed={200}, case=f"{case}.create")
        if not (isinstance(resp, dict) and resp.get("ok") is True):
            raise RuntimeError(f"{case}: not ok {status} {resp!r}")
        orders = resp["result"]["orders"]  # type: ignore[index]
        if len(orders) != 1:
            raise RuntimeError(f"{case}: a digital-only cart must be one order, got {len(orders)}")
        order_id = orders[0]["id"]
        self.created_orders.append({"id": order_id, "token": token, "case": case})
        hold = self.order_hold(order_id)
        if hold["stock_held"] or hold["hold_source"]:
            raise RuntimeError(f"{case}: checkout must not hold {hold!r}")
        if listing_stock(aggregate_id) != stock_before:
            raise RuntimeError(f"{case}: checkout moved the listing's stock")
        facts = digital_order_facts(order_id)
        expected_facts = {
            "fulfillment": "digital",
            "digital_kind": kind,
            "no_address": True,
            "no_receipt": True,
            "pins": 0,
            "access_rows": 0,
            "email_rows": 1 if kind == "email" else 0,
        }
        if facts != expected_facts:
            raise RuntimeError(f"{case}: order facts {facts!r} != {expected_facts!r}")

        download = f"/v1/orders/{order_id}/digital-delivery/0"
        email_path = f"/v1/orders/{order_id}/delivery-email"

        def nothing_released(stage: str) -> dict[str, Any]:
            reads = {
                "download": self.refused_read(
                    token, download, reason="not_paid", case=f"{case}.{stage}.download"
                )
            }
            if kind == "email":
                assert seller_token is not None
                reads["seller_email"] = self.refused_read(
                    seller_token, email_path, reason="not_paid", case=f"{case}.{stage}.seller_email"
                )
                status_b, own = api(self.base, "GET", email_path, token)
                stop_on_unexpected_4xx(status_b, own, allowed={200}, case=f"{case}.{stage}.buyer_email")
                if not (isinstance(own, dict) and own.get("delivery_email") == email):
                    raise RuntimeError(f"{case}.{stage}: the buyer could not read their own address")
                reads["buyer_email"] = {"http": status_b}
            else:
                reads["buyer_email"] = self.refused_read(
                    token, email_path, reason="", case=f"{case}.{stage}.buyer_email", status_expected=404
                )
            after = digital_order_facts(order_id)
            if after["pins"] or after["access_rows"] or not after["no_receipt"]:
                raise RuntimeError(f"{case}.{stage}: something was released {after!r}")
            return reads

        pending_reads = nothing_released("pending")
        bind_status, bind_resp = self.bind_payment(token, order_id, method, case=f"{case}.bind")
        if bind_status != 200:
            raise RuntimeError(f"{case}: bind http {bind_status} {bind_resp!r}")
        held = self.order_hold(order_id)
        if not held["stock_held"] or held["hold_source"] != "bind":
            raise RuntimeError(f"{case}: hold after bind {held!r}")
        ttl = float(
            sql_scalar(
                sql(
                    "SELECT extract(epoch from (hold_expires_at - now())) "
                    f"FROM orders WHERE id='{quote(order_id)}'::uuid;"
                )
            )
        )
        if not ttl_matches_window(ttl, window):
            raise RuntimeError(f"{case}: ttl {ttl} window={window}")
        stock_held = listing_stock(aggregate_id)
        if stock_held != (available - 1, reserved + 1):
            raise RuntimeError(f"{case}: stock during the hold {stock_held} from {stock_before}")
        held_reads = nothing_released("held")

        cancel = self.cancel_order(token, order_id, f"hold-smoke {case} release")
        stock_after = listing_stock(aggregate_id)
        if stock_after != stock_before:
            raise RuntimeError(f"{case}: stock after cancel {stock_after} != {stock_before}")
        cancelled_reads = nothing_released("cancelled")
        return {
            "case": case,
            "listing": aggregate_id,
            "kind": kind,
            "bind_method": method,
            "fiat_window_seconds": window,
            "order_id": order_id,
            "order_facts": facts,
            "pending_reads": pending_reads,
            "hold": held,
            "ttl_seconds": ttl,
            "stock_before": stock_before,
            "stock_during_hold": stock_held,
            "stock_after_cancel": stock_after,
            "held_reads": held_reads,
            "cancelled_reads": cancelled_reads,
            "cancel": cancel,
        }

    def verify_all_cancelled(self) -> list[dict[str, Any]]:
        verified = []
        for row in self.created_orders:
            raw = sql(
                "SELECT stock_held::text, coalesce(hold_source,''), "
                "coalesce(hold_expires_at::text,''), state "
                f"FROM orders WHERE id='{quote(row['id'])}'::uuid;"
            )
            hold = parse_hold_row(raw)
            if hold["state"] != "cancelled" or hold["stock_held"]:
                raise RuntimeError(f"leftover order {row['id']} hold={hold!r}")
            verified.append({"id": row["id"], "case": row["case"], "state": hold["state"]})
        return verified

    def cleanup_sessions(self) -> None:
        for sid in self.sessions:
            try:
                sql(f"DELETE FROM auth_sessions WHERE session_id='{quote(sid)}'::uuid;")
            except Exception as error:  # noqa: BLE001 — cleanup must not hide the smoke result
                print(f"session_cleanup_error={error}", file=sys.stderr)

    def run(self) -> dict[str, Any]:
        lifecycle = FixtureLifecycle(self.digital_kinds)
        lifecycle.check_environment()
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, terminate_as_exit)
        try:
            try:
                lifecycle.publish()
            except BaseException:
                print("HOLD_SMOKE=STOP the fixtures could not be published", file=sys.stderr)
                raise
            summary = self.run_cases()
        except BaseException:
            problems = lifecycle.retire()
            if problems:
                print(json.dumps({"fixtures_not_retired": problems}, indent=2), file=sys.stderr)
                print("HOLD_SMOKE_FIXTURES=NOT_RETIRED", file=sys.stderr)
            raise
        problems = lifecycle.retire()
        if problems:
            print(json.dumps({"fixtures_not_retired": problems}, indent=2))
            print("HOLD_SMOKE=STOP")
            raise RuntimeError("hold-smoke fixtures were not retired: " + "; ".join(problems))
        summary["fixtures"] = "ephemeral: swept, 404 on every Nexus and the service, guard PASS"
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        print("HOLD_SMOKE=PASS")
        return summary

    def run_cases(self) -> dict[str, Any]:
        try:
            shipping = pick_listing(shipping=True, require_bind=True)
            pickup = pick_listing(shipping=False, exclude={shipping["aggregate_id"]}, require_bind=True)
            self.cases.append(self.plaintext_refusal(shipping, case="plaintext_refused_under_flag"))
            shipping = refresh_listing(shipping["aggregate_id"])
            self.cases.append(
                self.two_buyer(
                    shipping,
                    fulfillment="shipping",
                    address=SHIPPING_ADDRESSES["california_free_text"],
                    case="shipping_california_free_text",
                )
            )
            shipping = refresh_listing(shipping["aggregate_id"])
            self.cases.append(
                self.one_buyer(
                    shipping,
                    fulfillment="shipping",
                    address=SHIPPING_ADDRESSES["ca_iso_suffix"],
                    case="shipping_ca_iso_suffix",
                )
            )
            shipping = refresh_listing(shipping["aggregate_id"])
            pickup = refresh_listing(pickup["aggregate_id"])
            self.cases.append(
                self.two_buyer(
                    pickup,
                    fulfillment="pickup",
                    address=None,
                    case="pickup_two_buyer",
                )
            )
            shipping = refresh_listing(shipping["aggregate_id"])
            self.cases.append(
                self.one_buyer(
                    shipping,
                    fulfillment="shipping",
                    address=SHIPPING_ADDRESSES["pt_empty_region"],
                    case="shipping_pt_empty_region",
                )
            )
            used = {shipping["aggregate_id"], pickup["aggregate_id"]}
            for kind in self.digital_kinds:
                listing = pick_digital_listing(kind, used)
                used.add(listing["aggregate_id"])
                self.cases.append(self.digital_case(kind, listing))
            verified = self.verify_all_cancelled()
            summary = {
                "shop_tag": SHOP_TAG,
                "shop_sha": SHOP_SHA,
                "marketplace_url": self.base,
                "railway_project": self.project,
                "fiat_window_seconds": self.fiat_window,
                "fixture_seller": FIXTURE_SELLER[:8],
                "digital_kinds": list(self.digital_kinds),
                "cases": self.cases,
                "orders_verified_cancelled": verified,
            }
            return summary
        except Exception:
            for row in list(self.created_orders):
                try:
                    self.cancel_order(row["token"], row["id"], "hold-smoke failure cleanup")
                except Exception as error:  # noqa: BLE001
                    print(f"cancel_cleanup_error={row['id']}:{error}", file=sys.stderr)
            print(
                json.dumps(
                    {
                        "hold_smoke": "STOP",
                        "shop_tag": SHOP_TAG,
                        "shop_sha": SHOP_SHA,
                        "marketplace_url": self.base,
                        "cases_completed": self.cases,
                        "created_orders": [{"id": row["id"], "case": row["case"]} for row in self.created_orders],
                    },
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            )
            print("HOLD_SMOKE=STOP")
            raise
        finally:
            self.cleanup_sessions()


def main() -> None:
    parser = argparse.ArgumentParser(description="Staging hold-smoke (never production).")
    parser.add_argument("--self-check", action="store_true", help="Validate payload shape without hitting staging.")
    parser.add_argument(
        "--digital-kinds",
        default="all",
        help="Digital delivery kinds to smoke: all (default), or a comma list of "
        f"{', '.join(DIGITAL_KINDS)}; an empty value runs none.",
    )
    args = parser.parse_args()
    digital_kinds = parse_digital_kinds(args.digital_kinds)
    if args.self_check:
        self_check()
        return
    for required in ("MARKETPLACE_URL", "RAILWAY_PROJECT_ID"):
        if required not in os.environ:
            raise SystemExit(f"HOLD_SMOKE=STOP missing {required}")
    os.environ.setdefault("RAILWAY_ENVIRONMENT", STAGING_ENVIRONMENT)
    os.environ.setdefault("RAILWAY_DATABASE_SERVICE", "Postgres")
    Smoke(digital_kinds).run()


if __name__ == "__main__":
    main()
