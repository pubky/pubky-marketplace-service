#!/usr/bin/env node
// Publishes, sweeps and guards the shipping and pickup fixtures hold-smoke.py picks from. hold-smoke.py runs
// these modes itself; the fixtures live only for one run (see Fixture lifecycle in its header).
//
// hold-smoke.py takes every listing from one dedicated staging seller
// (HOLD_SMOKE_FIXTURE_SELLER). That seller needs a qty-1, fixed-price,
// PayPal-railed shipping listing and a pickup-only one, both titled
// "SMOKE FIXTURE, do not buy". Its digital fixtures (one per delivery kind)
// come from the Shop's scripts/release/production-digital-fixture.mjs.
// The seller's payment config must already carry a throwaway PayPal email
// (never a real account): hold-smoke binds PayPal but never confirms a payment.
//
//   publish   publish whichever of the two fixtures is missing, register it
//             with the service, and wait for the index to show it
//   status    print each fixture's homeserver, index and service state
//   sweep     delete every listing record, media file and delivery ciphertext the seat holds, then wait for 404
//             on the homeserver, every guarded Nexus and the service (needs HOLD_SMOKE_FIXTURE_SELLER: the seat
//             must be the dedicated fixture seller)
//   guard     fail unless no guarded Nexus lists an active listing owned by HOLD_SMOKE_FIXTURE_SELLER or titled
//             "SMOKE FIXTURE, do not buy"; an unreadable stream fails too. Needs no seat.
//   check     offline: build both records and check their shape; no network
//
// Records follow pubky-app-specs: a shipped listing is
// fulfillmentMethods ['physical', 'shipping'] with a package and a shipping
// option; a pickup-only listing is ['pickup'] with neither (`physical` needs a
// shipping option, and a package or shipping option needs `physical`). The
// service gets the Shop's wire form, which drops `physical`.
//
// Environment:
//   HOLD_SMOKE_SEAT_FILE    env file holding PROOF_SEAT_SECRET_HEX for the
//                           fixture seller (keep it mode 600, outside the repo)
//   HOLD_SMOKE_FIXTURE_STATE  JSON file the listing ids are kept in (mode 600)
//   PUBKY_SDK_DIR           directory whose node_modules holds
//                           @synonymdev/pubky (0.11 API: startCookieAuthFlow,
//                           signinCookie) and @noble/hashes
//   MARKETPLACE_URL         default https://staging-api.pubky.app
//   NEXUS_URL               default https://nexusd-staging-production.up.railway.app
//   HTTP_RELAY              default https://httprelay.pubky.app/inbox
//   HOLD_SMOKE_FIXTURE_SELLER  optional for publish and status (the seat must match it when set); required
//                           for sweep and guard
//   HOLD_SMOKE_GUARD_NEXUS  comma list of Nexus hosts that must not list a fixture; default the production and
//                           staging Nexus (change it when the production Nexus moves)
// Staging only: it refuses a production service host.

import { chmodSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { crc32, deflateSync } from 'node:zlib';

const SERVICE = process.env.MARKETPLACE_URL ?? 'https://staging-api.pubky.app';
const NEXUS = process.env.NEXUS_URL ?? 'https://nexusd-staging-production.up.railway.app';
const RELAY = process.env.HTTP_RELAY ?? 'https://httprelay.pubky.app/inbox';
const PRODUCTION_HOST_MARKERS = ['marketplace-service-production-ce23', '-ce23.up.railway.app', 'marketplace-edge-production', 'shop.pubky.app'];
const LISTINGS = '/pub/pubky.app/marketplace/v1/listings';
const MEDIA = '/pub/pubky.app/marketplace/v1/media';
const KINDS = {
  shipping: { methods: ['physical', 'shipping'], service: ['shipping'] },
  pickup: { methods: ['pickup'], service: ['pickup'] },
};
const FIXTURE_TITLE_PREFIX = 'SMOKE FIXTURE, do not buy';
const TITLE = (kind) => `${FIXTURE_TITLE_PREFIX} hold-smoke ${kind}`;
const DELIVERABLES = '/pub/pubky.app/marketplace/v1/deliverables';
// Every Nexus a fixture must be absent from. The staging homeserver is indexed by the production Nexus as well,
// so a fixture published here is visible to real buyers until it is deleted.
const GUARD_NEXUS = (process.env.HOLD_SMOKE_GUARD_NEXUS ?? 'https://nexusd-production-7108.up.railway.app,https://nexusd-staging-production.up.railway.app')
  .split(',').map((url) => url.trim().replace(/\/$/, '')).filter(Boolean);
const STREAM_PAGE = 100;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const log = (text) => process.stderr.write(`${new Date().toISOString()} ${text}\n`);

// A plain 64x64 PNG, built here so the script carries no binary file.
function fixturePng(size = 64) {
  const chunk = (type, data) => {
    const body = Buffer.concat([Buffer.from(type, 'ascii'), data]);
    const out = Buffer.alloc(body.length + 8);
    out.writeUInt32BE(data.length, 0);
    body.copy(out, 4);
    out.writeUInt32BE(crc32(body), body.length + 4);
    return out;
  };
  const header = Buffer.alloc(13);
  header.writeUInt32BE(size, 0);
  header.writeUInt32BE(size, 4);
  header.set([8, 2, 0, 0, 0], 8);
  const row = Buffer.concat([Buffer.from([0]), Buffer.alloc(size * 3, 0x7a)]);
  const pixels = deflateSync(Buffer.concat(Array.from({ length: size }, () => row)));
  return {
    bytes: Buffer.concat([
      Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
      chunk('IHDR', header),
      chunk('IDAT', pixels),
      chunk('IEND', Buffer.alloc(0)),
    ]),
    width: size,
    height: size,
  };
}

export function fixtureRecord(kind, { listingId, pubky, media, now = new Date().toISOString() }) {
  const record = {
    recordType: 'listing', schemaVersion: 1, listingId, ownerPubky: pubky, title: TITLE(kind),
    description: 'Release hold-smoke fixture (staging). Do not buy.', categoryId: 'other', taxonomyVersion: 2,
    condition: 'good', adultOnly: false, attributes: null, tags: ['smoke-fixture', 'do-not-buy'], state: 'active', revision: 1,
    createdAt: now, updatedAt: now, fulfillmentMethods: KINDS[kind].methods, location: { countryCode: 'US', region: null }, media: [media],
    package: { heightMillimeters: 102, lengthMillimeters: 254, weightGrams: 454, widthMillimeters: 203 },
    returnPolicy: { acceptsReturns: false, buyerPaysReturnShipping: true, returnWindowDays: null },
    sale: { acceptsOffers: false, format: 'fixed_price', unitPrice: { amountMinor: 100, currency: 'USD', exponent: 2 } },
    shippingOptions: [{ estimatedMaxDays: 7, estimatedMinDays: 3, id: 'seller_flat_rate', label: 'Seller shipping', price: { amountMinor: 0, currency: 'USD', exponent: 2 }, pricing: 'flat' }],
    variants: [{ enabled: true, id: 'variant_1', mediaIds: [media.id], options: {}, priceOverride: null, quantity: 1, sku: null }],
  };
  if (kind === 'pickup') Object.assign(record, { package: null, shippingOptions: [] });
  return record;
}

export function registerCommand(kind, record) {
  return {
    version: 1, command_id: randomUUID(), aggregate_id: `listing:${record.ownerPubky}_${record.listingId}`, expected_revision: 0,
    issued_at: new Date().toISOString().replace(/\.\d{3}Z$/, '.000Z'), kind: 'listing.register',
    payload: {
      seller_pubky: record.ownerPubky, listing_id: record.listingId, title: record.title, listing_revision: 1,
      content_hash: record.media[0].contentHash, quantity: 1,
      unit_price: { amount_minor: 100, currency: 'USD', exponent: 2 }, shipping_minor: 0,
      sale_format: 'fixed_price', fulfillment_methods: KINDS[kind].service,
    },
  };
}

export function fixtureLeaks(listings, sellers) {
  return listings
    .filter((l) => sellers.includes(l.owner_id) || String(l.title ?? '').startsWith(FIXTURE_TITLE_PREFIX))
    .map((l) => ({ owner: String(l.owner_id).slice(0, 8), id: l.id, title: l.title }));
}

async function activeStream(nexus) {
  const all = [];
  for (let skip = 0; ; skip += STREAM_PAGE) {
    const response = await fetch(`${nexus}/v0/stream/listings?state=active&limit=${STREAM_PAGE}&skip=${skip}`);
    if (response.status !== 200) throw new Error(`${nexus} listing stream answered ${response.status}`);
    const page = await response.json();
    if (!Array.isArray(page)) throw new Error(`${nexus} listing stream is not a list`);
    all.push(...page);
    if (page.length < STREAM_PAGE) return all;
  }
}

// Fails (exit 1) when any Nexus lists an active fixture. An unreadable stream also fails: the guard never assumes clean.
async function guard() {
  const sellers = [requireEnv('HOLD_SMOKE_FIXTURE_SELLER')];
  const leaks = [];
  const unreadable = [];
  for (const nexus of GUARD_NEXUS) {
    try {
      for (const leak of fixtureLeaks(await activeStream(nexus), sellers)) leaks.push({ nexus: new URL(nexus).host, ...leak });
    } catch (error) {
      unreadable.push(`${nexus}: ${error.message}`);
    }
  }
  console.log(JSON.stringify({ guard: leaks.length || unreadable.length ? 'FAIL' : 'PASS', nexus: GUARD_NEXUS.length, leaks, unreadable }));
  process.exitCode = leaks.length || unreadable.length || GUARD_NEXUS.length === 0 ? 1 : 0;
}

function check() {
  const pubky = 'y'.repeat(52);
  const media = { id: '0'.repeat(32), type: 'image', url: `pubky://${pubky}${MEDIA}/${'0'.repeat(32)}`, contentHash: 'a'.repeat(64), mimeType: 'image/png', byteSize: 1, width: 64, height: 64, altText: 'x' };
  const shipping = fixtureRecord('shipping', { listingId: 'hsfx_shipping_x', pubky, media });
  const pickup = fixtureRecord('pickup', { listingId: 'hsfx_pickup_x', pubky, media });
  const failures = [];
  if (shipping.fulfillmentMethods.join() !== 'physical,shipping' || !shipping.package || shipping.shippingOptions.length !== 1) failures.push('shipping record shape');
  if (pickup.fulfillmentMethods.join() !== 'pickup' || pickup.package !== null || pickup.shippingOptions.length !== 0) failures.push('pickup record shape');
  if (registerCommand('pickup', pickup).payload.fulfillment_methods.join() !== 'pickup') failures.push('pickup wire methods');
  if (registerCommand('shipping', shipping).payload.fulfillment_methods.join() !== 'shipping') failures.push('shipping wire methods');
  if (![shipping, pickup].every((r) => r.title.startsWith('SMOKE FIXTURE, do not buy') && r.variants[0].quantity === 1)) failures.push('title or quantity');
  const png = fixturePng();
  if (png.bytes.readUInt32BE(16) !== 64 || png.bytes.toString('ascii', 12, 16) !== 'IHDR') failures.push('fixture png');
  if (!['https://marketplace-service-production-ce23.up.railway.app', 'https://marketplace-edge-production.up.railway.app'].every((u) => PRODUCTION_HOST_MARKERS.some((m) => u.includes(m)))) failures.push('production guard');
  const stream = [
    { owner_id: pubky, id: 'a', title: 'anything' },
    { owner_id: 'z'.repeat(52), id: 'b', title: TITLE('pickup') },
    { owner_id: 'z'.repeat(52), id: 'c', title: 'Pav test: active, buy me' },
  ];
  if (fixtureLeaks(stream, [pubky]).map((l) => l.id).join() !== 'a,b') failures.push('leak detection');
  if (fixtureLeaks(stream, []).length !== 1) failures.push('leak detection by title only');
  console.log(failures.length ? `HOLD_SMOKE_FIXTURES=CHECK_FAIL ${failures.join(', ')}` : 'HOLD_SMOKE_FIXTURES=CHECK_PASS');
  process.exitCode = failures.length ? 1 : 0;
}

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`set ${name} (see the header of this script)`);
  return value;
}

async function main(mode) {
  if (PRODUCTION_HOST_MARKERS.some((marker) => SERVICE.includes(marker))) throw new Error('staging only: refusing a production service host');
  const require = createRequire(join(requireEnv('PUBKY_SDK_DIR'), 'package.json'));
  const { AuthFlowKind, Keypair, Pubky } = require('@synonymdev/pubky');
  const { blake3 } = require('@noble/hashes/blake3.js');
  const { bytesToHex } = require('@noble/hashes/utils.js');

  const statePath = requireEnv('HOLD_SMOKE_FIXTURE_STATE');
  const secretLine = readFileSync(requireEnv('HOLD_SMOKE_SEAT_FILE'), 'utf8').split('\n').find((l) => l.startsWith('PROOF_SEAT_SECRET_HEX='));
  if (!secretLine) throw new Error('the seat file has no PROOF_SEAT_SECRET_HEX');
  const seat = Keypair.fromSecret(new Uint8Array(Buffer.from(secretLine.slice('PROOF_SEAT_SECRET_HEX='.length).trim(), 'hex')));
  const pk = seat.publicKey.z32();
  if (process.env.HOLD_SMOKE_FIXTURE_SELLER && process.env.HOLD_SMOKE_FIXTURE_SELLER !== pk) throw new Error('the seat is not HOLD_SMOKE_FIXTURE_SELLER');
  const readState = () => (existsSync(statePath) ? JSON.parse(readFileSync(statePath, 'utf8')) : {});
  const writeState = (value) => {
    writeFileSync(statePath, `${JSON.stringify(value)}\n`, { mode: 0o600 });
    chmodSync(statePath, 0o600);
  };
  const publicStorage = new Pubky().publicStorage;
  const onHomeserver = async (id) => await publicStorage.exists(`pubky://${pk}${LISTINGS}/${id}`).catch(() => null);
  const indexStatus = async (path) => (await fetch(`${NEXUS}${path}`)).status;
  const waitForUrl = async (url, want, timeoutMs = 180_000) => {
    const start = Date.now();
    let status = null;
    while (Date.now() - start < timeoutMs) {
      status = (await fetch(url).catch(() => null))?.status ?? null;
      if (status === want) return { status, ms: Date.now() - start };
      await sleep(5_000);
    }
    return { status, ms: Date.now() - start, timedOut: true };
  };
  const waitFor = (path, want, timeoutMs) => waitForUrl(`${NEXUS}${path}`, want, timeoutMs);

  const names = async (path) => {
    const out = [];
    let cursor = null;
    for (;;) {
      const page = await publicStorage.list(`pubky://${pk}${path}/`, cursor, false, STREAM_PAGE).catch((error) => {
        if (/404|not found/i.test(String(error?.message ?? error))) return [];
        throw error;
      });
      out.push(...page.map((url) => url.split('/').pop()));
      if (page.length < STREAM_PAGE) return out;
      cursor = page[page.length - 1];
    }
  };

  // Deletes every listing record, media file and delivery ciphertext the fixture seller holds, then waits until
  // each deleted listing is gone from the homeserver, every guarded Nexus and the service. The seat is dedicated
  // to fixtures (sweep requires HOLD_SMOKE_FIXTURE_SELLER), so nothing under these paths is anyone else's.
  async function sweep() {
    const session = await new Pubky().signer(seat).signinCookie();
    const ids = await names(LISTINGS);
    try {
      for (const id of ids) await session.storage.delete(`${LISTINGS}/${id}`);
      for (const path of [MEDIA, DELIVERABLES]) {
        for (const name of await names(path)) await session.storage.delete(`${path}/${name}`);
      }
    } finally {
      session.free?.();
    }
    writeState({});
    const failures = [];
    for (const id of ids) {
      if ((await onHomeserver(id)) !== false) failures.push(`${id}: still on the homeserver`);
      for (const nexus of GUARD_NEXUS) {
        const gone = await waitForUrl(`${nexus}/v0/listing/${pk}/${id}`, 404);
        if (gone.timedOut) failures.push(`${id}: ${new URL(nexus).host} answers ${gone.status}`);
      }
      const gone = await waitForUrl(`${SERVICE}/v0/listings/${pk}/${id}/availability`, 404);
      if (gone.timedOut) failures.push(`${id}: the service answers ${gone.status}`);
    }
    const leftover = [...(await names(LISTINGS)), ...(await names(MEDIA)), ...(await names(DELIVERABLES))];
    if (leftover.length) failures.push(`the homeserver still holds ${leftover.length} fixture file(s)`);
    console.log(JSON.stringify({ sweep: failures.length ? 'FAIL' : 'PASS', seller: pk.slice(0, 8), listingsRemoved: ids.length, failures }));
    process.exitCode = failures.length ? 1 : 0;
  }

  const sessions = [];
  async function bearer() {
    const client = new Pubky();
    const flow = client.startCookieAuthFlow('/pub/pubky.app/:rw', AuthFlowKind.signin(), RELAY);
    const tokenPromise = flow.awaitToken();
    await client.signer(seat).approveAuthRequest(flow.authorizationUrl);
    const response = await fetch(`${SERVICE}/v1/auth/sessions`, { method: 'POST', headers: { 'content-type': 'application/octet-stream' }, body: (await tokenPromise).toBytes() });
    if (!response.ok) throw new Error(`session mint ${response.status}`);
    const body = await response.json();
    sessions.push(body);
    return body.token;
  }

  try {
    if (mode === 'sweep') {
      if (!process.env.HOLD_SMOKE_FIXTURE_SELLER) throw new Error('sweep deletes everything the seat owns under the marketplace paths: set HOLD_SMOKE_FIXTURE_SELLER to the dedicated fixture seller');
      await sweep();
      return;
    }
    if (mode === 'publish') {
      const user = await waitFor(`/v0/user/${pk}`, 200);
      if (user.timedOut) throw new Error(`the index has no user for the seat (status ${user.status}); publish its profile first`);
      const session = await new Pubky().signer(seat).signinCookie();
      try {
        const state = readState();
        for (const kind of Object.keys(KINDS)) {
          if (state[kind] && (await onHomeserver(state[kind])) === true) {
            log(`${kind}: ${state[kind]} already published`);
            continue;
          }
          const listingId = `hsfx_${kind}_${Date.now().toString(36)}`;
          state[kind] = listingId;
          writeState(state);
          const png = fixturePng();
          const mediaId = randomUUID().replaceAll('-', '');
          await session.storage.putBytes(`${MEDIA}/${mediaId}`, new Uint8Array(png.bytes));
          const media = {
            id: mediaId, type: 'image', url: `pubky://${pk}${MEDIA}/${mediaId}`, contentHash: bytesToHex(blake3(new Uint8Array(png.bytes))),
            mimeType: 'image/png', byteSize: png.bytes.length, width: png.width, height: png.height, altText: 'Hold-smoke fixture',
          };
          const record = fixtureRecord(kind, { listingId, pubky: pk, media });
          try {
            await session.storage.putJson(`${LISTINGS}/${listingId}`, record);
          } catch (error) {
            await session.storage.delete(`${MEDIA}/${mediaId}`).catch(() => log(`${kind}: orphan media ${mediaId} left; delete it`));
            throw error;
          }
          const reg = await fetch(`${SERVICE}/v1/commands`, {
            method: 'POST', headers: { authorization: `Bearer ${await bearer()}`, 'content-type': 'application/json' },
            body: JSON.stringify(registerCommand(kind, record)),
          });
          if (reg.status !== 200) throw new Error(`${kind}: register ${reg.status}`);
          const indexed = await waitFor(`/v0/listing/${pk}/${listingId}`, 200);
          log(`${kind}: ${listingId} index ${indexed.status} after ${indexed.ms} ms`);
          if (indexed.timedOut) throw new Error(`${kind}: the index did not show ${listingId}`);
        }
      } finally {
        session.free?.();
      }
    }
    const state = readState();
    const out = {};
    for (const kind of Object.keys(KINDS)) {
      const id = state[kind];
      const service = id ? await fetch(`${SERVICE}/v0/listings/${pk}/${id}/availability`) : null;
      out[kind] = id
        ? { id, homeserver: await onHomeserver(id), index: await indexStatus(`/v0/listing/${pk}/${id}`), service: service.status }
        : null;
    }
    console.log(JSON.stringify({ seller: pk.slice(0, 8), fixtures: out }));
    process.exitCode = Object.values(out).every((f) => f && f.homeserver === true && f.index === 200 && f.service === 200) ? 0 : 1;
  } finally {
    for (const { token, session_id: id } of sessions.splice(0)) {
      await fetch(`${SERVICE}/v1/auth/sessions/${id}`, { method: 'DELETE', headers: { authorization: `Bearer ${token}` } }).catch(() => {});
    }
  }
}

const mode = process.argv[2];
if (mode === 'check') check();
else if (mode === 'guard') await guard();
else if (mode === 'publish' || mode === 'status' || mode === 'sweep') await main(mode);
else {
  console.error('usage: hold-smoke-fixtures.mjs publish | status | sweep | guard | check');
  process.exitCode = 2;
}
