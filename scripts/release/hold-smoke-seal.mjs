#!/usr/bin/env node
// Seals a delivery address the way the Shop does, for hold-smoke.py.
//
// The Shop (pubky/pubky-marketplace, src/libs/commerce/delivery-envelope.ts)
// seals the buyer's address to each shipped seller's published delivery key
// with RFC 9180 HPKE: DHKEM(X25519, HKDF-SHA256) / HKDF-SHA256 /
// ChaCha20-Poly1305 from `hpke-js`, info = the context string, aad =
// `context|checkout command id|buyer|seller|key id`, plaintext = the
// canonical address JSON padded to 2048 bytes (0x80 delimiter, zero fill).
// Wire bytes: enc (32) || ciphertext (2048 + 16), base64url, no padding.
// This file repeats only those framing constants; the HPKE itself is the
// same library at the same pins the Shop uses.
//
// Staging sellers publish no delivery key we hold, and the service never
// opens an envelope, so each request seals to a fresh throwaway recipient
// key. The private half is dropped unless the caller is `open` or
// `roundtrip`, which exist only to prove this framing against a Shop-made
// fixture.
//
//   seal       {address, checkout_command_id, buyer_pubky, sellers[]}
//                -> {envelopes: [{seller_pubky, key_id, envelope}]}
//   open       {envelope, recipient_private_key_b64url, binding}
//                -> {address}
//   roundtrip  {address, checkout_command_id, buyer_pubky, seller_pubky}
//                -> {address}   (seals to a fresh key, opens it, nothing else leaves)
//
// Input is one JSON object on stdin. `HPKE_CWD` names a directory whose
// node_modules holds @hpke/core, @hpke/chacha20poly1305 and
// @hpke/dhkem-x25519 (see the hold-smoke.py docstring).

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';

const CONTEXT = 'pubky-marketplace/delivery-address/v1';
const HPKE_ENC_BYTES = 32;
const BUCKET_BYTES = 2048;
const AEAD_TAG_BYTES = 16;
const ENVELOPE_BYTES = HPKE_ENC_BYTES + BUCKET_BYTES + AEAD_TAG_BYTES;
const PAD_DELIMITER = 0x80;
const KEY_ID_PATTERN = /^[0-9a-f]{32}$/;

const hpkeCwd = process.env.HPKE_CWD;
if (!hpkeCwd) throw new Error('set HPKE_CWD to a directory whose node_modules has the @hpke packages');
const require = createRequire(join(hpkeCwd, 'package.json'));
const { CipherSuite, HkdfSha256 } = require('@hpke/core');
const { Chacha20Poly1305 } = require('@hpke/chacha20poly1305');
const { DhkemX25519HkdfSha256 } = require('@hpke/dhkem-x25519');

const suite = new CipherSuite({
  kem: new DhkemX25519HkdfSha256(),
  kdf: new HkdfSha256(),
  aead: new Chacha20Poly1305(),
});
const encoder = new TextEncoder();
const toB64Url = (bytes) => Buffer.from(bytes).toString('base64url');
const fromB64Url = (text) => {
  const bytes = new Uint8Array(Buffer.from(text, 'base64url'));
  if (toB64Url(bytes) !== text) throw new Error('not canonical base64url');
  return bytes;
};

const keyIdOf = (publicKey) => createHash('sha256').update(publicKey).digest().subarray(0, 16).toString('hex');

const canonicalAddressJson = (address) =>
  JSON.stringify({
    name: address.name,
    line1: address.line1,
    line2: address.line2,
    city: address.city,
    region: address.region,
    postalCode: address.postal_code,
    countryCode: address.country_code,
  });

const aadOf = ({ checkout_command_id, buyer_pubky, seller_pubky, key_id }) =>
  encoder.encode(`${CONTEXT}|${checkout_command_id.toLowerCase()}|${buyer_pubky}|${seller_pubky}|${key_id}`);

function pad(plaintext) {
  if (plaintext.byteLength > BUCKET_BYTES - 1) throw new Error('address too long to seal');
  const padded = new Uint8Array(BUCKET_BYTES);
  padded.set(plaintext, 0);
  padded[plaintext.byteLength] = PAD_DELIMITER;
  return padded;
}

function unpad(padded) {
  let end = padded.byteLength - 1;
  while (end >= 0 && padded[end] === 0) end -= 1;
  if (end < 0 || padded[end] !== PAD_DELIMITER) throw new Error('bad padding');
  return padded.subarray(0, end);
}

async function freshRecipient() {
  const pair = await suite.kem.generateKeyPair();
  const publicKey = new Uint8Array(await suite.kem.serializePublicKey(pair.publicKey));
  const privateKey = new Uint8Array(await suite.kem.serializePrivateKey(pair.privateKey));
  return { publicKey, privateKey, keyId: keyIdOf(publicKey) };
}

async function seal({ address, binding, publicKey }) {
  if (keyIdOf(publicKey) !== binding.key_id) throw new Error('key id does not match the public key');
  const recipientPublicKey = await suite.kem.deserializePublicKey(publicKey);
  const sealed = await suite.seal(
    { recipientPublicKey, info: encoder.encode(CONTEXT) },
    pad(encoder.encode(canonicalAddressJson(address))),
    aadOf(binding),
  );
  const wire = new Uint8Array(HPKE_ENC_BYTES + sealed.ct.byteLength);
  wire.set(new Uint8Array(sealed.enc), 0);
  wire.set(new Uint8Array(sealed.ct), HPKE_ENC_BYTES);
  const envelope = toB64Url(wire);
  if (wire.byteLength !== ENVELOPE_BYTES) throw new Error(`envelope is ${wire.byteLength} bytes, want ${ENVELOPE_BYTES}`);
  return envelope;
}

async function open({ envelope, privateKey, binding }) {
  const wire = fromB64Url(envelope);
  if (wire.byteLength !== ENVELOPE_BYTES) throw new Error('envelope has the wrong size');
  const recipientKey = await suite.kem.deserializePrivateKey(privateKey);
  const padded = new Uint8Array(
    await suite.open(
      { recipientKey, enc: wire.subarray(0, HPKE_ENC_BYTES), info: encoder.encode(CONTEXT) },
      wire.subarray(HPKE_ENC_BYTES),
      aadOf(binding),
    ),
  );
  const parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(unpad(padded)));
  return {
    name: parsed.name,
    line1: parsed.line1,
    line2: parsed.line2,
    city: parsed.city,
    region: parsed.region,
    postal_code: parsed.postalCode,
    country_code: parsed.countryCode,
  };
}

const [mode] = process.argv.slice(2);
const input = JSON.parse(readFileSync(0, 'utf8'));
let output;
if (mode === 'seal') {
  const envelopes = [];
  for (const seller_pubky of input.sellers) {
    const recipient = await freshRecipient();
    const binding = {
      checkout_command_id: input.checkout_command_id,
      buyer_pubky: input.buyer_pubky,
      seller_pubky,
      key_id: recipient.keyId,
    };
    envelopes.push({
      seller_pubky,
      key_id: recipient.keyId,
      envelope: await seal({ address: input.address, binding, publicKey: recipient.publicKey }),
    });
  }
  for (const { key_id } of envelopes) if (!KEY_ID_PATTERN.test(key_id)) throw new Error('derived key id is malformed');
  output = { envelopes };
} else if (mode === 'open') {
  output = {
    address: await open({
      envelope: input.envelope,
      privateKey: fromB64Url(input.recipient_private_key_b64url),
      binding: input.binding,
    }),
  };
} else if (mode === 'roundtrip') {
  const recipient = await freshRecipient();
  const binding = {
    checkout_command_id: input.checkout_command_id,
    buyer_pubky: input.buyer_pubky,
    seller_pubky: input.seller_pubky,
    key_id: recipient.keyId,
  };
  const envelope = await seal({ address: input.address, binding, publicKey: recipient.publicKey });
  output = { address: await open({ envelope, privateKey: recipient.privateKey, binding }) };
} else {
  throw new Error(`unknown mode ${JSON.stringify(mode)}; use seal, open or roundtrip`);
}
process.stdout.write(JSON.stringify(output));
