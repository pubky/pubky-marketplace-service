import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { inspect } from 'node:util';
import test from 'node:test';
import { mintFixtureBearer, withFixtureGrant } from './fixture-grant-session.mjs';

const pubky = 'y'.repeat(52);
const challengeId = '11111111-1111-4111-8111-111111111111';
const sessionId = '22222222-2222-4222-8222-222222222222';
const service = 'https://marketplace.example.test';
const path = `/pub/pubky.app/marketplace/v1/session-proofs/${challengeId}`;
const capabilities = '/priv/pubky.app/:rw,/pub/pubky.app/marketplace-service/v1/:rw';
const response = (body, status = 201) => new Response(JSON.stringify(body), { status });

function fixture({ mint = {}, status = 201, writeFails = false, deleteFails = false, revokeFails = false } = {}) {
  const events = [];
  const signed = [];
  const issued = {
    token: 'a'.repeat(43), session_id: sessionId, pubky, capabilities,
    expires_at: new Date(Date.now() + 60_000).toISOString(), ...mint,
  };
  const session = {
    info: { publicKey: { z32: () => pubky } },
    grant: { createCustomPop: async (data) => { signed.push(data); return { grant: 'signed-grant', pop: 'signed-pop' }; } },
    storage: {
      putJson: async (target, body) => {
        events.push(['put', target, body]);
        if (writeFails) throw new Error('storage unavailable');
      },
      delete: async (target) => {
        events.push(['delete-proof', target]);
        if (deleteFails) throw new Error('delete unavailable: sensitive-proof-token');
      },
    },
    signout: async () => { events.push(['signout']); },
  };
  const fetchImpl = async (url, options) => {
    events.push([options.method, url, options]);
    if (options.method === 'DELETE') {
      if (revokeFails) throw new Error('revoke unavailable: sensitive-bearer-token');
      return new Response(null, { status: 204 });
    }
    assert.deepEqual(JSON.parse(options.body), { grant: 'signed-grant', pop: 'signed-pop' });
    if (url.endsWith('/grant-session-challenges')) {
      return response({ challenge_id: challengeId, expires_at: new Date(Date.now() + 120_000).toISOString() });
    }
    assert.equal(url, `${service}/v1/auth/grant-sessions`);
    return response(issued, status);
  };
  return { events, signed, issued, session, args: { session, pubky, service, fetchImpl } };
}

test('mints only after a live proof write and deletes it without retiring a borrowed grant', async () => {
  const f = fixture();
  assert.deepEqual(await mintFixtureBearer(f.args), f.issued);
  assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'POST', 'delete-proof']);
  assert.deepEqual(f.events[1], ['put', path, {
    aud: service, challenge_id: challengeId, domain: 'marketplace-service/grant-session-proof/v1', pubky,
  }]);
  assert.deepEqual(f.signed, [
    { aud: service, purpose: 'marketplace-service/grant-session-challenge/v1' },
    { aud: service, purpose: 'marketplace-service/grant-session/v1', challenge_id: challengeId },
  ]);
});

test('refuses another identity before requesting a challenge', async () => {
  const f = fixture();
  await assert.rejects(mintFixtureBearer({ ...f.args, pubky: 'z'.repeat(52) }), /expected seller/);
  assert.deepEqual(f.events, []);
});

test('failed writes do not mint and still attempt proof deletion', async () => {
  const f = fixture({ writeFails: true });
  await assert.rejects(mintFixtureBearer(f.args), /proof write failed/);
  assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'delete-proof']);
});

test('server refusal deletes proof and does not fall back to AuthToken', async () => {
  const f = fixture({ status: 403 });
  await assert.rejects(mintFixtureBearer(f.args), /403/);
  assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'POST', 'delete-proof']);
});

for (const [name, mint] of [
  ['wrong identity', { pubky: 'z'.repeat(52) }],
  ['unexpected capabilities', { capabilities: '/:rw' }],
  ['missing private capability', { capabilities: '/pub/pubky.app/marketplace-service/v1/:rw' }],
  ['expired bearer', { expires_at: '2000-01-01T00:00:00Z' }],
]) {
  test(`refuses and revokes ${name}`, async () => {
    const f = fixture({ mint });
    await assert.rejects(mintFixtureBearer(f.args), /invalid fixture marketplace session/);
    assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'POST', 'DELETE', 'delete-proof']);
    assert.equal(f.events[3][1], `${service}/v1/auth/sessions/${sessionId}`);
  });
}

test('cleanup failure revokes a minted bearer before rejecting', async () => {
  const f = fixture({ deleteFails: true });
  await assert.rejects(mintFixtureBearer(f.args), /proof cleanup failed/);
  assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'POST', 'delete-proof', 'DELETE']);
});

test('path-shaped challenge is refused before any storage write', async () => {
  const f = fixture();
  await assert.rejects(mintFixtureBearer({ ...f.args, fetchImpl: async () => response({
    challenge_id: '../../listings/anything', expires_at: new Date().toISOString(),
  }) }), /invalid fixture grant challenge/);
  assert.deepEqual(f.events, []);
});

test('owned grant is signed out on success and on action failure', async () => {
  const f = fixture();
  const seat = {};
  const client = { signer: (actual) => {
    assert.equal(actual, seat);
    return { signin: async (id) => { assert.equal(id, 'fixture.example'); return f.session; } };
  } };
  const options = { client, seat, clientId: 'fixture.example' };
  assert.equal(await withFixtureGrant(options, async () => 7), 7);
  await assert.rejects(withFixtureGrant(options, async () => { throw new Error('action failed'); }), /action failed/);
  assert.deepEqual(f.events, [['signout'], ['signout']]);
  await assert.rejects(withFixtureGrant({ ...options, clientId: '' }, async () => {}), /HOLD_SMOKE_GRANT_CLIENT_ID/);
});

test('owned grant preserves action failure when signout also fails', async () => {
  const primary = new Error('fixture publish failed');
  let signouts = 0;
  const client = { signer: () => ({ signin: async () => ({ signout: async () => {
    signouts++;
    throw new Error('sensitive-signout-token');
  } }) }) };
  await assert.rejects(withFixtureGrant({ client, seat: {}, clientId: 'fixture.example' }, async () => {
    throw primary;
  }), (error) => {
    assert.ok(error instanceof AggregateError);
    assert.equal(error.message, primary.message);
    assert.equal(error.cause, primary);
    assert.equal(error.errors[0], primary);
    assert.equal(error.errors[1].message, 'fixture homeserver grant sign-out failed');
    assert.doesNotMatch(inspect(error), /sensitive-signout-token/);
    return true;
  });
  assert.equal(signouts, 1);
});

test('owned grant still fails when only signout fails', async () => {
  const client = { signer: () => ({ signin: async () => ({ signout: async () => {
    throw new Error('sensitive-signout-token');
  } }) }) };
  await assert.rejects(withFixtureGrant({ client, seat: {}, clientId: 'fixture.example' }, async () => 7), (error) => {
    assert.equal(error.message, 'fixture homeserver grant sign-out failed');
    assert.doesNotMatch(inspect(error), /sensitive-signout-token/);
    return true;
  });
});

for (const [name, options, primaryMessage] of [
  ['proof write', { writeFails: true }, 'fixture grant proof write failed'],
  ['mint request', { status: 403 }, 'fixture grant request failed (403)'],
]) {
  test(`preserves ${name} failure when proof deletion also fails`, async () => {
    const f = fixture({ ...options, deleteFails: true });
    await assert.rejects(mintFixtureBearer(f.args), (error) => {
      assert.ok(error instanceof AggregateError);
      assert.equal(error.message, primaryMessage);
      assert.equal(error.cause, error.errors[0]);
      assert.deepEqual(error.errors.map((cause) => cause.message), [
        primaryMessage, 'fixture grant proof cleanup failed',
      ]);
      assert.doesNotMatch(inspect(error), /sensitive-proof-token/);
      return true;
    });
    assert.equal(f.events.at(-1)[0], 'delete-proof');
  });
}

test('preserves invalid mint while attempting failed bearer revocation and proof deletion', async () => {
  const f = fixture({ mint: { capabilities: '/:rw' }, revokeFails: true, deleteFails: true });
  await assert.rejects(mintFixtureBearer(f.args), (error) => {
    assert.ok(error instanceof AggregateError);
    assert.equal(error.message, 'invalid fixture marketplace session');
    assert.equal(error.cause, error.errors[0]);
    assert.deepEqual(error.errors.map((cause) => cause.message), [
      'invalid fixture marketplace session', 'fixture session cleanup failed', 'fixture grant proof cleanup failed',
      'fixture session cleanup failed',
    ]);
    assert.doesNotMatch(inspect(error), /sensitive-(?:proof|bearer)-token/);
    return true;
  });
  assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'POST', 'DELETE', 'delete-proof', 'DELETE']);
});

test('proof cleanup retires a rejected bearer after its first revocation fails', async () => {
  const f = fixture({ mint: { capabilities: '/:rw' }, deleteFails: true });
  const originalFetch = f.args.fetchImpl;
  let revocations = 0;
  f.args.fetchImpl = async (url, options) => {
    if (options.method === 'DELETE' && ++revocations === 1) {
      f.events.push(['DELETE', url, options]);
      return new Response(null, { status: 503 });
    }
    return originalFetch(url, options);
  };
  await assert.rejects(mintFixtureBearer(f.args), (error) => {
    assert.equal(error.message, 'invalid fixture marketplace session');
    assert.equal(error.cause, error.errors[0]);
    assert.doesNotMatch(inspect(error), /sensitive-(?:proof|bearer)-token/);
    return true;
  });
  assert.equal(revocations, 2);
  assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'POST', 'DELETE', 'delete-proof', 'DELETE']);
});

test('preserves proof cleanup failure when revoking an otherwise valid bearer also fails', async () => {
  const f = fixture({ deleteFails: true, revokeFails: true });
  await assert.rejects(mintFixtureBearer(f.args), (error) => {
    assert.ok(error instanceof AggregateError);
    assert.equal(error.message, 'fixture grant proof cleanup failed');
    assert.equal(error.cause, error.errors[0]);
    assert.deepEqual(error.errors.map((cause) => cause.message), [
      'fixture grant proof cleanup failed', 'fixture session cleanup failed',
    ]);
    assert.doesNotMatch(inspect(error), /sensitive-(?:proof|bearer)-token/);
    return true;
  });
  assert.deepEqual(f.events.map(([event]) => event), ['POST', 'put', 'POST', 'delete-proof', 'DELETE']);
});

test('hold smoke runner contains no deprecated cookie SDK calls', () => {
  const source = readFileSync(new URL('./hold-smoke-fixtures.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /\.(?:startCookieAuthFlow|resumeCookieAuthFlow|signinCookie|signupCookie)\s*\(/);
});
