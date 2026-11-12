// Node release fixtures use the same challenge/live-proof contract as Shop.
// No SDK import here: the fixture runner supplies its configured Pubky client.
const PROOF_PATH = '/pub/pubky.app/marketplace/v1/session-proofs/';
const INVENTORY = '/pub/pubky.app/marketplace-service/v1/:rw';
const PRIVATE = '/priv/pubky.app/:rw';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const TOKEN = /^[A-Za-z0-9_-]{43}$/;

function throwFailures(failures) {
  if (failures.length > 1) {
    const primary = failures[0];
    throw new AggregateError(failures, primary instanceof Error ? primary.message : 'Operation failed before cleanup', {
      cause: primary,
    });
  }
  if (failures.length) throw failures[0];
}

export async function withFixtureGrant({ client, seat, clientId }, action) {
  if (!clientId) throw new Error('set HOLD_SMOKE_GRANT_CLIENT_ID to a service-allowed grant client id');
  // Return the minted grant before background PKDNS publication can fail.
  const session = await client.signer(seat).signin(clientId)
    .catch(() => { throw new Error('fixture homeserver grant sign-in failed'); });
  const failures = [];
  let result;
  try {
    result = await action(session);
  } catch (error) {
    failures.push(error);
  }
  // free() only drops the JS handle; signout retires this owned grant.
  try {
    await session.signout();
  } catch {
    failures.push(new Error('fixture homeserver grant sign-out failed'));
  }
  throwFailures(failures);
  return result;
}

/** Borrowed session: the caller keeps responsibility for retiring its grant. */
export async function mintFixtureBearer({ session, pubky, service, fetchImpl = fetch }) {
  const audience = new URL(service).origin;
  const base = service.replace(/\/$/, '');
  if (!session.grant || session.info.publicKey.z32() !== pubky) {
    throw new Error('fixture grant does not belong to the expected seller');
  }
  const post = async (path, purpose, extra = {}) => {
    const proof = await session.grant.createCustomPop({ aud: audience, purpose, ...extra })
      .catch(() => { throw new Error('fixture grant signing failed'); });
    const response = await fetchImpl(`${base}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ grant: proof.grant, pop: proof.pop }),
      signal: AbortSignal.timeout(30_000),
    }).catch(() => { throw new Error('fixture grant request unavailable'); });
    // A response may contain credentials. Never include it in an error.
    if (response.status !== 201) throw new Error(`fixture grant request failed (${response.status})`);
    return await response.json().catch(() => { throw new Error('invalid fixture grant response'); });
  };
  const challenge = await post('/v1/auth/grant-session-challenges', 'marketplace-service/grant-session-challenge/v1');
  if (!UUID.test(challenge?.challenge_id) || !Number.isFinite(Date.parse(challenge?.expires_at))) {
    throw new Error('invalid fixture grant challenge');
  }
  const path = `${PROOF_PATH}${challenge.challenge_id}`;
  let minted;
  const failures = [];
  try {
    await session.storage.putJson(path, {
      aud: audience,
      challenge_id: challenge.challenge_id,
      domain: 'marketplace-service/grant-session-proof/v1',
      pubky,
    }).catch(() => { throw new Error('fixture grant proof write failed'); });
    minted = await post('/v1/auth/grant-sessions', 'marketplace-service/grant-session/v1', {
      challenge_id: challenge.challenge_id,
    });
    const capabilities = typeof minted?.capabilities === 'string'
      ? minted.capabilities.split(',').map((value) => value.trim()).sort().join(',')
      : '';
    if (!TOKEN.test(minted?.token) || !UUID.test(minted?.session_id) || minted?.pubky !== pubky
      || capabilities !== [INVENTORY, PRIVATE].sort().join(',')
      || !Number.isFinite(Date.parse(minted?.expires_at)) || Date.parse(minted.expires_at) <= Date.now()) {
      throw new Error('invalid fixture marketplace session');
    }
  } catch (error) {
    failures.push(error);
  }
  if (failures.length > 0) {
    try {
      await revokeRejectedMint();
    } catch (error) {
      failures.push(error);
    }
  }
  try {
    await session.storage.delete(path);
  } catch {
    failures.push(new Error('fixture grant proof cleanup failed'));
    // A failed earlier revocation leaves minted set for this existing cleanup attempt.
    try {
      await revokeRejectedMint();
    } catch (error) {
      failures.push(error);
    }
  }
  throwFailures(failures);
  return minted;

  async function revokeRejectedMint() {
    if (!TOKEN.test(minted?.token) || !UUID.test(minted?.session_id)) return;
    const response = await fetchImpl(`${base}/v1/auth/sessions/${minted.session_id}`, {
      method: 'DELETE',
      headers: { authorization: `Bearer ${minted.token}` },
      signal: AbortSignal.timeout(30_000),
    }).catch(() => null);
    if (!response?.ok) throw new Error('fixture session cleanup failed');
    minted = undefined;
  }
}
