//! The grant exchange: `POST /auth/grant/session` of an approved grant and a
//! proof of possession of its client key, at the homeserver the grant's
//! issuer names, for the session that homeserver then vouches for.
//!
//! The request goes through [`HomeserverTransport`], not the `pubky` SDK's
//! grant flow, whose requests follow redirects, allow plain HTTP and connect
//! to whatever address a record names. The issuer's homeserver must also be
//! the one the service is configured with (`HOMESERVER_PUBKY`, the homeserver
//! at `HOMESERVER_URL` every pubky is read from); any other is refused before
//! a request is sent to it.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pubky::pkarr;
use pubky::{GrantClaims, Keypair, PublicKey};
use pubky_common::auth::grant_session_responses::{GrantSessionInfo, GrantSessionResponse};
use pubky_common::auth::jws::{sign_jws, PopNonce, POP_JWS_TYP};
use pubky_common::auth::pop::PopProofClaims;
use tokio::time::Instant;

use crate::homeserver_transport::{key_prefix, EndpointPolicy, HomeserverTransport, RecordCheck};

pub const GRANT_SESSION_PATH: &str = "/auth/grant/session";
/// A grant session answer is a bearer and a few fields of metadata.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// Why an exchange produced no session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeRefusal {
    /// The grant's issuer names a homeserver other than the configured one.
    /// Nothing was sent to it.
    HomeserverNotAllowed,
    /// No homeserver or endpoint was found, no connection could be made, the
    /// request failed, or the homeserver refused or gave an unreadable answer.
    Failed,
}

pub struct GrantExchange {
    transport: HomeserverTransport,
    homeserver: pkarr::PublicKey,
}

impl GrantExchange {
    /// Exchanges grants only at `homeserver`, reached through `pkarr`'s
    /// records at addresses `policy` allows.
    pub fn new(
        pkarr: pkarr::Client,
        homeserver: PublicKey,
        policy: EndpointPolicy,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            transport: HomeserverTransport::new(pkarr, policy)?,
            homeserver: homeserver.into(),
        })
    }

    /// Exchanges `grant_jws` (whose decoded claims are `claims`), proving
    /// possession of `client_key`, for a session at the issuer's homeserver.
    /// The session is for the grant's issuer, id and client id, at that
    /// homeserver; any other answer is refused. The request is given until
    /// `deadline`; the caller bounds the lookups before it.
    pub async fn exchange(
        &self,
        grant_jws: &str,
        claims: &GrantClaims,
        client_key: &Keypair,
        deadline: Instant,
    ) -> Result<GrantSessionInfo, ExchangeRefusal> {
        let issuer = claims.iss.z32();
        let user: pkarr::PublicKey = claims.iss.clone().into();
        let homeserver = match self.transport.homeserver_of(&user).await {
            Ok(Some(homeserver)) => homeserver,
            Ok(None) => {
                tracing::warn!(pubky_prefix = %key_prefix(&issuer), "grant exchange found no homeserver record");
                return Err(ExchangeRefusal::Failed);
            }
            Err(_) => {
                tracing::warn!(pubky_prefix = %key_prefix(&issuer), "grant exchange homeserver lookup failed");
                return Err(ExchangeRefusal::Failed);
            }
        };
        if homeserver != self.homeserver {
            tracing::warn!(
                pubky_prefix = %key_prefix(&issuer),
                homeserver_prefix = %key_prefix(&homeserver.to_z32()),
                "grant exchange refused: the homeserver is not the configured one"
            );
            return Err(ExchangeRefusal::HomeserverNotAllowed);
        }
        let route = match self.transport.route(&homeserver).await {
            Ok(route) if !route.is_empty() => route,
            Ok(_) => {
                tracing::warn!(
                    homeserver_prefix = %key_prefix(&homeserver.to_z32()),
                    "grant exchange found no homeserver endpoint at an allowed address"
                );
                return Err(ExchangeRefusal::Failed);
            }
            Err(_) => {
                tracing::warn!(
                    homeserver_prefix = %key_prefix(&homeserver.to_z32()),
                    "grant exchange endpoint lookup failed"
                );
                return Err(ExchangeRefusal::Failed);
            }
        };
        let body = serde_json::to_vec(&serde_json::json!({
            "grant": grant_jws,
            "pop": proof_of_possession(client_key, &homeserver, claims),
        }))
        .map_err(|_| ExchangeRefusal::Failed)?;
        let timeout = deadline.saturating_duration_since(Instant::now());
        let sent = self
            .transport
            .send(&homeserver, &route, GRANT_SESSION_PATH, |client, url| {
                client
                    .post(url)
                    .timeout(timeout)
                    .header("pubky-host", issuer.as_str())
                    .header("content-type", "application/json")
                    .body(body.clone())
            })
            .await;
        let response = match sent {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::warn!(
                    homeserver_prefix = %key_prefix(&homeserver.to_z32()),
                    status = response.status().as_u16(),
                    "grant exchange refused by the homeserver"
                );
                return Err(ExchangeRefusal::Failed);
            }
            Err(unsent) => {
                tracing::warn!(
                    homeserver_prefix = %key_prefix(&homeserver.to_z32()),
                    unsent = ?unsent,
                    "grant exchange request failed"
                );
                return Err(ExchangeRefusal::Failed);
            }
        };
        let session = match read_session(response).await {
            Some(answer) => answer.session,
            None => {
                tracing::warn!(
                    homeserver_prefix = %key_prefix(&homeserver.to_z32()),
                    "grant exchange answer is not a grant session"
                );
                return Err(ExchangeRefusal::Failed);
            }
        };
        if let Some(field) = mismatched_field(&session, &homeserver, claims) {
            tracing::warn!(
                homeserver_prefix = %key_prefix(&homeserver.to_z32()),
                field,
                "grant exchange answer is not a session for the exchanged grant"
            );
            return Err(ExchangeRefusal::Failed);
        }
        Ok(session)
    }

    /// Checks that the configured homeserver's Pkarr record names the host
    /// of `homeserver_url` (`HOMESERVER_URL`) among its endpoints, and an
    /// endpoint the exchange may connect to. Reads the record only, within
    /// `timeout`, and logs the outcome once: at error level when the record
    /// is missing or does not match.
    pub async fn check_homeserver_url(
        &self,
        homeserver_url: &str,
        timeout: Duration,
    ) -> RecordCheck {
        let homeserver_prefix = self.homeserver.to_z32();
        let homeserver_prefix = key_prefix(&homeserver_prefix);
        let Some(host) = url::Url::parse(homeserver_url)
            .ok()
            .and_then(|url| url.host().map(|host| host.to_owned()))
        else {
            tracing::error!(
                homeserver_prefix,
                "HOMESERVER_URL names no host to check HOMESERVER_PUBKY against"
            );
            return RecordCheck::NotNamed;
        };
        let check = tokio::time::timeout(
            timeout,
            self.transport.check_record(&self.homeserver, &host),
        )
        .await
        .unwrap_or(RecordCheck::Unavailable);
        match check {
            RecordCheck::Named => tracing::info!(
                homeserver_prefix,
                homeserver_host = %host,
                "HOMESERVER_PUBKY's record names HOMESERVER_URL's host"
            ),
            RecordCheck::NotNamed => tracing::error!(
                homeserver_prefix,
                homeserver_host = %host,
                "HOMESERVER_PUBKY's record does not name HOMESERVER_URL's host; grant sign-ins \
                 of users at HOMESERVER_URL will be refused"
            ),
            RecordCheck::NoAllowedEndpoint => tracing::error!(
                homeserver_prefix,
                homeserver_host = %host,
                "HOMESERVER_PUBKY's record names no endpoint the grant exchange may connect to"
            ),
            RecordCheck::NoRecord => tracing::error!(
                homeserver_prefix,
                "HOMESERVER_PUBKY has no Pkarr record; grant exchanges will fail"
            ),
            RecordCheck::Unavailable => tracing::warn!(
                homeserver_prefix,
                "HOMESERVER_PUBKY's Pkarr record could not be read to check it against HOMESERVER_URL"
            ),
        }
        check
    }
}

/// The first field of `session` that differs from what exchanging the grant
/// of `claims` at `homeserver` must answer, if any.
fn mismatched_field(
    session: &GrantSessionInfo,
    homeserver: &pkarr::PublicKey,
    claims: &GrantClaims,
) -> Option<&'static str> {
    if session.pubky != claims.iss {
        Some("pubky")
    } else if pkarr::PublicKey::from(session.homeserver.clone()) != *homeserver {
        Some("homeserver")
    } else if session.grant_id != claims.jti {
        Some("grant_id")
    } else if session.client_id != claims.client_id {
        Some("client_id")
    } else {
        None
    }
}

/// A proof, signed with the grant's client key, that binds this request to
/// `homeserver` and the grant's id, as the `pubky` SDK signs it.
fn proof_of_possession(
    client_key: &Keypair,
    homeserver: &pkarr::PublicKey,
    claims: &GrantClaims,
) -> String {
    let iat = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    sign_jws(
        client_key,
        POP_JWS_TYP,
        &PopProofClaims {
            aud: homeserver.clone().into(),
            gid: claims.jti.clone(),
            nonce: PopNonce::generate(),
            iat,
        },
    )
}

async fn read_session(mut response: pubky_reqwest::Response) -> Option<GrantSessionResponse> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).ok()
}
