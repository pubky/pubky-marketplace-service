import { defineRailway, image, preserve, project, service } from "railway/iac";

// Last resort for a per-service CaC repo. Prefer one .railway file for the
// project and drop this if you later combine services into that file.
export const partial = "marketplace-service";

const STAGING_PROJECT_ID = "c991d768-4a3c-42ea-b5ed-eaa22d4916ed";
const STAGING_ENVIRONMENT_ID = "c67a6435-bb23-453b-9169-764bfa0312e1";
const PRODUCTION_PROJECT_ID = "75faa4fe-466c-4277-977f-1d8e4e31df8c";
const PRODUCTION_ENVIRONMENT_ID = "404919ad-fb95-4621-9f45-b7f993dfa8ae";

// `image()` takes a string only. `preserve()` is a variable value, not a
// source. Do not read this from process.env: a 0/0/0 plan must be a property
// of git, not of the operator's shell. The release train MUST bump each
// constant in the same PR as the IMAGE connect for that seat.
const STAGING_IMAGE =
  "ghcr.io/pubky/pubky-marketplace-service@sha256:f8046b0c295aeb88b61a1f0a73dcd1e8eafccbc06eb424f5e858ef2ae96799b8";
const PRODUCTION_IMAGE =
  "ghcr.io/pubky/pubky-marketplace-service@sha256:b498078ce0de1548fd6b4e2fd5b4a1a20175c6db6b12594a20995f1d81166ff5";

const sharedDeploy = {
  healthcheckPath: "/ready",
  healthcheckTimeout: 120,
  overlapSeconds: 60,
  drainingSeconds: 15,
};

const sharedEnv = {
  ADDRESS_SEARCH_CLIENT_IP_HEADER: preserve(),
  ALLOWED_ORIGINS: preserve(),
  ARBITRUM_RPC_URL: preserve(),
  BITCOIN_PAYMENT_WINDOW_SECONDS: preserve(),
  CHECKOUT_HOLD_WINDOW_SECONDS: preserve(),
  CONNECTOR_CREDENTIAL_ENCRYPTION_KEY: preserve(),
  CONNECTOR_CREDENTIAL_ENCRYPTION_KEY_PREVIOUS: preserve(),
  CONNECTOR_PUBLIC_BASE_URL: preserve(),
  CONNECTOR_RETURN_URL: preserve(),
  CONNECTORS_ENABLED: preserve(),
  FIAT_PAYMENT_WINDOW_SECONDS: preserve(),
  LISTING_AVAILABILITY_CLIENT_IP_HEADER: preserve(),
  ATTESTOR_ORDER_SALT: preserve(),
  ATTESTOR_SECRET_KEY: preserve(),
  DATABASE_URL: preserve(),
  DIGITAL_DELIVERY_ENCRYPTION_KEY: preserve(),
  DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS: preserve(),
  HOMESERVER_URL: preserve(),
  LOCKS_BUNDLE_ENCRYPTION_KEY: preserve(),
  LOCKS_LOOKUP_HMAC_KEY: preserve(),
  LOCKS_SERVER_URL: preserve(),
  NEXUS_URL: preserve(),
  PAYKIT_REQUEST_SIGNING_KEY: preserve(),
  PAYKIT_MAX_PREPARE_TTL_SECONDS: preserve(),
  PAYKIT_SERVER_API: preserve(),
  PAYKIT_SERVER_URL: preserve(),
  PICKUP_DETAILS_ENCRYPTION_KEY: preserve(),
  PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS: preserve(),
  PRIV_DATA_KEY_ENCRYPTION_KEY: preserve(),
  PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS: preserve(),
  PUBLIC_APP_ORIGIN: preserve(),
  PUBLIC_SERVICE_ORIGIN: preserve(),
  REQUIRE_ENCRYPTED_DELIVERY_ADDRESS: preserve(),
  REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED: preserve(),
  REFUSAL_AUDIT_DATABASE_URL: preserve(),
  REFUSAL_AUDIT_HMAC_KEY_EPOCH: preserve(),
  REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH: preserve(),
  REFUSAL_AUDIT_HMAC_PREVIOUS_ROOT_B64: preserve(),
  REFUSAL_AUDIT_HMAC_ROOT_B64: preserve(),
  REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED: preserve(),
  REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED: preserve(),
  REFUSAL_AUDIT_RETENTION_DATABASE_URL: preserve(),
  RUST_LOG: preserve(),
  SANDBOX_PAYMENTS_ENABLED: preserve(),
  STRIPE_KEY_ENCRYPTION_KEY: preserve(),
};

const grantAndGatewayEnv = {
  GRANT_FLOW_ENCRYPTION_KEY_B64: preserve(),
  GRANT_FLOW_KEY_EPOCH: preserve(),
  GRANT_FLOW_PREVIOUS_ENCRYPTION_KEY_B64: preserve(),
  GRANT_FLOW_PREVIOUS_KEY_EPOCH: preserve(),
  GRANT_RESULT_HMAC_KEY_EPOCH: preserve(),
  GRANT_RESULT_HMAC_PREVIOUS_KEY_EPOCH: preserve(),
  GRANT_RESULT_HMAC_PREVIOUS_ROOT_B64: preserve(),
  GRANT_RESULT_HMAC_ROOT_B64: preserve(),
  MARKETPLACE_GRANT_CLIENT_ID: preserve(),
  MARKETPLACE_GRANT_CLIENT_IP_HEADER: preserve(),
  MARKETPLACE_GRANT_CREATE_PER_IP_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_CREATE_PER_PUBKY_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_FLOW_ENABLED: preserve(),
  MARKETPLACE_GRANT_FLOW_TTL_SECONDS: preserve(),
  MARKETPLACE_GRANT_MAX_LIVE_FLOWS: preserve(),
  MARKETPLACE_GRANT_REAPER_BATCH_SIZE: preserve(),
  MARKETPLACE_GRANT_RELAY_POLL_MILLISECONDS: preserve(),
  MARKETPLACE_GRANT_RELAY_URL: preserve(),
  MARKETPLACE_GRANT_RESULT_PER_PRINCIPAL_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_STATUS_PER_FLOW_PER_MINUTE: preserve(),
  MARKETPLACE_GRANT_TERMINAL_RETENTION_SECONDS: preserve(),
  MARKETPLACE_GRANT_VERIFY_LEASE_SECONDS: preserve(),
  MARKETPLACE_GRANT_WORKER_BATCH_SIZE: preserve(),
  PAYPAL_IPN_VERIFY_URL: preserve(),
  SHIPPO_API_BASE: preserve(),
  SHOP_BFF_REQUEST_VERIFYING_KEYS_JSON: preserve(),
  SHOP_GRANT_ASSERTION_ISSUER: preserve(),
  SHOP_GRANT_ASSERTION_VERIFYING_KEYS_JSON: preserve(),
  STRIPE_API_BASE: preserve(),
};

// Staging must never take live PayPal money.
const stagingPaypalSandboxEnv = {
  PAYPAL_CHECKOUT_URL: "https://www.sandbox.paypal.com/cgi-bin/webscr",
  PAYPAL_IPN_VERIFY_URL: "https://ipnpb.sandbox.paypal.com/cgi-bin/webscr",
};

// Sign-in-minted marketplace sessions (GS1). Both seats hold them; the staging
// connector worker references the service's values, so they sit in
// `stagingEnv` and not in a service-only group.
const grantSessionEnv = {
  MARKETPLACE_GRANT_SESSION_CLIENT_IDS: preserve(),
  MARKETPLACE_GRANT_SESSION_ENABLED: preserve(),
};

// The multi-asset Paykit contract and the USDT rail. The connector worker
// does not hold these, so they stay out of `stagingEnv`, which the worker
// graph is derived from.
const usdtEnv = {
  PAYKIT_MARKETPLACE_ASSETS: preserve(),
  PAYKIT_MARKETPLACE_CONTRACT: preserve(),
  USDT_PAYMENTS_ENABLED: preserve(),
};

const stagingEnv = {
  ...sharedEnv,
  ...grantAndGatewayEnv,
  ...stagingPaypalSandboxEnv,
  ...grantSessionEnv,
  AUTH_SESSION_TTL_SECONDS: preserve(),
};

const productionEnv = {
  ...sharedEnv,
  ...grantAndGatewayEnv,
  ...grantSessionEnv,
};

function resolvedProjectId(ctx: { projectId?: string }): string | undefined {
  return ctx.projectId ?? process.env.RAILWAY_PROJECT_ID;
}

function resolvedEnvironmentId(ctx: { environmentId?: string }): string | undefined {
  return ctx.environmentId ?? process.env.RAILWAY_ENVIRONMENT_ID;
}

export default defineRailway((ctx) => {
  const projectId = resolvedProjectId(ctx);
  const environmentId = resolvedEnvironmentId(ctx);
  const isProduction = projectId === PRODUCTION_PROJECT_ID;
  const isStaging = projectId === STAGING_PROJECT_ID;
  if (!isProduction && !isStaging) {
    throw new Error(
      `Unknown Railway project ${projectId ?? "(none)"}. This file covers pubky-marketplace-staging (${STAGING_PROJECT_ID}) and pubky-marketplace-production (${PRODUCTION_PROJECT_ID}).`,
    );
  }
  if (isStaging && environmentId && environmentId !== STAGING_ENVIRONMENT_ID) {
    throw new Error(
      `Refuse to evaluate the staging graph against environment ${environmentId}. Set RAILWAY_ENVIRONMENT_ID=${STAGING_ENVIRONMENT_ID}.`,
    );
  }
  if (isProduction && environmentId && environmentId !== PRODUCTION_ENVIRONMENT_ID) {
    throw new Error(
      `Refuse to evaluate the production graph against environment ${environmentId}. Set RAILWAY_ENVIRONMENT_ID=${PRODUCTION_ENVIRONMENT_ID}.`,
    );
  }

  const marketplace_service = service("marketplace-service", {
    source: image(isStaging ? STAGING_IMAGE : PRODUCTION_IMAGE),
    // Live production still records builder DOCKERFILE (legacy toml).
    // Omitting it plans `build.builder DOCKERFILE → null`. Staging live has
    // no builder; declaring one would add it.
    ...(isProduction ? { build: { builder: "DOCKERFILE" as const } } : {}),
    deploy: sharedDeploy,
    env: { ...(isStaging ? stagingEnv : productionEnv), ...usdtEnv },
    // Staging custom host is CLI-attached. Omitting this list deletes it.
    // Generated *.up.railway.app stays CLI.
    domains: isStaging ? ["staging-api.pubky.app"] : [],
  });

  // Same image and environment as marketplace-service, its own start command,
  // on both seats. The binary exits while CONNECTORS_ENABLED is off. Every
  // variable is preserved: the live worker holds `${{marketplace-service.NAME}}`
  // references to the service's variables, and an apply must not rewrite them.
  const connector_worker = service("connector-worker", {
    source: image(isStaging ? STAGING_IMAGE : PRODUCTION_IMAGE),
    deploy: { startCommand: "connector-worker" },
    env: Object.fromEntries(
      Object.keys(isStaging ? stagingEnv : productionEnv).map((name) => [name, preserve()]),
    ),
  });

  return project(isStaging ? "pubky-marketplace-staging" : "pubky-marketplace-production", {
    resources: [marketplace_service, connector_worker],
  });
});
