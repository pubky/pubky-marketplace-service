-- Sellers paykit-server is known to hold. Additive and directly rerunnable.
-- There is NO DOWN migration.
--
-- paykit-server never deletes a Creator, and its initial `/setup` refuses a
-- Creator it already holds, including one whose reconnect stopped after the
-- credentials were stored (`setup_required` on every rail until a reconnect
-- completes). `setup_required` itself cannot tell that Creator from one
-- paykit-server has never seen, so the service records the first time it read
-- a seller as `ready`. From then on a seller who reads `setup_required` is
-- sent to the reconnect flow, the only one paykit-server accepts for them.
-- A row says "Paykit holds this seller", never "Paykit can pay this seller
-- now": readiness is still asked on every own-config read.
--
-- A new, empty table: no index on an existing table, no rewrite. A binary
-- from before this migration never reads or writes it.

SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS paykit_known_sellers (
    seller_pubky   TEXT        PRIMARY KEY,
    first_ready_at TIMESTAMPTZ NOT NULL
);
