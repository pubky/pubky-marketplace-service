-- Permanent single-seller ownership for the life of this database. The one-row
-- primary key makes a second binding impossible. Ordinary UPDATE, DELETE and
-- TRUNCATE are refused unless the operator release helper enables its
-- transaction-local authority. Directly rerunnable; there is no down migration.

CREATE TABLE IF NOT EXISTS store_seller_binding (
    singleton   BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    seller_pubky TEXT NOT NULL
        CHECK (seller_pubky <> '')
        CHECK (seller_pubky ~ '^[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$'),
    bound_by    TEXT NOT NULL CHECK (bound_by IN ('claim', 'env')),
    claimed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    claim_session_id UUID,
    service_commit TEXT
);

CREATE OR REPLACE FUNCTION guard_store_seller_binding_release()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF current_setting('app.allow_store_binding_release', true) IS NOT DISTINCT FROM 'on' THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        ELSIF TG_OP = 'UPDATE' THEN
            RETURN NEW;
        END IF;
        RETURN NULL;
    END IF;
    RAISE EXCEPTION 'store seller binding is permanent; use the operator release helper on an empty store';
END
$$;

DROP TRIGGER IF EXISTS store_seller_binding_row_release_guard ON store_seller_binding;
CREATE TRIGGER store_seller_binding_row_release_guard
BEFORE UPDATE OR DELETE ON store_seller_binding
FOR EACH ROW EXECUTE FUNCTION guard_store_seller_binding_release();

DROP TRIGGER IF EXISTS store_seller_binding_truncate_release_guard ON store_seller_binding;
CREATE TRIGGER store_seller_binding_truncate_release_guard
BEFORE TRUNCATE ON store_seller_binding
FOR EACH STATEMENT EXECUTE FUNCTION guard_store_seller_binding_release();
