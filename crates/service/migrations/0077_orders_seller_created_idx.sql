-- no-transaction
CREATE INDEX CONCURRENTLY orders_seller_created_idx ON orders (seller_pubky, created_at)
