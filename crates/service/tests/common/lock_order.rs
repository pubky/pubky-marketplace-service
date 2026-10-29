//! Forcing an interleaving between real transactions: a test transaction
//! holds a row, the contenders are started one by one, and each is seen
//! blocked behind the ones before it (`pg_blocking_pids`) before the next
//! starts. Postgres hands a released row to its waiters in the order they
//! queued, so committing the holder decides who goes first.

use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

/// Locks the order row in a transaction of its own; returns it and its backend pid.
pub async fn hold_order_row(
    pool: &PgPool,
    order_id: Uuid,
) -> (Transaction<'static, Postgres>, i32) {
    let mut tx = pool.begin().await.expect("holder transaction");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .expect("holder backend");
    sqlx::query("SELECT id FROM orders WHERE id = $1 FOR UPDATE")
        .bind(order_id)
        .execute(&mut *tx)
        .await
        .expect("hold the order row");
    (tx, pid)
}

/// Waits until a session not in `known` is blocked by one of `blockers`, and
/// returns its pid. Panics after five seconds: the contender never reached
/// the lock, so the interleaving was not the one the test set up.
pub async fn wait_until_blocked_by(pool: &PgPool, blockers: &[i32], known: &[i32]) -> i32 {
    for _ in 0..500 {
        let waiter: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity \
             WHERE datname = current_database() AND pg_blocking_pids(pid) && $1 \
             AND NOT (pid = ANY($2)) LIMIT 1",
        )
        .bind(blockers)
        .bind(known)
        .fetch_optional(pool)
        .await
        .expect("lock waiters");
        if let Some(pid) = waiter {
            return pid;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("no contender queued behind {blockers:?}");
}
