mod common;

use axum::http::StatusCode;
use common::{send, test_app};
use serde_json::Value;
use sqlx::PgPool;

// `/health` reports only the short build SHA, never a version, build time or
// host detail. Without `GIT_COMMIT` in the test env it is "unknown".
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn health_reports_the_build_commit_only(pool: PgPool) {
    let app = test_app(pool).await;
    let (status, body) = send(app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let commit = body["commit"].as_str().expect("commit is a string");
    assert!(
        commit == "unknown"
            || ((7..=12).contains(&commit.len()) && commit.bytes().all(|b| b.is_ascii_hexdigit())),
        "{commit}"
    );
    for key in ["version", "built_at", "hostname", "host"] {
        assert!(body.get(key).is_none(), "{key} must not be exposed: {body}");
    }
}
