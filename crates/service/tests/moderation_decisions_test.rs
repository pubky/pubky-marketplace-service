//! A moderation decision recorded by the operator command reaches the owner
//! of the restricted listing, shop or review as a `moderation_decision`
//! notification carrying the statement of reasons, and nobody else
//! (`docs/moderation-decisions.md`).

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{new_actor, send, test_app, TestApp};
use marketplace_service::clock::Clock;
use marketplace_service::moderation_decisions::{record, Decision, DecisionInput, Recorded};
use marketplace_service::workers::drain_outbox;

fn listing_input(seller: &str, decided_at: DateTime<Utc>) -> DecisionInput {
    DecisionInput {
        restriction: "listing_removed".into(),
        target_uri: format!("pubky://{seller}/pub/pubky.app/marketplace/v1/listings/lst_1"),
        territorial_scope: "Every region where Pubky Market is offered".into(),
        ends_at: None,
        decided_at,
        basis: "notice".into(),
        facts: "A report said the listing offers a prescription-only medicine. The title and \
                photos show the medicine and its prescription label."
            .into(),
        automated_detection: false,
        automated_decision: false,
        automated_means: None,
        ground: "terms".into(),
        ground_reference: "Pubky Terms of Service, Prohibited goods".into(),
        explanation: "The Terms prohibit listing anything that is illegal to offer, and \
                      prescription-only medicines cannot lawfully be sold without a prescription."
            .into(),
    }
}

async fn notifications(app: &TestApp, token: &str) -> Vec<Value> {
    drain_outbox(&app.pool, None, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/notifications",
        Some(token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "notifications read failed: {body}");
    body["notifications"]
        .as_array()
        .expect("notifications array")
        .clone()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_seller_receives_the_statement_of_reasons(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let bystander = new_actor(&app).await;
    let now = app.clock.now();
    let decided_at = now - Duration::minutes(10);
    let decision =
        Decision::validate(listing_input(&seller.pubky, decided_at), now).expect("valid decision");

    let id = record(&app.pool, &decision, "moderator-1", now)
        .await
        .expect("decision recorded")
        .id;

    let rows = notifications(&app, &seller.token).await;
    let notice: Vec<&Value> = rows
        .iter()
        .filter(|row| row["type"] == json!("moderation_decision"))
        .collect();
    assert_eq!(notice.len(), 1, "one notice: {rows:?}");
    let notice = notice[0];
    assert_eq!(
        notice["aggregate_id"],
        json!(format!("moderation_decision:{id}"))
    );
    assert_eq!(notice["actor_pubky"], json!("system"));
    assert_eq!(notice["recipient_pubky"], json!(seller.pubky));
    let statement = &notice["moderation_decision"];
    assert_eq!(statement["id"], json!(id));
    assert_eq!(statement["restriction"], json!("listing_removed"));
    assert_eq!(
        statement["target_uri"],
        json!(format!(
            "pubky://{}/pub/pubky.app/marketplace/v1/listings/lst_1",
            seller.pubky
        ))
    );
    assert_eq!(
        statement["territorial_scope"],
        json!("Every region where Pubky Market is offered")
    );
    assert_eq!(statement["ends_at"], Value::Null);
    assert_eq!(
        statement["decided_at"],
        json!(marketplace_service::clock::format_timestamp(decided_at))
    );
    assert_eq!(statement["basis"], json!("notice"));
    assert_eq!(statement["automated_detection"], json!(false));
    assert_eq!(statement["automated_decision"], json!(false));
    assert_eq!(statement["automated_means"], Value::Null);
    assert_eq!(statement["ground"], json!("terms"));
    assert_eq!(
        statement["ground_reference"],
        json!("Pubky Terms of Service, Prohibited goods")
    );
    assert!(statement["facts"]
        .as_str()
        .is_some_and(|facts| facts.starts_with("A report said")));
    assert!(statement["explanation"]
        .as_str()
        .is_some_and(|explanation| explanation.starts_with("The Terms prohibit")));
    assert!(
        statement.get("decided_by").is_none(),
        "the operator is not shown: {statement}"
    );

    let others = notifications(&app, &bystander.token).await;
    assert!(
        others
            .iter()
            .all(|row| row["type"] != json!("moderation_decision")),
        "only the owner is told: {others:?}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_notification_never_carries_another_recipients_decision(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let other = new_actor(&app).await;
    let now = app.clock.now();
    let decision = Decision::validate(
        listing_input(&seller.pubky, now - Duration::minutes(1)),
        now,
    )
    .expect("valid decision");
    let id = record(&app.pool, &decision, "moderator-1", now)
        .await
        .expect("decision recorded")
        .id;
    drain_outbox(&app.pool, None, now, 30)
        .await
        .expect("outbox drains");

    // A row addressed to someone else that names the seller's decision.
    let event_id: Uuid = sqlx::query_scalar(
        "SELECT event_id FROM notifications WHERE aggregate_id = $1 AND recipient_pubky = $2",
    )
    .bind(format!("moderation_decision:{id}"))
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("the seller's notification");
    sqlx::query(
        "INSERT INTO notifications (id, event_id, recipient_pubky, actor_pubky, type, \
         aggregate_id, created_at) VALUES ($1, $2, $3, 'system', 'moderation_decision', $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(event_id)
    .bind(&other.pubky)
    .bind(format!("moderation_decision:{id}"))
    .bind(now)
    .execute(&app.pool)
    .await
    .expect("a misaddressed row");

    let rows = notifications(&app, &other.token).await;
    let misaddressed = rows
        .iter()
        .find(|row| row["type"] == json!("moderation_decision"))
        .expect("the misaddressed row is listed");
    assert_eq!(misaddressed["moderation_decision"], Value::Null);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn other_notifications_carry_no_decision(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let now = app.clock.now();
    let decision = Decision::validate(
        listing_input(&seller.pubky, now - Duration::minutes(1)),
        now,
    )
    .expect("valid decision");
    let id = record(&app.pool, &decision, "moderator-1", now)
        .await
        .expect("decision recorded")
        .id;
    // Another type whose aggregate happens to name the decision.
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, 'test:other', 1, 'system', 'test.other', $3)",
    )
    .bind(event_id)
    .bind(Uuid::new_v4())
    .bind(now)
    .execute(&app.pool)
    .await
    .expect("an unrelated event");
    sqlx::query(
        "INSERT INTO notifications (id, event_id, recipient_pubky, actor_pubky, type, \
         aggregate_id, created_at) VALUES ($1, $2, $3, 'system', 'order_created', $4, $5)",
    )
    .bind(Uuid::new_v4())
    .bind(event_id)
    .bind(&seller.pubky)
    .bind(format!("moderation_decision:{id}"))
    .bind(now)
    .execute(&app.pool)
    .await
    .expect("another type");

    let rows = notifications(&app, &seller.token).await;
    let other_type = rows
        .iter()
        .find(|row| row["type"] == json!("order_created"))
        .expect("the other row is listed");
    assert_eq!(other_type["moderation_decision"], Value::Null);
    let notice = rows
        .iter()
        .find(|row| row["type"] == json!("moderation_decision"))
        .expect("the notice is listed");
    assert_eq!(notice["moderation_decision"]["id"], json!(id));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn decisions_are_append_only(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let now = app.clock.now();
    let decision = Decision::validate(
        listing_input(&seller.pubky, now - Duration::minutes(1)),
        now,
    )
    .expect("valid decision");
    let id = record(&app.pool, &decision, "moderator-1", now)
        .await
        .expect("decision recorded")
        .id;

    sqlx::query("UPDATE moderation_decisions SET facts = 'changed' WHERE id = $1")
        .bind(id)
        .execute(&app.pool)
        .await
        .expect_err("a decision is never edited");
    sqlx::query("DELETE FROM moderation_decisions WHERE id = $1")
        .bind(id)
        .execute(&app.pool)
        .await
        .expect_err("a decision is never deleted");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn recording_writes_one_event_and_a_replay_writes_nothing(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let now = app.clock.now();
    let decision = Decision::validate(
        listing_input(&seller.pubky, now - Duration::minutes(1)),
        now,
    )
    .expect("valid decision");

    let first = record(&app.pool, &decision, "moderator-1", now)
        .await
        .expect("decision recorded");
    assert!(first.created);
    assert_eq!(first.id, decision.id());
    let (command_id, revision, actor, kind): (Uuid, i64, String, String) = sqlx::query_as(
        "SELECT command_id, revision, actor_pubky, kind FROM events WHERE aggregate_id = $1",
    )
    .bind(format!("moderation_decision:{}", first.id))
    .fetch_one(&app.pool)
    .await
    .expect("one event");
    assert_eq!(
        (command_id, revision, actor.as_str(), kind.as_str()),
        (first.id, 1, "system", "moderation.decision_recorded")
    );

    let replay = record(
        &app.pool,
        &decision,
        "moderator-2",
        now + Duration::minutes(5),
    )
    .await
    .expect("a replay is not an error");
    assert_eq!(
        replay,
        Recorded {
            id: first.id,
            created: false
        }
    );
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM moderation_decisions), \
         (SELECT COUNT(*) FROM events WHERE aggregate_id LIKE 'moderation_decision:%'), \
         (SELECT COUNT(*) FROM outbox WHERE kind = 'notification.moderation_decision')",
    )
    .fetch_one(&app.pool)
    .await
    .expect("counts");
    assert_eq!(counts, (1, 1, 1), "one decision, one event, one notice");
    let rows = notifications(&app, &seller.token).await;
    assert_eq!(
        rows.iter()
            .filter(|row| row["type"] == json!("moderation_decision"))
            .count(),
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_failed_record_leaves_nothing_behind(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let now = app.clock.now();
    let decision = Decision::validate(
        listing_input(&seller.pubky, now - Duration::minutes(1)),
        now,
    )
    .expect("valid decision");
    // An event already holds the decision's aggregate revision, so the
    // event insert fails after the decision row is written.
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, 1, 'system', 'test.blocker', $4)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(format!("moderation_decision:{}", decision.id()))
    .bind(now)
    .execute(&app.pool)
    .await
    .expect("blocking event");

    record(&app.pool, &decision, "moderator-1", now)
        .await
        .expect_err("the event insert fails");

    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM moderation_decisions), \
         (SELECT COUNT(*) FROM outbox WHERE kind = 'notification.moderation_decision')",
    )
    .fetch_one(&app.pool)
    .await
    .expect("counts");
    assert_eq!(counts, (0, 0), "the decision rolled back with the event");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_table_refuses_a_statement_the_command_would_refuse(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let other = new_actor(&app).await;
    let now = app.clock.now();
    struct Row {
        recipient: String,
        restriction: &'static str,
        target: String,
        facts: &'static str,
        automated: bool,
        means: Option<&'static str>,
    }
    let insert = |row: Row| {
        let pool = app.pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO moderation_decisions (id, recipient_pubky, restriction, target_uri, \
                 territorial_scope, decided_at, basis, facts, automated_detection, \
                 automated_decision, automated_means, ground, ground_reference, explanation, \
                 decided_by, created_at) VALUES ($1, $2, $3, $4, 'All', $5, 'notice', $6, $7, \
                 false, $8, 'terms', 'g', 'e', 'moderator-1', $5)",
            )
            .bind(Uuid::new_v4())
            .bind(row.recipient)
            .bind(row.restriction)
            .bind(row.target)
            .bind(now)
            .bind(row.facts)
            .bind(row.automated)
            .bind(row.means)
            .execute(&pool)
            .await
        }
    };
    let listing = |owner: &str, id: &str| {
        format!("pubky://{owner}/pub/pubky.app/marketplace/v1/listings/{id}")
    };
    let row = |restriction: &'static str, target: String| Row {
        recipient: seller.pubky.clone(),
        restriction,
        target,
        facts: "f",
        automated: false,
        means: None,
    };

    insert(row("listing_removed", listing(&seller.pubky, "a")))
        .await
        .expect("a listing");
    insert(row(
        "shop_suspended",
        format!("pubky://{}/pub/pubky.app/profile.json", seller.pubky),
    ))
    .await
    .expect("a shop");
    insert(row(
        "review_excluded",
        format!(
            "pubky://{}/pub/pubky.app/marketplace/v1/reviews/r-1",
            seller.pubky
        ),
    ))
    .await
    .expect("a review");

    let refused = [
        (
            "another owner's target",
            Row {
                recipient: other.pubky.clone(),
                ..row("listing_removed", listing(&seller.pubky, "a"))
            },
        ),
        (
            "not a pubky",
            Row {
                recipient: "A".repeat(52),
                ..row("listing_removed", listing(&"A".repeat(52), "a"))
            },
        ),
        (
            "a review path for a listing",
            row(
                "listing_removed",
                format!(
                    "pubky://{}/pub/pubky.app/marketplace/v1/reviews/r",
                    seller.pubky
                ),
            ),
        ),
        (
            "a trailing segment",
            row("listing_removed", listing(&seller.pubky, "a/b")),
        ),
        (
            "an id outside the charset",
            row("listing_removed", listing(&seller.pubky, "a.b")),
        ),
        (
            "a listing path for a shop",
            row("shop_suspended", listing(&seller.pubky, "a")),
        ),
        (
            "a profile path for a review",
            row(
                "review_text_hidden",
                format!("pubky://{}/pub/pubky.app/profile.json", seller.pubky),
            ),
        ),
        (
            "blank facts",
            Row {
                facts: "   ",
                ..row("listing_removed", listing(&seller.pubky, "a"))
            },
        ),
        (
            "automation without means",
            Row {
                automated: true,
                ..row("listing_removed", listing(&seller.pubky, "a"))
            },
        ),
        (
            "means without automation",
            Row {
                means: Some("unused"),
                ..row("listing_removed", listing(&seller.pubky, "a"))
            },
        ),
    ];
    for (case, refused_row) in refused {
        insert(refused_row).await.expect_err(case);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0094_reruns_after_its_version_is_forgotten(pool: PgPool) {
    sqlx::raw_sql(include_str!("../migrations/0094_moderation_decisions.sql"))
        .execute(&pool)
        .await
        .expect("0094 applies again over its own table");
}
