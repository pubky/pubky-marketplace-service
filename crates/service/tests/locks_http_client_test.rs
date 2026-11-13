//! The production Lock Server lifecycle client against a real HTTP server
//! speaking the documented `POST /verification-task-lookups` shapes
//! (Locks `docs/API.md`, `client_binding` since v0.1.1-rc2).

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use marketplace_service::locks::{
    HttpLocksClient, LocksClientBinding, LocksLifecycleClient, LocksLookupOutcome, LocksTaskStatus,
};
use serde_json::{json, Value};

const CREATOR: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy";
const BUNDLE: &str = "000G40R40M30E209185GR38E1W";
const RESOURCE: &str = "pubkytkrq8zmwb8a3m9k15csu3q17qmfgqnp9dskbrg9uq1rydpyxp7qy/pub/app.locks/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json";
const REFERENCE: &str = "x9Kq_3-jW0sN8uYv1tB6rZpLmE4cA2dHfG7iOoQwXyU";

#[derive(Clone)]
struct LockServer {
    response: Arc<Mutex<(StatusCode, Value)>>,
    requests: Arc<Mutex<Vec<Value>>>,
}

async fn lookup(
    State(server): State<LockServer>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    server.requests.lock().expect("requests").push(body);
    let (status, response) = server.response.lock().expect("response").clone();
    (status, Json(response))
}

async fn serve(status: StatusCode, response: Value) -> (HttpLocksClient, LockServer) {
    let server = LockServer {
        response: Arc::new(Mutex::new((status, response))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let router = Router::new()
        .route("/verification-task-lookups", post(lookup))
        .with_state(server.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let address = listener.local_addr().expect("local address");
    tokio::spawn(async move { axum::serve(listener, router).await.expect("serve") });
    let client = HttpLocksClient::new(&format!("http://{address}/")).expect("client");
    (client, server)
}

fn lifecycle(status: &str, client_binding: Option<Value>) -> Value {
    let mut body = json!({
        "creator": CREATOR,
        "bundle_id": BUNDLE,
        "status": status,
        "submitted_at": "2026-11-09T09:00:00Z",
        "started_at": null,
        "completed_at": null,
        "failure_message": null,
        "status_message": null,
        "admission_deadline_at": null,
        "terminal_reason": null,
    });
    if let Some(binding) = client_binding {
        body["client_binding"] = binding;
    }
    body
}

fn echoed() -> Value {
    json!({
        "client_reference": REFERENCE,
        "pubky_lock_resource": RESOURCE,
        "criterion_ids": ["criterion-1"],
    })
}

#[tokio::test]
async fn a_completed_task_returns_the_echoed_binding_and_only_the_handle_is_sent() {
    let (client, server) = serve(StatusCode::OK, lifecycle("completed", Some(echoed()))).await;
    let outcome = client.lookup(CREATOR, BUNDLE).await;
    assert_eq!(
        outcome,
        LocksLookupOutcome::Completed(Some(LocksClientBinding {
            client_reference: REFERENCE.to_string(),
            pubky_lock_resource: RESOURCE.to_string(),
            criterion_ids: vec!["criterion-1".to_string()],
        }))
    );
    assert_eq!(
        *server.requests.lock().expect("requests"),
        vec![json!({"creator": CREATOR, "bundle_id": BUNDLE})],
        "the request carries the handle and nothing the service minted"
    );
}

#[tokio::test]
async fn a_completed_task_from_a_server_without_binding_support_has_none() {
    let (client, _) = serve(StatusCode::OK, lifecycle("completed", None)).await;
    assert_eq!(
        client.lookup(CREATOR, BUNDLE).await,
        LocksLookupOutcome::Completed(None)
    );
}

#[tokio::test]
async fn a_malformed_binding_on_a_completed_task_is_reported_as_malformed() {
    let (client, _) = serve(
        StatusCode::OK,
        lifecycle("completed", Some(json!({"client_reference": REFERENCE}))),
    )
    .await;
    assert_eq!(
        client.lookup(CREATOR, BUNDLE).await,
        LocksLookupOutcome::CompletedMalformedBinding
    );
}

#[tokio::test]
async fn non_completed_statuses_do_not_depend_on_the_binding() {
    for (status, expected) in [
        ("pending", LocksTaskStatus::Pending),
        ("in_progress", LocksTaskStatus::InProgress),
        ("failed", LocksTaskStatus::Failed),
        ("expired", LocksTaskStatus::Expired),
    ] {
        let (client, _) = serve(StatusCode::OK, lifecycle(status, Some(echoed()))).await;
        assert_eq!(
            client.lookup(CREATOR, BUNDLE).await,
            LocksLookupOutcome::Status(expected),
            "{status}"
        );
    }
}

#[tokio::test]
async fn only_the_documented_not_found_is_definitive() {
    let (client, _) = serve(
        StatusCode::NOT_FOUND,
        json!({"error": {"code": "verification_task_not_found"}}),
    )
    .await;
    assert_eq!(
        client.lookup(CREATOR, BUNDLE).await,
        LocksLookupOutcome::NotFound
    );
    let (client, _) = serve(
        StatusCode::NOT_FOUND,
        json!({"error": {"code": "no_route"}}),
    )
    .await;
    assert_eq!(
        client.lookup(CREATOR, BUNDLE).await,
        LocksLookupOutcome::Unavailable
    );
}
