//! Drives the Docker WooCommerce (`tests/fixtures/woocommerce/stack`) for the
//! one-time recording run. Only used when `WOO_LIVE=1`; CI replays what this
//! captured.
//!
//! What it needs, all set up by `stack/tools/record.sh`:
//! * the stack up, with the capture proxy on :8443 in front of nginx on :8444;
//! * the receiver on :18443 writing what WooCommerce sends outward;
//! * an admin REST key of its own (`stack/.local/admin-key`) for the store-side
//!   changes a merchant would make (stock, tracking, cancelling), sent
//!   straight to nginx so they are not part of what the connector recorded.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use base64::Engine;
use serde_json::{json, Value};

use super::replay::{Delivery, FIXTURE_ROOT};

pub struct LiveStack {
    dir: PathBuf,
    receiver: PathBuf,
    docker: Vec<String>,
    direct: reqwest::Client,
    recorder: reqwest::Client,
    admin: (String, String),
    record: bool,
}

pub fn ca_certificate() -> reqwest::Certificate {
    let pem = std::fs::read(format!("{FIXTURE_ROOT}/stack/certs/ca.pem"))
        .expect("stack/certs/ca.pem (run stack/certs/gen.sh)");
    reqwest::Certificate::from_pem(&pem).expect("CA certificate")
}

impl LiveStack {
    pub fn from_env() -> Option<Self> {
        if std::env::var("WOO_LIVE").ok().as_deref() != Some("1") {
            return None;
        }
        let dir = PathBuf::from(format!("{FIXTURE_ROOT}/stack"));
        let key_file = std::fs::read_to_string(dir.join(".local/admin-key"))
            .expect("stack/.local/admin-key (run stack/tools/record.sh)");
        let mut lines = key_file.lines();
        let admin = (
            lines.next().expect("admin key").to_string(),
            lines.next().expect("admin secret").to_string(),
        );
        let client = || {
            reqwest::Client::builder()
                .add_root_certificate(ca_certificate())
                .timeout(Duration::from_secs(120))
                .build()
                .expect("client")
        };
        Some(Self {
            receiver: PathBuf::from(
                std::env::var("WOO_RECEIVER_DIR").unwrap_or_else(|_| "/tmp/woo-recv".into()),
            ),
            docker: std::env::var("WOO_DOCKER")
                .unwrap_or_else(|_| "sudo docker".into())
                .split_whitespace()
                .map(str::to_string)
                .collect(),
            direct: client(),
            recorder: client(),
            admin,
            record: std::env::var("WOO_RECORD").ok().as_deref() == Some("1"),
            dir,
        })
    }

    pub fn site(&self) -> String {
        "localhost:8443".into()
    }

    pub fn recording(&self) -> bool {
        self.record
    }

    /// Tells the capture proxy which fixture file the next exchanges belong to.
    pub async fn label(&self, name: &str) {
        self.recorder
            .post(format!(
                "https://localhost:8443/__fixture/label?name={name}"
            ))
            .send()
            .await
            .expect("label request")
            .error_for_status()
            .expect("label accepted");
    }

    fn compose(&self, extra: &[&str]) -> Command {
        let mut command = Command::new(&self.docker[0]);
        command
            .args(&self.docker[1..])
            .args(["compose", "--profile", "tools", "run", "--rm", "-T", "cli"])
            .args(extra)
            .current_dir(&self.dir);
        command
    }

    pub fn wp(&self, args: &[&str]) -> String {
        let mut all = vec!["wp", "--path=/var/www/html"];
        all.extend_from_slice(args);
        let output = self.compose(&all).output().expect("docker compose runs");
        assert!(
            output.status.success(),
            "wp {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    /// WooCommerce queues webhook deliveries; with cron disabled in the stack,
    /// running the queue is what delivers them.
    pub fn flush_webhooks(&self) {
        self.wp(&["action-scheduler", "run"]);
    }

    /// Removes every webhook from the store, so a scenario starts with only the
    /// webhooks it registers itself (earlier scenarios leave theirs behind, and
    /// the store would deliver to those too).
    pub async fn wipe_webhooks(&self) {
        let listed = self.rest("GET", "/wc/v3/webhooks?per_page=100", None).await;
        for hook in listed.as_array().expect("hooks") {
            let id = hook["id"].as_i64().expect("id");
            self.rest("DELETE", &format!("/wc/v3/webhooks/{id}?force=true"), None)
                .await;
        }
    }

    /// The secret the store holds for a webhook (the REST API never returns it).
    pub fn webhook_secret(&self, id: i64) -> String {
        self.wp(&["eval", &format!("echo wc_get_webhook({id})->get_secret();")])
            .trim()
            .to_string()
    }

    pub async fn rest(&self, method: &str, path: &str, body: Option<Value>) -> Value {
        let mut request = self
            .direct
            .request(
                method.parse().expect("method"),
                format!("https://localhost:8444/wp-json{path}"),
            )
            .basic_auth(&self.admin.0, Some(&self.admin.1));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("store request");
        let status = response.status();
        let text = response.text().await.expect("store response");
        assert!(status.is_success(), "{method} {path}: {status} {text}");
        serde_json::from_str(&text).unwrap_or(Value::Null)
    }

    fn receiver_files(&self) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.receiver)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        files
    }

    pub fn receiver_mark(&self) -> usize {
        self.receiver_files().len()
    }

    fn read_delivery(path: &Path) -> Delivery {
        serde_json::from_slice(&std::fs::read(path).expect("receiver file")).expect("receiver JSON")
    }

    /// Deliveries the store sent since `mark`, oldest first.
    pub fn deliveries_since(&self, mark: usize) -> Vec<Delivery> {
        self.receiver_files()
            .iter()
            .skip(mark)
            .map(|path| Self::read_delivery(path))
            .collect()
    }

    /// Plays the merchant approving the wc-auth request, and returns the body
    /// WooCommerce posted to the callback.
    pub fn approve(&self, authorize_url: &str) -> Vec<u8> {
        let mark = self.receiver_mark();
        let password = std::fs::read_to_string(self.dir.join(".local/admin-password"))
            .expect("admin password")
            .trim()
            .to_string();
        let output = Command::new("python3")
            .arg(self.dir.join("tools/wc_auth_approve.py"))
            .arg(authorize_url)
            .arg(self.dir.join("certs/ca.pem"))
            .arg("https://localhost:8444")
            .env("ADMIN_PASSWORD", password)
            .output()
            .expect("approve script runs");
        assert!(
            output.status.success(),
            "approval failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        self.deliveries_since(mark)
            .into_iter()
            .find(|delivery| {
                delivery
                    .headers
                    .get("content-type")
                    .is_some_and(|ct| ct.starts_with("application/json"))
                    && !delivery.headers.contains_key("x-wc-webhook-topic")
            })
            .expect("the store posted its keys to the callback")
            .body()
    }

    /// Makes the stack's proxy drop the `Authorization` header before it
    /// reaches WordPress, like a host that strips it.
    pub fn strip_authorization(&self, on: bool) {
        let file = self.dir.join("nginx/extra/strip-auth.conf");
        if on {
            std::fs::write(&file, "proxy_set_header Authorization \"\";\n")
                .expect("write nginx conf");
        } else {
            let _ = std::fs::remove_file(&file);
        }
        let output = Command::new(&self.docker[0])
            .args(&self.docker[1..])
            .args(["compose", "exec", "-T", "proxy", "nginx", "-s", "reload"])
            .current_dir(&self.dir)
            .output()
            .expect("nginx reload runs");
        assert!(output.status.success(), "nginx reload failed");
        std::thread::sleep(Duration::from_millis(800));
    }

    pub fn plain_permalinks(&self, on: bool) {
        self.wp(&["rewrite", "structure", if on { "" } else { "/%postname%/" }]);
        self.wp(&["rewrite", "flush"]);
    }

    pub fn woocommerce_active(&self, on: bool) {
        self.wp(&[
            "plugin",
            if on { "activate" } else { "deactivate" },
            "woocommerce",
        ]);
    }

    /// Stores a capture as a fixture when recording.
    pub fn save_delivery(&self, name: &str, delivery: &Delivery, secret: Option<&str>) {
        if !self.record {
            return;
        }
        let directory = format!("{FIXTURE_ROOT}/deliveries");
        std::fs::create_dir_all(&directory).expect("deliveries dir");
        let headers: serde_json::Map<String, Value> = delivery
            .headers
            .iter()
            .filter(|(name, _)| {
                name.starts_with("x-wc-") || *name == "content-type" || *name == "user-agent"
            })
            .map(|(name, value)| (name.clone(), Value::String(value.clone())))
            .collect();
        let mut record = json!({
            "headers": headers,
            "body_base64": delivery.body_base64,
        });
        if let Some(secret) = secret {
            record["webhook_secret"] = json!(secret);
        }
        std::fs::write(
            format!("{directory}/{name}.json"),
            serde_json::to_string_pretty(&record).expect("json") + "\n",
        )
        .expect("write delivery fixture");
    }

    /// Saves the wc-auth callback with the key pair replaced by fixed values:
    /// the replay never compares them, and they are not a secret worth keeping.
    pub fn save_callback(&self, name: &str, body: &[u8]) {
        if !self.record {
            return;
        }
        let mut parsed: Value = serde_json::from_slice(body).expect("callback JSON");
        parsed["consumer_key"] = json!(format!("ck_{}", "a".repeat(40)));
        parsed["consumer_secret"] = json!(format!("cs_{}", "b".repeat(40)));
        parsed["user_id"] = json!("<nonce>");
        let delivery = Delivery {
            headers: [(
                "content-type".to_string(),
                "application/json;charset=UTF-8".to_string(),
            )]
            .into_iter()
            .collect(),
            body_base64: base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&parsed).expect("json")),
            webhook_secret: None,
        };
        self.save_delivery(name, &delivery, None);
    }
}

/// Puts the store back as the other scenarios expect it, even when a test
/// panics halfway through a change.
pub struct RestoreStore<'a>(pub &'a LiveStack);

impl Drop for RestoreStore<'_> {
    fn drop(&mut self) {
        self.0.strip_authorization(false);
        let _ = Command::new(&self.0.docker[0])
            .args(&self.0.docker[1..])
            .args(["compose", "--profile", "tools", "run", "--rm", "-T", "cli", "sh", "-c"])
            .arg("wp --path=/var/www/html plugin activate woocommerce; wp --path=/var/www/html rewrite structure '/%postname%/'; wp --path=/var/www/html rewrite flush")
            .current_dir(&self.0.dir)
            .output();
    }
}
