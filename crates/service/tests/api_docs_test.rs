//! `docs/api.md` carries a route table. It must list exactly the paths the
//! router in `src/http.rs` mounts, so a route cannot be added or removed
//! without the reference noticing.

use std::collections::BTreeSet;
use std::path::PathBuf;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn router_paths() -> BTreeSet<String> {
    let source = std::fs::read_to_string(repository_root().join("crates/service/src/http.rs"))
        .expect("http.rs is readable");
    let mut paths = BTreeSet::new();
    let mut rest = source.as_str();
    while let Some(index) = rest.find(".route(") {
        rest = &rest[index + ".route(".len()..];
        let start = rest.find('"').expect("route has a path literal") + 1;
        let end = start + rest[start..].find('"').expect("path literal closes");
        paths.insert(rest[start..end].to_string());
        rest = &rest[end..];
    }
    paths
}

fn documented_paths() -> BTreeSet<String> {
    let docs = std::fs::read_to_string(repository_root().join("docs/api.md"))
        .expect("docs/api.md is readable");
    const METHODS: [&str; 5] = ["GET", "POST", "PUT", "PATCH", "DELETE"];
    docs.lines()
        .filter_map(|line| {
            let cells: Vec<&str> = line.split('|').map(str::trim).collect();
            let methods = cells.get(1)?;
            let path = cells.get(2)?;
            let is_method_cell = !methods.is_empty()
                && methods
                    .split(',')
                    .all(|method| METHODS.contains(&method.trim()));
            if !is_method_cell {
                return None;
            }
            path.strip_prefix("`/")
                .and_then(|path| path.strip_suffix('`'))
                .map(|path| format!("/{path}"))
        })
        .collect()
}

#[test]
fn api_reference_route_table_matches_the_router() {
    let router = router_paths();
    let documented = documented_paths();
    assert!(
        router.len() > 40,
        "router parsing found only {} routes",
        router.len()
    );
    let undocumented: Vec<_> = router.difference(&documented).collect();
    let unknown: Vec<_> = documented.difference(&router).collect();
    assert!(
        undocumented.is_empty(),
        "routes missing from docs/api.md: {undocumented:?}"
    );
    assert!(
        unknown.is_empty(),
        "docs/api.md lists paths the router does not mount: {unknown:?}"
    );
}
