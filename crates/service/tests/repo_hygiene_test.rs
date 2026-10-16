//! Generated files stay out of the repository.

use std::path::PathBuf;
use std::process::Command;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn python_bytecode_is_ignored_and_the_schema_script_has_none_tracked() {
    let ignore = std::fs::read_to_string(root().join(".gitignore")).expect(".gitignore");
    assert!(
        ignore.lines().any(|line| line.trim() == "__pycache__/"),
        "`__pycache__/` is ignored"
    );
    let tracked = Command::new("git")
        .args(["ls-files", "scripts/__pycache__"])
        .current_dir(root())
        .output();
    let Ok(tracked) = tracked else {
        return;
    };
    if !tracked.status.success() {
        return;
    }
    assert!(
        tracked.stdout.is_empty(),
        "tracked bytecode: {}",
        String::from_utf8_lossy(&tracked.stdout)
    );
}
