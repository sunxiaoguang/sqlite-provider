#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(path: PathBuf) -> Self {
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn assert_no_external_runner_reference(path: &Path) {
    let content = fs::read_to_string(path).unwrap();
    let banned = concat!("py", "test");
    assert!(
        !content.to_ascii_lowercase().contains(banned),
        "unexpected external runner reference in {}",
        path.display()
    );
}

#[test]
fn python_runtime_paths_do_not_reference_external_runner() {
    let root = repo_root();
    let static_paths = [
        "Makefile",
        "scripts/run_sqlite_provider_py.py",
        "tests/abi_blackbox/test_sqlite_abi.py",
    ];

    for rel in static_paths {
        assert_no_external_runner_reference(&root.join(rel));
    }

    let scenario_dir = root.join("tests/sqlite_provider_py");
    for entry in fs::read_dir(&scenario_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("py") {
            assert_no_external_runner_reference(&path);
        }
    }
}

#[test]
fn standalone_runner_supports_skip_and_tmp_path_without_external_runner() {
    let root = repo_root();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let package_name = format!("tmp_py_runner_{}_{}", std::process::id(), nonce);
    let package_dir = root.join(&package_name);
    let _guard = TempDirGuard::new(package_dir.clone());

    fs::write(package_dir.join("__init__.py"), "").unwrap();
    fs::write(
        package_dir.join("test_runner_smoke.py"),
        r#"from pathlib import Path

class TestSkipped(Exception):
    pass

def test_pass() -> None:
    assert True

def test_tmp_path(tmp_path: Path) -> None:
    probe = tmp_path / "probe.txt"
    probe.write_text("ok", encoding="utf-8")
    assert probe.read_text(encoding="utf-8") == "ok"

def test_skip() -> None:
    raise TestSkipped("expected skip")
"#,
    )
    .unwrap();

    let output = Command::new("python3")
        .arg("scripts/run_sqlite_provider_py.py")
        .arg(&package_name)
        .current_dir(&root)
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "runner failed\nstdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
    assert!(
        stdout.contains("summary: total=3 passed=2 skipped=1 failed=0"),
        "unexpected runner summary\nstdout:\n{}\nstderr:\n{}",
        stdout,
        stderr
    );
}
