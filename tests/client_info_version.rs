use std::fs;
use std::path::PathBuf;

use mcp_stdio_rs::McpStdioClient;
use serde_json::Value;

fn fixture_script() -> String {
    format!(
        "{}/tests/fixtures/synthetic_mcp_server.py",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn manifest_file(name: &str) -> String {
    fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(name))
        .unwrap_or_else(|error| panic!("failed to read {name}: {error}"))
}

fn features_section(manifest: &str) -> &str {
    let start = manifest
        .find("[features]")
        .expect("Cargo.toml must keep a [features] section");
    let rest = &manifest[start + "[features]".len()..];
    match rest.find("\n[") {
        Some(index) => &rest[..index],
        None => rest,
    }
}

/// RAII guard so the fixture sink file is removed on every path, including
/// panics between spawn and the final assertion.
struct SinkGuard {
    path: PathBuf,
}

impl SinkGuard {
    fn new(path: PathBuf) -> Self {
        let _ = fs::remove_file(&path);
        Self { path }
    }
}

impl Drop for SinkGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// End-to-end: the initialize request the fixture actually receives must carry
/// the crate version. At the current 0.1.0 this coincides with the old
/// hard-coded literal, so this test guards future bumps; the
/// `client_info_version_comes_from_cargo_pkg_version` test below is the one
/// that fails on the pre-fix code.
#[tokio::test]
async fn initialize_request_carries_crate_version() {
    let sink = SinkGuard::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/client-info-version-{}.json",
        std::process::id()
    )));
    let sink_str = sink
        .path
        .to_str()
        .expect("sink path must be UTF-8")
        .to_owned();
    let script = fixture_script();

    let client = McpStdioClient::spawn(
        "python3",
        &["-u", script.as_str()],
        &[
            ("MCP_STDIO_SMOKE_SERVER", "normal"),
            ("MCP_STDIO_CLIENT_INFO_SINK", sink_str.as_str()),
        ],
    )
    .await
    .expect("spawn synthetic MCP server");
    client.shutdown().await.expect("shutdown client");

    let observed: Value = serde_json::from_str(
        &fs::read_to_string(&sink.path).expect("fixture must record clientInfo"),
    )
    .expect("recorded clientInfo must be JSON");

    assert_eq!(
        observed.get("name").and_then(Value::as_str),
        Some("mcp-stdio-rs")
    );
    assert_eq!(
        observed.get("version").and_then(Value::as_str),
        Some(env!("CARGO_PKG_VERSION")),
        "clientInfo.version must track the crate version"
    );
}

/// The wire version must come from the crate metadata, not a literal that can
/// drift on the next version bump.
#[test]
fn client_info_version_comes_from_cargo_pkg_version() {
    let source = manifest_file("src/lib.rs");

    assert!(
        source.contains(r#"env!("CARGO_PKG_VERSION")"#),
        "clientInfo.version must be built from env!(\"CARGO_PKG_VERSION\")"
    );
    assert!(
        !source.contains(r#""version": "0.1.0""#),
        "src/lib.rs must not hard-code a clientInfo version literal"
    );
}

/// The `rmcp-sdk` feature enables nothing (rmcp is only package metadata), so
/// it must not exist for CI `--all-features` runs to "cover".
#[test]
fn manifest_defines_no_rmcp_sdk_feature() {
    let manifest = manifest_file("Cargo.toml");
    let section = features_section(&manifest);

    for line in section.lines() {
        let name = line.split(['=', ' ']).next().unwrap_or("").trim();
        assert_ne!(
            name, "rmcp-sdk",
            "Cargo.toml [features] must not define the no-op rmcp-sdk feature"
        );
    }
}
