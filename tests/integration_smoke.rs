use std::io::{self, BufRead, Write};
use std::time::{Duration, Instant};

use mcp_stdio_rs::{ContentBlock, McpStdioClient, McpStdioClientOptions};
use serde_json::{Value, json};

#[tokio::test]
async fn smoke_echo_tool_over_stdio_child() {
    let (exe, args) = synthetic_server_command();

    let client = McpStdioClient::spawn(
        exe,
        &args,
        &[
            ("MCP_STDIO_SMOKE_SERVER", "normal"),
            ("MCP_STDIO_DELAY_EOF_MS", "2200"),
        ],
    )
    .await
    .expect("spawn synthetic MCP server");

    let tools = client.list_tools().await.expect("list tools");
    assert!(tools.iter().any(|tool| tool.name == "echo"));
    assert!(tools.iter().any(|tool| tool.name == "second-page-tool"));

    let result = client
        .call_tool("echo", json!({ "msg": "hello" }))
        .await
        .expect("call echo tool");

    assert!(
        result
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text(text) if text == "hello"))
    );
    assert!(result.content.iter().any(|block| {
        matches!(
            block,
            ContentBlock::EmbeddedResource {
                uri,
                mime_type: Some(mime_type),
                text: Some(text),
                blob: None,
            } if uri == "mnemos://memory/1"
                && mime_type == "text/plain"
                && text == "remembered content"
        )
    }));

    let shutdown_started = Instant::now();
    client.shutdown().await.expect("shutdown client");
    assert!(shutdown_started.elapsed() >= Duration::from_millis(2100));
}

#[tokio::test]
async fn initialization_timeout_is_enforced() {
    let (exe, args) = synthetic_server_command();
    let options = McpStdioClientOptions {
        initialization_timeout: Duration::from_millis(75),
        shutdown_grace_period: Duration::from_millis(75),
        ..McpStdioClientOptions::default()
    };

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        McpStdioClient::spawn_with_options(
            exe,
            &args,
            &[("MCP_STDIO_SMOKE_SERVER", "stall-initialize")],
            options,
        ),
    )
    .await
    .expect("client must enforce its initialization timeout");
    let error = match result {
        Ok(client) => {
            client.shutdown().await.expect("shutdown unexpected client");
            panic!("stalled initialization must fail");
        }
        Err(error) => error,
    };
    assert!(format!("{error:#}").contains("timed out"));
}

#[tokio::test]
async fn request_timeout_is_enforced() {
    let (exe, args) = synthetic_server_command();
    let options = McpStdioClientOptions {
        request_timeout: Duration::from_millis(75),
        shutdown_grace_period: Duration::from_millis(75),
        ..McpStdioClientOptions::default()
    };
    let client = McpStdioClient::spawn_with_options(
        exe,
        &args,
        &[("MCP_STDIO_SMOKE_SERVER", "stall-request")],
        options,
    )
    .await
    .expect("spawn stalled-request server");

    let result = tokio::time::timeout(Duration::from_secs(1), client.list_tools())
        .await
        .expect("client must enforce its request timeout");
    assert!(
        result
            .expect_err("stalled request must fail")
            .to_string()
            .contains("timed out")
    );
    client.shutdown().await.expect("shutdown client");
}

#[tokio::test]
async fn shutdown_is_bounded_when_descendant_holds_stdout() {
    let (exe, args) = synthetic_server_command();
    let options = McpStdioClientOptions {
        shutdown_grace_period: Duration::from_millis(75),
        ..McpStdioClientOptions::default()
    };
    let client = McpStdioClient::spawn_with_options(
        exe,
        &args,
        &[("MCP_STDIO_SMOKE_SERVER", "descendant-holds-stdout")],
        options,
    )
    .await
    .expect("spawn server with stdout-inheriting descendant");

    tokio::time::timeout(Duration::from_millis(500), client.shutdown())
        .await
        .expect("stdout-inheriting descendant must not block shutdown")
        .expect("shutdown client");
}

#[tokio::test]
async fn write_timeout_poisoned_transport_rejects_later_requests() {
    let (exe, args) = synthetic_server_command();
    let options = McpStdioClientOptions {
        request_timeout: Duration::from_millis(75),
        shutdown_grace_period: Duration::from_millis(75),
        ..McpStdioClientOptions::default()
    };
    let client = McpStdioClient::spawn_with_options(
        exe,
        &args,
        &[("MCP_STDIO_SMOKE_SERVER", "stop-reading")],
        options,
    )
    .await
    .expect("spawn server that stops reading after initialization");

    let error = client
        .call_tool("echo", json!({"payload": "x".repeat(2 * 1024 * 1024)}))
        .await
        .expect_err("blocked partial frame must time out");
    assert!(error.to_string().contains("writer timed out"));

    let later = client
        .list_tools()
        .await
        .expect_err("poisoned transport must reject later requests");
    assert!(later.to_string().contains("transport is closed"));
    client.shutdown().await.expect("shutdown client");
}

#[tokio::test]
async fn repeated_pagination_cursor_is_rejected() {
    let (exe, args) = synthetic_server_command();
    let client = McpStdioClient::spawn(exe, &args, &[("MCP_STDIO_SMOKE_SERVER", "repeat-cursor")])
        .await
        .expect("spawn repeated-cursor server");

    let error = client
        .list_tools()
        .await
        .expect_err("repeated cursor must be rejected");
    assert!(error.to_string().contains("repeated cursor"));
    client.shutdown().await.expect("shutdown client");
}

#[tokio::test]
async fn pagination_has_an_overall_deadline() {
    let (exe, args) = synthetic_server_command();
    let options = McpStdioClientOptions {
        request_timeout: Duration::from_millis(100),
        shutdown_grace_period: Duration::from_millis(75),
        ..McpStdioClientOptions::default()
    };
    let client = McpStdioClient::spawn_with_options(
        exe,
        &args,
        &[("MCP_STDIO_SMOKE_SERVER", "endless-pages")],
        options,
    )
    .await
    .expect("spawn endless pagination server");

    let error = client
        .list_tools()
        .await
        .expect_err("overall pagination deadline must be enforced");
    assert!(error.to_string().contains("pagination timed out"));
    client.shutdown().await.expect("shutdown client");
}

#[tokio::test]
async fn pagination_page_and_tool_limits_are_enforced() {
    let (exe, args) = synthetic_server_command();
    let page_client =
        McpStdioClient::spawn(exe, &args, &[("MCP_STDIO_SMOKE_SERVER", "too-many-pages")])
            .await
            .expect("spawn excessive-page server");
    let page_error = page_client
        .list_tools()
        .await
        .expect_err("page limit must be enforced");
    assert!(page_error.to_string().contains("100-page limit"));
    page_client.shutdown().await.expect("shutdown page client");

    let tool_client =
        McpStdioClient::spawn(exe, &args, &[("MCP_STDIO_SMOKE_SERVER", "too-many-tools")])
            .await
            .expect("spawn excessive-tool server");
    let tool_error = tool_client
        .list_tools()
        .await
        .expect_err("tool limit must be enforced");
    assert!(tool_error.to_string().contains("10000-tool limit"));
    tool_client.shutdown().await.expect("shutdown tool client");
}

#[tokio::test]
async fn missing_tools_field_is_a_protocol_error() {
    let (exe, args) = synthetic_server_command();
    let client = McpStdioClient::spawn(exe, &args, &[("MCP_STDIO_SMOKE_SERVER", "missing-tools")])
        .await
        .expect("spawn missing-tools server");

    let error = client
        .list_tools()
        .await
        .expect_err("missing tools must not decode as an empty list");
    assert!(format!("{error:#}").contains("missing field `tools`"));
    client.shutdown().await.expect("shutdown client");
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_client_kills_child() {
    let (exe, args) = synthetic_server_command();
    let client = McpStdioClient::spawn(exe, &args, &[("MCP_STDIO_SMOKE_SERVER", "ignore-eof")])
        .await
        .expect("spawn ignore-EOF server");
    let result = client
        .call_tool("echo", json!({ "msg": "__pid__" }))
        .await
        .expect("read child pid");
    let pid = result
        .content
        .iter()
        .find_map(|block| match block {
            ContentBlock::Text(text) => text.parse::<i32>().ok(),
            _ => None,
        })
        .expect("tool result must contain child pid");

    drop(client);

    let stopped = tokio::time::timeout(Duration::from_secs(2), async {
        while process_exists(pid) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(stopped.is_ok(), "child {pid} survived client drop");
}

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    unsafe { kill(pid, 0) == 0 }
}

#[tokio::test]
async fn malformed_response_fails_request_and_transport() {
    let (exe, args) = synthetic_server_command();
    let client = McpStdioClient::spawn(exe, &args, &[("MCP_STDIO_SMOKE_SERVER", "malformed")])
        .await
        .expect("spawn malformed-response server");

    let first = tokio::time::timeout(Duration::from_secs(1), client.list_tools())
        .await
        .expect("malformed response must not leave request pending")
        .expect_err("malformed response must fail request");
    assert!(first.to_string().contains("invalid JSON-RPC"));

    let second = client
        .list_tools()
        .await
        .expect_err("closed transport must reject later requests");
    assert!(second.to_string().contains("transport is closed"));
    client.shutdown().await.expect("shutdown client");
}

#[tokio::test]
async fn rejects_unsupported_initialize_version() {
    let (exe, args) = synthetic_server_command();

    let error = match McpStdioClient::spawn(
        exe,
        &args,
        &[("MCP_STDIO_SMOKE_SERVER", "unsupported-version")],
    )
    .await
    {
        Ok(client) => {
            client.shutdown().await.expect("shutdown unexpected client");
            panic!("unsupported protocol version must fail initialization");
        }
        Err(error) => error,
    };
    assert!(error.to_string().contains("unsupported protocol version"));
}

fn synthetic_server_command() -> (&'static str, [&'static str; 2]) {
    (
        "python3",
        [
            "-u",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/synthetic_mcp_server.py"
            ),
        ],
    )
}

#[test]
fn mcp_stdio_smoke_server() {
    if std::env::var_os("MCP_STDIO_SMOKE_SERVER").is_none() {
        return;
    }

    run_synthetic_server();
    std::process::exit(0);
}

fn run_synthetic_server() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mode = std::env::var("MCP_STDIO_SMOKE_SERVER").unwrap();
    let mut lines = stdin.lock().lines();

    while let Some(line) = lines.next() {
        let line = line.expect("read stdin line");
        let request: Value = serde_json::from_str(&line).expect("parse request JSON");
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let id = request.get("id").cloned();

        match method {
            "initialize" => write_response(
                &mut stdout,
                id,
                json!({
                    "protocolVersion": if mode == "unsupported-version" {
                        "2099-01-01"
                    } else {
                        "2024-11-05"
                    },
                    "capabilities": {},
                    "serverInfo": {
                        "name": "synthetic-mcp-smoke-server",
                        "version": "0.1.0"
                    }
                }),
            ),
            "notifications/initialized" => {}
            "tools/list" if mode == "malformed" => {
                writeln!(stdout, "{{not valid JSON").expect("write malformed response");
                stdout.flush().expect("flush malformed response");
            }
            "tools/list" => {
                if request.pointer("/params/cursor").is_none() {
                    write_response(
                        &mut stdout,
                        id,
                        json!({
                            "tools": [{
                                "name": "echo",
                                "description": "Echoes the msg argument.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "msg": { "type": "string" }
                                    },
                                    "required": ["msg"]
                                }
                            }],
                            "nextCursor": "page-2"
                        }),
                    );
                } else {
                    assert_eq!(
                        request.pointer("/params/cursor").and_then(Value::as_str),
                        Some("page-2")
                    );
                    write_response(
                        &mut stdout,
                        id,
                        json!({
                            "tools": [{
                                "name": "second-page-tool",
                                "inputSchema": {
                                    "type": "object"
                                }
                            }]
                        }),
                    );
                }
            }
            "tools/call" => {
                writeln!(
                    stdout,
                    "{}",
                    json!({ "jsonrpc": "2.0", "id": "health-1", "method": "ping" })
                )
                .expect("write ping request");
                stdout.flush().expect("flush ping request");
                let ping_response: Value = serde_json::from_str(
                    &lines
                        .next()
                        .expect("client must reply to ping")
                        .expect("read ping response"),
                )
                .expect("parse ping response");
                assert_eq!(ping_response.get("id"), Some(&json!("health-1")));
                assert_eq!(ping_response.get("result"), Some(&json!({})));

                let text = request
                    .pointer("/params/arguments/msg")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                write_response(
                    &mut stdout,
                    id,
                    json!({
                        "content": [
                            {
                                "type": "text",
                                "text": text
                            },
                            {
                                "type": "resource",
                                "resource": {
                                    "uri": "mnemos://memory/1",
                                    "mimeType": "text/plain",
                                    "text": "remembered content"
                                }
                            },
                        ],
                        "isError": false
                    }),
                );
            }
            other => write_error(&mut stdout, id, -32601, &format!("unknown method {other}")),
        }
    }

    if let Ok(delay) = std::env::var("MCP_STDIO_DELAY_EOF_MS") {
        std::thread::sleep(Duration::from_millis(
            delay.parse().expect("valid EOF delay"),
        ));
    }
}

fn write_response(stdout: &mut io::Stdout, id: Option<Value>, result: Value) {
    let response = json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    });
    writeln!(stdout, "{response}").expect("write response");
    stdout.flush().expect("flush response");
}

fn write_error(stdout: &mut io::Stdout, id: Option<Value>, code: i64, message: &str) {
    let response = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message
        }
    });
    writeln!(stdout, "{response}").expect("write error response");
    stdout.flush().expect("flush error response");
}
