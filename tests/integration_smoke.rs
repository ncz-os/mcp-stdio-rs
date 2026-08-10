use std::io::{self, BufRead, Write};
use std::time::{Duration, Instant};

use mcp_stdio_rs::{ContentBlock, McpStdioClient};
use serde_json::{Value, json};

#[tokio::test]
async fn smoke_echo_tool_over_stdio_child() {
    let exe = std::env::current_exe().expect("current test executable path");
    let exe = exe.to_str().expect("test executable path must be UTF-8");
    let args = ["--exact", "mcp_stdio_smoke_server", "--nocapture"];

    let client = McpStdioClient::spawn(
        exe,
        &args,
        &[
            ("MCP_STDIO_SMOKE_SERVER", "normal"),
            ("MCP_STDIO_DELAY_EOF_MS", "300"),
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
    assert!(shutdown_started.elapsed() >= Duration::from_millis(250));
}

#[tokio::test]
async fn malformed_response_fails_request_and_transport() {
    let exe = std::env::current_exe().expect("current test executable path");
    let exe = exe.to_str().expect("test executable path must be UTF-8");
    let args = ["--exact", "mcp_stdio_smoke_server", "--nocapture"];
    let client = McpStdioClient::spawn(
        exe,
        &args,
        &[("MCP_STDIO_SMOKE_SERVER", "malformed")],
    )
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
    let exe = std::env::current_exe().expect("current test executable path");
    let exe = exe.to_str().expect("test executable path must be UTF-8");
    let args = ["--exact", "mcp_stdio_smoke_server", "--nocapture"];

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
        std::thread::sleep(Duration::from_millis(delay.parse().expect("valid EOF delay")));
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
