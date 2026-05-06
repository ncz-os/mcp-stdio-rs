use std::io::{self, BufRead, Write};

use mcp_stdio_rs::{ContentBlock, McpStdioClient};
use serde_json::{Value, json};

#[tokio::test]
async fn smoke_echo_tool_over_stdio_child() {
    let exe = std::env::current_exe().expect("current test executable path");
    let exe = exe.to_str().expect("test executable path must be UTF-8");
    let args = ["--exact", "mcp_stdio_smoke_server", "--nocapture"];

    let client = McpStdioClient::spawn(exe, &args, &[("MCP_STDIO_SMOKE_SERVER", "1")])
        .await
        .expect("spawn synthetic MCP server");

    let tools = client.list_tools().await.expect("list tools");
    assert!(tools.iter().any(|tool| tool.name == "echo"));

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

    client.shutdown().await.expect("shutdown client");
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

    for line in stdin.lock().lines() {
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
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "serverInfo": {
                        "name": "synthetic-mcp-smoke-server",
                        "version": "0.1.0"
                    }
                }),
            ),
            "notifications/initialized" => {}
            "tools/list" => write_response(
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
                    }]
                }),
            ),
            "tools/call" => {
                let text = request
                    .pointer("/params/arguments/msg")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                write_response(
                    &mut stdout,
                    id,
                    json!({
                        "content": [{
                            "type": "text",
                            "text": text
                        }],
                        "isError": false
                    }),
                );
            }
            "shutdown" => {
                write_response(&mut stdout, id, json!({}));
                break;
            }
            "notifications/exit" => break,
            other => write_error(&mut stdout, id, -32601, &format!("unknown method {other}")),
        }
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
