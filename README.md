# mcp-stdio-rs

Minimal embeddable Rust MCP stdio client for MNEMOS and compatible servers.

## Quick start

For the current alpha, add it from a Git dependency:

```toml
[dependencies]
mcp-stdio-rs = { git = "https://example.com/your-org/mcp-stdio-rs", version = "0.1.0" }
```

Or, once published:

```sh
cargo add mcp-stdio-rs
```

## Public API summary

- `McpStdioClient::spawn(cmd, args, env)` starts an MCP server process over stdio and completes the initialize handshake.
- `client.list_tools()` returns the server's available `ToolDef` values.
- `client.call_tool(name, args)` sends `tools/call` with JSON arguments and returns `ToolResult`.
- `client.shutdown()` sends a best-effort shutdown, kills the child process if needed, and joins the stdout reader task.

## Example

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let c = mcp_stdio_rs::McpStdioClient::spawn("ssh", &["ultra", "mnemos-mcp"], &[]).await?;
    let r = c.call_tool("search_memories", serde_json::json!({ "query": "release notes" })).await?;
    println!("{r:#?}");
    c.shutdown().await?;
    Ok(())
}
```

## Why not tonic+gRPC

MCP over stdio is process-spawned newline-delimited JSON-RPC: one JSON object per line on the server child's stdin/stdout. It does not require HTTP/2, protobuf schemas, a gRPC server, or a generated tonic client.

## Compatibility

- Rust 1.86+
- MCP protocol version `2024-11-05`
- Pure Rust client code with no Python runtime linkage
