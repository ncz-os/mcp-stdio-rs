use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>;

/// An MCP client connected to a child process over newline-delimited JSON-RPC.
pub struct McpStdioClient {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    pending: PendingMap,
    reader_task: JoinHandle<()>,
    next_id: AtomicU64,
}

impl McpStdioClient {
    /// Spawn a child process as the MCP server, complete the MCP initialize handshake,
    /// return a ready client. cmd is the executable, args are its arguments, env is
    /// additional environment key-value pairs to inject.
    pub async fn spawn(cmd: &str, args: &[&str], env: &[(&str, &str)]) -> Result<Self> {
        let mut command = Command::new(cmd);
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        for (key, value) in env {
            command.env(key, value);
        }

        let mut child = command
            .spawn()
            .with_context(|| format!("failed to spawn MCP server command `{cmd}`"))?;
        let stdin = child
            .stdin
            .take()
            .context("child process stdin was not piped")?;
        let stdout = child
            .stdout
            .take()
            .context("child process stdout was not piped")?;

        let pending = Arc::new(Mutex::new(HashMap::new()));
        let reader_task = tokio::spawn(reader_loop(stdout, Arc::clone(&pending)));

        let client = Self {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            pending,
            reader_task,
            next_id: AtomicU64::new(1),
        };

        let initialize_params = json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "mcp-stdio-rs",
                "version": "0.1.0"
            }
        });

        if let Err(err) = client
            .send_request_with_id(0, "initialize", initialize_params)
            .await
            .context("MCP initialize request failed")
        {
            let _ = client.shutdown().await;
            return Err(err);
        }

        if let Err(err) = client
            .send_notification("notifications/initialized", json!({}))
            .await
            .context("failed to send MCP initialized notification")
        {
            let _ = client.shutdown().await;
            return Err(err);
        }

        Ok(client)
    }

    /// List available MCP tools from the server.
    pub async fn list_tools(&self) -> Result<Vec<ToolDef>> {
        let response = self.send_request("tools/list", json!({})).await?;
        let result: ToolsListResult =
            serde_json::from_value(response).context("failed to decode MCP tools/list response")?;

        Ok(result
            .tools
            .into_iter()
            .map(|tool| ToolDef {
                name: tool.name,
                description: tool.description.unwrap_or_default(),
                input_schema: tool.input_schema,
            })
            .collect())
    }

    /// Call a named tool with JSON args, return the result.
    pub async fn call_tool(&self, name: &str, args: Value) -> Result<ToolResult> {
        let response = self
            .send_request(
                "tools/call",
                json!({
                    "name": name,
                    "arguments": args
                }),
            )
            .await?;
        decode_tool_result(response)
    }

    /// Send shutdown / kill child process, join reader task, return.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self
            .send_request_no_response("shutdown", json!({}))
            .await
            .map_err(|err| {
                debug!("best-effort shutdown request could not be sent: {err:#}");
                err
            });
        let _ = self
            .send_notification("notifications/exit", json!({}))
            .await
            .map_err(|err| {
                debug!("best-effort exit notification could not be sent: {err:#}");
                err
            });

        let Self {
            child,
            stdin,
            pending: _,
            reader_task,
            next_id: _,
        } = self;

        drop(stdin);

        let mut child = child.into_inner();
        if child.try_wait()?.is_none() {
            child
                .start_kill()
                .context("failed to kill MCP server child")?;
            let _ = child
                .wait()
                .await
                .context("failed to wait for MCP server child")?;
        }

        reader_task
            .await
            .context("MCP stdout reader task failed to join")?;

        Ok(())
    }

    async fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.send_request_with_id(id, method, params).await
    }

    async fn send_request_with_id(&self, id: u64, method: &str, params: Value) -> Result<Value> {
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id, sender);

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params: Some(params),
        };

        if let Err(err) = self.write_json(&request).await {
            self.pending.lock().await.remove(&id);
            return Err(err);
        }

        receiver
            .await
            .map_err(|_| anyhow!("MCP server closed before responding to request {id}"))?
    }

    async fn send_request_no_response(&self, method: &str, params: Value) -> Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params: Some(params),
        };
        self.write_json(&request).await
    }

    async fn send_notification(&self, method: &str, params: Value) -> Result<()> {
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params: Some(params),
        };
        self.write_json(&notification).await
    }

    async fn write_json<T>(&self, message: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        let mut line = serde_json::to_vec(message).context("failed to encode JSON-RPC message")?;
        line.push(b'\n');

        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(&line)
            .await
            .context("failed to write JSON-RPC message to MCP server stdin")?;
        stdin
            .flush()
            .await
            .context("failed to flush MCP server stdin")?;

        Ok(())
    }
}

/// MCP tool definition returned by `tools/list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// MCP tool call result.
#[derive(Debug)]
pub struct ToolResult {
    pub content: Vec<ContentBlock>,
    pub is_error: bool,
}

/// MCP content blocks supported by this minimal client.
#[derive(Debug)]
pub enum ContentBlock {
    Text(String),
    Image { data: Vec<u8>, mime_type: String },
    ResourceLink(String),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcNotification {
    jsonrpc: String,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ToolsListResult {
    #[serde(default)]
    tools: Vec<WireToolDef>,
}

#[derive(Debug, Deserialize)]
struct WireToolDef {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(rename = "inputSchema", default)]
    input_schema: Value,
}

async fn reader_loop(stdout: ChildStdout, pending: PendingMap) {
    let mut lines = BufReader::new(stdout).lines();

    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                dispatch_line(&pending, &line).await;
            }
            Ok(None) => {
                fail_pending(&pending, "MCP server stdout closed").await;
                return;
            }
            Err(err) => {
                let message = format!("failed to read MCP server stdout: {err:#}");
                fail_pending(&pending, &message).await;
                return;
            }
        }
    }
}

async fn dispatch_line(pending: &PendingMap, line: &str) {
    let value = match serde_json::from_str::<Value>(line) {
        Ok(value) => value,
        Err(err) => {
            warn!("ignoring invalid JSON-RPC line from MCP server: {err:#}");
            return;
        }
    };

    let Some(id) = value.get("id").and_then(Value::as_u64) else {
        debug!("ignoring MCP notification or request from server: {value}");
        return;
    };

    let Some(sender) = pending.lock().await.remove(&id) else {
        warn!("received MCP response for unknown request id {id}");
        return;
    };

    let response = if let Some(error) = value.get("error") {
        Err(anyhow!("MCP request {id} failed: {error}"))
    } else if let Some(result) = value.get("result") {
        Ok(result.clone())
    } else {
        Err(anyhow!(
            "MCP response for request {id} did not contain result or error"
        ))
    };

    let _ = sender.send(response);
}

async fn fail_pending(pending: &PendingMap, message: &str) {
    let mut pending = pending.lock().await;
    for (_, sender) in pending.drain() {
        let _ = sender.send(Err(anyhow!(message.to_string())));
    }
}

fn decode_tool_result(value: Value) -> Result<ToolResult> {
    let is_error = value
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let content = value
        .get("content")
        .and_then(Value::as_array)
        .context("MCP tools/call response missing content array")?
        .iter()
        .cloned()
        .map(decode_content_block)
        .collect::<Result<Vec<_>>>()?;

    Ok(ToolResult { content, is_error })
}

fn decode_content_block(value: Value) -> Result<ContentBlock> {
    let block_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();

    match block_type {
        "text" => Ok(ContentBlock::Text(
            value
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        )),
        "image" => {
            let data = value
                .get("data")
                .and_then(Value::as_str)
                .context("MCP image content block missing data")?;
            let mime_type = value
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream")
                .to_string();
            Ok(ContentBlock::Image {
                data: decode_base64(data)?,
                mime_type,
            })
        }
        "resource_link" | "resourceLink" => Ok(ContentBlock::ResourceLink(
            value
                .get("uri")
                .or_else(|| value.get("url"))
                .and_then(Value::as_str)
                .context("MCP resource link content block missing uri")?
                .to_string(),
        )),
        "resource" => {
            let uri = value
                .get("resource")
                .and_then(|resource| resource.get("uri"))
                .or_else(|| value.get("uri"))
                .and_then(Value::as_str)
                .context("MCP resource content block missing uri")?;
            Ok(ContentBlock::ResourceLink(uri.to_string()))
        }
        other => Err(anyhow!("unsupported MCP content block type `{other}`")),
    }
}

fn decode_base64(input: &str) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    let mut buffer = [0_u8; 4];
    let mut buffer_len = 0;

    for byte in input.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        if byte == b'=' {
            buffer[buffer_len] = 64;
        } else {
            buffer[buffer_len] =
                base64_value(byte).ok_or_else(|| anyhow!("invalid base64 byte `{byte}`"))?;
        }
        buffer_len += 1;

        if buffer_len == 4 {
            output.push((buffer[0] << 2) | (buffer[1] >> 4));
            if buffer[2] != 64 {
                output.push((buffer[1] << 4) | (buffer[2] >> 2));
            }
            if buffer[3] != 64 {
                output.push((buffer[2] << 6) | buffer[3]);
            }
            buffer_len = 0;
        }
    }

    if buffer_len != 0 {
        return Err(anyhow!("invalid base64 length"));
    }

    Ok(output)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_rpc_request_round_trips() {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 7,
            method: "tools/list".to_string(),
            params: Some(json!({})),
        };

        let encoded = serde_json::to_string(&request).unwrap();
        let decoded: JsonRpcRequest = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn decodes_base64_image_payload() {
        assert_eq!(decode_base64("aGVsbG8=").unwrap(), b"hello");
    }
}
