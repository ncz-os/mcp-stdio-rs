use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, warn};

type PendingMap = Arc<StdMutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>;
type SharedStdin = Arc<Mutex<ChildStdin>>;
const MAX_JSON_RPC_LINE_BYTES: usize = 8 * 1024 * 1024;
const SUPPORTED_PROTOCOL_VERSION: &str = "2024-11-05";

/// Deadlines used by [`McpStdioClient`].
#[derive(Debug, Clone, Copy)]
pub struct McpStdioClientOptions {
    /// Maximum time allowed for the initialize request and initialized notification.
    pub initialization_timeout: Duration,
    /// Maximum time allowed for each tools/list or tools/call request.
    pub request_timeout: Duration,
    /// Time allowed for graceful exit before SIGTERM, and again before a force-kill.
    pub shutdown_grace_period: Duration,
}

impl Default for McpStdioClientOptions {
    fn default() -> Self {
        Self {
            initialization_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            // MNEMOS reserves up to five seconds to drain pending audit writes.
            shutdown_grace_period: Duration::from_secs(6),
        }
    }
}

/// An MCP client connected to a child process over newline-delimited JSON-RPC.
pub struct McpStdioClient {
    child: Mutex<Child>,
    stdin: SharedStdin,
    pending: PendingMap,
    transport_open: Arc<AtomicBool>,
    reader_task: JoinHandle<()>,
    next_id: AtomicU64,
    options: McpStdioClientOptions,
}

impl McpStdioClient {
    /// Spawn a child process as the MCP server, complete the MCP initialize handshake,
    /// return a ready client. cmd is the executable, args are its arguments, env is
    /// additional environment key-value pairs to inject.
    pub async fn spawn(cmd: &str, args: &[&str], env: &[(&str, &str)]) -> Result<Self> {
        Self::spawn_with_options(cmd, args, env, McpStdioClientOptions::default()).await
    }

    /// Spawn and initialize an MCP server using explicit request and shutdown deadlines.
    pub async fn spawn_with_options(
        cmd: &str,
        args: &[&str],
        env: &[(&str, &str)],
        options: McpStdioClientOptions,
    ) -> Result<Self> {
        let mut command = Command::new(cmd);
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

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

        let stdin = Arc::new(Mutex::new(stdin));
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let transport_open = Arc::new(AtomicBool::new(true));
        let reader_task = tokio::spawn(reader_loop(
            stdout,
            Arc::clone(&pending),
            Arc::downgrade(&stdin),
            Arc::clone(&transport_open),
        ));

        let client = Self {
            child: Mutex::new(child),
            stdin,
            pending,
            transport_open,
            reader_task,
            next_id: AtomicU64::new(1),
            options,
        };

        let initialize_params = json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "mcp-stdio-rs",
                "version": "0.1.0"
            }
        });

        let initialize_response = match client
            .send_request_with_id(
                0,
                "initialize",
                initialize_params,
                options.initialization_timeout,
            )
            .await
            .context("MCP initialize request failed")
        {
            Ok(response) => response,
            Err(err) => {
                let _ = client.shutdown().await;
                return Err(err);
            }
        };

        let initialize_result = serde_json::from_value::<InitializeResult>(initialize_response)
            .context("failed to decode MCP initialize response");
        let initialize_result = match initialize_result {
            Ok(result) if result.protocol_version == SUPPORTED_PROTOCOL_VERSION => result,
            Ok(result) => {
                let version = result.protocol_version;
                let _ = client.shutdown().await;
                return Err(anyhow!(
                    "MCP server selected unsupported protocol version `{version}`"
                ));
            }
            Err(err) => {
                let _ = client.shutdown().await;
                return Err(err);
            }
        };
        if !initialize_result.capabilities.is_object() {
            let _ = client.shutdown().await;
            return Err(anyhow!(
                "MCP initialize response capabilities must be an object"
            ));
        }

        let initialized = timeout(
            options.initialization_timeout,
            client.send_notification("notifications/initialized", json!({})),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "MCP initialized notification timed out after {:?}",
                options.initialization_timeout
            )
        })
        .and_then(|result| result)
        .context("failed to send MCP initialized notification");
        if let Err(err) = initialized {
            let _ = client.shutdown().await;
            return Err(err);
        }

        Ok(client)
    }

    /// List available MCP tools from the server.
    pub async fn list_tools(&self) -> Result<Vec<ToolDef>> {
        let mut tools = Vec::new();
        let mut cursor = None;

        loop {
            let params = match &cursor {
                Some(cursor) => json!({ "cursor": cursor }),
                None => json!({}),
            };
            let response = self.send_request("tools/list", params).await?;
            let result: ToolsListResult = serde_json::from_value(response)
                .context("failed to decode MCP tools/list response")?;

            tools.extend(result.tools.into_iter().map(|tool| ToolDef {
                name: tool.name,
                description: tool.description.unwrap_or_default(),
                input_schema: tool.input_schema,
            }));

            match result.next_cursor {
                Some(next_cursor) => cursor = Some(next_cursor),
                None => return Ok(tools),
            }
        }
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

    /// Close the transport and stop the child process, allowing graceful cleanup first.
    pub async fn shutdown(self) -> Result<()> {
        let Self {
            child,
            stdin,
            pending: _,
            transport_open: _,
            reader_task,
            next_id: _,
            options,
        } = self;

        drop(stdin);

        let mut child = child.into_inner();
        if !wait_for_child(&mut child, options.shutdown_grace_period).await? {
            request_termination(&mut child)?;
            if !wait_for_child(&mut child, options.shutdown_grace_period).await? {
                child
                    .start_kill()
                    .context("failed to force-kill MCP server child")?;
                child
                    .wait()
                    .await
                    .context("failed to wait for force-killed MCP server child")?;
            }
        }

        reader_task
            .await
            .context("MCP stdout reader task failed to join")?;

        Ok(())
    }

    async fn send_request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.send_request_with_id(id, method, params, self.options.request_timeout)
            .await
    }

    async fn send_request_with_id(
        &self,
        id: u64,
        method: &str,
        params: Value,
        request_timeout: Duration,
    ) -> Result<Value> {
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = lock_pending(&self.pending);
            if !self.transport_open.load(Ordering::SeqCst) {
                return Err(anyhow!("MCP server transport is closed"));
            }
            pending.insert(id, sender);
        }
        let _pending_guard = PendingRequestGuard::new(id, Arc::clone(&self.pending));

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params: Some(params),
        };

        timeout(request_timeout, async {
            self.write_json(&request).await?;
            receiver
                .await
                .map_err(|_| anyhow!("MCP server closed before responding to request {id}"))?
        })
        .await
        .map_err(|_| anyhow!("MCP request {id} ({method}) timed out after {request_timeout:?}"))?
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
        write_json(&self.stdin, message).await
    }
}

struct PendingRequestGuard {
    id: u64,
    pending: PendingMap,
}

impl PendingRequestGuard {
    fn new(id: u64, pending: PendingMap) -> Self {
        Self { id, pending }
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        lock_pending(&self.pending).remove(&self.id);
    }
}

fn lock_pending(
    pending: &PendingMap,
) -> std::sync::MutexGuard<'_, HashMap<u64, oneshot::Sender<Result<Value>>>> {
    pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    Image {
        data: Vec<u8>,
        mime_type: String,
    },
    ResourceLink(String),
    EmbeddedResource {
        uri: String,
        mime_type: Option<String>,
        text: Option<String>,
        blob: Option<Vec<u8>>,
    },
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
    #[serde(rename = "nextCursor")]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    protocol_version: String,
    capabilities: Value,
    #[serde(rename = "serverInfo")]
    _server_info: ServerInfo,
}

#[derive(Debug, Deserialize)]
struct ServerInfo {
    #[serde(rename = "name")]
    _name: String,
    #[serde(rename = "version")]
    _version: String,
}

#[derive(Debug, Deserialize)]
struct WireToolDef {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(rename = "inputSchema", default)]
    input_schema: Value,
}

async fn reader_loop(
    stdout: ChildStdout,
    pending: PendingMap,
    stdin: std::sync::Weak<Mutex<ChildStdin>>,
    transport_open: Arc<AtomicBool>,
) {
    let mut reader = BufReader::new(stdout);
    let mut line = Vec::new();

    loop {
        match read_capped_line(&mut reader, &mut line).await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                if !dispatch_line(&pending, &stdin, &transport_open, &line).await {
                    return;
                }
            }
            Ok(None) => {
                transport_open.store(false, Ordering::SeqCst);
                fail_pending(&pending, "MCP server stdout closed").await;
                return;
            }
            Err(err) => {
                transport_open.store(false, Ordering::SeqCst);
                let message = format!("failed to read MCP server stdout: {err:#}");
                fail_pending(&pending, &message).await;
                return;
            }
        }
    }
}

async fn read_capped_line<R>(reader: &mut R, buffer: &mut Vec<u8>) -> Result<Option<String>>
where
    R: AsyncBufRead + Unpin,
{
    buffer.clear();

    loop {
        let available = reader
            .fill_buf()
            .await
            .context("failed to read MCP server stdout")?;
        if available.is_empty() {
            if buffer.is_empty() {
                return Ok(None);
            }
            return Err(anyhow!("MCP server stdout closed mid-frame"));
        }

        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if buffer.len() + take > MAX_JSON_RPC_LINE_BYTES {
            return Err(anyhow!(
                "MCP server stdout line exceeded {MAX_JSON_RPC_LINE_BYTES} bytes"
            ));
        }

        buffer.extend_from_slice(&available[..take]);
        reader.consume(take);

        if buffer.last() == Some(&b'\n') {
            buffer.pop();
            if buffer.last() == Some(&b'\r') {
                buffer.pop();
            }
            let line = std::str::from_utf8(buffer)
                .context("MCP server stdout line was not valid UTF-8")?
                .to_string();
            return Ok(Some(line));
        }
    }
}

async fn dispatch_line(
    pending: &PendingMap,
    stdin: &std::sync::Weak<Mutex<ChildStdin>>,
    transport_open: &AtomicBool,
    line: &str,
) -> bool {
    let value = match serde_json::from_str::<Value>(line) {
        Ok(value) => value,
        Err(err) => {
            let message = format!("invalid JSON-RPC line from MCP server: {err:#}");
            warn!("{message}");
            transport_open.store(false, Ordering::SeqCst);
            fail_pending(pending, &message).await;
            return false;
        }
    };

    if let Some(method) = value.get("method").and_then(Value::as_str) {
        if method == "ping" {
            let Some(id) = value.get("id") else {
                debug!("ignoring MCP ping notification without an id");
                return true;
            };
            let Some(stdin) = stdin.upgrade() else {
                transport_open.store(false, Ordering::SeqCst);
                fail_pending(pending, "MCP transport closed while replying to ping").await;
                return false;
            };
            let response = json!({ "jsonrpc": "2.0", "id": id, "result": {} });
            if let Err(err) = write_json(&stdin, &response).await {
                let message = format!("failed to reply to MCP ping: {err:#}");
                transport_open.store(false, Ordering::SeqCst);
                fail_pending(pending, &message).await;
                return false;
            }
        } else {
            debug!("ignoring unsupported MCP server request or notification: {value}");
        }
        return true;
    }

    let Some(id) = value.get("id").and_then(Value::as_u64) else {
        debug!("ignoring MCP message without a numeric response id: {value}");
        return true;
    };

    let Some(sender) = lock_pending(pending).remove(&id) else {
        warn!("received MCP response for unknown request id {id}");
        return true;
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
    true
}

async fn write_json<T>(stdin: &Mutex<ChildStdin>, message: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let mut line = serde_json::to_vec(message).context("failed to encode JSON-RPC message")?;
    line.push(b'\n');

    let mut stdin = stdin.lock().await;
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

async fn wait_for_child(child: &mut Child, duration: Duration) -> Result<bool> {
    match timeout(duration, child.wait()).await {
        Ok(status) => {
            status.context("failed to wait for MCP server child")?;
            Ok(true)
        }
        Err(_) => Ok(false),
    }
}

#[cfg(unix)]
fn request_termination(child: &mut Child) -> Result<()> {
    let pid = child.id().context("MCP server child had no process id")?;
    let pid = i32::try_from(pid).context("MCP server child process id exceeded i32")?;
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    // SIGTERM gives the server one final bounded opportunity to clean up.
    if unsafe { kill(pid, 15) } == -1 {
        let error = std::io::Error::last_os_error();
        if child.try_wait()?.is_none() {
            return Err(error).context("failed to terminate MCP server child");
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn request_termination(child: &mut Child) -> Result<()> {
    child
        .start_kill()
        .context("failed to terminate MCP server child")
}

async fn fail_pending(pending: &PendingMap, message: &str) {
    let mut pending = lock_pending(pending);
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
            let resource = value
                .get("resource")
                .context("MCP resource content block missing resource")?;
            let uri = resource
                .get("uri")
                .and_then(Value::as_str)
                .context("MCP resource content block missing uri")?;
            let mime_type = resource
                .get("mimeType")
                .and_then(Value::as_str)
                .map(str::to_string);
            let text = resource
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string);
            let blob = resource
                .get("blob")
                .and_then(Value::as_str)
                .map(decode_base64)
                .transpose()?;
            if text.is_none() && blob.is_none() {
                return Err(anyhow!(
                    "MCP embedded resource content block missing text or blob"
                ));
            }
            Ok(ContentBlock::EmbeddedResource {
                uri: uri.to_string(),
                mime_type,
                text,
                blob,
            })
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

    #[test]
    fn default_deadlines_cover_mnemos_audit_drain() {
        let options = McpStdioClientOptions::default();

        assert_eq!(options.initialization_timeout, Duration::from_secs(30));
        assert_eq!(options.request_timeout, Duration::from_secs(30));
        assert_eq!(options.shutdown_grace_period, Duration::from_secs(6));
    }

    #[tokio::test]
    async fn cancelling_request_removes_pending_entry() {
        let mut command = Command::new("sh");
        command
            .args(["-c", "exec cat"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().expect("spawn silent child");
        let stdin = Arc::new(Mutex::new(child.stdin.take().expect("piped stdin")));
        let stdout = child.stdout.take().expect("piped stdout");
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let transport_open = Arc::new(AtomicBool::new(true));
        let reader_task = tokio::spawn(reader_loop(
            stdout,
            Arc::clone(&pending),
            Arc::downgrade(&stdin),
            Arc::clone(&transport_open),
        ));
        let client = McpStdioClient {
            child: Mutex::new(child),
            stdin,
            pending: Arc::clone(&pending),
            transport_open,
            reader_task,
            next_id: AtomicU64::new(1),
            options: McpStdioClientOptions {
                shutdown_grace_period: Duration::from_millis(25),
                ..McpStdioClientOptions::default()
            },
        };

        let cancelled = timeout(
            Duration::from_millis(25),
            client.send_request_with_id(7, "tools/list", json!({}), Duration::from_secs(30)),
        )
        .await;

        assert!(cancelled.is_err(), "request should be externally cancelled");
        assert!(lock_pending(&pending).is_empty());
        client.shutdown().await.expect("shutdown silent child");
    }

    #[test]
    fn preserves_embedded_resource_payloads() {
        let text = decode_content_block(json!({
            "type": "resource",
            "resource": {
                "uri": "mnemos://memory/text",
                "mimeType": "text/plain",
                "text": "remembered content"
            }
        }))
        .unwrap();
        assert!(matches!(
            text,
            ContentBlock::EmbeddedResource {
                uri,
                mime_type: Some(mime_type),
                text: Some(text),
                blob: None,
            } if uri == "mnemos://memory/text"
                && mime_type == "text/plain"
                && text == "remembered content"
        ));

        let blob = decode_content_block(json!({
            "type": "resource",
            "resource": {
                "uri": "mnemos://memory/blob",
                "blob": "aGVsbG8="
            }
        }))
        .unwrap();
        assert!(matches!(
            blob,
            ContentBlock::EmbeddedResource {
                uri,
                mime_type: None,
                text: None,
                blob: Some(blob),
            } if uri == "mnemos://memory/blob" && blob.as_slice() == b"hello"
        ));
    }

    #[tokio::test]
    async fn capped_reader_accepts_normal_line() {
        let mut reader = BufReader::new(b"{\"jsonrpc\":\"2.0\"}\n".as_slice());
        let mut buffer = Vec::new();

        let line = read_capped_line(&mut reader, &mut buffer).await.unwrap();

        assert_eq!(line.as_deref(), Some(r#"{"jsonrpc":"2.0"}"#));
    }

    #[tokio::test]
    async fn capped_reader_rejects_oversized_line() {
        let input = vec![b'x'; MAX_JSON_RPC_LINE_BYTES + 1];
        let mut reader = BufReader::new(input.as_slice());
        let mut buffer = Vec::new();

        let error = read_capped_line(&mut reader, &mut buffer)
            .await
            .expect_err("oversized line should fail")
            .to_string();

        assert!(error.contains("exceeded"));
    }
}
