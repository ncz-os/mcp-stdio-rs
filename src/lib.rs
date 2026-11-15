use std::collections::{HashMap, HashSet, VecDeque};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout, timeout_at};
use tracing::{debug, warn};

type PendingMap = Arc<StdMutex<HashMap<u64, oneshot::Sender<Result<Value>>>>>;
type WriterSender = mpsc::Sender<WriteCommand>;
const MAX_JSON_RPC_LINE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TOOL_PAGES: usize = 100;
const MAX_TOOLS: usize = 10_000;
const WRITER_QUEUE_CAPACITY: usize = 32;
const SUPPORTED_PROTOCOL_VERSION: &str = "2024-11-05";
/// Default bound for captured child stderr (last bytes kept per client).
const DEFAULT_STDERR_CAP_BYTES: usize = 8 * 1024;
/// Hard ceiling for a caller-supplied stderr capture bound.
const MAX_STDERR_CAP_BYTES: usize = 1024 * 1024;
/// How long transport-closed errors wait for a final stderr drain before
/// attaching whatever tail has been captured so far.
const STDERR_EOF_WAIT: Duration = Duration::from_millis(250);
/// Marker prefix used when a captured stderr tail is attached to an error.
const STDERR_TAIL_MARKER: &str = "MCP server stderr (tail):";

/// Controls what happens to the child server process's stderr stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StderrPolicy {
    /// Child stderr is inherited by the parent process (historical behavior).
    /// Nothing is captured and errors carry no stderr tail.
    Inherit,
    /// Child stderr is piped and drained into a bounded ring buffer holding
    /// at most `max_bytes` of the most recent output.
    Capture {
        /// Maximum number of recent stderr bytes retained.
        max_bytes: usize,
    },
}

impl StderrPolicy {
    /// Capture the last 8 KiB of child stderr.
    pub fn capture() -> Self {
        Self::Capture {
            max_bytes: DEFAULT_STDERR_CAP_BYTES,
        }
    }

    /// Capture the last `max_bytes` of child stderr.
    ///
    /// Values above 1 MiB are clamped to 1 MiB.
    pub fn capture_with_max_bytes(max_bytes: usize) -> Self {
        Self::Capture {
            max_bytes: max_bytes.min(MAX_STDERR_CAP_BYTES),
        }
    }
}

impl Default for StderrPolicy {
    fn default() -> Self {
        Self::Inherit
    }
}

#[cfg(test)]
impl StderrPolicy {
    fn max_bytes_for_test(self) -> usize {
        match self {
            Self::Inherit => 0,
            Self::Capture { max_bytes } => max_bytes,
        }
    }
}

/// Bounded ring buffer holding the most recent stderr bytes plus an
/// EOF signal so error paths can wait for the final drain.
struct StderrState {
    buffer: StdMutex<StderrBuffer>,
    drained: tokio::sync::Notify,
}

struct StderrBuffer {
    chunks: VecDeque<u8>,
    len: usize,
    cap: usize,
    eof: bool,
}

impl StderrState {
    fn new(cap: usize) -> Self {
        Self {
            buffer: StdMutex::new(StderrBuffer {
                chunks: VecDeque::new(),
                len: 0,
                cap,
                eof: false,
            }),
            drained: tokio::sync::Notify::new(),
        }
    }

    fn push_bytes(&self, bytes: &[u8]) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // cap == 0 stores nothing but the caller keeps draining, so a
        // chatty child can never block on a full stderr pipe.
        for &byte in bytes {
            if buffer.cap == 0 {
                return;
            }
            if buffer.len == buffer.cap {
                buffer.chunks.pop_front();
            } else {
                buffer.len += 1;
            }
            buffer.chunks.push_back(byte);
        }
    }

    fn mark_eof(&self) {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        buffer.eof = true;
        self.drained.notify_waiters();
    }

    fn snapshot(&self) -> (Vec<u8>, bool) {
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            buffer.chunks.iter().copied().collect::<Vec<u8>>(),
            buffer.eof,
        )
    }

    fn tail_string(&self) -> String {
        let (bytes, _) = self.snapshot();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

type StderrTail = Arc<StderrState>;

/// Deadlines and stdio behavior used by [`McpStdioClient`].
#[derive(Debug, Clone, Copy)]
pub struct McpStdioClientOptions {
    /// Maximum time allowed for the initialize request and initialized notification.
    pub initialization_timeout: Duration,
    /// Maximum time allowed for a tools/call request or the complete paginated
    /// tools/list operation.
    pub request_timeout: Duration,
    /// Time allowed for graceful exit before SIGTERM, and again before a force-kill.
    pub shutdown_grace_period: Duration,
    /// What to do with the child server process's stderr stream.
    /// Defaults to [`StderrPolicy::Inherit`]; use
    /// [`StderrPolicy::capture`](StderrPolicy::capture) to retain the tail
    /// and have it appended to transport-closed errors.
    pub stderr: StderrPolicy,
}

impl Default for McpStdioClientOptions {
    fn default() -> Self {
        Self {
            initialization_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            // MNEMOS reserves up to five seconds to drain pending audit writes.
            shutdown_grace_period: Duration::from_secs(6),
            stderr: StderrPolicy::Inherit,
        }
    }
}

/// An MCP client connected to a child process over newline-delimited JSON-RPC.
pub struct McpStdioClient {
    child: Mutex<Child>,
    writer: WriterSender,
    pending: PendingMap,
    transport_open: Arc<AtomicBool>,
    writer_task: JoinHandle<()>,
    reader_task: JoinHandle<()>,
    stderr_task: Option<JoinHandle<()>>,
    stderr_tail: Option<StderrTail>,
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
            .kill_on_drop(true);
        match options.stderr {
            StderrPolicy::Inherit => {
                command.stderr(Stdio::inherit());
            }
            StderrPolicy::Capture { .. } => {
                command.stderr(Stdio::piped());
            }
        }

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

        let (stderr_tail, stderr_task) = match options.stderr {
            StderrPolicy::Inherit => (None, None),
            StderrPolicy::Capture { max_bytes } => {
                let stderr = child
                    .stderr
                    .take()
                    .context("child process stderr was not piped")?;
                let tail = Arc::new(StderrState::new(max_bytes));
                let task = tokio::spawn(stderr_drain_loop(stderr, Arc::clone(&tail)));
                (Some(tail), Some(task))
            }
        };

        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let transport_open = Arc::new(AtomicBool::new(true));
        let (writer, writer_receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
        let writer_task = tokio::spawn(writer_loop(
            stdin,
            writer_receiver,
            Arc::clone(&pending),
            Arc::clone(&transport_open),
            stderr_tail.clone(),
        ));
        let reader_task = tokio::spawn(reader_loop(
            stdout,
            Arc::clone(&pending),
            writer.downgrade(),
            Arc::clone(&transport_open),
            options.request_timeout,
            stderr_tail.clone(),
        ));

        let client = Self {
            child: Mutex::new(child),
            writer,
            pending,
            transport_open,
            writer_task,
            reader_task,
            stderr_task,
            stderr_tail,
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
                let err = with_stderr_tail(err, &client.stderr_tail).await;
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
            client.send_notification(
                "notifications/initialized",
                json!({}),
                options.initialization_timeout,
            ),
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
        timeout(self.options.request_timeout, self.list_tools_pages())
            .await
            .map_err(|_| {
                anyhow!(
                    "MCP tools/list pagination timed out after {:?}",
                    self.options.request_timeout
                )
            })?
    }

    async fn list_tools_pages(&self) -> Result<Vec<ToolDef>> {
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();

        for page in 1..=MAX_TOOL_PAGES {
            let params = match &cursor {
                Some(cursor) => json!({ "cursor": cursor }),
                None => json!({}),
            };
            let response = self.send_request("tools/list", params).await?;
            let result: ToolsListResult = serde_json::from_value(response)
                .context("failed to decode MCP tools/list response")?;

            if tools.len().saturating_add(result.tools.len()) > MAX_TOOLS {
                return Err(anyhow!(
                    "MCP tools/list response exceeded the {MAX_TOOLS}-tool limit"
                ));
            }
            tools.extend(result.tools.into_iter().map(|tool| ToolDef {
                name: tool.name,
                description: tool.description.unwrap_or_default(),
                input_schema: tool.input_schema,
            }));

            match result.next_cursor {
                Some(next_cursor) => {
                    if !seen_cursors.insert(next_cursor.clone()) {
                        return Err(anyhow!(
                            "MCP tools/list response repeated cursor `{next_cursor}`"
                        ));
                    }
                    if page == MAX_TOOL_PAGES {
                        return Err(anyhow!(
                            "MCP tools/list response exceeded the {MAX_TOOL_PAGES}-page limit"
                        ));
                    }
                    cursor = Some(next_cursor);
                }
                None => return Ok(tools),
            }
        }

        unreachable!("page loop either returns a result or rejects its final cursor")
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

    /// Return the captured child stderr tail (most recent bytes, lossy UTF-8).
    /// Returns an empty string unless [`StderrPolicy::Capture`] was selected.
    pub fn stderr_tail(&self) -> String {
        self.stderr_tail
            .as_ref()
            .map(|tail| tail.tail_string())
            .unwrap_or_default()
    }

    /// Close the transport and stop the child process, allowing graceful cleanup first.
    pub async fn shutdown(self) -> Result<()> {
        let Self {
            child,
            writer,
            pending: _,
            transport_open: _,
            writer_task,
            reader_task,
            stderr_task,
            stderr_tail: _,
            next_id: _,
            options,
        } = self;

        drop(writer);
        writer_task.abort();
        let _ = writer_task.await;

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

        // A descendant can keep the stdout pipe open after the direct child exits.
        // Once that child is reaped there is no more shutdown work for the reader,
        // so do not let an inherited descriptor make shutdown unbounded.
        reader_task.abort();
        match reader_task.await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(error).context("MCP stdout reader task failed to join"),
        }
        if let Some(stderr_task) = stderr_task {
            // `timeout` consumes the handle, so borrow an abort handle first;
            // on expiry abort the drain instead of leaking it with the pipe open.
            let abort = stderr_task.abort_handle();
            match timeout(options.shutdown_grace_period, stderr_task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => {
                    return Err(error).context("MCP stderr reader task failed to join");
                }
                Err(_) => {
                    // The child is already reaped above; do not leak the drain
                    // task holding the stderr pipe open.
                    abort.abort();
                }
            }
        }

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
        if !self.transport_open.load(Ordering::SeqCst) {
            let tail = collect_stderr_tail(&self.stderr_tail).await;
            return Err(with_tail_suffix(
                anyhow!("MCP server transport is closed"),
                &tail,
            ));
        }
        lock_pending(&self.pending).insert(id, sender);
        let _pending_guard = PendingRequestGuard::new(id, Arc::clone(&self.pending));

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params: Some(params),
        };

        let deadline = Instant::now() + request_timeout;
        self.write_json(&request, deadline).await?;
        let response = timeout_at(deadline, receiver).await.map_err(|_| {
            anyhow!("MCP request {id} ({method}) timed out after {request_timeout:?}")
        })?;
        match response {
            Ok(value) => value,
            Err(_) => {
                let tail = collect_stderr_tail(&self.stderr_tail).await;
                Err(with_tail_suffix(
                    anyhow!("MCP server closed before responding to request {id}"),
                    &tail,
                ))
            }
        }
    }

    async fn send_notification(
        &self,
        method: &str,
        params: Value,
        write_timeout: Duration,
    ) -> Result<()> {
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params: Some(params),
        };
        self.write_json(&notification, Instant::now() + write_timeout)
            .await
    }

    async fn write_json<T>(&self, message: &T, deadline: Instant) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        enqueue_json(&self.writer, message, deadline).await
    }
}

struct WriteCommand {
    line: Vec<u8>,
    deadline: Instant,
    completion: oneshot::Sender<Result<()>>,
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

/// Best-effort snapshot of the captured stderr tail. Waits briefly for the
/// drain task to signal EOF (child exit closes the pipe) so a server that
/// prints its reason and exits has its final bytes included; falls back to
/// whatever has been captured so far. Empty when `Inherit` is in effect.
async fn collect_stderr_tail(stderr_tail: &Option<StderrTail>) -> String {
    let Some(tail) = stderr_tail else {
        return String::new();
    };
    // Wait for the drain task to observe pipe EOF (child exit closes it),
    // bounded by STDERR_EOF_WAIT. Loop until the buffer stops changing so a
    // wake that races a final push cannot truncate the tail.
    let deadline = Instant::now() + STDERR_EOF_WAIT;
    let bytes: Vec<u8> = loop {
        let (snapshot, eof) = tail.snapshot();
        if eof {
            // One final snapshot: mark_eof holds no data, so a push racing
            // the EOF flag lands either before or inside this read.
            let (fresh, _) = tail.snapshot();
            break if fresh.len() >= snapshot.len() {
                fresh
            } else {
                snapshot
            };
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break snapshot;
        }
        let _ = timeout(remaining, tail.drained.notified()).await;
    };
    String::from_utf8_lossy(&bytes).into_owned()
}

fn with_tail_suffix(error: anyhow::Error, tail: &str) -> anyhow::Error {
    let trimmed = tail.trim();
    if trimmed.is_empty() {
        return error;
    }
    anyhow!("{error:#}\n{STDERR_TAIL_MARKER}\n{trimmed}")
}

async fn with_stderr_tail(error: anyhow::Error, stderr_tail: &Option<StderrTail>) -> anyhow::Error {
    let tail = collect_stderr_tail(stderr_tail).await;
    with_tail_suffix(error, &tail)
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
    #[serde(rename = "inputSchema", deserialize_with = "deserialize_object")]
    input_schema: Value,
}

fn deserialize_object<'de, D>(deserializer: D) -> std::result::Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    if value.is_object() {
        Ok(value)
    } else {
        Err(serde::de::Error::custom("expected an object"))
    }
}

/// Drain a captured child stderr pipe into the bounded tail buffer.
/// Runs until EOF (child exit closes the pipe); never fails the client.
async fn stderr_drain_loop(mut stderr: ChildStderr, tail: StderrTail) {
    let mut chunk = vec![0u8; 4096];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => tail.push_bytes(&chunk[..n]),
            Err(_) => break,
        }
    }
    tail.mark_eof();
}

async fn writer_loop(
    mut stdin: ChildStdin,
    mut receiver: mpsc::Receiver<WriteCommand>,
    pending: PendingMap,
    transport_open: Arc<AtomicBool>,
    stderr_tail: Option<StderrTail>,
) {
    while let Some(command) = receiver.recv().await {
        let result = timeout_at(command.deadline, async {
            stdin
                .write_all(&command.line)
                .await
                .context("failed to write JSON-RPC message to MCP server stdin")?;
            stdin
                .flush()
                .await
                .context("failed to flush MCP server stdin")
        })
        .await;

        let error = match result {
            Ok(Ok(())) => {
                let _ = command.completion.send(Ok(()));
                continue;
            }
            Ok(Err(error)) => format!("MCP transport writer failed: {error:#}"),
            Err(_) => "MCP transport writer timed out while writing a frame".to_string(),
        };

        let error = with_tail_suffix(anyhow!(error), &collect_stderr_tail(&stderr_tail).await);
        transport_open.store(false, Ordering::SeqCst);
        let _ = command.completion.send(Err(anyhow!("{error:#}")));
        fail_pending(&pending, &format!("{error:#}")).await;
        return;
    }

    transport_open.store(false, Ordering::SeqCst);
    let tail = collect_stderr_tail(&stderr_tail).await;
    let closed = format!(
        "{:#}",
        with_tail_suffix(anyhow!("MCP transport writer closed"), &tail)
    );
    fail_pending(&pending, &closed).await;
}

async fn reader_loop(
    stdout: ChildStdout,
    pending: PendingMap,
    writer: mpsc::WeakSender<WriteCommand>,
    transport_open: Arc<AtomicBool>,
    write_timeout: Duration,
    stderr_tail: Option<StderrTail>,
) {
    let mut reader = BufReader::new(stdout);
    let mut line = Vec::new();

    loop {
        match read_capped_line(&mut reader, &mut line).await {
            Ok(Some(line)) => {
                if line.trim().is_empty() {
                    continue;
                }
                if !dispatch_line(&pending, &writer, &transport_open, write_timeout, &line).await {
                    return;
                }
            }
            Ok(None) => {
                transport_open.store(false, Ordering::SeqCst);
                let tail = collect_stderr_tail(&stderr_tail).await;
                let message = format!(
                    "{:#}",
                    with_tail_suffix(anyhow!("MCP server stdout closed"), &tail)
                );
                fail_pending(&pending, &message).await;
                return;
            }
            Err(err) => {
                transport_open.store(false, Ordering::SeqCst);
                let tail = collect_stderr_tail(&stderr_tail).await;
                let message = format!(
                    "{:#}",
                    with_tail_suffix(anyhow!("failed to read MCP server stdout: {err:#}"), &tail)
                );
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
    writer: &mpsc::WeakSender<WriteCommand>,
    transport_open: &AtomicBool,
    write_timeout: Duration,
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
            let Some(writer) = writer.upgrade() else {
                transport_open.store(false, Ordering::SeqCst);
                fail_pending(pending, "MCP transport closed while replying to ping").await;
                return false;
            };
            let response = json!({ "jsonrpc": "2.0", "id": id, "result": {} });
            if let Err(err) = enqueue_json(&writer, &response, Instant::now() + write_timeout).await
            {
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

async fn enqueue_json<T>(writer: &WriterSender, message: &T, deadline: Instant) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let mut line = serde_json::to_vec(message).context("failed to encode JSON-RPC message")?;
    line.push(b'\n');
    let (completion, written) = oneshot::channel();
    let command = WriteCommand {
        line,
        deadline,
        completion,
    };

    timeout_at(deadline, writer.send(command))
        .await
        .map_err(|_| anyhow!("MCP transport writer queue timed out"))?
        .map_err(|_| anyhow!("MCP transport writer is closed"))?;
    written
        .await
        .map_err(|_| anyhow!("MCP transport writer closed before completing a frame"))?
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
    let is_error = match value.get("isError") {
        Some(is_error) => is_error
            .as_bool()
            .context("MCP tools/call response isError must be a boolean")?,
        None => false,
    };
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
        .context("MCP content block missing type")?;

    match block_type {
        "text" => Ok(ContentBlock::Text(
            value
                .get("text")
                .and_then(Value::as_str)
                .context("MCP text content block missing text")?
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
                .context("MCP image content block missing mimeType")?
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
    if input.len() % 4 != 0 {
        return Err(anyhow!("invalid base64 length"));
    }

    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    let quartet_count = input.len() / 4;

    for (index, quartet) in input.as_bytes().chunks_exact(4).enumerate() {
        let final_quartet = index + 1 == quartet_count;
        let first = base64_value(quartet[0])
            .ok_or_else(|| anyhow!("invalid base64 byte `{}`", quartet[0]))?;
        let second = base64_value(quartet[1])
            .ok_or_else(|| anyhow!("invalid base64 byte `{}`", quartet[1]))?;

        output.push((first << 2) | (second >> 4));

        if quartet[2] == b'=' {
            if !final_quartet || quartet[3] != b'=' || second & 0x0f != 0 {
                return Err(anyhow!("invalid base64 padding"));
            }
            continue;
        }

        let third = base64_value(quartet[2])
            .ok_or_else(|| anyhow!("invalid base64 byte `{}`", quartet[2]))?;
        output.push((second << 4) | (third >> 2));

        if quartet[3] == b'=' {
            if !final_quartet || third & 0x03 != 0 {
                return Err(anyhow!("invalid base64 padding"));
            }
            continue;
        }

        let fourth = base64_value(quartet[3])
            .ok_or_else(|| anyhow!("invalid base64 byte `{}`", quartet[3]))?;
        output.push((third << 6) | fourth);
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
        assert_eq!(decode_base64("aA==").unwrap(), b"h");
        assert_eq!(
            decode_base64("AAECAwQF").unwrap(),
            b"\0\x01\x02\x03\x04\x05"
        );
    }

    #[test]
    fn rejects_invalid_base64_padding_and_trailing_bits() {
        for invalid in [
            "====",
            "=AAA",
            "AA=A",
            "A===",
            "aA==AAAA",
            "aGVsbG8=AAAA",
            "AB==",
            "AAB=",
            "aGVs\nbG8=",
        ] {
            assert!(
                decode_base64(invalid).is_err(),
                "invalid base64 was accepted: {invalid:?}"
            );
        }
    }

    #[test]
    fn rejects_missing_mandatory_tools_list_fields() {
        let missing_tools =
            serde_json::from_value::<ToolsListResult>(json!({})).expect_err("tools is mandatory");
        assert!(missing_tools.to_string().contains("tools"));

        let missing_schema = serde_json::from_value::<ToolsListResult>(json!({
            "tools": [{"name": "missing-schema"}]
        }))
        .expect_err("inputSchema is mandatory");
        assert!(missing_schema.to_string().contains("inputSchema"));

        serde_json::from_value::<ToolsListResult>(json!({"tools": {}}))
            .expect_err("tools must be an array");
        serde_json::from_value::<ToolsListResult>(json!({
            "tools": [{"name": "wrong-schema", "inputSchema": 7}]
        }))
        .expect_err("inputSchema must be an object");
    }

    #[test]
    fn rejects_missing_mandatory_content_fields() {
        let text = decode_content_block(json!({"type": "text"})).expect_err("text is mandatory");
        assert!(text.to_string().contains("missing text"));

        let image = decode_content_block(json!({"type": "image", "data": "aGVsbG8="}))
            .expect_err("mimeType is mandatory");
        assert!(image.to_string().contains("missing mimeType"));

        decode_content_block(json!({"type": "text", "text": 7}))
            .expect_err("text must be a string");

        decode_tool_result(json!({"content": [], "isError": "false"}))
            .expect_err("isError must be a boolean when present");
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
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let pending = Arc::new(StdMutex::new(HashMap::new()));
        let transport_open = Arc::new(AtomicBool::new(true));
        let (writer, writer_receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
        let writer_task = tokio::spawn(writer_loop(
            stdin,
            writer_receiver,
            Arc::clone(&pending),
            Arc::clone(&transport_open),
            None,
        ));
        let reader_task = tokio::spawn(reader_loop(
            stdout,
            Arc::clone(&pending),
            writer.downgrade(),
            Arc::clone(&transport_open),
            Duration::from_secs(30),
            None,
        ));
        let client = McpStdioClient {
            child: Mutex::new(child),
            writer,
            pending: Arc::clone(&pending),
            transport_open,
            writer_task,
            reader_task,
            stderr_task: None,
            stderr_tail: None,
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

    #[test]
    fn stderr_ring_keeps_most_recent_bytes_and_clamps() {
        let tail = StderrState::new(8);
        tail.push_bytes(b"hello world");
        assert_eq!(tail.tail_string(), "lo world");

        assert_eq!(
            StderrPolicy::capture().max_bytes_for_test(),
            DEFAULT_STDERR_CAP_BYTES
        );
        assert_eq!(
            StderrPolicy::capture_with_max_bytes(usize::MAX).max_bytes_for_test(),
            MAX_STDERR_CAP_BYTES
        );

        // cap == 0 discards without growing the buffer.
        let empty = StderrState::new(0);
        empty.push_bytes(b"discarded");
        assert_eq!(empty.tail_string(), "");
        // Invalid UTF-8 renders lossy rather than failing.
        empty.mark_eof();
        let utf = StderrState::new(4);
        utf.push_bytes(&[0xff, 0xfe, b'a', b'b']);
        assert!(utf.tail_string().contains('a'));

        let (bytes, eof) = empty.snapshot();
        assert!(bytes.is_empty() && eof);
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
