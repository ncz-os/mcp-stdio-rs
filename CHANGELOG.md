# Changelog

## Unreleased

- Bumped `tokio` lockfile entry from 1.52.1 to 1.54.1 to clear RUSTSEC-2026-0386
  (`AsyncFd` I/O-safety advisory). No source changes required.

## 0.1.0

- Initial minimal MCP stdio client library.
- Added JSON-RPC initialize, initialized notification, `tools/list`, `tools/call`, and shutdown handling.
- Added `mcp-stdio-cli` for listing and calling tools from a spawned MCP server.
