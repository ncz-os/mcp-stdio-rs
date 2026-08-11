#!/usr/bin/env python3
import json
import os
import sys
import time


mode = os.environ["MCP_STDIO_SMOKE_SERVER"]


def send(result, request_id):
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)


for line in sys.stdin:
    request = json.loads(line)
    method = request.get("method", "")
    request_id = request.get("id")

    if method == "initialize":
        if mode == "stall-initialize":
            continue
        send(
            {
                "protocolVersion": (
                    "2099-01-01" if mode == "unsupported-version" else "2024-11-05"
                ),
                "capabilities": {},
                "serverInfo": {"name": "synthetic-mcp-smoke-server", "version": "0.1.0"},
            },
            request_id,
        )
    elif method == "notifications/initialized":
        pass
    elif method == "tools/list" and mode == "malformed":
        print("{not valid JSON", flush=True)
    elif method == "tools/list" and mode == "stall-request":
        pass
    elif method == "tools/list":
        if "cursor" not in request.get("params", {}):
            send(
                {
                    "tools": [
                        {
                            "name": "echo",
                            "description": "Echoes the msg argument.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"msg": {"type": "string"}},
                                "required": ["msg"],
                            },
                        }
                    ],
                    "nextCursor": "page-2",
                },
                request_id,
            )
        else:
            assert request["params"]["cursor"] == "page-2"
            send(
                {
                    "tools": [
                        {
                            "name": "second-page-tool",
                            "inputSchema": {"type": "object"},
                        }
                    ]
                },
                request_id,
            )
    elif method == "tools/call":
        print(
            json.dumps(
                {"jsonrpc": "2.0", "id": "health-1", "method": "ping"}
            ),
            flush=True,
        )
        ping_response = json.loads(next(sys.stdin))
        assert ping_response["id"] == "health-1" and ping_response["result"] == {}
        text = request.get("params", {}).get("arguments", {}).get("msg", "")
        if text == "__pid__":
            text = str(os.getpid())
        send(
            {
                "content": [
                    {"type": "text", "text": text},
                    {
                        "type": "resource",
                        "resource": {
                            "uri": "mnemos://memory/1",
                            "mimeType": "text/plain",
                            "text": "remembered content",
                        },
                    },
                ],
                "isError": False,
            },
            request_id,
        )


delay = os.environ.get("MCP_STDIO_DELAY_EOF_MS")
if delay:
    time.sleep(int(delay) / 1000)
if mode == "ignore-eof":
    while True:
        time.sleep(30)
