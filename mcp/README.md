# ChronoQueue MCP Server

Integrate ChronoQueue with AI assistants via the Model Context Protocol (MCP).

## What is this?

The ChronoQueue MCP Server exposes ChronoQueue's task queue operations as AI-accessible tools through the Model Context Protocol. This allows AI assistants like Claude to:

- Create and manage task queues
- Post and retrieve messages
- Monitor queue states and statistics
- Create scheduled tasks
- Build distributed workflows

## Quick Start

### Installation

```bash
# From npm (once published)
npm install -g @chronoqueue/mcp-server

# Or run directly with npx
npx @chronoqueue/mcp-server
```

### Local Development

```bash
cd mcp
npm install
npm run build
npm start
```

### Claude Desktop Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%/Claude/claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "chronoqueue": {
      "command": "npx",
      "args": ["-y", "@chronoqueue/mcp-server"],
      "env": {
        "CHRONOQUEUE_ADDRESS": "localhost:9000",
        "CHRONOQUEUE_INSECURE": "true"
      }
    }
  }
}
```

### VS Code with GitHub Copilot

Add to `.vscode/mcp.json` in your workspace:

```json
{
  "github.copilot.chat.mcp": {
    "servers": {
      "chronoqueue": {
        "command": "node",
        "args": ["${workspaceFolder}/mcp/dist/index.js"],
        "env": {
          "CHRONOQUEUE_ADDRESS": "localhost:9000",
          "CHRONOQUEUE_INSECURE": "true"
        }
      }
    }
  }
}
```

**📖 [Complete VS Code Setup Guide](./VSCODE_SETUP.md)**

### Cursor IDE Configuration

Add to `.cursor/mcp.json` in your workspace:

```json
{
  "mcpServers": {
    "chronoqueue": {
      "command": "node",
      "args": ["./dist/index.js"],
      "cwd": "${workspaceFolder}/mcp",
      "env": {
        "CHRONOQUEUE_ADDRESS": "localhost:9000"
      }
    }
  }
}
```

## Available Tools

The MCP server exposes **13 tools** for interacting with ChronoQueue:

### Queue Management (4 tools)

#### `create_queue`

Create a new queue with configuration options.

```
Arguments:
- queue_name (required): Queue identifier
- queue_type: "simple" or "exclusive" (default: simple)
- lease_duration: Message lease time (e.g., "30s", "5m")
- max_attempts: Max retries before DLQ (default: 3)
- auto_create_dlq: Auto-create dead letter queue
- dlq_name: Custom DLQ name
```

#### `delete_queue`

Delete an existing queue.

```
Arguments:
- queue_name (required): Queue to delete
```

#### `list_queues`

List all queues with their configurations.

#### `get_queue_state`

Get real-time statistics for a queue.

```
Arguments:
- queue_name (required): Queue to inspect
```

### Message Operations (5 tools)

#### `post_message`

Post a message to a queue.

```
Arguments:
- queue_name (required): Target queue
- message_id (required): Unique identifier
- payload (required): JSON object
- priority: 1-10 (default: 5)
- lease_duration: Override default lease
- schema_id: Schema ID for validation (e.g., "user.profile.v1")
- schema_version: Schema version number for validation
```

#### `get_next_message`

Retrieve next message for processing (leases the message).

```
Arguments:
- queue_name (required): Queue to consume from
```

#### `peek_messages`

Preview messages without consuming them.

```
Arguments:
- queue_name (required): Queue to peek at
- limit: Number of messages (default: 10)
```

#### `acknowledge_message`

Acknowledge message processing completion.

```
Arguments:
- queue_name (required)
- message_id (required)
- status (required): "completed" or "errored"
- stream_entry_id (required): From get_next_message
```

#### `renew_message_lease`

Extend lease time for long-running processing.

```
Arguments:
- queue_name (required)
- message_id (required)
- stream_entry_id (required)
- lease_duration: New lease time
```

### Scheduling Tools (3 tools)

#### `create_schedule`

Create automated message posting schedule.

```
Arguments:
- schedule_id (required): Unique identifier
- queue_name (required): Target queue
- schedule_type (required): "cron" or "calendar"
- payload (required): Message to post
- cron_expression: For cron schedules (e.g., "*/5 * * * *")
- calendar_type: For calendar schedules (once/weekly/daily/business_days)
- times_of_day: Array of times (e.g., ["09:00", "17:00"])
- days_of_week: Array of days (1=Mon, 7=Sun)
- priority: Message priority (1-10)
- enabled: Active status (default: true)
```

#### `list_schedules`

List all configured schedules.

#### `delete_schedule`

Delete a schedule.

```
Arguments:
- schedule_id (required): Schedule to delete
```

### Schema Management (1 tool)

#### `register_schema`

Register a JSON schema for message validation.

```
Arguments:
- schema_id (required): Unique schema identifier (e.g., "user.profile.v1")
- name (required): Human-readable schema name
- content (required): JSON Schema content as a JSON string
- description: Schema description
- content_type: Schema type (default: "json-schema")
```

## Configuration

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `CHRONOQUEUE_ADDRESS` | ChronoQueue server address | `localhost:9000` |
| `CHRONOQUEUE_INSECURE` | Use insecure connection | `true` |
| `CHRONOQUEUE_CERT_PATH` | Path to client certificate | - |
| `CHRONOQUEUE_KEY_PATH` | Path to client key | - |
| `CHRONOQUEUE_CA_PATH` | Path to CA certificate | - |
| `CHRONOQUEUE_TIMEOUT` | Operation timeout | `30s` |

## Usage Examples

### Example 1: Task Queue for AI Agents

```
You: Create a queue called "agent-tasks" for coordinating AI agents

Claude uses: create_queue
Result: Queue created with default settings

You: Post a task to analyze a document

Claude uses: post_message with:
{
  "queue_name": "agent-tasks",
  "message_id": "task-001",
  "payload": {
    "type": "analyze_document",
    "document_url": "https://example.com/doc.pdf",
    "analysis_type": "summary"
  },
  "priority": 7
}

You: Check queue status

Claude uses: get_queue_state
Result: Shows 1 pending message
```

### Example 2: Scheduled Reports

```
You: Create a daily report schedule at 9 AM

Claude uses: create_schedule with:
{
  "schedule_id": "daily-report",
  "queue_name": "reports",
  "schedule_type": "calendar",
  "calendar_type": "daily",
  "times_of_day": ["09:00"],
  "payload": {
    "report_type": "daily_summary",
    "recipients": ["team@example.com"]
  }
}
```

### Example 3: Message Processing Workflow

```
You: Get the next task from agent-tasks

Claude uses: get_next_message
Result: Returns message with stream_entry_id

You: The task is complete

Claude uses: acknowledge_message with status "completed"
```

## Development

### Project Structure

```
mcp/
├── src/
│   ├── index.ts              # Entry point
│   ├── server.ts             # MCP server setup
│   ├── config.ts             # Configuration management
│   ├── chronoqueue-client.ts # gRPC client wrapper
│   ├── types/
│   │   └── chronoqueue.ts    # Type definitions
│   └── tools/
│       ├── index.ts          # Tool registry
│       └── handlers.ts       # Tool implementations
├── examples/
│   ├── claude-desktop-config.json
│   └── cursor-mcp-config.json
└── package.json
```

### Build & Test

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Start server
npm start

# Development mode (watch)
npm run dev

# Lint code
npm run lint

# Format code
npm run format

# Run tests
npm test

# Run tests with coverage
npm run test:coverage
```

### Testing

The MCP server includes comprehensive unit tests:

- **46 tests** covering all tools and functionality
- **Tool definitions** - Validates all 13 tools are properly registered
- **Tool handlers** - Tests queue, message, schedule, and schema operations
- **Client functionality** - Tests gRPC client and protobuf conversions

Run tests with:

```bash
npm test              # Run tests in watch mode
npm test -- --run     # Run tests once
npm run test:coverage # Run with coverage report
```

## Phase Status

- ✅ Phase 1: Project structure setup
- ✅ Phase 2: Core implementation
  - MCP server with SDK integration
  - gRPC client wrapper
  - TypeScript type definitions
  - Configuration management
- ✅ Phase 3: Tool implementation (13/13 tools complete)
  - ✅ Queue management tools (4)
  - ✅ Message operation tools (5)
  - ✅ Scheduling tools (3)
  - ✅ Schema management tools (1)
- ✅ Phase 4: Testing & validation
  - ✅ Unit tests (46 tests passing)
  - ✅ Integration tests
  - ✅ Tool definitions validated
- ✅ Phase 5: Documentation & examples
- ⏳ Phase 6: npm publishing

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to ChronoQueue server

**Solution**:

1. Verify ChronoQueue is running: `ps aux | grep chronoqueue`
2. Check address: `CHRONOQUEUE_ADDRESS=localhost:9000` (default ChronoQueue port)
3. For remote servers, set `CHRONOQUEUE_INSECURE=false` and provide certificates

### Proto Loading Errors

**Problem**: Cannot find proto files

**Solution**: Ensure proto files are symlinked or copied from main ChronoQueue repo:

```bash
cd mcp
ln -s ../proto proto
```

### MCP Connection Issues

**Problem**: Claude Desktop doesn't see the server

**Solution**:

1. Restart Claude Desktop after config changes
2. Check server logs in Claude's developer console
3. Verify npx can access the package: `npx @chronoqueue/mcp-server --help`

## Contributing

See main [ChronoQueue CONTRIBUTING.md](../CONTRIBUTING.md) for contribution guidelines.

## License

MIT - See [LICENSE](../LICENSE) for details.

## Links

- [ChronoQueue GitHub](https://github.com/adrien19/chronoqueue)
- [Model Context Protocol](https://modelcontextprotocol.io)
- [Claude Desktop](https://claude.ai/download)
