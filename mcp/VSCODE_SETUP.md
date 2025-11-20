# Using ChronoQueue MCP Server with VS Code

The ChronoQueue MCP server is now integrated with VS Code through GitHub Copilot Chat!

## Prerequisites

1. **VS Code**: Latest version with GitHub Copilot Chat extension
2. **ChronoQueue Server**: Running on `localhost:9000` (or configure your address)
3. **MCP Server**: Built and ready in `/mcp/dist/`

## Setup

The MCP server is already configured in `.vscode/mcp.json`:

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

## Quick Start

### 1. Start ChronoQueue Server

```bash
# Terminal 1: Start ChronoQueue
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev
```

### 2. (Optional) - Build MCP Server (if not already built)

```bash
# Terminal 2: Build MCP server
cd mcp
npm install
npm run build
```

### 3. Reload VS Code Window

Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac) and select:

- **"Developer: Reload Window"**

This will load the MCP configuration.

### 4. Open GitHub Copilot Chat

Press `Ctrl+Shift+I` (or `Cmd+Shift+I` on Mac) or click the chat icon in the sidebar.

### 5. Use ChronoQueue Tools

You can now ask Copilot to interact with ChronoQueue:

**Example prompts:**

```
Create a queue named "email-notifications"
```

```
Post a message to the email-notifications queue with priority 8
```

```
Show me all queues in the system
```

```
Get the state of the email-notifications queue
```

```
Peek at the next 5 messages in the email-notifications queue
```

```
Create a schedule to post a daily report at 9 AM
```

## Available Tools

When you ask Copilot to work with ChronoQueue, it has access to 13 tools:

### Queue Management

- `create_queue` - Create new queues
- `delete_queue` - Delete queues
- `list_queues` - List all queues
- `get_queue_state` - Get queue statistics

### Message Operations

- `post_message` - Post messages
- `get_next_message` - Retrieve messages for processing
- `peek_messages` - Preview messages without consuming
- `acknowledge_message` - Mark messages as processed
- `renew_message_lease` - Extend processing time

### Scheduling

- `create_schedule` - Set up recurring tasks
- `list_schedules` - View active schedules
- `delete_schedule` - Remove schedules

### Schema Management

- `register_schema` - Register JSON schemas for message validation

## Configuration Options

### Custom ChronoQueue Address

If your ChronoQueue server is on a different host/port, update `.vscode/mcp.json`:

```json
{
  "github.copilot.chat.mcp": {
    "servers": {
      "chronoqueue": {
        "command": "node",
        "args": ["${workspaceFolder}/mcp/dist/index.js"],
        "env": {
          "CHRONOQUEUE_ADDRESS": "your-host:your-port",
          "CHRONOQUEUE_INSECURE": "true"
        }
      }
    }
  }
}
```

### Using TLS/mTLS

For secure connections:

```json
{
  "github.copilot.chat.mcp": {
    "servers": {
      "chronoqueue": {
        "command": "node",
        "args": ["${workspaceFolder}/mcp/dist/index.js"],
        "env": {
          "CHRONOQUEUE_ADDRESS": "chronoqueue.example.com:443",
          "CHRONOQUEUE_INSECURE": "false",
          "CHRONOQUEUE_CERT_PATH": "/path/to/client.crt",
          "CHRONOQUEUE_KEY_PATH": "/path/to/client.key",
          "CHRONOQUEUE_CA_PATH": "/path/to/ca.crt"
        }
      }
    }
  }
}
```

### Multiple Environments

You can configure multiple ChronoQueue instances:

```json
{
  "github.copilot.chat.mcp": {
    "servers": {
      "chronoqueue-dev": {
        "command": "node",
        "args": ["${workspaceFolder}/mcp/dist/index.js"],
        "env": {
          "CHRONOQUEUE_ADDRESS": "localhost:9000",
          "CHRONOQUEUE_INSECURE": "true"
        }
      },
      "chronoqueue-prod": {
        "command": "node",
        "args": ["${workspaceFolder}/mcp/dist/index.js"],
        "env": {
          "CHRONOQUEUE_ADDRESS": "prod.example.com:443",
          "CHRONOQUEUE_INSECURE": "false",
          "CHRONOQUEUE_CERT_PATH": "/path/to/prod-client.crt",
          "CHRONOQUEUE_KEY_PATH": "/path/to/prod-client.key",
          "CHRONOQUEUE_CA_PATH": "/path/to/prod-ca.crt"
        }
      }
    }
  }
}
```

## Troubleshooting

### MCP Server Not Found

**Error**: "Cannot find module" or "Command not found"

**Solution**: Make sure the MCP server is built:

```bash
cd mcp && npm run build
```

### Connection Refused

**Error**: "gRPC connection refused" or "ECONNREFUSED"

**Solution**: Verify ChronoQueue server is running:

```bash
# Check if server is running (gRPC health check)
grpcurl -plaintext localhost:9000 list || echo "Server not running"

# Start the server
# From the repository root:
export REDIS_PASSWORD='mypassword' && make server-dev
```

### Tools Not Appearing

**Solution**:

1. Reload VS Code window: `Ctrl+Shift+P` → "Developer: Reload Window"
2. Check Output panel: `View` → `Output` → Select "GitHub Copilot Chat"
3. Verify settings.json syntax is valid JSON

### MCP Server Crashes

**Check logs**:

```bash
# Run MCP server directly to see errors
cd mcp
CHRONOQUEUE_ADDRESS=localhost:9000 node dist/index.js
```

## Example Workflow

Here's a complete example of using ChronoQueue through Copilot Chat:

```
You: Create a queue called "order-processing" with max 5 retry attempts

Copilot: [Uses create_queue tool]
✓ Queue created successfully
Queue: order-processing
Type: simple
Max Attempts: 5
Auto-create DLQ: Yes

You: Post a message to order-processing with order ID 12345

Copilot: [Uses post_message tool]
✓ Message posted successfully
Queue: order-processing
Message ID: order-12345
Priority: 5

You: Show me the queue state

Copilot: [Uses get_queue_state tool]
📊 Queue State: order-processing
Status: ACTIVE
Type: SIMPLE
Messages:
  • Pending:   1
  • Running:   0
  • Completed: 0
  • Errored:   0
```

## Advanced: Custom Prompts

You can be very specific with your requests:

```
Create a queue named "high-priority-tasks" with:
- Exclusive type for ordered processing
- 10 second lease duration
- 3 max retry attempts
- Custom DLQ name "critical-failures"
```

```
Post a message to the notifications queue with:
- Message ID: "user-signup-123"
- Payload: {"email": "user@example.com", "event": "signup"}
- Priority: 9 (high priority)
- Scheduled for 2 hours from now
```

## Benefits

Using ChronoQueue through Copilot Chat provides:

- **Natural language interface** - No need to remember exact API syntax
- **Context-aware suggestions** - Copilot understands your queue operations
- **Rapid prototyping** - Test queue operations without writing code
- **Documentation** - Ask Copilot about ChronoQueue features
- **Workflow automation** - Chain multiple operations in a conversation

## More Information

- [MCP Server Documentation](./README.md)
- [ChronoQueue Documentation](../README.md)
- [VS Code MCP Integration](https://code.visualstudio.com/docs/copilot/copilot-mcp)
