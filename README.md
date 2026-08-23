# ChronoQueue

[![CI](https://github.com/adrien19/chronoqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/adrien19/chronoqueue/actions/workflows/ci.yml)
[![Release](https://github.com/adrien19/chronoqueue/actions/workflows/release.yml/badge.svg)](https://github.com/adrien19/chronoqueue/actions/workflows/release.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/adrien19/chronoqueue)](https://goreportcard.com/report/github.com/adrien19/chronoqueue)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

ChronoQueue is a persistent job queue and execution-supervision service. It provides priority-based message processing, leases and heartbeats, retries and dead-letter queues, and recurring or delayed scheduling through gRPC and HTTP APIs.

---

> **Project status**
>
> ChronoQueue is actively developed. Core queue workflows are implemented and covered by unit, integration, and end-to-end tests. Review the documented limitations and validate ChronoQueue against your workload and operational requirements before deployment.

---

## Features

The capabilities below are available in the current release. Items marked **Evolving** or **Partial** identify areas still under active development.

| Capability | Status | Current scope |
| --- | --- | --- |
| Queue and message lifecycle | ✅ Implemented | Create, list, and delete queues; post, bulk-post, claim, peek, acknowledge, and cancel messages. |
| Priority processing | ✅ Implemented | Numeric priorities with FIFO ordering at the same priority, plus configurable strict, weighted, and age-boosted selection. |
| Execution supervision | ✅ Implemented | Server-owned leases, attempt IDs, heartbeats, lease renewal, timeout reclaim, and stale-worker protection. |
| Retries and dead-letter queues | ✅ Implemented | Configurable attempt limits, lease-timeout retries, automatic DLQ creation, inspection, requeue, delete, purge, and statistics. |
| Scheduling | 🧪 Evolving | Delayed messages and recurring cron or timezone-aware calendar schedules, including validation, previews, history, pause, and resume. Custom expression rules are reserved and rejected by the v2 server. |
| Payload schemas | ✅ Implemented | Versioned JSON Schema registration, queue-level enforcement, validation, listing, and deletion. |
| Message retention | ✅ Implemented | Delete-on-ack, time-based retention, or indefinite retention with background cleanup. |
| Storage | ✅ Implemented | PostgreSQL and SQLite backends with schema migrations. PostgreSQL is the recommended backend; SQLite is intended for local and smaller deployments. |
| APIs and tooling | ✅ Implemented | gRPC API, REST gateway, embedded OpenAPI/Swagger UI, Go client, and `chronoq` CLI. |
| Monitoring | 🚧 Partial | Prometheus and Grafana provide operational metrics; the web dashboard provides queue state through live and polled views. |
| Web administration | 🚧 Partial | Queue/message inspection and creation, schedule creation/pause/resume/delete, schema management, and DLQ actions. Additional settings areas remain under development. |
| Operational security | 🧪 Evolving | API-key authentication, TLS/mTLS, payload encryption with local or Vault keys, per-principal rate limiting, and protected metrics. |
| Additional SDKs and MCP | ↗ External | The in-repository Go client is implemented. Python support remains work in progress; the TypeScript SDK and MCP server are maintained separately and are in early development. |

## Getting Started

### Prerequisites

- [PostgreSQL](https://www.postgresql.org/) or [SQLite](https://www.sqlite.org/) (for storage)
- [Go](https://go.dev/) (use the version declared in [`go.mod`](./go.mod))

### Installation

#### Quick Install (Recommended)

The fastest way to install ChronoQueue is using the installation script:

**Linux / macOS:**

```bash
# Install latest version to /usr/local/bin
curl -fsSL https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.sh | bash

# Install a specific version
curl -fsSL https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.sh | bash -s -- 1.2.1

# Install to a custom directory (no sudo required)
curl -fsSL https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.sh | CHRONOQUEUE_INSTALL_DIR="$HOME/.chronoqueue" bash
```

**Windows (PowerShell):**

```powershell
# Install latest version
powershell -Command "iwr -useb https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.ps1 | iex"

# Install a specific version
$s=iwr -useb https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.ps1; `
$b=[ScriptBlock]::Create($s); invoke-command -ScriptBlock $b -ArgumentList '1.2.1'

# Install to a custom directory
$Env:CHRONOQUEUE_INSTALL_DIR="C:\tools\chronoqueue"
powershell -Command "iwr -useb https://raw.githubusercontent.com/adrien19/chronoqueue/develop/install/install.ps1 | iex"
```

The scripts automatically:

- Download the latest release (or specified version)
- Verify archive checksums and confirm the installed binary reports the requested release tag, commit, and build date
- Install the binary to your PATH
- Work on Linux, macOS, and Windows

#### Docker Compose Option

You can also run ChronoQueue locally with [Docker Compose](https://docs.docker.com/compose/). The repository provides configurations for PostgreSQL and SQLite.

1. Clone the repository:

   ```bash
   git clone https://github.com/adrien19/chronoqueue.git
   ```

2. Change to the deployment directory and start the PostgreSQL configuration:

    ```bash
    cd deploy
    docker compose -f docker-compose.postgres.yaml up
    ```

#### Run Server Option

1. Clone the repository:

   ```bash
   git clone https://github.com/adrien19/chronoqueue.git
   ```

2. Download the server dependencies:

    ```bash
    go mod download
    ```

3. Configure your environment:

   - Start with [`.env.example`](./.env.example) and choose PostgreSQL (recommended) or SQLite.
   - For PostgreSQL, set `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`. Production mode defaults to `POSTGRES_SSLMODE=verify-full`; set `POSTGRES_ROOT_CERT` when a custom CA certificate is required. Development mode defaults to `POSTGRES_SSLMODE=disable` for local use.
   - For SQLite, set `SQLITE_DB_PATH` (for example, `/data/chronoqueue.db`).
   - Production mode enables authentication by default. Set `API_KEYS` to a comma-separated list; clients can authenticate with the `api-key` header or an `Authorization: Bearer` token. The CLI and web UI read `CHRONOQUEUE_API_KEY`; the CLI also accepts `--api-key`. Development mode can enable authentication with `AUTH_ENABLED=true`.
   - Production mode requires TLS for the gRPC and HTTP endpoints. Set `CERT_FILE` and `KEY_FILE`. To require gRPC client certificates, also set `CA_CERT_FILE`; the internal HTTP gateway then needs `GATEWAY_CLIENT_CERT_FILE` and `GATEWAY_CLIENT_KEY_FILE`.
   - HTTP gateway timeouts default to `5s` for request headers, `15s` for reads, `30s` for writes, and `60s` for idle connections. Override them with `HTTP_READ_HEADER_TIMEOUT`, `HTTP_READ_TIMEOUT`, `HTTP_WRITE_TIMEOUT`, and `HTTP_IDLE_TIMEOUT`.
   - Production mode requires payload encryption and an explicit `ENCRYPTION_KEY_SOURCE_TYPE`. Use `VAULT` for normal production deployments. `LOCAL` keys require `ALLOW_LOCAL_ENCRYPTION_KEY_IN_PRODUCTION=true`. Follow the [encryption-key rotation procedure](./ENCRYPTION_KEY_ROTATION.md) before changing an active key.
   - Production mode enables configurable per-principal API rate limiting as an abuse-protection baseline. The default is 100 RPCs/second with a burst of 200. The limiter counts RPCs rather than individual messages, so a bulk post consumes one token. With authentication enabled, each API key has one bucket shared by all RPC methods, and each server instance applies its own limits. Tune or disable the limiter when equivalent controls are enforced upstream. Configure it with `RATE_LIMIT_ENABLED`, `RATE_LIMIT_REQUESTS_PER_SECOND`, `RATE_LIMIT_BURST`, and `RATE_LIMIT_MAX_BUCKETS`; the last setting caps the number of in-memory principal buckets.
   - Metrics are enabled and bearer-protected by default in production. Set `METRICS_BEARER_TOKEN` and configure Prometheus to send it in the `Authorization: Bearer` header, or disable metrics with `METRICS_ENABLED=false`.

4. Start the ChronoQueue server:

    ```bash
    # Development mode with PostgreSQL (recommended)
    go run . server --dev --grpc-addr :9000

    # Development mode with SQLite
    go run . server --dev --grpc-addr :9000 --storage-type sqlite --sqlite-db-path chronoqueue.db
    ```

To use mTLS, generate the required certificates or use the provided [`generate_certs.sh`](./generate_certs.sh) helper for local evaluation.

### Web UI

ChronoQueue includes a built-in web interface for monitoring and managing your queues, schedules, and dead letter queues.

#### Starting the Web UI

1. Build the UI assets (first time only):

    ```bash
    cd cmd/chronoq/web-ui
    npm install
    npm run build:css
    cd ../../..
    ```

2. Build the ChronoQueue binary:

    ```bash
    go build -o chronoqueue .
    ```

3. Start the UI server:

    ```bash
    ./chronoqueue web-ui start --port 8081 --grpc-address localhost:9000 --skip-ssl
    ```

4. Open your browser to `http://localhost:8081`

The UI binds to `127.0.0.1` by default. A non-loopback bind such as `--host 0.0.0.0` requires `CHRONOQUEUE_UI_TLS_CERT_FILE` and `CHRONOQUEUE_UI_TLS_KEY_FILE`; the UI then serves HTTPS with TLS 1.2 or newer. Enable Basic authentication with `CHRONOQUEUE_UI_AUTH_ENABLED=true`, `CHRONOQUEUE_UI_AUTH_USERNAME`, and `CHRONOQUEUE_UI_AUTH_PASSWORD`. `--skip-ssl` affects only the UI-to-gRPC connection.

#### UI Features

- **📊 Live Dashboard**: Monitor queue state, message counts, and service availability through polling and SSE views
- **📋 Queue Management**: View queue details, browse messages, and inspect message content
- **⏰ Schedule Management**: Create, pause, resume, and delete cron and calendar-based schedules
- **💀 DLQ Management**: Inspect failed messages, requeue or purge items from dead letter queues
- **🧬 Schema Management**: Register and inspect schema versions, validate payloads, and delete versions
- **🔄 Live Updates**: HTMX-powered polling and SSE fragments without full-page refreshes

#### Development Mode

The dev container includes the tooling and configuration used for local development.

Before building the dev container, create the required `chronoqueue-devnet` network:

```bash
docker network create chronoqueue-devnet
```

For UI development with auto-reloading CSS:

```bash
# Terminal 1: Watch CSS changes
make ui-watch

# Terminal 2: Run the server
go run . server --dev --grpc-addr :9000

# Terminal 3: Run the UI
go run . web-ui start --port 8081 --skip-ssl
```

## AI Integration

### Model Context Protocol (MCP) Server

An early-development **Model Context Protocol (MCP) server** is maintained in the separate [TypeScript SDK repository](https://github.com/adrien19/chronoqueue-typescript-sdk). It is intended to let MCP-compatible assistants interact with ChronoQueue queues and schedules.

> Package availability and setup instructions may change while the MCP server is under development. Check the TypeScript SDK repository for its current status.

**Quick Start:**

```bash
# From npm (once published)
npm install -g @chronoqueue/mcp-server

# Or run directly with npx
npx @chronoqueue/mcp-server
```

**Features:**

- 🤖 13 MCP tools for queue, message, and schedule operations
- 🔌 Works with VS Code (GitHub Copilot), Claude Desktop, Cursor IDE, and any MCP-compatible client
- 🔐 Secure gRPC communication with ChronoQueue server
- 📝 Type-safe TypeScript implementation

For complete documentation and setup guides, visit the [TypeScript SDK repository](https://github.com/adrien19/chronoqueue-typescript-sdk).

## Documentation

Documentation currently lives alongside the relevant components:

- Start the HTTP gateway with `--dev` or `--enable-api-docs` and open `/docs/` for the embedded Swagger UI, or inspect the generated [OpenAPI specification](./pkg/gateway/chronoqueue.swagger.json).
- See the [API validation and error contract](./API_VALIDATION.md) for queue/message configuration rules and gRPC-to-HTTP error mappings.
- See the [deployment guide](./deploy/README.md), [monitoring guide](./monitoring/README.md), [test guide](./tests/README.md), and [examples](./examples/README.md).
- The protobuf service contract is defined in [`proto/queueservice/v1/service.proto`](./proto/queueservice/v1/service.proto).

## 🤔 Why not just use Kafka or RabbitMQ?

Kafka and RabbitMQ are general-purpose **message brokers**. ChronoQueue focuses on **job execution and supervision**. The distinction matters when leases, retries, and attempt ownership are part of the server-side contract.

---

### Different priorities

Kafka and RabbitMQ are designed for capabilities such as:

- High-throughput message delivery
- Fan-out and pub/sub
- Backpressure control
- Durable message storage
- Consumer group mechanics

Both brokers provide retry, timeout, and dead-letter building blocks through broker configuration and client behavior. Applications commonly remain responsible for coordinating job-level execution state across workers. ChronoQueue instead exposes leases, heartbeats, attempt IDs, and retry exhaustion as first-class queue operations.

---

### ✅ What ChronoQueue Adds

ChronoQueue can treat each message as a **job with an execution contract**.

Each message attempt has:

- Server-enforced lease
- Heartbeat supervision
- Automatic retry
- Dead-letter on exhaustion
- Strong ownership via `attempt_id`

#### Core Execution Model

| Feature | ChronoQueue |
| -------- | -------------- |
| Per-message lease | ✅ |
| Heartbeat-driven lease extension | ✅ |
| Max execution cap | ✅ |
| Automatic timeout detection | ✅ |
| Attempt-based retries | ✅ |
| Dead-letter queue | ✅ |
| Stale worker prevention | ✅ |

This model lets the server determine whether an attempt still owns the job and remains within its configured processing window.

---

### 🧠 Example: Long-Running File Download

#### Kafka / RabbitMQ

- Consumer starts download
- Network stalls for 10 minutes
- The application must detect the stalled job
- Retry and dead-letter behavior depends on broker and client configuration

#### ChronoQueue Features

- Job leased for 3s base + up to 10s extension
- Worker sends heartbeat every 1s
- Lease extends gradually
- If:
  - Heartbeats stop → auto timeout
  - Max extension exceeded → auto failure
- Message is retried or moved to the DLQ automatically

**The server enforces the configured lease and retry state transitions.**

---

### 🔐 Ownership & Safety

Traditional broker/client setup:

- Ownership is represented by broker-specific consumer groups, sessions, or delivery state
- If workers race or reconnect, behavior can become ambiguous

ChronoQueue:

- Ownership = server-generated `attempt_id`
- Every:
  - Heartbeat
  - ACK
  - Failure
  must match the active attempt
- **Stale workers are automatically rejected**

---

### 🛠 When Should You Use ChronoQueue?

Consider ChronoQueue when you need:

- ✅ Server-enforced attempt time bounds
- ✅ Automatic retries on timeout
- ✅ Server-side heartbeats
- ✅ Job-level supervision
- ✅ Strong worker ownership

Kafka or RabbitMQ may be a better fit when you primarily need:

- ✅ Raw throughput
- ✅ Stateless consumers
- ✅ Event streaming
- ✅ Fire-and-forget messaging

---

### 🧩 Mental Model

- **Kafka/RabbitMQ** = Message Delivery Systems
- **ChronoQueue** = Job Execution & Supervision System

ChronoQueue's execution model is closer to supervised job or activity processing than to a delivery-only queue.

## Examples & Use Cases

The [`examples/`](./examples/) directory contains sample applications demonstrating ChronoQueue integration patterns:

### 🎯 Featured Example: Interview Evaluation Platform

A sample application showcasing several ChronoQueue capabilities through an interview evaluation workflow:

- **Priority Queues**: Urgent vs standard evaluation processing
- **Scheduled Messages**: Business hours-based message delivery
- **Calendar Schedules**: Automated daily/weekly analytics reports
- **DLQ & Retry Logic**: Robust error handling and retry mechanisms
- **Schema Validation**: Structured message validation
- **Queue-based Workload Separation**: Separate queues for each example tenant
- **Heartbeat & Lease Renewal**: Worker health monitoring
- **Real-time Updates**: Server-Sent Events (SSE) integration

**Tech Stack**: Next.js 14, Go, SQLite, Clerk Auth, Tailwind CSS

**[View All Examples →](./examples/README.md)**

The examples demonstrate integration patterns and are intended for learning and evaluation; review and harden them before adapting them to production systems.

## Contributing

We welcome contributions! Please read our **[Contributing Guidelines](./CONTRIBUTING.md)** for detailed information on:

- 🚀 **Development Setup** - Using dev containers for consistent development
- 🧪 **Testing Guidelines** - Unit, integration, and E2E test patterns
- 📝 **Code Standards** - Go style guide and best practices
- 🔄 **Pull Request Process** - Workflow and review expectations
- 🏗️ **CI/CD Pipeline** - Understanding automated checks

**Quick Start for Contributors**:

1. **Use the Dev Container** (Recommended) - Zero configuration, everything pre-installed
2. **Fork and clone** the repository
3. **Create a feature branch** from `develop`
4. **Make your changes** with tests
5. **Run tests locally**: `make test-all`
6. **Submit a pull request** with clear description

For questions, open an issue or start a GitHub Discussion.

## License

ChronoQueue is licensed under [MIT License](./LICENSE).

## Acknowledgments

Special thanks to everyone who contributes to and evaluates ChronoQueue.
