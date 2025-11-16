# ChronoQueue

[![CI](https://github.com/adrien19/chronoqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/adrien19/chronoqueue/actions/workflows/ci.yml)
[![Release](https://github.com/adrien19/chronoqueue/actions/workflows/release.yml/badge.svg)](https://github.com/adrien19/chronoqueue/actions/workflows/release.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/adrien19/chronoqueue)](https://goreportcard.com/report/github.com/adrien19/chronoqueue)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

ChronoQueue is queue management system designed to handle high-volume message processing with efficiency and reliability. It offers a priority-based messaging system, real-time monitoring, and flexible scheduling options, making it an ideal solution for complex asynchronous task management.

## Features

- **Priority Queue Management:** ChronoQueue allows users to assign priorities to messages, ensuring that critical tasks are processed first. This feature is crucial for systems where task urgency varies significantly.

- **Real-time Monitoring and Analytics (WIP):** A dashboard provides a comprehensive overview of all queues and messages, including real-time updates on message statuses, queue health, and system performance metrics.

- **Flexible Scheduling (WIP):** Supports both calendar-based and cron expression scheduling, allowing precise control over when messages are processed.

- **High Scalability and Performance:** Designed to handle millions of messages efficiently, ChronoQueue ensures high throughput and low latency even under heavy loads.

- **Robust Error Handling and Retry Mechanisms:** Automated handling of failed messages with customizable retry policies and error tracking.

- **Secure and Compliant:** Adheres to best practices in security and data handling, ensuring that your data is safe and compliant with relevant regulations.

- **Customizable and Extensible:** Easily adaptable to specific use cases, with support for custom extensions and integrations.

- **Detailed Documentation and Community Support (WIP):** Comprehensive guides, API documentation, and a supportive community for troubleshooting and best practices.

## Getting Started

### Prerequisites

- [Redis](https://redis.io/)
- [Go](https://golang.org/) (for server-side & client-side SDK)
- [Python (WIP)](https://www.python.org/) (for client-side SDKs)

### Installation

#### Docker Compose Option

The easiest way to get started locally is to use [docker-compose](https://docs.docker.com/compose/). Simply:

1. Clone the repository:

   ```bash
   git clone https://github.com/adrien19/chronoqueue.git
   ```

2. Cd into deploy - `cd deploy` and run:

    ```bash
    docker-compose up
    ```

#### Run Server Option

1. Clone the repository:

   ```bash
   git clone https://github.com/adrien19/chronoqueue.git
   ```

2. Install dependencies:

    ```bash
    # For Go server
    go mod tidy

    # For Python/Go clients
    pip install chronoqueuesdk
    # or
    go get https://github.com/adrien19/chronoqueue/client
    ```

3. Configure your environment (refer to the .env.example file for guidance).

4. Start the ChronoQueue server:

    ```bash
    go run main.go server --dev --server :9000 --redis-password mypassword
    ```

If you choose to use mTLS option, you will need to generate certificates. You can use already provided script `generate_certs.sh` to quickly generate these certificates.

### Web UI

ChronoQueue includes a built-in web interface for monitoring and managing your queues, schedules, and dead letter queues.

#### Starting the Web UI

1. Build the UI assets (first time only):

    ```bash
    cd cmd/chronoq/ui
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
    ./chronoqueue ui start --port 8080 --grpc-address localhost:9000
    ```

4. Open your browser to `http://localhost:8080`

#### UI Features

- **📊 Real-time Dashboard**: Monitor queue metrics, message counts, and system health
- **📋 Queue Management**: View queue details, browse messages, and inspect message content
- **⏰ Schedule Management**: Create, edit, and manage cron and calendar-based schedules
- **💀 DLQ Management**: Inspect failed messages, requeue or purge items from dead letter queues
- **🔄 Live Updates**: HTMX-powered real-time updates without page refreshes

#### Development Mode

For UI development with auto-reloading CSS:

```bash
# Terminal 1: Watch CSS changes
make ui-watch

# Terminal 2: Run the server
go run main.go server --dev --server :9000

# Terminal 3: Run the UI
go run main.go ui start --port 8080
```

## Documentation

For detailed documentation, including API references and usage examples, visit [ChronoQueue Docs](./docs/)

## Examples & Use Cases

The [`examples/`](./examples/) directory contains comprehensive real-world applications demonstrating ChronoQueue features and best practices:

### 🎯 Featured Example: Interview Evaluation Platform

A complete sample application showcasing **all ChronoQueue capabilities** through a practical interview evaluation system:

- **Priority Queues**: Urgent vs standard evaluation processing
- **Scheduled Messages**: Business hours-based message delivery
- **Calendar Schedules**: Automated daily/weekly analytics reports
- **DLQ & Retry Logic**: Robust error handling and retry mechanisms
- **Schema Validation**: Structured message validation
- **Multi-tenant Isolation**: Secure tenant data separation
- **Heartbeat & Lease Renewal**: Worker health monitoring
- **Real-time Updates**: Server-Sent Events (SSE) integration

**Tech Stack**: Next.js 14, Go, SQLite, Clerk Auth, Tailwind CSS

**[View All Examples →](./examples/README.md)**

Whether you're a beginner learning the basics or an advanced user exploring multi-tenant patterns, the examples provide production-ready code and architectural guidance to help you build queue-based applications effectively.

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

For questions or discussions, feel free to open an issue or join our community channels.

## License

ChronoQueue is licensed under [MIT License](./LICENSE).

## Acknowledgments

Special thanks to all the contributors and users who have made ChronoQueue a robust and evolving system.
