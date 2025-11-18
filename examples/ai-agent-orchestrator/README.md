# AI Agent Task Orchestrator

A production-ready demonstration of how ChronoQueue orchestrates multiple specialized AI agents working in parallel to solve complex tasks through intelligent task decomposition, coordination, and result aggregation.

## 🎯 What This Demo Demonstrates

### Core Capabilities

- ✅ **Multi-agent coordination** - Coordinator decomposes tasks using LLM and routes to specialized agents
- ✅ **Parallel execution** - Multiple agents process subtasks simultaneously
- ✅ **Priority-based routing** - High-priority tasks processed first
- ✅ **Long-running tasks** - Heartbeat renewal for tasks taking >5 minutes
- ✅ **Intelligent retry** - Exponential backoff with jitter for API failures
- ✅ **DLQ management** - Failed tasks moved to Dead Letter Queues for analysis
- ✅ **Result aggregation** - LLM-powered synthesis of multiple agent results

### Monitoring & Observability (Phase 4)

- ✅ **Real-time monitoring** - Visual dashboard with queue statistics
- ✅ **Agent health metrics** - Status, throughput, error rates per agent
- ✅ **Task lifecycle tracking** - Status command shows task progression
- ✅ **Log aggregation** - Centralized log viewing with filtering
- ✅ **Redis integration** - Direct metrics queries for accuracy

## 📋 Prerequisites

- Docker and Docker Compose (for Redis)
- Go 1.23+
- ChronoQueue server running

## 🚀 Quick Start

### 1. Start ChronoQueue Server

```bash
# From the repository root
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev

# Run the Web UI in another terminal session
make ui-dev
```

### 2. Build the Orchestrator

```bash
cd examples/ai-agent-orchestrator
make build
```

### 3. Initialize the System

```bash
# Create the queues for the agents
make run-init
```

**Expected output:**

```
🚀 Initializing AI Agent Orchestrator...

✓ Created queue: agent-coordinator
  Type: PRIORITY, Lease: 2m0s, Max Attempts: 3
  Use case: Task decomposition and routing
  DLQ: agent-coordinator-dlq

✓ Created queue: agent-web-search
  Type: PRIORITY, Lease: 5m0s, Max Attempts: 5
  Use case: Web research and API calls
  DLQ: agent-web-search-dlq

... (more queues)

✓ Initialization complete!

Next steps:
  1. Start agents:     ./ai-orchestrator agents --all
  2. Submit a task:    ./ai-orchestrator submit tasks/competitor-analysis.json
  3. Monitor progress: ./ai-orchestrator monitor --follow
```

## 📚 Usage

### Submit a Task

```bash
# Submit a competitor analysis task
./ai-orchestrator submit tasks/competitor-analysis.json

# Submit with custom priority
./ai-orchestrator submit tasks/market-research.json --priority 10
```

### Start Agents

```bash
# Start all agents
./ai-orchestrator agents --all --workers 2

# Start specific agents
./ai-orchestrator agents --web-search --workers 3
./ai-orchestrator agents --code-analyzer --workers 2
```

### Monitor System

```bash
# One-time status check
./ai-orchestrator monitor

# Follow mode (continuous updates)
./ai-orchestrator monitor --follow
```

### Check Task Status

```bash
./ai-orchestrator status task-comp-001
```

## 🏗️ Architecture

### Agent Types

1. **Coordinator Agent** - Decomposes tasks using LLM, routes to specialized agents
2. **Web Search Agent** - Web research, API calls, data gathering
3. **Code Analyzer Agent** - Repository analysis, code review
4. **Data Processor Agent** - Data analysis, statistics, visualization
5. **Aggregator Agent** - Synthesizes results from multiple agents using LLM
6. **Notification Agent** - Delivers final reports via email/webhook

### Queue Structure

```
agent-coordinator      → Task decomposition and routing (2m lease, 3 attempts)
agent-web-search       → Web research (5m lease, 5 attempts)
agent-code-analyzer    → Code analysis (10m lease, 3 attempts)
agent-data-processor   → Data processing (5m lease, 3 attempts)
agent-aggregator       → Result synthesis (3m lease, 3 attempts)
agent-notification     → Delivery (1m lease, 5 attempts)
```

Each queue has a corresponding DLQ (e.g., `agent-web-search-dlq`) for failed messages.

## 📖 Example Tasks

### Competitor Analysis

```json
{
  "task_type": "competitor_analysis",
  "description": "Comprehensive competitive analysis for example.com",
  "input": {
    "company_url": "https://example.com",
    "include_code_analysis": true,
    "include_traffic_analysis": true
  },
  "priority": 8
}
```

### Market Research

```json
{
  "task_type": "market_research",
  "description": "Market research for AI automation tools",
  "input": {
    "market_segment": "AI automation tools",
    "geographic_focus": "North America"
  },
  "priority": 5
}
```

### Code Review

```json
{
  "task_type": "code_review",
  "description": "Code review for GitHub repository",
  "input": {
    "repository_url": "https://github.com/example/repo",
    "focus_areas": ["security", "performance"]
  },
  "priority": 7
}
```

## 🔄 Message Flow

```
1. User submits task
   ↓
2. Coordinator Agent receives task
   ↓ (Uses LLM to decompose)
   ├─→ Web Search Agent (parallel)
   ├─→ Code Analyzer Agent (parallel)
   └─→ Data Processor Agent (parallel)
   ↓
3. Agents process independently
   ↓ (Send heartbeats for long tasks)
   ↓ (Retry with exponential backoff on failures)
   ↓
4. Results sent to Aggregator Agent
   ↓ (LLM synthesizes final report)
   ↓
5. Notification Agent delivers report
```

## 🎬 Demo Scenarios

### Simple Demo (Single Agent)

```bash
make demo-simple
```

Demonstrates:

- Basic task submission
- Single agent processing
- Result retrieval

### Complex Demo (Multi-Agent)

```bash
make demo-complex
```

Demonstrates:

- Task decomposition
- Parallel agent execution
- Priority handling
- Failure recovery
- DLQ management

## 🧪 Implementation Status

### Phase 1: Foundation ✅ COMPLETE

- ✅ CLI framework with cobra commands
- ✅ Queue creation and initialization
- ✅ Task data models (Task, SubTask, AgentResult)
- ✅ Example task files (competitor-analysis, market-research, code-review)
- ✅ Build system and makefile
- ✅ Demo task submission

### Phase 2: Coordinator Agent ✅ COMPLETE

- ✅ LLM-powered task decomposition (using MockLLM)
- ✅ Task routing to specialized agent queues
- ✅ Dependency management and tracking
- ✅ Base agent infrastructure with worker pools
- ✅ Message acknowledgment and error handling

### Phase 3: Specialized Agents ✅ COMPLETE

- ✅ Web Search Agent - processes search queries, generates mock results
- ✅ Code Analyzer Agent - analyzes code, returns quality metrics
- ✅ Data Processor Agent - processes data, generates statistics
- ✅ Aggregator Agent - collects dependencies, synthesizes with LLM
- ✅ Base agent with worker pool pattern
- ✅ structpb serialization for complex nested types
- ✅ Optional result posting (continues on queue not found)

### Phase 4: Monitoring & Observability ✅ COMPLETE

- ✅ Real-time monitoring dashboard (`monitor` command)
- ✅ Agent health metrics (status, throughput, error rates)
- ✅ Task status tracking (`status` command)
- ✅ Log aggregation and viewing (`logs` command)
- ✅ Redis integration for accurate metrics
- ✅ Visual indicators (colors, emojis, box drawing)
- ✅ Follow mode for continuous updates

### Phase 5+: Future Enhancements

- ⏳ Advanced LLM integration (real OpenAI/Anthropic API)
- ⏳ Scheduled maintenance tasks (nightly cleanup)
- ⏳ Performance optimizations
- ⏳ Enhanced DLQ analysis and recovery
- ⏳ Web UI for monitoring
- ⏳ Metrics export (Prometheus/Grafana)

## 📊 Monitoring & Observability

### Monitor Command

View real-time system status with comprehensive dashboard:

```bash
# Single snapshot
./ai-orchestrator monitor

# Continuous updates (refresh every 3s)
./ai-orchestrator monitor --follow
```

**Dashboard displays:**

- **System Overview**: Uptime, total/completed/failed tasks, active agents
- **Queue Status**: Pending, completed, failed, and DLQ message counts per queue
- **Agent Health**: Health status (✅/⚠️/❌), DLQ depth, error rate, throughput

**Health Status Criteria:**

- ✅ **HEALTHY**: DLQ < 5, error rate < 20%, queue depth manageable
- ⚠️  **DEGRADED**: DLQ 5-10, error rate 20-50%, or queue depth > 100
- ❌ **UNHEALTHY**: DLQ > 10 or error rate > 50%

### Status Command

Track individual task lifecycle:

```bash
./ai-orchestrator status task-comp-001
```

**Shows:**

- Coordinator status (PENDING/PROCESSING/COMPLETED/ERRORED)
- Subtask breakdown by agent queue
- Pending message counts per queue
- Tips for real-time monitoring

### Logs Command

View and filter agent logs:

```bash
# Tail last 50 lines
./ai-orchestrator logs

# Show specific number of lines
./ai-orchestrator logs --lines 100

# Filter by task ID or keyword
./ai-orchestrator logs --filter "task-comp-001"
./ai-orchestrator logs --filter "ERROR"

# Show logs from last 30 minutes
./ai-orchestrator logs --since 30m

# Follow mode (stream logs continuously)
./ai-orchestrator logs --follow

# Combine filters
./ai-orchestrator logs --lines 200 --filter "web-search" --since 1h
```

**Log files:**

- `coordinator.log` - Task decomposition and routing logs
- `agents.log` - Specialized agent processing logs

**Features:**

- Color-coded by severity (RED=error, YELLOW=warn, GREEN=info)
- Log level distribution summary
- Time-based filtering
- Text filtering (task ID, agent name, keywords)
- Real-time streaming with `--follow`

## 🚦 Quick Start Guide

### Complete Walkthrough

```bash
# 1. Start ChronoQueue server (if not already running)
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && && make server-dev

# 2. Build orchestrator
cd examples/ai-agent-orchestrator
make build

# 3. Initialize queues
./ai-orchestrator init

# 4. Start coordinator (in separate terminal)
./ai-orchestrator coordinator --workers 2

# 5. Start agents (in separate terminal)
./ai-orchestrator agents --all --workers 2

# 6. Monitor system (in separate terminal)
./ai-orchestrator monitor --follow

# 7. Submit a task
./ai-orchestrator submit tasks/competitor-analysis.json

# 8. Check task status
./ai-orchestrator status task-comp-001

# 9. View logs
./ai-orchestrator logs --filter "task-comp-001"

# 10. Cleanup when done
./ai-orchestrator cleanup
```

### Verify Installation

```bash
# Check all commands are available
./ai-orchestrator --help

# Test queue initialization
./ai-orchestrator init

# View monitoring dashboard
./ai-orchestrator monitor
```

## 🧹 Cleanup

```bash
# Remove all queues and demo data
make clean

# Or manually
./ai-orchestrator cleanup
```

## 🔧 Configuration

Environment variables:

- `CHRONOQUEUE_SERVER` - Server address (default: `localhost:9000`)
- `LLM_API_KEY` - OpenAI API key for LLM features (optional, uses MockLLM by default)
- `LLM_MODEL` - LLM model to use (default: `gpt-4`)

## � Tips & Best Practices

### Performance Optimization

- Adjust worker counts based on workload: `--workers 3` for high throughput
- Use priority flag for urgent tasks: `--priority 10`
- Monitor DLQ regularly to identify failing task patterns

### Troubleshooting

- Check agent health status: `./ai-orchestrator monitor`
- View error logs: `./ai-orchestrator logs --filter ERROR`
- Inspect task state: `./ai-orchestrator status <task-id>`
- Verify Redis connection: Check for "maintnotifications" warnings (harmless)

### Production Readiness

- ✅ Worker pool concurrency for horizontal scaling
- ✅ Health monitoring with status indicators
- ✅ DLQ for failed message analysis
- ✅ Graceful shutdown with signal handling
- ✅ Structured logging with levels
- ⏳ Replace MockLLM with real LLM integration
- ⏳ Add metrics export (Prometheus)
- ⏳ Implement scheduled maintenance

## 📚 Learn More

- [ChronoQueue Documentation](../../README.md)
- [Message Queue Best Practices](../../docs/)
- [Redis Configuration](../../deploy/docker-compose.yaml)

## 🤝 Contributing

This is a demonstration example. For improvements or suggestions, please contribute to the main ChronoQueue project.

## 📄 License

Same as ChronoQueue parent project.
