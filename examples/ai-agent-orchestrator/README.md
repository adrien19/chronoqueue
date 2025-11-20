# AI Agent Task Orchestrator

A production-ready demonstration of how **ChronoQueue** (a message queue system) orchestrates multiple specialized AI agents working in parallel to solve complex tasks through intelligent task decomposition, coordination, and result aggregation.

This example showcases both **Mock Agents** (fast simulation) and **LLM-Powered Agents** (real AI using Ollama) to demonstrate different use cases and deployment scenarios.

## 🎯 What This Demo Demonstrates

### Core Capabilities

- ✅ **Multi-agent coordination** - Coordinator decomposes tasks using LLM and routes to specialized agents
- ✅ **Parallel execution** - Multiple agents process subtasks simultaneously
- ✅ **Dual-mode agents** - Mock agents (fast simulation) + LLM agents (real AI)
- ✅ **Priority-based routing** - High-priority tasks processed first
- ✅ **Long-running tasks** - Heartbeat renewal for tasks taking >5 minutes
- ✅ **Intelligent retry** - Exponential backoff with jitter for API failures
- ✅ **DLQ management** - Failed tasks moved to Dead Letter Queues for analysis
- ✅ **Result aggregation** - LLM-powered synthesis of multiple agent results

### Real LLM Integration (Phase 5)

- ✅ **Ollama integration** - Use local LLM models (llama3.2:3b, qwen2.5-coder:7b)
- ✅ **LLM Writer Agent** - Creative content generation (jokes, articles, stories)
- ✅ **LLM Researcher Agent** - Research and analysis tasks
- ✅ **LLM Coder Agent** - Code generation and programming tasks
- ✅ **Direct routing** - Simple LLM tasks bypass complex decomposition

### Monitoring & Observability

- ✅ **Real-time monitoring** - Visual dashboard with queue statistics
- ✅ **Agent health metrics** - Status, throughput, error rates per agent
- ✅ **Task lifecycle tracking** - Status command shows task progression
- ✅ **Log aggregation** - Centralized log viewing with filtering

## 📋 Prerequisites

### Required

- **Go 1.23+** - For building the orchestrator
- **ChronoQueue server** - Message queue backend (see setup below)

### Optional (for LLM features)

- **Ollama** - For running local LLM models ([install guide](https://ollama.ai))
- **Docker & Docker Compose** - For containerized deployment (recommended)

## 🚀 Quick Start

> **New to ChronoQueue?** ChronoQueue is a message queue system that manages task distribution between services. Think of it as a smart post office for your application messages.

You have **three options** for running this example:

### Option 1: Local Setup (Mock Agents Only - Fastest) ⚡

Best for quick demos and testing without LLM dependencies.

```bash
# 1. Start ChronoQueue server (in terminal 1)
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev

# 2. Build orchestrator (in terminal 2)
cd examples/ai-agent-orchestrator
make build

# 3. Initialize queues
./ai-orchestrator init --server localhost:9000 --insecure

# 4. Start coordinator with mock LLM (in terminal 3)
./ai-orchestrator coordinator --workers 2 --server localhost:9000 --insecure -v

# 5. Start mock agents (in terminal 4)
./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure -v

# 6. Submit a task (in terminal 2)
./ai-orchestrator submit tasks/competitor-analysis.json
```

### Option 2: Local Setup with Ollama (Real LLM) 🤖

Run LLM-powered agents using local Ollama models.

#### Step 1: Install and Start Ollama

```bash
# Install Ollama (if not installed)
# Visit: https://ollama.ai

# Start Ollama service
ollama serve

# Pull required models (in another terminal)
ollama pull llama3.2:3b      # ~2GB - for general tasks
ollama pull qwen2.5-coder:7b # ~4.7GB - for coding tasks
```

#### Step 2: Start ChronoQueue

```bash
# Terminal 1: Start ChronoQueue
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev
```

#### Step 3: Run Orchestrator with LLM Agents

```bash
# Terminal 2: Build and initialize
cd examples/ai-agent-orchestrator
make build
./ai-orchestrator init --server localhost:9000 --insecure

# Terminal 3: Start coordinator with Ollama
./ai-orchestrator coordinator --workers 2 \
  --llm-provider ollama \
  --llm-base-url http://localhost:11434 \
  --server localhost:9000 --insecure -v

# Terminal 4: Start LLM agents
./ai-orchestrator agents \
  --llm-writer --llm-researcher --llm-coder \
  --llm-provider ollama \
  --llm-base-url http://localhost:11434 \
  --workers 2 --server localhost:9000 --insecure -v

# Terminal 5: (Optional) Start mock agents alongside LLM agents
./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure -v

# Terminal 2: Submit LLM tasks
./ai-orchestrator submit tasks/llm-jokes.json      # Creative writing
./ai-orchestrator submit tasks/llm-research.json   # Research
./ai-orchestrator submit tasks/llm-coding.json     # Code generation
```

### Option 3: Docker Compose (Complete Setup) 🐳

Fully containerized deployment with Ollama included.

```bash
# One-command setup (downloads models, starts everything)
cd examples/ai-agent-orchestrator
make docker-setup

# Initialize queues
./ai-orchestrator init --server localhost:9000 --insecure

# Submit tasks
./ai-orchestrator submit tasks/llm-jokes.json
./ai-orchestrator monitor --follow
```

---

### Understanding the Output

After initialization, you'll see:

```
🚀 Initializing AI Agent Orchestrator...

✓ Created queue: agent-coordinator
  Type: SIMPLE, Lease: 2m0s, Max Attempts: 3
  Use case: Task decomposition and routing
  DLQ: agent-coordinator-dlq

✓ Created queue: agent-web-search
  Type: SIMPLE, Lease: 5m0s, Max Attempts: 5
  Use case: Web research and API calls
  DLQ: agent-web-search-dlq

✓ Created queue: agent-llm-writer
  Type: SIMPLE, Lease: 3m0s, Max Attempts: 3
  Use case: LLM-powered creative writing
  DLQ: agent-llm-writer-dlq

... (more queues)

✓ Initialization complete!
```

**What are these queues?**

- Each agent has its own message queue (like a dedicated inbox)
- **Lease**: How long an agent can work on a task before timeout
- **Max Attempts**: Retry count before sending to Dead Letter Queue (DLQ)
- **DLQ**: Failed messages go here for analysis

## 📚 Usage Guide

### Understanding Agent Types

This orchestrator supports **two types of agents**:

#### Mock Agents (Fast Simulation)

Best for testing and demos without LLM dependencies. Return simulated results instantly.

| Agent | Queue | Purpose |
|-------|-------|---------|
| **Coordinator** | agent-coordinator | Routes tasks to specialized agents |
| **Web Search** | agent-web-search | Simulates web research and API calls |
| **Code Analyzer** | agent-code-analyzer | Simulates code review and analysis |
| **Data Processor** | agent-data-processor | Simulates data analysis and statistics |
| **Aggregator** | agent-aggregator | Synthesizes results from multiple agents |
| **Notification** | agent-notification | Simulates delivery notifications |

**Start mock agents:**

```bash
./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure -v

# Or start specific agents
./ai-orchestrator agents --web-search --code-analyzer --workers 2
```

#### LLM Agents (Real AI)

Use actual LLM models via Ollama for real content generation. Requires Ollama installation.

| Agent | Queue | Model | Purpose |
|-------|-------|-------|---------|
| **LLM Writer** | agent-llm-writer | llama3.2:3b | Creative writing, jokes, articles |
| **LLM Researcher** | agent-llm-researcher | llama3.2:3b | Research and analysis |
| **LLM Coder** | agent-llm-coder | qwen2.5-coder:7b | Code generation |

**Start LLM agents:**

```bash
./ai-orchestrator agents \
  --llm-writer --llm-researcher --llm-coder \
  --llm-provider ollama \
  --llm-base-url http://localhost:11434 \
  --workers 2 --server localhost:9000 --insecure -v

# Or start individual LLM agents
./ai-orchestrator agents --llm-writer --llm-provider ollama --llm-base-url http://localhost:11434
```

**You can run both types simultaneously!** Mock agents handle complex multi-step tasks, while LLM agents handle creative/research/coding tasks.

---

### Submitting Tasks

#### Mock Agent Tasks (Complex Multi-Agent)

These tasks are decomposed into multiple subtasks and routed to specialized mock agents:

```bash
# Competitor analysis (web-search + code-analyzer + data-processor)
./ai-orchestrator submit tasks/competitor-analysis.json

# Market research (web-search + data-processor)
./ai-orchestrator submit tasks/market-research.json --priority 10

# Code review (code-analyzer only)
./ai-orchestrator submit tasks/code-review.json
```

#### LLM Agent Tasks (Simple, Direct to AI)

These tasks go directly to a single LLM agent for processing:

```bash
# Creative writing (generates jokes with explanations)
./ai-orchestrator submit tasks/llm-jokes.json

# Research task (analyzes message queues in distributed systems)
./ai-orchestrator submit tasks/llm-research.json

# Code generation (creates Fibonacci function with memoization)
./ai-orchestrator submit tasks/llm-coding.json
```

**Task Priority:** Use `--priority` (1-10) to control processing order. Higher = processed first.

---

### Monitoring Your Tasks

#### Real-Time Dashboard

```bash
# One-time snapshot
./ai-orchestrator monitor

# Continuous updates (refreshes every 3 seconds)
./ai-orchestrator monitor --follow
```

**Dashboard shows:**

- System uptime and total task counts
- Queue status (pending, completed, errored messages)
- Agent health (✅ healthy, ⚠️ degraded, ❌ unhealthy)
- DLQ depths and error rates

#### Check Specific Task

```bash
./ai-orchestrator status task-comp-001
./ai-orchestrator status task-jokes-001
```

**Shows:**

- Coordinator queue status
- Subtask breakdown by agent
- Current processing state

#### View Logs

```bash
# Tail last 50 lines
./ai-orchestrator logs

# Filter by task ID
./ai-orchestrator logs --filter "task-jokes-001"

# Show errors only
./ai-orchestrator logs --filter "ERROR"

# Follow live logs
./ai-orchestrator logs --follow

# Show last 100 lines from specific time
./ai-orchestrator logs --lines 100 --since 30m
```

## 🏗️ Architecture

### System Components

```
┌─────────────┐
│    User     │ Submits tasks via CLI
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│  ChronoQueue    │ Message queue backend (manages all queues)
│   (External)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐          ┌──────────────┐
│  Coordinator    │◄─────────┤    Ollama    │ (Optional)
│    Agent        │   LLM    │ LLM Service  │
└────────┬────────┘          └──────────────┘
         │
         ├────────────┬─────────────────┐
         │            │                 │
         ▼            ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Mock Agents  │  │  LLM Agents  │  │  Aggregator  │
│ • web-search │  │  • writer    │  │  (Synthesis) │
│ • analyzer   │  │  • researcher│  │              │
│ • processor  │  │  • coder     │  │              │
└──────────────┘  └──────────────┘  └──────────────┘
         │            │                 │
         └────────────┴─────────────────┘
                      │
                      ▼
              ┌──────────────┐
              │    Results   │
              │    Queue     │
              └──────────────┘
```

### Message Flow

#### Complex Multi-Agent Task (Mock Agents)

```
1. User submits "competitor_analysis" task
   ↓
2. ChronoQueue stores in agent-coordinator queue
   ↓
3. Coordinator Agent:
   - Fetches task from queue
   - Uses LLM to decompose into subtasks
   - Creates: [web-search, code-analyzer, data-processor]
   - Sends each subtask to respective queue
   ↓
4. Specialized Agents (parallel execution):
   - Web Search: Researches competitor website
   - Code Analyzer: Reviews their codebase
   - Data Processor: Analyzes traffic data
   - Each agent sends result to agent-results queue
   ↓
5. Aggregator Agent:
   - Waits for all dependencies
   - Collects results from agent-results
   - Uses LLM to synthesize final report
   - Sends to agent-notification
   ↓
6. Notification Agent:
   - Delivers final report
```

#### Simple LLM Task (Direct Routing)

```
1. User submits "llm_creative" task
   ↓
2. ChronoQueue stores in agent-coordinator queue
   ↓
3. Coordinator Agent:
   - Recognizes as simple LLM task
   - Routes directly to agent-llm-writer (no decomposition)
   ↓
4. LLM Writer Agent:
   - Fetches from agent-llm-writer queue
   - Calls Ollama with user prompt
   - Generates creative content
   - Stores result in agent-results
```

### Queue Configuration

| Queue | Lease Time | Max Attempts | DLQ | Purpose |
|-------|-----------|--------------|-----|---------|
| agent-coordinator | 2m | 3 | ✓ | Task decomposition and routing |
| agent-web-search | 5m | 5 | ✓ | Web research and API calls |
| agent-code-analyzer | 10m | 3 | ✓ | Code review and analysis |
| agent-data-processor | 5m | 3 | ✓ | Data processing and statistics |
| agent-aggregator | 3m | 3 | ✓ | Result synthesis |
| agent-notification | 1m | 5 | ✓ | Report delivery |
| agent-llm-writer | 3m | 3 | ✓ | Creative content generation |
| agent-llm-researcher | 5m | 3 | ✓ | Research and analysis |
| agent-llm-coder | 5m | 3 | ✓ | Code generation |
| agent-results | 5m | 3 | ✓ | Historical results storage |

**Lease Time**: How long an agent can process before timeout  
**Max Attempts**: Retry count before moving to DLQ  
**DLQ**: Dead Letter Queue for failed messages

## 📖 Available Tasks

### Mock Agent Tasks (Complex Multi-Step)

These demonstrate ChronoQueue's ability to orchestrate multiple agents working in parallel.

#### 1. Competitor Analysis

**File**: `tasks/competitor-analysis.json`  
**Type**: `competitor_analysis`  
**Agents Used**: web-search, code-analyzer, data-processor, aggregator

```json
{
  "task_id": "task-comp-001",
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

**What happens:**

1. Coordinator decomposes into 3 parallel subtasks
2. Web Search Agent researches competitor website
3. Code Analyzer Agent reviews their technology stack
4. Data Processor Agent analyzes traffic patterns
5. Aggregator synthesizes findings into final report

#### 2. Market Research

**File**: `tasks/market-research.json`  
**Type**: `market_research`  
**Agents Used**: web-search, data-processor, aggregator

```json
{
  "task_id": "task-market-001",
  "task_type": "market_research",
  "description": "Market research for AI automation tools",
  "input": {
    "market_segment": "AI automation tools",
    "geographic_focus": "North America"
  },
  "priority": 5
}
```

#### 3. Code Review

**File**: `tasks/code-review.json`  
**Type**: `code_review`  
**Agents Used**: code-analyzer

```json
{
  "task_id": "task-review-001",
  "task_type": "code_review",
  "description": "Code review for GitHub repository",
  "input": {
    "repository_url": "https://github.com/example/repo",
    "focus_areas": ["security", "performance"]
  },
  "priority": 7
}
```

---

### LLM Agent Tasks (Real AI Content)

These demonstrate real LLM integration using Ollama models. Requires Ollama installation.

#### 1. Creative Writing (Jokes)

**File**: `tasks/llm-jokes.json`  
**Type**: `llm_creative`  
**Agent Used**: llm-writer  
**Model**: llama3.2:3b

```json
{
  "task_id": "task-jokes-001",
  "task_type": "llm_creative",
  "description": "Generate programming jokes with explanations",
  "input": {
    "prompt": "Generate 3 programming jokes and explain why they're funny",
    "tone": "humorous and educational",
    "length": "medium"
  },
  "priority": 7
}
```

**Expected output**: Real AI-generated jokes with explanations (~500 words, ~6 seconds)

#### 2. Research Task

**File**: `tasks/llm-research.json`  
**Type**: `llm_research`  
**Agent Used**: llm-researcher  
**Model**: llama3.2:3b

```json
{
  "task_id": "task-research-001",
  "task_type": "llm_research",
  "description": "Research message queues in distributed systems",
  "input": {
    "topic": "Message queues and their role in building scalable distributed systems",
    "depth": "detailed with examples"
  },
  "priority": 8
}
```

**Expected output**: Detailed research paper (~900 words, ~14 seconds)

#### 3. Code Generation

**File**: `tasks/llm-coding.json`  
**Type**: `llm_coding`  
**Agent Used**: llm-coder  
**Model**: qwen2.5-coder:7b

```json
{
  "task_id": "task-code-001",
  "task_type": "llm_coding",
  "description": "Implement Fibonacci with memoization",
  "input": {
    "task": "Write a Go function to calculate Fibonacci numbers with memoization",
    "language": "go"
  },
  "priority": 9
}
```

**Expected output**: Functional Go code (~70 lines, ~23 seconds)

---

## 🎬 Complete Walkthrough Examples

### Example 1: Mock Agents (Fast Demo)

Demonstrates multi-agent orchestration without LLM dependencies.

```bash
# Terminal 1: Start ChronoQueue
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev

# Terminal 2: Setup orchestrator
cd examples/ai-agent-orchestrator
make build
./ai-orchestrator init --server localhost:9000 --insecure

# Terminal 3: Start coordinator
./ai-orchestrator coordinator --workers 2 --server localhost:9000 --insecure -v

# Terminal 4: Start all mock agents
./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure -v

# Terminal 5: Monitor (optional)
./ai-orchestrator monitor --follow

# Terminal 2: Submit task and check status
./ai-orchestrator submit tasks/competitor-analysis.json
sleep 5
./ai-orchestrator status task-comp-001
```

**What you'll see:**

- Task decomposed into 3 subtasks (web-search, code-analyzer, data-processor)
- All agents process in parallel
- Results aggregated into final report
- Total time: ~10-15 seconds (simulated)

---

### Example 2: LLM Agents (Real AI)

Demonstrates real AI content generation using Ollama.

**Prerequisites**: Ollama installed with models downloaded

```bash
# Terminal 1: Start Ollama (if not running)
ollama serve

# Terminal 2: Verify models
ollama list
# Should show: llama3.2:3b and qwen2.5-coder:7b

# Terminal 3: Start ChronoQueue
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev

# Terminal 4: Setup orchestrator
cd examples/ai-agent-orchestrator
make build
./ai-orchestrator init --server localhost:9000 --insecure

# Terminal 5: Start coordinator with Ollama
./ai-orchestrator coordinator --workers 2 \
  --llm-provider ollama \
  --llm-base-url http://localhost:11434 \
  --server localhost:9000 --insecure -v

# Terminal 6: Start LLM agents
./ai-orchestrator agents \
  --llm-writer --llm-researcher --llm-coder \
  --llm-provider ollama \
  --llm-base-url http://localhost:11434 \
  --workers 2 --server localhost:9000 --insecure -v

# Terminal 4: Submit LLM tasks
./ai-orchestrator submit tasks/llm-jokes.json
./ai-orchestrator submit tasks/llm-research.json
./ai-orchestrator submit tasks/llm-coding.json

# Monitor progress
./ai-orchestrator monitor --follow

# Check results (after ~30 seconds)
./ai-orchestrator status task-jokes-001
./ai-orchestrator status task-research-001
./ai-orchestrator status task-code-001
```

**What you'll see:**

- Each task routes directly to appropriate LLM agent
- Real Ollama calls with token generation
- Creative content, research, and code generated by AI
- Total time: 6-23 seconds per task (real LLM inference)

---

### Example 3: Hybrid Setup (Both Agent Types)

Run mock and LLM agents simultaneously for maximum flexibility.

```bash
# Start ChronoQueue (Terminal 1)
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev

# Setup (Terminal 2)
cd examples/ai-agent-orchestrator
make build
./ai-orchestrator init --server localhost:9000 --insecure

# Start coordinator with Ollama (Terminal 3)
./ai-orchestrator coordinator --workers 2 \
  --llm-provider ollama --llm-base-url http://localhost:11434 \
  --server localhost:9000 --insecure -v

# Start mock agents (Terminal 4)
./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure -v

# Start LLM agents (Terminal 5)
./ai-orchestrator agents \
  --llm-writer --llm-researcher --llm-coder \
  --llm-provider ollama --llm-base-url http://localhost:11434 \
  --workers 2 --server localhost:9000 --insecure -v

# Submit mixed tasks (Terminal 2)
./ai-orchestrator submit tasks/competitor-analysis.json  # → Mock agents
./ai-orchestrator submit tasks/llm-jokes.json            # → LLM writer
./ai-orchestrator submit tasks/market-research.json      # → Mock agents
./ai-orchestrator submit tasks/llm-coding.json           # → LLM coder

# Monitor all
./ai-orchestrator monitor --follow
```

**Use case**: Production scenarios where you need both fast simulation and real AI capabilities.

## 🔧 CLI Command Reference

### Initialization

```bash
# Initialize all queues (run once before starting agents)
./ai-orchestrator init [flags]

Flags:
  --server string    ChronoQueue server address (default "localhost:9000")
  --insecure        Use insecure connection (default true)
```

### Coordinator

```bash
# Start coordinator agent
./ai-orchestrator coordinator [flags]

Flags:
  --workers int           Number of worker goroutines (default 2)
  --llm-provider string   LLM provider: "mock" or "ollama" (default "mock")
  --llm-model string      LLM model name (default "llama3.2:3b")
  --llm-base-url string   Ollama base URL (default "http://localhost:11434")
  --server string         ChronoQueue server address (default "localhost:9000")
  --insecure             Use insecure connection
  -v, --verbose          Verbose output
```

### Agents

```bash
# Start agents
./ai-orchestrator agents [flags]

Mock Agent Flags:
  --all                Start all mock agents
  --web-search         Start web search agent
  --code-analyzer      Start code analyzer agent
  --data-processor     Start data processor agent
  --aggregator         Start aggregator agent
  --notification       Start notification agent

LLM Agent Flags:
  --llm-writer         Start LLM writer agent
  --llm-researcher     Start LLM researcher agent
  --llm-coder          Start LLM coder agent
  --llm-provider string   LLM provider (default "ollama")
  --llm-model string      LLM model (default "llama3.2:3b")
  --llm-base-url string   Ollama URL (default "http://localhost:11434")

Common Flags:
  --workers int        Number of workers per agent (default 2)
  --server string      ChronoQueue server address (default "localhost:9000")
  --insecure          Use insecure connection
  -v, --verbose       Verbose output
```

### Task Management

```bash
# Submit a task
./ai-orchestrator submit <task-file.json> [flags]
  -p, --priority int    Task priority 1-10 (default 5)

# Check task status
./ai-orchestrator status <task-id>

# Monitor system
./ai-orchestrator monitor [flags]
  --follow             Continuous updates (Ctrl+C to stop)

# View logs
./ai-orchestrator logs [flags]
  --lines int          Number of lines to show (default 50)
  --filter string      Filter by text (task ID, agent, keyword)
  --since string       Show logs from time ago (e.g., "30m", "1h")
  --follow             Stream logs continuously

# Cleanup
./ai-orchestrator cleanup [flags]
  --server string      ChronoQueue server address (default "localhost:9000")
  --insecure          Use insecure connection
```

---

## 🧪 Implementation Status

### ✅ Phase 1: Foundation (Complete)

- CLI framework with cobra commands
- Queue creation and initialization
- Task data models (Task, SubTask, AgentResult)
- Example task files
- Build system and makefile

### ✅ Phase 2: Coordinator Agent (Complete)

- LLM-powered task decomposition
- Task routing to specialized queues
- Dependency management
- Worker pool infrastructure
- Message acknowledgment and error handling

### ✅ Phase 3: Specialized Mock Agents (Complete)

- Web Search Agent (mock data gathering)
- Code Analyzer Agent (mock code review)
- Data Processor Agent (mock analysis)
- Aggregator Agent (result synthesis)
- Notification Agent (delivery simulation)

### ✅ Phase 4: Monitoring & Observability (Complete)

- Real-time monitoring dashboard
- Agent health metrics
- Task status tracking
- Log aggregation and filtering
- Redis integration for accuracy

### ✅ Phase 5: Real LLM Integration (Complete)

- Ollama client implementation
- Factory pattern for LLM providers
- LLM Writer Agent (creative content)
- LLM Researcher Agent (research/analysis)
- LLM Coder Agent (code generation)
- Direct routing for simple LLM tasks
- Tested with llama3.2:3b and qwen2.5-coder:7b

### ✅ Phase 6: Docker Deployment (Complete)

- docker-compose.yaml with Ollama service
- Multi-stage Dockerfile for optimization
- Automated setup scripts
- Model download automation
- Health checks and volume persistence
- Comprehensive deployment documentation

### 🔮 Future Enhancements

- OpenAI/Anthropic API integration
- Scheduled maintenance tasks
- Enhanced DLQ analysis and recovery
- Web UI for monitoring
- Metrics export (Prometheus/Grafana)
- Kubernetes deployment manifests

## � Docker Deployment

For containerized deployment with Ollama included, see:

- **[QUICKSTART_DOCKER.md](QUICKSTART_DOCKER.md)** - 5-minute quick start
- **[docs/DOCKER_DEPLOYMENT.md](docs/DOCKER_DEPLOYMENT.md)** - Comprehensive guide

### Quick Docker Setup

```bash
# One-command setup (builds images, downloads models, starts services)
make docker-setup

# Or manually
docker-compose build
docker-compose up -d ollama
docker exec -it ai-orchestrator-ollama ollama pull llama3.2:3b
docker exec -it ai-orchestrator-ollama ollama pull qwen2.5-coder:7b
docker-compose up -d coordinator agents-mock agents-llm
```

**Docker deployment includes:**

- Ollama service with automatic model management
- Coordinator with Ollama integration
- All mock agents
- All LLM agents
- Health checks and auto-restart
- Volume persistence for models

---

## � Common Scenarios

### Scenario 1: I want to test quickly without installing Ollama

Use **Mock Agents Only** (Option 1 from Quick Start):

```bash
# Start ChronoQueue
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword' && make server-dev

# Setup and run
cd examples/ai-agent-orchestrator
make build && ./ai-orchestrator init --server localhost:9000 --insecure

# Terminal 1: Coordinator
./ai-orchestrator coordinator --workers 2 --server localhost:9000 --insecure -v

# Terminal 2: Mock agents
./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure -v

# Terminal 3: Submit and monitor
./ai-orchestrator submit tasks/competitor-analysis.json
./ai-orchestrator monitor --follow
```

**Result**: Fast simulation showing multi-agent orchestration

---

### Scenario 2: I want to use real AI for content generation

Use **LLM Agents** (Option 2 from Quick Start):

**Prerequisites**: Install Ollama and download models first

```bash
# 1. Install Ollama: https://ollama.ai
# 2. Pull models
ollama pull llama3.2:3b
ollama pull qwen2.5-coder:7b

# 3. Start services and agents (see Option 2 in Quick Start)
# 4. Submit LLM tasks
./ai-orchestrator submit tasks/llm-jokes.json
./ai-orchestrator submit tasks/llm-research.json
./ai-orchestrator submit tasks/llm-coding.json
```

**Result**: Real AI-generated content (jokes, research, code)

---

### Scenario 3: I want everything in Docker

Use **Docker Compose** (Option 3 from Quick Start):

```bash
make docker-setup  # Installs everything including Ollama
```

**Result**: Complete containerized environment with one command

---

### Scenario 4: ChronoQueue won't start

**Problem**: ChronoQueue needs Redis running first.

**Solution**:

```bash
# Check if Redis is running (from project root)
docker ps | grep redis

# If not, start the full ChronoQueue setup:
cd /workspaces/chronoqueue
export REDIS_PASSWORD='mypassword'
make server-dev
```

**Verify**: Check logs for "Server listening on :9000"

---

### Scenario 5: Ollama connection errors

**Problem**: `connection refused` when calling Ollama

**Solutions**:

```bash
# 1. Check if Ollama is running
curl http://localhost:11434/api/tags

# 2. Start Ollama if not running
ollama serve

# 3. Verify models are downloaded
ollama list

# 4. If in Docker, use correct URL
# Host machine: http://localhost:11434
# Docker container: http://host.docker.internal:11434
```

---

### Scenario 6: Tasks stuck in queue

**Problem**: Tasks submitted but not processing

**Checklist**:

```bash
# 1. Are agents running?
ps aux | grep ai-orchestrator

# 2. Check queue status
./ai-orchestrator monitor

# 3. Look for errors in logs
./ai-orchestrator logs --filter ERROR

# 4. Verify ChronoQueue connection
# Logs should show: "Connected to ChronoQueue at localhost:9000"
```

**Common causes**:

- Agents not started
- Wrong server address (check `--server` flag)
- ChronoQueue not running
- Network connectivity issues

## 💡 Tips & Best Practices

### Performance Optimization

**Worker Counts**: Adjust based on workload and available CPU

```bash
# Light load (development)
--workers 2

# Medium load (demo/staging)
--workers 4

# Heavy load (production)
--workers 8
```

**Task Priority**: Control processing order (1=lowest, 10=highest)

```bash
./ai-orchestrator submit tasks/urgent-task.json --priority 10
./ai-orchestrator submit tasks/background-task.json --priority 3
```

**LLM Model Selection**: Balance speed vs quality

- `llama3.2:1b` - Fastest, less capable
- `llama3.2:3b` - Good balance (recommended)
- `llama3.2:7b` - Better quality, slower
- `qwen2.5-coder:7b` - Best for code generation

### Monitoring Best Practices

1. **Watch DLQ depths**: High DLQ = systemic problems

   ```bash
   ./ai-orchestrator monitor  # Check "DLQ" column
   ```

2. **Filter logs by task**: Track specific task lifecycle

   ```bash
   ./ai-orchestrator logs --filter "task-comp-001" --follow
   ```

3. **Check agent health**: Identify struggling agents

   ```bash
   ./ai-orchestrator monitor  # Look for ⚠️ or ❌ status
   ```

### Production Deployment Checklist

✅ **Completed Features**:

- Worker pool concurrency for scaling
- Health monitoring with status indicators
- DLQ for failed message analysis
- Graceful shutdown with signal handling
- Structured logging with levels
- Real LLM integration (Ollama)
- Docker deployment with orchestration

🔄 **Recommended Additions**:

- Configure resource limits in docker-compose
- Set up log rotation for production
- Add Prometheus metrics export
- Implement alerting on DLQ depth
- Use TLS for ChronoQueue connections (remove `--insecure`)
- Set up backup for Ollama model volume

### Scaling Guidelines

**Horizontal Scaling** (multiple instances):

```bash
# Scale LLM agents for more throughput
docker-compose up -d --scale agents-llm=3
```

**Vertical Scaling** (more workers per instance):

```bash
# Increase workers for CPU-bound tasks
./ai-orchestrator agents --llm-coder --workers 6
```

**Resource Estimation**:

- Coordinator: 500MB RAM, 0.5 CPU
- Mock Agent: 200MB RAM, 0.2 CPU per worker
- LLM Agent: 1GB RAM, 1 CPU per worker (+ model memory)
- Ollama (llama3.2:3b): 4GB RAM
- Ollama (qwen2.5-coder:7b): 8GB RAM

---

## 🧹 Cleanup

### Stop All Processes

```bash
# Kill all running orchestrator processes
pkill -f ai-orchestrator

# Or for Docker
docker-compose down
```

### Remove Queues

```bash
# Clean up all queues and data
make clean

# Or manually
./ai-orchestrator cleanup --server localhost:9000 --insecure
```

### Remove Docker Volumes (including models)

```bash
# Warning: This deletes downloaded Ollama models (~7GB)
docker-compose down -v
```

---

## 🔧 Configuration Reference

### Environment Variables

```bash
# ChronoQueue connection
export CHRONOQUEUE_SERVER="localhost:9000"

# Ollama configuration
export OLLAMA_BASE_URL="http://localhost:11434"
export LLM_PROVIDER="ollama"
export LLM_MODEL="llama3.2:3b"
export LLM_CODING_MODEL="qwen2.5-coder:7b"

# Coordinator settings
export COORDINATOR_WORKERS=2

# Agent settings
export AGENT_WORKERS=2
export AGENT_VERBOSE=true
```

### Command-Line Flags Priority

Flags override environment variables:

```bash
# Uses environment variable
./ai-orchestrator coordinator

# Overrides with flag
./ai-orchestrator coordinator --workers 4 --llm-model "llama3.2:7b"
```

### Configuration Files

- `.env` - Environment variables (copy from `.env.example`)
- `docker-compose.yaml` - Service orchestration
- `tasks/*.json` - Task definitions

## 📚 Additional Resources

### Documentation

- **[docs/DOCKER_DEPLOYMENT.md](docs/DOCKER_DEPLOYMENT.md)** - Comprehensive Docker guide
- **[CHANGELOG.md](CHANGELOG.md)** - Version history and changes
- **[ChronoQueue Documentation](../../README.md)** - Main project docs

### Example Task Files

Located in `tasks/` directory:

**Mock Agent Tasks:**

- `competitor-analysis.json` - Multi-agent analysis workflow
- `market-research.json` - Market research with data processing
- `code-review.json` - Repository code review

**LLM Agent Tasks:**

- `llm-jokes.json` - Creative writing (programming jokes)
- `llm-research.json` - Research on message queues
- `llm-coding.json` - Code generation (Fibonacci with memoization)

### Architecture Diagrams

```
Local Setup:
┌──────────────┐
│ ChronoQueue  │ ← Start with: make server-dev
└──────┬───────┘
       │
┌──────┴───────┐
│ Orchestrator │ ← CLI tool (this project)
└──────┬───────┘
       │
┌──────┴───────┬──────────────┐
│ Coordinator  │  Ollama      │ (Optional)
└──────┬───────┴──────────────┘
       │
┌──────┴───────┬──────────────┐
│ Mock Agents  │  LLM Agents  │
└──────────────┴──────────────┘

Docker Setup:
┌─────────────────────────────┐
│   docker-compose.yaml       │
├─────────────┬───────────────┤
│ ollama      │ coordinator   │
├─────────────┼───────────────┤
│ agents-mock │ agents-llm    │
└─────────────┴───────────────┘
```

### Key Concepts

**ChronoQueue**: A message queue system that manages asynchronous task distribution. Acts as the backbone for agent communication.

**Agent**: An independent worker that processes specific types of tasks (e.g., web search, code analysis, content generation).

**Coordinator**: Special agent that receives tasks, decomposes them using LLM, and routes subtasks to specialized agents.

**Queue**: A message inbox for each agent. Tasks wait here until an agent worker is available to process them.

**Lease**: Time limit for processing a message. If exceeded, message becomes available for retry.

**DLQ (Dead Letter Queue)**: Storage for messages that failed after max retry attempts. Allows analysis of systemic failures.

**Worker Pool**: Multiple concurrent goroutines processing messages from the same queue. Increases throughput.

### Understanding Message Flow

1. **Task Submission**: User submits JSON task via CLI → ChronoQueue stores in coordinator queue
2. **Task Decomposition**: Coordinator fetches task → Uses LLM to break into subtasks → Sends to agent queues
3. **Parallel Processing**: Multiple agents fetch from their queues → Process independently → Send results
4. **Result Aggregation**: Aggregator waits for all subtasks → Collects results → Synthesizes final report
5. **Delivery**: Notification agent delivers final result

**Simple LLM tasks** skip decomposition and go directly to appropriate LLM agent.

---

## ❓ FAQ

### General Questions

**Q: Do I need to know Go to run this?**  
A: No, just run the pre-built binary. Go is only needed if you want to modify code.

**Q: What is ChronoQueue?**  
A: A message queue system (like RabbitMQ or Kafka) for managing asynchronous task distribution between services.

**Q: Can I use this in production?**  
A: This is a demonstration example. For production, review security settings, add monitoring, and remove `--insecure` flags.

### Agent Questions

**Q: What's the difference between Mock and LLM agents?**  
A: Mock agents return fast simulated responses. LLM agents use real AI (Ollama) for content generation.

**Q: Can I run both types together?**  
A: Yes! See Example 3 in the walkthrough section.

**Q: How do I know which agent to use?**  
A: Task type determines routing. `llm_creative` → LLM Writer, `competitor_analysis` → Mock agents.

### Ollama Questions

**Q: Do I need Ollama for mock agents?**  
A: No, mock agents work without Ollama. Only needed for LLM agents.

**Q: Which models should I download?**  
A: `llama3.2:3b` (general tasks) and `qwen2.5-coder:7b` (coding). See Option 2 in Quick Start.

**Q: Can I use different models?**  
A: Yes, use `--llm-model` flag. Any Ollama-compatible model works.

**Q: Models are slow to download**  
A: Normal. llama3.2:3b is ~2GB, qwen2.5-coder:7b is ~4.7GB. Takes 10-30 minutes depending on internet speed.

### Troubleshooting Questions

**Q: "Connection refused" errors**  
A: Check if ChronoQueue is running: `docker ps | grep redis` and `make server-dev`

**Q: Tasks not processing**  
A: Verify agents are started. Check `ps aux | grep ai-orchestrator` and logs.

**Q: High memory usage**  
A: LLM models require significant RAM (4-8GB). Adjust Docker resource limits if needed.

**Q: Logs show "maintnotifications" warnings**  
A: Harmless Redis notifications. Does not affect functionality.

---

## 🤝 Contributing

This is a demonstration example showcasing ChronoQueue capabilities. For improvements or suggestions:

1. Report issues in the main ChronoQueue repository
2. Fork and submit pull requests
3. Share feedback on agent patterns and use cases

## 📄 License

Same as ChronoQueue parent project.

---

**Built with ChronoQueue** | [Project Repository](../../) | [Report Issues](../../issues)
