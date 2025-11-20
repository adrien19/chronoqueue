# Docker Deployment Guide

This guide explains how to deploy the AI Agent Orchestrator using Docker Compose with Ollama integration.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 8GB RAM (for running LLM models)
- 10GB+ free disk space (for model storage)

## Quick Start

### 1. Automated Setup (Recommended)

Run the automated setup script:

```bash
./scripts/docker-setup.sh
```

This script will:

- Build Docker images
- Start Ollama service
- Download required LLM models (llama3.2:3b, qwen2.5-coder:7b)
- Start all agent services

### 2. Manual Setup

#### Step 1: Build Images

```bash
docker-compose build
```

#### Step 2: Start Ollama

```bash
docker-compose up -d ollama
```

Wait for Ollama to be healthy:

```bash
docker-compose ps ollama
```

#### Step 3: Pull Models

```bash
# Pull llama3.2:3b (for general tasks)
docker exec -it ai-orchestrator-ollama ollama pull llama3.2:3b

# Pull qwen2.5-coder:7b (for coding tasks)
docker exec -it ai-orchestrator-ollama ollama pull qwen2.5-coder:7b
```

Or use the convenience script:

```bash
./scripts/pull-models.sh
```

#### Step 4: Start All Services

```bash
# Start coordinator and agents
docker-compose up -d coordinator agents-mock agents-llm
```

## Service Architecture

The Docker Compose setup includes:

### Core Services

1. **ollama** - Ollama LLM service
   - Port: 11434
   - Models: llama3.2:3b, qwen2.5-coder:7b
   - Health check enabled

2. **coordinator** - Task decomposition service
   - Connects to: Ollama, ChronoQueue
   - Workers: 2
   - Uses Ollama for intelligent task routing

3. **agents-mock** - Fast simulation agents
   - Agents: web-search, code-analyzer, data-processor, aggregator, notification
   - Workers: 2 per agent
   - No LLM dependency

4. **agents-llm** - LLM-powered agents
   - Agents: llm-writer, llm-researcher, llm-coder
   - Connects to: Ollama
   - Workers: 2 per agent

### External Dependencies

- **chronoqueue** - Must be running separately
  - Default: localhost:9000
  - Configure via `CHRONOQUEUE_SERVER` environment variable

## Configuration

### Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` to customize:

```bash
# Ollama
OLLAMA_BASE_URL=http://ollama:11434

# ChronoQueue
CHRONOQUEUE_SERVER=chronoqueue:9000

# LLM Models
LLM_MODEL=llama3.2:3b
LLM_CODING_MODEL=qwen2.5-coder:7b

# Workers
COORDINATOR_WORKERS=2
AGENT_WORKERS=2
```

### Using External Ollama

If you have Ollama running on your host machine:

1. Update docker-compose.yaml:

```yaml
coordinator:
  environment:
    - OLLAMA_BASE_URL=http://host.docker.internal:11434
```

2. Or set in `.env`:

```bash
OLLAMA_BASE_URL=http://host.docker.internal:11434
```

## Usage

### Initialize Queues

Before submitting tasks, initialize the queue system:

```bash
docker-compose exec coordinator /app/ai-orchestrator init --server chronoqueue:9000 --insecure
```

Or from your host (if you have the binary):

```bash
./ai-orchestrator init --server localhost:9000 --insecure
```

### Submit Tasks

#### LLM-Powered Tasks

```bash
# Creative writing
./ai-orchestrator submit tasks/llm-jokes.json

# Research
./ai-orchestrator submit tasks/llm-research.json

# Code generation
./ai-orchestrator submit tasks/llm-coding.json
```

#### Complex Tasks (Mock Agents)

```bash
./ai-orchestrator submit tasks/competitor-analysis.json
./ai-orchestrator submit tasks/market-research.json
```

### Monitor Progress

```bash
# Real-time monitoring
./ai-orchestrator monitor --follow

# Check specific task
./ai-orchestrator status <task-id>
```

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f coordinator
docker-compose logs -f agents-llm
docker-compose logs -f ollama

# Last 100 lines
docker-compose logs --tail=100 agents-llm
```

## Management

### Start Services

```bash
# All services
docker-compose up -d

# Specific service
docker-compose up -d coordinator
```

### Stop Services

```bash
# All services
docker-compose down

# Stop services (keeps volumes/models)
docker-compose down

# Remove everything including volumes
docker-compose down -v
```

### Restart Services

```bash
# All services
docker-compose restart

# Specific service
docker-compose restart coordinator
```

### Scale Agents

Increase the number of agent workers:

```bash
docker-compose up -d --scale agents-llm=3
```

## Troubleshooting

### Ollama Not Responding

Check if Ollama is healthy:

```bash
docker-compose ps ollama
docker-compose logs ollama
```

Test Ollama API:

```bash
curl http://localhost:11434/api/tags
```

### Models Not Downloaded

Manually pull models:

```bash
docker exec -it ai-orchestrator-ollama ollama list
docker exec -it ai-orchestrator-ollama ollama pull llama3.2:3b
```

### Coordinator Can't Connect to ChronoQueue

Ensure ChronoQueue is running and accessible:

```bash
# Check ChronoQueue
nc -zv localhost 9000

# Or use grpcurl
grpcurl -plaintext localhost:9000 list
```

Update the server address if needed:

```bash
docker-compose exec coordinator /app/ai-orchestrator status --server chronoqueue:9000
```

### Agent Errors

Check agent logs:

```bash
docker-compose logs agents-llm
docker-compose logs agents-mock
```

Restart specific agents:

```bash
docker-compose restart agents-llm
```

### Out of Memory

LLM models require significant RAM. Check Docker resources:

```bash
docker stats
```

Increase Docker memory allocation in Docker Desktop settings (minimum 8GB recommended).

### Models Taking Too Long to Download

Model sizes:

- llama3.2:3b: ~2GB
- qwen2.5-coder:7b: ~4.7GB

Monitor download progress:

```bash
docker-compose logs -f ollama
```

## Performance Tuning

### Adjust Worker Count

Edit docker-compose.yaml:

```yaml
coordinator:
  command: >
    /app/ai-orchestrator coordinator
    --workers 4  # Increase for better throughput
```

### Use Faster Models

Smaller models = faster responses:

```yaml
environment:
  - LLM_MODEL=llama3.2:1b  # Faster but less capable
```

### Enable GPU Acceleration

If you have an NVIDIA GPU:

```yaml
ollama:
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            count: 1
            capabilities: [gpu]
```

## Maintenance

### Update Models

Pull latest model versions:

```bash
docker exec -it ai-orchestrator-ollama ollama pull llama3.2:3b
docker exec -it ai-orchestrator-ollama ollama pull qwen2.5-coder:7b
```

### Clean Up Old Models

List models:

```bash
docker exec -it ai-orchestrator-ollama ollama list
```

Remove unused models:

```bash
docker exec -it ai-orchestrator-ollama ollama rm <model-name>
```

### Backup Ollama Data

The Ollama data volume contains all downloaded models:

```bash
# Backup
docker run --rm -v ai-agent-orchestrator_ollama_data:/data -v $(pwd):/backup alpine tar czf /backup/ollama-backup.tar.gz /data

# Restore
docker run --rm -v ai-agent-orchestrator_ollama_data:/data -v $(pwd):/backup alpine tar xzf /backup/ollama-backup.tar.gz -C /
```

## Security Considerations

### Production Deployment

For production use:

1. Enable TLS for ChronoQueue:

```yaml
coordinator:
  command: >
    /app/ai-orchestrator coordinator
    --server chronoqueue:9000
    # Remove --insecure flag
```

2. Use secrets for sensitive configuration:

```yaml
secrets:
  chronoqueue_cert:
    file: ./certs/chronoqueue.crt
```

3. Restrict network access:

```yaml
networks:
  ai-orchestrator-network:
    driver: bridge
    internal: true  # No external access
```

4. Set resource limits:

```yaml
coordinator:
  deploy:
    resources:
      limits:
        cpus: '2'
        memory: 2G
```

## Architecture Diagram

```text
┌─────────────────┐
│   User/Client   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│  ChronoQueue    │◄─────┤  Coordinator    │
│   (External)    │      │  (Task Router)  │
└────────┬────────┘      └────────┬────────┘
         │                        │
         │                        ▼
         │               ┌─────────────────┐
         │               │     Ollama      │
         │               │   (LLM Service) │
         │               └────────┬────────┘
         │                        │
         ▼                        ▼
┌─────────────────┐      ┌─────────────────┐
│  Mock Agents    │      │   LLM Agents    │
│  • web-search   │      │  • llm-writer   │
│  • code-analyze │      │  • llm-research │
│  • data-process │      │  • llm-coder    │
└─────────────────┘      └─────────────────┘
```

## Next Steps

- Review [../README.md](../README.md) for detailed usage
- Check [../PHASE5_TEST_RESULTS.md](../PHASE5_TEST_RESULTS.md) for performance benchmarks
- Explore example tasks in `tasks/` directory
