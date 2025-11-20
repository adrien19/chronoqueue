#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║        AI Agent Orchestrator - Docker Compose Setup                 ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Step 1: Build Docker images
echo -e "${BLUE}🔨 Step 1: Building Docker images...${NC}"
cd "$PROJECT_DIR"
if docker-compose build; then
    echo -e "${GREEN}✓ Docker images built successfully${NC}"
else
    echo -e "${RED}❌ Failed to build Docker images${NC}"
    exit 1
fi
echo ""

# Step 2: Start Ollama service
echo -e "${BLUE}🚀 Step 2: Starting Ollama service...${NC}"
docker-compose up -d ollama
echo -e "${YELLOW}⏳ Waiting for Ollama to be ready...${NC}"
sleep 10

# Wait for Ollama health check
max_retries=30
retry_count=0
while ! docker-compose ps ollama | grep -q "healthy" && [ $retry_count -lt $max_retries ]; do
    echo -e "${YELLOW}   Still waiting... (${retry_count}/${max_retries})${NC}"
    sleep 2
    retry_count=$((retry_count + 1))
done

if [ $retry_count -eq $max_retries ]; then
    echo -e "${RED}❌ Ollama service did not become healthy in time${NC}"
    echo -e "${YELLOW}💡 Check logs: docker-compose logs ollama${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Ollama service is running and healthy${NC}"
echo ""

# Step 3: Pull models
echo -e "${BLUE}📥 Step 3: Downloading LLM models...${NC}"
echo -e "${YELLOW}   This step may take 10-30 minutes depending on your internet speed${NC}"
echo ""

# Pull models using docker exec
echo -e "${BLUE}   Pulling llama3.2:3b...${NC}"
if docker exec ai-orchestrator-ollama ollama pull llama3.2:3b; then
    echo -e "${GREEN}   ✓ llama3.2:3b downloaded${NC}"
else
    echo -e "${RED}   ❌ Failed to download llama3.2:3b${NC}"
fi

echo ""
echo -e "${BLUE}   Pulling qwen2.5-coder:7b...${NC}"
if docker exec ai-orchestrator-ollama ollama pull qwen2.5-coder:7b; then
    echo -e "${GREEN}   ✓ qwen2.5-coder:7b downloaded${NC}"
else
    echo -e "${RED}   ❌ Failed to download qwen2.5-coder:7b${NC}"
fi

echo ""
echo -e "${GREEN}✓ Model download complete${NC}"
echo ""

# Step 4: Start remaining services
echo -e "${BLUE}🚀 Step 4: Starting AI Agent services...${NC}"
echo -e "${YELLOW}   Note: Skipping ChronoQueue (use existing server)${NC}"

docker-compose up -d coordinator agents-mock agents-llm

echo -e "${GREEN}✓ All services started${NC}"
echo ""

# Step 5: Display status
echo -e "${BLUE}📊 Step 5: Service Status${NC}"
docker-compose ps
echo ""

# Summary
echo -e "${BLUE}════════════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ Setup Complete!${NC}"
echo ""
echo -e "${BLUE}Services running:${NC}"
echo -e "  • ${GREEN}Ollama${NC} (LLM service) - http://localhost:11434"
echo -e "  • ${GREEN}Coordinator${NC} (Task decomposition)"
echo -e "  • ${GREEN}Mock Agents${NC} (web-search, code-analyzer, data-processor)"
echo -e "  • ${GREEN}LLM Agents${NC} (writer, researcher, coder)"
echo ""
echo -e "${YELLOW}💡 Next steps:${NC}"
echo -e "   1. Initialize queues: ${GREEN}./ai-orchestrator init --server localhost:9000 --insecure${NC}"
echo -e "   2. Submit a task: ${GREEN}./ai-orchestrator submit tasks/llm-jokes.json${NC}"
echo -e "   3. Monitor progress: ${GREEN}./ai-orchestrator monitor --follow${NC}"
echo ""
echo -e "${YELLOW}📝 Useful commands:${NC}"
echo -e "   • View logs: ${GREEN}docker-compose logs -f [service-name]${NC}"
echo -e "   • Stop services: ${GREEN}docker-compose down${NC}"
echo -e "   • Restart service: ${GREEN}docker-compose restart [service-name]${NC}"
echo ""
