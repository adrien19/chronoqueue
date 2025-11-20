#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default Ollama URL
OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"

# Models to pull
MODELS=(
    "llama3.2:3b"
    "qwen2.5-coder:7b"
)

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           Ollama Model Downloader for AI Orchestrator              ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if Ollama is running
echo -e "${YELLOW}🔍 Checking Ollama service...${NC}"
if ! curl -s "${OLLAMA_URL}/api/tags" > /dev/null 2>&1; then
    echo -e "${RED}❌ Error: Ollama service is not running at ${OLLAMA_URL}${NC}"
    echo -e "${YELLOW}💡 Tip: Start Ollama with 'docker-compose up -d ollama'${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Ollama service is running${NC}"
echo ""

# Function to check if model exists
model_exists() {
    local model=$1
    curl -s "${OLLAMA_URL}/api/tags" | grep -q "\"name\":\"${model}\""
}

# Function to pull model
pull_model() {
    local model=$1
    echo -e "${BLUE}📥 Pulling model: ${model}${NC}"
    echo -e "${YELLOW}   This may take several minutes depending on model size...${NC}"
    
    # Use curl to call Ollama API
    response=$(curl -s -X POST "${OLLAMA_URL}/api/pull" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"${model}\"}" 2>&1)
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully pulled: ${model}${NC}"
        return 0
    else
        echo -e "${RED}❌ Failed to pull: ${model}${NC}"
        return 1
    fi
}

# Pull each model
echo -e "${BLUE}📦 Downloading models...${NC}"
echo ""

failed_models=()
for model in "${MODELS[@]}"; do
    if model_exists "$model"; then
        echo -e "${GREEN}✓ Model already exists: ${model}${NC}"
    else
        if pull_model "$model"; then
            echo ""
        else
            failed_models+=("$model")
            echo ""
        fi
    fi
done

# Summary
echo ""
echo -e "${BLUE}════════════════════════════════════════════════════════════════════════${NC}"
if [ ${#failed_models[@]} -eq 0 ]; then
    echo -e "${GREEN}✓ All models downloaded successfully!${NC}"
    echo ""
    echo -e "${BLUE}Available models:${NC}"
    for model in "${MODELS[@]}"; do
        echo -e "  • ${model}"
    done
    echo ""
    echo -e "${YELLOW}💡 Next steps:${NC}"
    echo -e "   1. Start the AI orchestrator: ${GREEN}docker-compose up -d${NC}"
    echo -e "   2. Initialize queues: ${GREEN}./ai-orchestrator init${NC}"
    echo -e "   3. Submit a task: ${GREEN}./ai-orchestrator submit tasks/llm-jokes.json${NC}"
    exit 0
else
    echo -e "${RED}❌ Failed to download ${#failed_models[@]} model(s):${NC}"
    for model in "${failed_models[@]}"; do
        echo -e "  • ${model}"
    done
    echo ""
    echo -e "${YELLOW}💡 Troubleshooting:${NC}"
    echo -e "   • Check your internet connection"
    echo -e "   • Verify Ollama service is running: ${GREEN}docker-compose ps ollama${NC}"
    echo -e "   • Check Ollama logs: ${GREEN}docker-compose logs ollama${NC}"
    echo -e "   • Try pulling manually: ${GREEN}docker exec -it ai-orchestrator-ollama ollama pull <model>${NC}"
    exit 1
fi
