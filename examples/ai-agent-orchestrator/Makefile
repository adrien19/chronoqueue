.PHONY: help build run-init run-coordinator run-agents clean deps test docker-build docker-up docker-down docker-logs docker-setup

# Default target
help:
	@echo "AI Agent Orchestrator - Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make build         - Build the ai-orchestrator binary"
	@echo "  make deps          - Download Go dependencies"
	@echo "  make run-init      - Initialize all queues"
	@echo "  make run-coordinator - Start coordinator agent (mock LLM)"
	@echo "  make run-coordinator-ollama - Start coordinator with Ollama"
	@echo "  make run-agents    - Start all specialized agents"
	@echo "  make demo-simple   - Run simple demo"
	@echo "  make demo-complex  - Run complex demo"
	@echo "  make clean         - Clean up build artifacts and queues"
	@echo "  make test          - Run tests"
	@echo ""
	@echo "Docker targets:"
	@echo "  make docker-build  - Build Docker images"
	@echo "  make docker-up     - Start all services with Docker Compose"
	@echo "  make docker-down   - Stop all Docker services"
	@echo "  make docker-logs   - View Docker logs"
	@echo "  make docker-setup  - Complete Docker setup with models"
	@echo ""

# Build the binary
build: deps
	@echo "Building ai-orchestrator..."
	go build -o ai-orchestrator .
	@echo "✓ Build complete: ./ai-orchestrator"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Initialize queues
run-init: build
	@echo "Initializing queues..."
	./ai-orchestrator init --server localhost:9000 --insecure

# Run coordinator agent (with mock LLM by default)
run-coordinator: build
	@echo "Starting coordinator agent..."
	@mkdir -p logs
	./ai-orchestrator coordinator --workers 2 --server localhost:9000 --insecure -v 2>&1 | tee logs/coordinator.log

# Run coordinator with Ollama
run-coordinator-ollama: build
	@echo "Starting coordinator agent with Ollama..."
	@mkdir -p logs
	./ai-orchestrator coordinator --workers 2 --llm-provider ollama --llm-base-url http://host.docker.internal:11434 --server localhost:9000 --insecure -v 2>&1 | tee logs/coordinator.log

# Run all specialized agents
run-agents: build
	@echo "Starting all agents..."
	@mkdir -p logs
	./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure -v 2>&1 | tee logs/agents.log

# Run simple demo
demo-simple: build
	@echo "Running simple demo..."
	@bash scripts/demo-simple.sh

# Run complex demo
demo-complex: build
	@echo "Running complex demo..."
	@bash scripts/demo-complex.sh

# Clean up
clean:
	@echo "Cleaning up..."
	@pkill -f "\./ai-orchestrator (coordinator|agents)" || true
	@sleep 1
	./ai-orchestrator cleanup --server localhost:9000 --insecure 2>/dev/null || true
	rm -f ai-orchestrator
	rm -f *.log
	@echo "✓ Cleanup complete"

# Run tests
test:
	@echo "Running tests..."
	go test -v ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Lint code
lint:
	@echo "Linting code..."
	golangci-lint run ./... || true

# Docker targets
docker-build:
	@echo "Building Docker images..."
	docker-compose build

docker-up:
	@echo "Starting Docker services..."
	docker-compose up -d

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down

docker-logs:
	@echo "Viewing Docker logs..."
	docker-compose logs -f

docker-setup:
	@echo "Running complete Docker setup..."
	@bash scripts/docker-setup.sh

docker-pull-models:
	@echo "Pulling Ollama models..."
	@bash scripts/pull-models.sh
