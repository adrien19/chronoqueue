.PHONY: help build run-init run-coordinator run-agents clean deps test

# Default target
help:
	@echo "AI Agent Orchestrator - Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make build         - Build the ai-orchestrator binary"
	@echo "  make deps          - Download Go dependencies"
	@echo "  make run-init      - Initialize all queues"
	@echo "  make run-coordinator - Start coordinator agent"
	@echo "  make run-agents    - Start all specialized agents"
	@echo "  make demo-simple   - Run simple demo"
	@echo "  make demo-complex  - Run complex demo"
	@echo "  make clean         - Clean up build artifacts and queues"
	@echo "  make test          - Run tests"
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

# Run coordinator agent
run-coordinator: build
	@echo "Starting coordinator agent..."
	./ai-orchestrator coordinator --workers 2 --server localhost:9000 --insecure

# Run all specialized agents
run-agents: build
	@echo "Starting all agents..."
	./ai-orchestrator agents --all --workers 2 --server localhost:9000 --insecure

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
