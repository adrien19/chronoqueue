.PHONY: help stop clean build-api build-workers build-frontend build-all start-chronoqueue start-api start-workers start-frontend start-all run kill-all health logs

# Absolute paths
WORKSPACE := /workspaces/chronoqueue
BACKEND_DIR := $(WORKSPACE)/examples/interview-platform/backend
FRONTEND_DIR := $(WORKSPACE)/examples/interview-platform/frontend
LOGS_DIR := $(WORKSPACE)/logs
CHRONOQUEUE_BIN := $(WORKSPACE)/dist/linux_arm64/release/chronoqueue-server

# Binary paths
API_BIN := $(BACKEND_DIR)/bin/api
WORKERS_BIN := $(BACKEND_DIR)/bin/workers

# Process names for matching
CHRONOQUEUE_PROC := chronoqueue-server
API_PROC := bin/api
WORKERS_PROC := bin/workers
FRONTEND_PROC := next dev

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

## help: Display this help message
help:
	@echo "Interview Platform - Makefile Commands"
	@echo ""
	@echo "Setup Commands:"
	@echo "  make clean          - Flush Redis and clean up logs"
	@echo "  make build-all      - Build all services (API + Workers)"
	@echo "  make build-api      - Build backend API only"
	@echo "  make build-workers  - Build workers only"
	@echo "  make build-frontend - Install frontend dependencies"
	@echo ""
	@echo "Service Control:"
	@echo "  make start-all      - Start all services in order"
	@echo "  make stop           - Stop all services gracefully"
	@echo "  make kill-all       - Force kill all services by process name"
	@echo "  make run            - Clean, build, and start all services"
	@echo ""
	@echo "Individual Services:"
	@echo "  make start-chronoqueue - Start ChronoQueue server"
	@echo "  make start-api         - Start backend API"
	@echo "  make start-workers     - Start worker processes"
	@echo "  make start-frontend    - Start frontend dev server"
	@echo ""
	@echo "Individual Stop/Restart:"
	@echo "  make stop-frontend     - Stop frontend (devcontainer-safe)"
	@echo "  make stop-api          - Stop backend API"
	@echo "  make stop-workers      - Stop worker processes"
	@echo "  make stop-chronoqueue  - Stop ChronoQueue server"
	@echo "  make restart-frontend  - Safely restart frontend (recommended)"
	@echo "  make restart-api       - Restart backend API"
	@echo "  make restart-workers   - Restart worker processes"
	@echo ""
	@echo "Monitoring:"
	@echo "  make health         - Check health of all services"
	@echo "  make check-ports    - Check which ports are in use"
	@echo "  make logs           - Tail all service logs"
	@echo "  make logs-api       - Tail API logs"
	@echo "  make logs-workers   - Tail worker logs"
	@echo "  make logs-frontend  - Tail frontend logs"
	@echo "  make logs-chronoqueue - Tail ChronoQueue logs"
	@echo ""
	@echo "$(RED)Dangerous Commands (use with caution):$(NC)"
	@echo "  make kill-ports     - Kill by port (may break devcontainer!)"
	@echo ""

## stop: Gracefully stop all services
stop:
	@echo "$(YELLOW)Stopping all services...$(NC)"
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh chronoqueue
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh api
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh workers
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh frontend
	@echo "$(GREEN)All services stopped$(NC)"

## stop-frontend: Safely stop frontend dev server (devcontainer-safe)
stop-frontend:
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh frontend

## stop-api: Stop backend API server
stop-api:
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh api

## stop-workers: Stop worker processes
stop-workers:
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh workers

## stop-chronoqueue: Stop ChronoQueue server
stop-chronoqueue:
	@bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh chronoqueue

## kill-all: Force kill all services (same as stop, kept for backwards compatibility)
kill-all: stop

## kill-ports: Force kill processes on service ports (WARNING: May break devcontainer port forwarding!)
kill-ports:
	@echo "$(RED)WARNING: This may break VS Code devcontainer port forwarding!$(NC)"
	@echo "$(YELLOW)Use 'make stop-frontend' instead for safer frontend cleanup.$(NC)"
	@echo ""
	@read -p "Continue anyway? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "$(YELLOW)Killing processes on service ports...$(NC)"
	@echo "Checking ChronoQueue (port 50051)..."
	@lsof -ti:50051 | xargs kill -9 2>/dev/null || true
	@echo "Checking API (port 8080)..."
	@lsof -ti:8080 | xargs kill -9 2>/dev/null || true
	@echo "Checking Frontend (port 3000)..."
	@lsof -ti:3000 | xargs kill -9 2>/dev/null || true
	@echo "$(GREEN)Port cleanup complete$(NC)"

## clean: Flush Redis and clean logs
clean:
	@echo "$(YELLOW)Flushing Redis...$(NC)"
	@docker exec redis redis-cli -a mypassword FLUSHALL && echo "$(GREEN)Redis flushed successfully$(NC)"
	@echo "$(YELLOW)Cleaning logs...$(NC)"
	@mkdir -p $(LOGS_DIR)
	@rm -f $(LOGS_DIR)/*.log
	@echo "$(GREEN)Logs cleaned$(NC)"

## build-api: Build backend API
build-api:
	@echo "$(YELLOW)Building backend API...$(NC)"
	@cd $(BACKEND_DIR) && CGO_ENABLED=1 go build -o bin/api .
	@echo "$(GREEN)Backend API built successfully$(NC)"

## build-workers: Build worker processes
build-workers:
	@echo "$(YELLOW)Building workers...$(NC)"
	@cd $(BACKEND_DIR) && CGO_ENABLED=1 go build -o bin/workers ./cmd/workers
	@echo "$(GREEN)Workers built successfully$(NC)"

## build-frontend: Install frontend dependencies
build-frontend:
	@echo "$(YELLOW)Installing frontend dependencies...$(NC)"
	@cd $(FRONTEND_DIR) && npm install
	@echo "$(GREEN)Frontend dependencies installed$(NC)"

## build-all: Build all services
build-all: build-api build-workers
	@echo "$(GREEN)All services built successfully$(NC)"

## start-chronoqueue: Start ChronoQueue server
start-chronoqueue:
	@echo "$(YELLOW)Starting ChronoQueue server...$(NC)"
	@mkdir -p $(LOGS_DIR)
	@nohup $(CHRONOQUEUE_BIN) server --grpc-addr :50051 --http-addr :8090 --dev --redis-password mypassword > $(LOGS_DIR)/chronoqueue.log 2>&1 & echo "$(GREEN)ChronoQueue started with PID: $$!$(NC)"
	@sleep 2
	@curl -s http://localhost:8090/health && echo "" || echo "$(RED)ChronoQueue health check failed$(NC)"

## start-api: Start backend API
start-api:
	@echo "$(YELLOW)Starting backend API...$(NC)"
	@mkdir -p $(LOGS_DIR)
	@cd $(BACKEND_DIR) && nohup ./bin/api > $(LOGS_DIR)/api.log 2>&1 & echo "$(GREEN)API started with PID: $$!$(NC)"
	@sleep 1

## start-workers: Start worker processes
start-workers:
	@echo "$(YELLOW)Starting workers...$(NC)"
	@mkdir -p $(LOGS_DIR)
	@cd $(BACKEND_DIR) && nohup ./bin/workers > $(LOGS_DIR)/workers.log 2>&1 & echo "$(GREEN)Workers started with PID: $$!$(NC)"
	@sleep 1

## start-frontend: Start frontend dev server
start-frontend:
	@echo "$(YELLOW)Starting frontend dev server...$(NC)"
	@mkdir -p $(LOGS_DIR)
	@cd $(FRONTEND_DIR) && nohup npm run dev > $(LOGS_DIR)/frontend.log 2>&1 & echo "$(GREEN)Frontend started with PID: $$!$(NC)"
	@sleep 1

## restart-frontend: Safely restart frontend dev server (devcontainer-safe)
restart-frontend: stop-frontend
	@echo "$(YELLOW)Waiting for port to be released...$(NC)"
	@sleep 2
	@$(MAKE) start-frontend

## restart-api: Restart backend API server
restart-api: stop-api
	@sleep 1
	@$(MAKE) start-api

## restart-workers: Restart worker processes
restart-workers: stop-workers
	@sleep 1
	@$(MAKE) start-workers

## start-all: Start all services in dependency order
start-all: start-chronoqueue start-api start-workers start-frontend
	@echo ""
	@echo "$(GREEN)================================================$(NC)"
	@echo "$(GREEN)All services started successfully!$(NC)"
	@echo "$(GREEN)================================================$(NC)"
	@echo ""
	@echo "Service URLs:"
	@echo "  - ChronoQueue gRPC: localhost:50051"
	@echo "  - ChronoQueue HTTP: http://localhost:8090"
	@echo "  - Backend API:      http://localhost:8080"
	@echo "  - Frontend:         http://localhost:3000"
	@echo ""
	@echo "Log files:"
	@echo "  - ChronoQueue: $(LOGS_DIR)/chronoqueue.log"
	@echo "  - API:         $(LOGS_DIR)/api.log"
	@echo "  - Workers:     $(LOGS_DIR)/workers.log"
	@echo "  - Frontend:    $(LOGS_DIR)/frontend.log"
	@echo ""
	@echo "Run 'make logs' to tail all logs"
	@echo "Run 'make health' to check service health"
	@echo ""

## run: Complete workflow - clean, build, and start all services
run: kill-ports clean build-all start-all
	@echo "$(GREEN)Platform is ready for use!$(NC)"

## health: Check health of all services
health:
	@echo "$(YELLOW)Checking service health...$(NC)"
	@echo ""
	@echo "ChronoQueue:"
	@curl -s http://localhost:8090/health && echo " $(GREEN)✓$(NC)" || echo "$(RED)✗ Not responding$(NC)"
	@echo ""
	@echo "Backend API:"
	@curl -s http://localhost:8080/api/health && echo " $(GREEN)✓$(NC)" || echo "$(RED)✗ Not responding$(NC)"
	@echo ""
	@echo "Frontend:"
	@curl -s http://localhost:3000 > /dev/null 2>&1 && echo "$(GREEN)✓ Running$(NC)" || echo "$(RED)✗ Not responding$(NC)"
	@echo ""
	@echo "Running processes:"
	@ps aux | grep -E '$(CHRONOQUEUE_PROC)|$(API_PROC)|$(WORKERS_PROC)|$(FRONTEND_PROC)' | grep -v grep || echo "$(RED)No services running$(NC)"

## check-ports: Check which ports are in use
check-ports:
	@echo "$(YELLOW)Checking port usage...$(NC)"
	@echo ""
	@echo "Port 50051 (ChronoQueue gRPC):"
	@lsof -i:50051 2>/dev/null && echo "" || echo "$(GREEN)✓ Available$(NC)"
	@echo ""
	@echo "Port 8090 (ChronoQueue HTTP):"
	@lsof -i:8090 2>/dev/null && echo "" || echo "$(GREEN)✓ Available$(NC)"
	@echo ""
	@echo "Port 8080 (Backend API):"
	@lsof -i:8080 2>/dev/null && echo "" || echo "$(GREEN)✓ Available$(NC)"
	@echo ""
	@echo "Port 3000 (Frontend):"
	@lsof -i:3000 2>/dev/null && echo "" || echo "$(GREEN)✓ Available$(NC)"

## logs: Tail all service logs
logs:
	@echo "$(YELLOW)Tailing all logs (Ctrl+C to exit)...$(NC)"
	@tail -f $(LOGS_DIR)/chronoqueue.log $(LOGS_DIR)/api.log $(LOGS_DIR)/workers.log $(LOGS_DIR)/frontend.log

## logs-chronoqueue: Tail ChronoQueue logs
logs-chronoqueue:
	@tail -f $(LOGS_DIR)/chronoqueue.log

## logs-api: Tail API logs
logs-api:
	@tail -f $(LOGS_DIR)/api.log

## logs-workers: Tail worker logs
logs-workers:
	@tail -f $(LOGS_DIR)/workers.log

## logs-frontend: Tail frontend logs
logs-frontend:
	@tail -f $(LOGS_DIR)/frontend.log

## rebuild-api: Rebuild and restart API only
rebuild-api: build-api
	@echo "$(YELLOW)Restarting API...$(NC)"
	-@pkill -f '$(API_PROC)' 2>/dev/null || true
	@sleep 1
	@$(MAKE) start-api

## rebuild-workers: Rebuild and restart workers only
rebuild-workers: build-workers
	@echo "$(YELLOW)Restarting workers...$(NC)"
	-@pkill -f '$(WORKERS_PROC)' 2>/dev/null || true
	@sleep 1
	@$(MAKE) start-workers

## restart-chronoqueue: Restart ChronoQueue only
restart-chronoqueue:
	@echo "$(YELLOW)Restarting ChronoQueue...$(NC)"
	-@pkill -f '$(CHRONOQUEUE_PROC)' 2>/dev/null || true
	@sleep 2
	@$(MAKE) start-chronoqueue

## quick-restart: Restart all services without rebuilding
quick-restart: stop start-all
	@echo "$(GREEN)All services restarted$(NC)"
