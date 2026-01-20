#!/bin/bash
# ChronoQueue Monitoring Stack Validation Script

set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "=========================================="
echo "ChronoQueue Monitoring Stack Validation"
echo "=========================================="
echo ""

# Function to check if a service is responding
check_http_service() {
    local name=$1
    local url=$2
    local expected_code=${3:-200}
    
    echo -n "Checking $name at $url... "
    
    if response=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 "$url" 2>/dev/null); then
        if [ "$response" -eq "$expected_code" ]; then
            echo -e "${GREEN}✓ OK${NC} (HTTP $response)"
            return 0
        else
            echo -e "${YELLOW}⚠ WARNING${NC} (HTTP $response, expected $expected_code)"
            return 1
        fi
    else
        echo -e "${RED}✗ FAILED${NC} (Connection failed)"
        return 1
    fi
}

# Function to check if metrics contain expected data
check_metrics() {
    local url=$1
    local metric_name=$2
    
    echo -n "Checking for metric '$metric_name'... "
    
    if curl -s --connect-timeout 5 "$url" 2>/dev/null | grep -q "$metric_name"; then
        echo -e "${GREEN}✓ FOUND${NC}"
        return 0
    else
        echo -e "${RED}✗ NOT FOUND${NC}"
        return 1
    fi
}

# Function to check Prometheus target status
check_prometheus_target() {
    echo -n "Checking Prometheus targets... "
    
    if targets=$(curl -s http://localhost:9090/api/v1/targets 2>/dev/null); then
        if echo "$targets" | grep -q '"health":"up"'; then
            up_count=$(echo "$targets" | grep -o '"health":"up"' | wc -l)
            echo -e "${GREEN}✓ OK${NC} ($up_count target(s) up)"
            return 0
        else
            echo -e "${RED}✗ FAILED${NC} (No targets up)"
            return 1
        fi
    else
        echo -e "${RED}✗ FAILED${NC} (Could not query Prometheus)"
        return 1
    fi
}

# Function to check Docker containers
check_docker_containers() {
    echo ""
    echo "Docker Container Status:"
    echo "------------------------"
    
    for container in chronoqueue-server chronoqueue-prometheus chronoqueue-grafana redis_container; do
        if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
            status=$(docker ps --filter "name=^${container}$" --format '{{.Status}}')
            echo -e "${GREEN}✓${NC} $container: $status"
        else
            echo -e "${RED}✗${NC} $container: NOT RUNNING"
        fi
    done
}

# Function to check Docker networks
check_docker_networks() {
    echo ""
    echo "Docker Network Status:"
    echo "----------------------"
    
    for network in chronoqueue-network demo_network; do
        if docker network ls --format '{{.Name}}' | grep -q "^${network}$"; then
            container_count=$(docker network inspect "$network" -f '{{len .Containers}}' 2>/dev/null || echo "0")
            echo -e "${GREEN}✓${NC} $network: $container_count container(s) connected"
        else
            echo -e "${RED}✗${NC} $network: NOT FOUND"
        fi
    done
}

echo "Step 1: Checking Docker Status"
echo "==============================="
check_docker_containers
check_docker_networks

echo ""
echo "Step 2: Checking Service Endpoints"
echo "===================================="
sleep 2  # Give services time to initialize

check_http_service "ChronoQueue HTTP API" "http://localhost:8080/health" 200 || true
check_http_service "ChronoQueue Metrics" "http://localhost:8080/metrics" 200 || true
check_http_service "Prometheus" "http://localhost:9090/-/healthy" 200 || true
check_http_service "Grafana" "http://localhost:3000/api/health" 200 || true
check_http_service "Redis Insight" "http://localhost:8001" 200 || true

echo ""
echo "Step 3: Checking Metrics Data"
echo "=============================="
check_metrics "http://localhost:8080/metrics" "chronoqueue_queues_total" || true
check_metrics "http://localhost:8080/metrics" "chronoqueue_messages_enqueued_total" || true
check_metrics "http://localhost:8080/metrics" "chronoqueue_messages_by_state" || true

echo ""
echo "Step 4: Checking Prometheus Integration"
echo "========================================="
check_prometheus_target || true

echo ""
echo "Step 5: Quick Links"
echo "==================="
echo "Grafana Dashboard:    http://localhost:3000 (admin/admin)"
echo "Prometheus:           http://localhost:9090"
echo "ChronoQueue Metrics:  http://localhost:8080/metrics"
echo "Redis Insight:        http://localhost:8001"
echo ""

# Summary
echo "=========================================="
echo "Validation Complete!"
echo "=========================================="
echo ""
echo "If all checks passed, your monitoring stack is ready!"
echo ""
echo "Next steps:"
echo "1. Open Grafana: http://localhost:3000"
echo "2. Login with admin/admin"
echo "3. Navigate to ChronoQueue folder → ChronoQueue - Main Dashboard"
echo "4. Create some queues and messages to see metrics populate"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f chronoqueuesvc"
echo "  docker-compose -f docker-compose.monitoring.yaml logs -f"
echo ""
