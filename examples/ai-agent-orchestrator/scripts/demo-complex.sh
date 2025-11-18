#!/bin/bash

# Complex Demo Script for AI Agent Orchestrator
# Demonstrates multi-agent coordination, priority handling, and parallel execution

set -e

echo "🎬 AI Agent Orchestrator - Complex Demo"
echo "========================================="
echo ""

# Check if chronoqueue server is running
echo "Step 1: Checking ChronoQueue server..."
if ! timeout 2 bash -c "echo > /dev/tcp/localhost/9000" 2>/dev/null; then
    echo "⚠️  Unable to verify ChronoQueue server at localhost:9000"
    echo "   Attempting to continue anyway..."
else
    echo "✓ ChronoQueue server is running"
fi
echo ""

# Initialize queues
echo "Step 2: Initializing queues..."
./ai-orchestrator init --server localhost:9000 --insecure
echo ""

# Submit multiple tasks with different priorities
echo "Step 3: Submitting multiple tasks with different priorities..."
echo ""

echo "Task 1: High-priority competitor analysis (priority: 8)"
./ai-orchestrator submit tasks/competitor-analysis.json --server localhost:9000 --insecure
echo ""

echo "Task 2: Medium-priority market research (priority: 5)"
./ai-orchestrator submit tasks/market-research.json --server localhost:9000 --insecure
echo ""

echo "Task 3: High-priority code review (priority: 7)"
./ai-orchestrator submit tasks/code-review.json --server localhost:9000 --insecure
echo ""

echo "Task 4: Low-priority web search (priority: 3)"
./ai-orchestrator submit tasks/web-search.json --server localhost:9000 --insecure
echo ""

echo "✓ All tasks submitted!"
echo ""
echo "Expected behavior:"
echo "  - Tasks processed in priority order: 8 → 7 → 5 → 3"
echo "  - Coordinator decomposes complex tasks (Phase 2 - Coming Soon)"
echo "  - Multiple agents process subtasks in parallel (Phase 3 - Coming Soon)"
echo "  - Results aggregated into final reports (Phase 4 - Coming Soon)"
echo ""
echo "Current status (Phase 1 - Foundation):"
echo "  ✓ Queues created"
echo "  ✓ Tasks submitted to coordinator queue"
echo "  ⏳ Coordinator agent implementation (Phase 2)"
echo "  ⏳ Specialized agents implementation (Phase 3)"
echo "  ⏳ Result aggregation (Phase 4)"
echo ""
echo "Next steps:"
echo "  1. Monitor queue state:"
echo "     ./ai-orchestrator monitor"
echo ""
echo "  2. Check submitted tasks in Redis:"
echo "     redis-cli ZRANGE agent-coordinator 0 -1 WITHSCORES"
echo ""
echo "  3. Implement Phase 2 (Coordinator Agent)"
echo "     See: Autonomous_AI_Agent_Task_Orchestrator_Example.md"
echo ""
