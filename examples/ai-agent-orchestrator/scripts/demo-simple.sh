#!/bin/bash

# Simple Demo Script for AI Agent Orchestrator
# Demonstrates basic task submission and completion

set -e

echo "🎬 AI Agent Orchestrator - Simple Demo"
echo "======================================"
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

# Submit a simple task
echo "Step 3: Submitting a simple task..."
echo "Task: Web search for contact information"
./ai-orchestrator submit tasks/web-search.json --server localhost:9000 --insecure
echo ""

echo "✓ Simple demo complete!"
echo ""
echo "Next steps:"
echo "  1. Start agents to process tasks:"
echo "     ./ai-orchestrator agents --all --workers 1"
echo ""
echo "  2. Monitor task progress:"
echo "     ./ai-orchestrator monitor"
echo ""
echo "  3. Try the complex demo:"
echo "     ./scripts/demo-complex.sh"
echo ""
