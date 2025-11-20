#!/bin/bash

# Cleanup Script for AI Agent Orchestrator
# Removes all queues and demo data

set -e

echo "🧹 AI Agent Orchestrator - Cleanup"
echo "==================================="
echo ""

# Check if chronoqueue server is running
echo "Checking ChronoQueue server..."
if ! timeout 2 bash -c "echo > /dev/tcp/localhost/9000" 2>/dev/null; then
    echo "❌ ChronoQueue server not running at localhost:9000"
    echo "   Cannot clean up queues without server connection"
    exit 1
fi
echo "✓ ChronoQueue server is running"
echo ""

echo "This will delete all AI orchestrator queues and their data."
read -p "Are you sure? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleanup cancelled"
    exit 0
fi

echo ""
echo "Cleaning up queues..."

# Use the CLI cleanup command
./ai-orchestrator cleanup --server localhost:9000 --insecure

echo ""
echo "✓ Cleanup complete!"
echo ""
