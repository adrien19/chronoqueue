#!/bin/bash
# Cleanup script for Event Processing System demo

echo "🧹 Cleaning up Event Processing System..."

# Stop any running event-processor workers and monitors
echo "  Stopping workers..."
pkill -f "event-processor worker" 2>/dev/null || true
pkill -f "monitor-scheduled.sh" 2>/dev/null || true

# Stop ChronoQueue server if running on default ports
echo "  Checking for ChronoQueue server..."
SERVER_PID=$(lsof -ti :9000 2>/dev/null)
if [ -n "$SERVER_PID" ]; then
    echo "  Stopping ChronoQueue server (PID: $SERVER_PID)..."
    kill $SERVER_PID 2>/dev/null || true
    sleep 1
fi

# Clean up generated files
echo "  Removing generated files..."
rm -f event-processor
rm -f events/test-load.json
rm -f events/generated.json

# Clean up log files
rm -f /tmp/chronoqueue.log 2>/dev/null || true

echo "✨ Cleanup complete!"
