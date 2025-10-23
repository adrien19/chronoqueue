#!/bin/bash
# Universal service stopper script
# Usage: ./stop-service.sh <service-name>
# 
# Safely stops service processes without killing the parent make process.
# This script avoids using xargs which can inadvertently kill the calling process.

SERVICE=$1

if [ -z "$SERVICE" ]; then
    echo "Error: Service name required"
    echo "Usage: $0 <service-name>"
    echo "Services: frontend, api, workers, chronoqueue"
    exit 1
fi

case "$SERVICE" in
    frontend)
        echo "Stopping Next.js frontend processes..."
        # Kill next dev launcher
        pkill -f "next dev" 2>/dev/null || true
        # Kill all next-server processes individually
        for pid in $(pgrep -f "next-server"); do
            if ps -p $pid -o cmd= | grep -q "next-server"; then
                kill -9 $pid 2>/dev/null || true
            fi
        done
        echo "Frontend processes stopped"
        ;;
    
    api)
        echo "Stopping API server..."
        # Kill by binary name
        pkill -f "bin/api" 2>/dev/null || true
        # Double-check and kill any remaining
        for pid in $(pgrep -f "bin/api"); do
            if ps -p $pid -o cmd= | grep -q "bin/api"; then
                kill -9 $pid 2>/dev/null || true
            fi
        done
        echo "API server stopped"
        ;;
    
    workers)
        echo "Stopping worker processes..."
        # Kill by binary name
        pkill -f "bin/workers" 2>/dev/null || true
        # Double-check and kill any remaining
        for pid in $(pgrep -f "bin/workers"); do
            if ps -p $pid -o cmd= | grep -q "bin/workers"; then
                kill -9 $pid 2>/dev/null || true
            fi
        done
        echo "Worker processes stopped"
        ;;
    
    chronoqueue)
        echo "Stopping ChronoQueue server..."
        # Kill by process name
        pkill -f "chronoqueue-server" 2>/dev/null || true
        # Double-check and kill any remaining
        for pid in $(pgrep -f "chronoqueue-server"); do
            if ps -p $pid -o cmd= | grep -q "chronoqueue-server"; then
                kill -9 $pid 2>/dev/null || true
            fi
        done
        echo "ChronoQueue server stopped"
        ;;
    
    *)
        echo "Error: Unknown service '$SERVICE'"
        echo "Supported services: frontend, api, workers, chronoqueue"
        exit 1
        ;;
esac

exit 0
