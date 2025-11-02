#!/bin/bash
# Quick demo of scheduled messages feature

echo "📅 ChronoQueue Scheduled Messages Demo"
echo "======================================"
echo ""

# Check if server is running
if ! lsof -ti :9000 > /dev/null 2>&1; then
    echo "❌ ChronoQueue server not running on port 9000"
    echo "   Start it with: ./chronoqueue server --dev --redis-password mypassword"
    exit 1
fi

echo "✅ ChronoQueue server is running"
echo ""

# Publish short-scheduled events (1-2 minutes for demo)
echo "📤 Publishing scheduled events (1-2 minute delays)..."
cat > /tmp/demo-scheduled.json <<EOF
{
    "events": [
        {
            "type": "email",
            "priority": "high",
            "schedule_in_minutes": 1,
            "data": {
                "recipient": "demo@example.com",
                "subject": "Demo: 1-minute scheduled email"
            }
        },
        {
            "type": "webhook",
            "priority": "medium",
            "schedule_in_minutes": 2,
            "data": {
                "webhook_url": "https://example.com/webhook",
                "payload": {"demo": "2-minute webhook"}
            }
        }
    ]
}
EOF

# Initialize event processor queue (if not already done - creates necessary queues)
./event-processor init --insecure

./event-processor publish /tmp/demo-scheduled.json --insecure
echo ""

# Check sorted sets
echo "🔍 Checking Redis sorted sets..."
echo ""
for q in email-notifications webhook-events; do
    echo "schedule:$q:"
    docker exec -i $(docker ps -q -f name=redis) redis-cli -a mypassword ZCARD schedule:$q 2>/dev/null
done
echo ""

# Show monitoring
echo "📊 Starting 30-second monitor (watch messages move to streams)..."
echo "   Messages will move when their scheduled time arrives"
echo ""
timeout 30 ./monitor-scheduled.sh email-notifications

echo ""
echo "✅ Demo complete!"
echo ""
echo "💡 Key Observations:"
echo "   • Messages start in schedule:{queue} sorted sets"
echo "   • Scheduler checks every 1 second"
echo "   • When time arrives, messages move to stream:{priority}:{queue}"
echo "   • Workers can then process them normally"
echo ""
