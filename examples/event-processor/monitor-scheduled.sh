#!/bin/bash
# Monitor scheduled messages moving to streams

REDIS_PASSWORD="mypassword"
QUEUE="${1:-email-notifications}"

echo "🔍 Monitoring scheduled messages for queue: $QUEUE"
echo "Press Ctrl+C to stop"
echo ""

while true; do
    clear
    echo "🔍 Monitoring: $QUEUE"
    echo "Time: $(date '+%H:%M:%S')"
    echo ""
    
    # Scheduled messages (sorted set)
    echo "📅 Scheduled Messages (schedule:$QUEUE):"
    SCHEDULED_COUNT=$(docker exec -i $(docker ps -q -f name=redis) redis-cli -a $REDIS_PASSWORD ZCARD schedule:$QUEUE 2>/dev/null)
    echo "  Count: $SCHEDULED_COUNT"
    
    if [ "$SCHEDULED_COUNT" -gt "0" ]; then
        echo "  Messages:"
        docker exec -i $(docker ps -q -f name=redis) redis-cli -a $REDIS_PASSWORD ZRANGE schedule:$QUEUE 0 -1 WITHSCORES 2>/dev/null | \
        awk 'NR%2==1 {msg=$0} NR%2==0 {
            timestamp=int($0/1000);
            cmd="date -d @" timestamp " +\"%H:%M:%S\"";
            cmd | getline scheduled_time;
            close(cmd);
            printf "    • %s → %s\n", msg, scheduled_time
        }'
    fi
    
    echo ""
    
    # Active streams (by priority)
    echo "🌊 Active Streams:"
    for priority in 25 50 75; do
        STREAM="stream:$priority:$QUEUE"
        STREAM_LEN=$(docker exec -i $(docker ps -q -f name=redis) redis-cli -a $REDIS_PASSWORD XLEN $STREAM 2>/dev/null)
        if [ "$STREAM_LEN" -gt "0" ]; then
            echo "  • $STREAM: $STREAM_LEN messages"
        fi
    done
    
    # Total stream count
    TOTAL_STREAM_COUNT=0
    for priority in 25 50 75; do
        STREAM="stream:$priority:$QUEUE"
        COUNT=$(docker exec -i $(docker ps -q -f name=redis) redis-cli -a $REDIS_PASSWORD XLEN $STREAM 2>/dev/null)
        TOTAL_STREAM_COUNT=$((TOTAL_STREAM_COUNT + COUNT))
    done
    echo "  Total in Streams: $TOTAL_STREAM_COUNT"
    
    echo ""
    echo "📊 Summary:"
    echo "  Scheduled (waiting): $SCHEDULED_COUNT"
    echo "  Active (processing): $TOTAL_STREAM_COUNT"
    
    sleep 2
done
