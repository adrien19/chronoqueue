#!/bin/bash
# Bulk Posting Demo Script
# This script demonstrates the bulk message posting feature of ChronoQueue

set -e

echo "=============================================="
echo "ChronoQueue Bulk Posting Demo"
echo "=============================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if event-processor binary exists
if [ ! -f "./event-processor" ]; then
    echo "Building event-processor..."
    go build -o event-processor .
fi

echo -e "${BLUE}Step 1: Initialize the system${NC}"
echo "Creating queues..."
./event-processor init
echo ""

echo -e "${BLUE}Step 2: Traditional Publishing (One-by-One)${NC}"
echo "Publishing 10 events individually..."
echo ""
time ./event-processor publish events/bulk-demo.json
echo ""

sleep 2

echo -e "${BLUE}Step 3: Bulk Publishing with ALL_OR_NOTHING Mode${NC}"
echo "Publishing 10 events in a single bulk operation..."
echo ""
time ./event-processor publish-bulk events/bulk-demo.json --mode all-or-nothing
echo ""

sleep 2

echo -e "${BLUE}Step 4: Bulk Publishing with BEST_EFFORT Mode${NC}"
echo "Publishing 20 events with partial success allowed..."
echo ""
time ./event-processor publish-bulk events/bulk-large-batch.json --mode best-effort
echo ""

sleep 2

echo -e "${BLUE}Step 5: Bulk Publishing Scheduled Messages${NC}"
echo "Publishing 10 scheduled events in bulk..."
echo ""
./event-processor publish-bulk events/bulk-scheduled.json
echo ""

sleep 2

echo -e "${BLUE}Step 6: View Queue Statistics${NC}"
./event-processor stats
echo ""

echo -e "${GREEN}=============================================="
echo "Demo Complete!"
echo "==============================================${NC}"
echo ""
echo "Key Observations:"
echo "  • Bulk posting is significantly faster (check the 'time' output)"
echo "  • ALL_OR_NOTHING ensures atomic operations"
echo "  • BEST_EFFORT allows partial success"
echo "  • Scheduled messages work with bulk posting"
echo ""
echo "Next steps:"
echo "  • Start workers: ./event-processor worker --type email"
echo "  • Monitor queues: ./event-processor monitor"
echo "  • View DLQ: ./event-processor dlq list"
