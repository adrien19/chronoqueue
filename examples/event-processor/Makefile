.PHONY: build clean run-init run-publish run-worker run-stats

BINARY=event-processor

build:
	go build -o $(BINARY) .

clean:
	rm -f $(BINARY)
	rm -rf events/test-load.json

tidy:
	go mod tidy

run-init: build
	./$(BINARY) init

run-publish-critical-webhook:
	./$(BINARY) publish events/critical-webhook.json

run-publish-mixed-batch:
	./$(BINARY) publish events/mixed-batch.json

run-publish-webhook-failures:
	./$(BINARY) publish events/webhook-failure.json

run-webhook-worker:
	./$(BINARY) worker --type=webhook --workers=2

run-notification-worker:
	./$(BINARY) worker --type=email --workers=2

run-sms-alerts-worker:
	./$(BINARY) worker --type=sms --workers=2

run-all-workers:
	./$(BINARY) worker --type=webhook --workers=2 &
	./$(BINARY) worker --type=email --workers=2 &
	./$(BINARY) worker --type=sms --workers=2

stop-workers:
	pkill -f "./$(BINARY) worker"

run-stats:
	./$(BINARY) stats

help:
	@echo "Event Processor Demo Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  build        - Build the event-processor binary"
	@echo "  clean        - Remove binary and generated files"
	@echo "  tidy         - Run go mod tidy"
	@echo "  run-init     - Initialize the system (create queues)"
	@echo "  run-publish  - Publish sample events"
	@echo "  run-worker   - Start a webhook worker"
	@echo "  run-stats    - Show queue statistics"
