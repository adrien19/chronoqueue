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

run-publish: build
	./$(BINARY) publish events/critical-webhook.json

run-worker: build
	./$(BINARY) worker --type=webhook --workers=2

run-stats: build
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
