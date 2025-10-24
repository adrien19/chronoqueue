# ChronoQueue Examples

This directory contains sample applications demonstrating how ChronoQueue can be integrated into various real-world use cases. Each example showcases different features and best practices for building queue-based applications.

## 📚 Available Examples

### 1. [Interview Evaluation Platform](./interview-platform/)

**Status**: 📝 Planning Phase

A comprehensive sample application demonstrating **all ChronoQueue features** through a practical interview evaluation system.

**Features Demonstrated**:

- ✅ Priority Queues (urgent vs standard evaluations)
- ✅ Scheduled Messages (business hours processing)
- ✅ Calendar Schedules (daily/weekly analytics reports)
- ✅ Dead Letter Queues (DLQ) & Retry Logic
- ✅ Schema Validation
- ✅ Multi-tenant Isolation
- ✅ Heartbeat & Lease Renewal
- ✅ Real-time Updates (SSE)

**Tech Stack**: Next.js 14, Go, SQLite, Clerk Auth, Tailwind CSS

**Use Case**: Learn how to build a complete queue-based application with frontend, backend, and worker services.

**[View Full Documentation →](./interview-platform/README.md)**

---

## 🚀 Getting Started

Each example contains:

- **README.md** - Overview and setup instructions
- **Architecture documentation** - Design decisions and patterns
- **Demo scripts** - Interactive demonstrations of features
- **Complete source code** - Production-ready examples

### Prerequisites

- Docker & Docker Compose (for ChronoQueue server)
- Go 1.21+ (for backend examples)
- Node.js 18+ (for frontend examples, if applicable)

### Quick Start

```bash
# Navigate to an example
cd interview-platform/

# Follow the README instructions
cat README.md
```

---

## 🎯 Learning Path

### For Beginners

1. Start with **Interview Platform** - Priority Queues basics
2. Run demo scripts to see features in action
3. Modify the code to experiment

### For Intermediate Users

1. Study the worker implementations
2. Understand DLQ and retry strategies
3. Implement your own message schemas

### For Advanced Users

1. Review multi-tenant architecture patterns
2. Optimize worker concurrency
3. Build custom monitoring and observability

---

## 🤝 Contributing

Have an interesting use case? We'd love to see it! Contributions are welcome:

1. Create a new directory under `examples/`
2. Include comprehensive README with:
   - Architecture overview
   - Setup instructions
   - Feature demonstrations
   - Learning resources
3. Ensure code is well-documented
4. Add demo scripts

---

## 📖 Additional Resources

- [ChronoQueue Main Documentation](../README.md)
- [Integration Tests](../tests/integration/) - See test examples for patterns
- [API Documentation](../docs/api/)

---

## 💡 Use Case Ideas

Looking for inspiration? Here are potential examples we'd like to see:

- **E-commerce Order Processing** - Payment processing, inventory updates, shipping notifications
- **IoT Data Pipeline** - Sensor data ingestion, real-time processing, analytics
- **Content Moderation System** - AI-powered content review with priority handling
- **Distributed Task Scheduler** - Cron-like job scheduling across multiple workers
- **Event-Driven Microservices** - Service-to-service communication patterns

Have another idea? Open an issue or submit a PR!
