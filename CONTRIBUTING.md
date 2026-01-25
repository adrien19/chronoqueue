# Contributing to ChronoQueue

Thank you for your interest in contributing to ChronoQueue! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [CI/CD Pipeline](#cicd-pipeline)

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please be respectful and professional in all interactions.

## Getting Started

### Prerequisites

- **Docker Desktop** or **Docker Engine** with Docker Compose
- **VS Code** (recommended) with Remote - Containers extension
- **Git**

### Setting Up Development Environment

#### 🎯 **Recommended: Using Dev Container** (Preferred Method)

The **dev container** is the recommended approach for developing ChronoQueue. It provides a consistent, portable development environment with all dependencies pre-installed.

**Benefits:**

- ✅ **Zero Configuration**: All tools and dependencies pre-installed
- ✅ **Consistency**: Same environment for all developers and CI
- ✅ **Isolation**: No conflicts with your local system
- ✅ **Quick Setup**: Ready to code in minutes
- ✅ **Includes**: Go 1.25, PostgreSQL, SQLite, Docker, kubectl, all dev tools

**Setup Steps:**

1. **Install prerequisites:**
   - [Docker Desktop](https://www.docker.com/products/docker-desktop) (Windows/Mac)
   - [VS Code](https://code.visualstudio.com/)
   - [Remote - Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

2. **Fork and clone the repository:**

   ```bash
   git clone https://github.com/YOUR_USERNAME/chronoqueue.git
   cd chronoqueue
   ```

3. **Open in dev container:**

   **Option A: VS Code**
   - Open the folder in VS Code
   - When prompted "Reopen in Container", click **Reopen in Container**
   - Or: Press `F1` → `Dev Containers: Reopen in Container`

   **Option B: Command Line**

   ```bash
   code .
   # VS Code will detect .devcontainer and prompt you
   ```

4. **Wait for container to build** (first time only, ~5-10 minutes)
   - Container includes: Go, PostgreSQL, SQLite, Docker-in-Docker, kubectl, and all dev tools
   - Subsequent starts are instant

5. **Verify setup:**

   ```bash
   # Check Go version
   go version  # Should show Go 1.25

   # Check tools are installed
   make check-linter
   make check-gotestsum

   # Run tests
   make test
   ```

6. **Start coding!** 🚀
   - All dependencies are ready
   - PostgreSQL and SQLite support is available
   - Docker-in-Docker is configured for integration tests

#### 🔧 **Alternative: Manual Setup** (Not Recommended)

If you cannot use dev containers, you can set up manually:

**Prerequisites:**

- Go 1.25 or later
- Docker and Docker Compose
- PostgreSQL 14+ or SQLite 3.35+ (for storage)
- Make
- Git

**Steps:**

1. **Fork and clone the repository:**

   ```bash
   git clone https://github.com/YOUR_USERNAME/chronoqueue.git
   cd chronoqueue
   ```

2. **Install dependencies:**

   ```bash
   go mod download
   ```

3. **Install development tools:**

   ```bash
   # Install golangci-lint
   make check-linter

   # Install gotestsum
   make check-gotestsum

   # Install proto tools (if working with proto files)
   make init-proto
   ```

4. **Verify setup:**

   ```bash
   make test
   ```

   > **Note**: Integration tests use testcontainers and will automatically manage PostgreSQL and SQLite containers. No manual database setup required!

## Development Workflow

### 1. Create a Feature Branch

Always create a new branch for your work:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

Branch naming conventions:

- `feature/` - New features
- `fix/` - Bug fixes
- `refactor/` - Code refactoring
- `docs/` - Documentation changes
- `test/` - Test additions or modifications
- `chore/` - Maintenance tasks

### 2. Make Your Changes

- Write clean, readable code
- Follow Go best practices and idioms
- Add tests for new functionality
- Update documentation as needed
- Keep commits atomic and focused

### 3. Test Your Changes

```bash
# Run all tests
make test

# Run linting
make lint

# Run formatting
make format

# Run the full check (format, test, lint)
make check
```

### 4. Commit Your Changes

Use conventional commit messages:

```bash
git commit -m "feat: add new queue scheduling feature"
git commit -m "fix: resolve memory leak in message processing"
git commit -m "docs: update API documentation"
```

Commit types:

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, missing semicolons, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `build`: Build system changes
- `ci`: CI/CD changes
- `chore`: Other changes (dependency updates, etc.)
- `revert`: Reverting previous commits

### 5. Push to Your Fork

```bash
git push origin feature/your-feature-name
```

## Pull Request Process

### Before Submitting

1. **Rebase on latest develop**

   ```bash
   git fetch upstream
   git rebase upstream/develop
   ```

2. **Run all checks locally**

   ```bash
   make check
   ```

3. **Update documentation**
   - Update README.md if needed
   - Add/update code comments
   - Update API documentation

### Submitting a Pull Request

1. **Create PR on GitHub**
   - Use a clear, descriptive title
   - Follow the PR template
   - Reference related issues

2. **PR Title Format**

   ```
   feat: add calendar-based scheduling
   fix: resolve race condition in message ack
   docs: improve getting started guide
   ```

3. **PR Description Should Include**
   - Summary of changes
   - Motivation and context
   - How to test the changes
   - Screenshots (if UI changes)
   - Checklist of completed items

### Review Process

- All PRs require at least one approval
- CI checks must pass
- Address review comments promptly
- Keep PR size reasonable (<500 lines when possible)

## Coding Standards

### Go Code Style

Follow the official Go style guide and these project-specific conventions:

1. **Use `gofmt` and `goimports`**

   ```bash
   make format
   ```

2. **Follow Go best practices**
   - Use meaningful variable names
   - Keep functions small and focused
   - Add package and function documentation
   - Handle errors explicitly

3. **Code Organization**
   - Group related functionality
   - Use appropriate package structure
   - Minimize dependencies between packages

### Example Code Style

```go
// Package queue provides priority queue management functionality.
package queue

import (
    "context"
    "errors"
    "time"
)

// QueueManager handles queue operations with priority support.
type QueueManager struct {
    storage repository.Storage
    logger *log.Logger
}

// CreateQueue creates a new priority queue with the given configuration.
// Returns an error if the queue already exists or validation fails.
func (qm *QueueManager) CreateQueue(ctx context.Context, cfg *QueueConfig) error {
    if err := cfg.Validate(); err != nil {
        return fmt.Errorf("invalid queue config: %w", err)
    }
    
    // Implementation...
    return nil
}
```

## Testing Guidelines

### Writing Tests

1. **Unit Tests**
   - Test individual functions/methods
   - Use table-driven tests
   - Mock external dependencies

   ```go
   func TestQueueManager_CreateQueue(t *testing.T) {
       tests := []struct {
           name    string
           cfg     *QueueConfig
           wantErr bool
       }{
           {
               name: "valid config",
               cfg:  &QueueConfig{Name: "test", Priority: 1},
               wantErr: false,
           },
           // More test cases...
       }
       
       for _, tt := range tests {
           t.Run(tt.name, func(t *testing.T) {
               // Test implementation...
           })
       }
   }
   ```

2. **Integration Tests**
   - Test with real dependencies (PostgreSQL/SQLite, ChronoQueue server)
   - Place in `tests/integration/`
   - Use build tag `//go:build integration`
   - **Note**: Integration tests use [testcontainers](https://golang.testcontainers.org/) to automatically manage Docker containers for PostgreSQL, SQLite, and other dependencies. When using the dev container, Docker-in-Docker is pre-configured, so tests work seamlessly without any additional setup.

3. **E2E Tests**
   - Test complete workflows
   - Place in `tests/e2e/`
   - Use build tag `//go:build e2e`
   - Also uses testcontainers for environment management

4. **Test Coverage**
   - Aim for >80% coverage for new code
   - Use `make ci-test` to generate coverage reports

### Running Tests

> **💡 Pro Tip**: When using the **dev container**, all test infrastructure (Docker, testcontainers, PostgreSQL, SQLite) is pre-configured. Simply run the commands below without any setup!

```bash
# All tests (unit, integration, e2e)
make test-all

# Unit tests only (fast)
make test

# Integration tests only
make test-integration

# E2E tests only
make test-e2e

# Specific package
go test ./pkg/queue/...

# With coverage (unit tests)
make ci-test

# With coverage (integration)
make ci-test-integration

# With coverage (all tests)
make ci-test-all

# Race detection
make test-race
```

**Testcontainers Note**: Integration and E2E tests automatically start required services (PostgreSQL/SQLite, ChronoQueue) in Docker containers and clean them up after tests complete. No manual service management needed!

## CI/CD Pipeline

All pull requests automatically trigger our CI pipeline. See [CI/CD Guide](.github/CI_CD_GUIDE.md) for details.

### CI Checks

Your PR must pass these checks:

- ✅ **Code Quality & Linting** - golangci-lint passes
- ✅ **Unit Tests** - All tests pass with good coverage
- ✅ **Integration Tests** - Integration tests pass
- ✅ **Build** - Binaries build successfully
- ✅ **Docker Build** - Docker image builds (for relevant changes)

### Local CI Simulation

Run the same checks locally:

```bash
# Lint (CI format)
make ci-lint

# Tests (CI format with coverage)
make ci-test

# Build (CI format)
make ci-build
```

## Project Structure

```
chronoqueue/
├── api/                 # Generated gRPC code
├── cmd/                 # Command-line tools
├── client/              # Go client SDK
├── deploy/              # Deployment configurations
├── docker/              # Docker and dev container files
├── docs/                # Documentation
├── examples/            # Example applications
├── images/              # Dockerfile for releases
├── internal/            # Internal packages
│   ├── encryption/      # Encryption logic
│   ├── server/          # Server implementation
│   └── util/            # Utilities
├── pkg/                 # Public packages
│   ├── calendar/        # Calendar scheduling
│   ├── chronoqueue/     # Core queue logic
│   ├── gateway/         # gRPC gateway
│   ├── log/             # Logging
│   ├── metrics/         # Metrics
│   ├── repository/      # Data access layer
│   └── schema/          # Data schemas
├── proto/               # Protocol buffer definitions
└── tests/               # Test files
    ├── e2e/             # End-to-end tests
    ├── integration/     # Integration tests
    └── fixtures/        # Test data
```

## Working with Protocol Buffers

If you're modifying `.proto` files:

1. **Install proto tools**

   ```bash
   make init-proto
   ```

2. **Modify proto files** in `proto/` directory

3. **Generate code**

   ```bash
   make gen-proto
   ```

4. **Commit both `.proto` and generated files**

## Questions and Support

- **Questions**: Open a GitHub Discussion
- **Bugs**: Open a GitHub Issue
- **Security**: Email <security@chronoqueue.io> (or repository owner)

## Recognition

Contributors will be recognized in:

- GitHub contributors list
- Release notes (for significant contributions)
- Project documentation (for major features)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to ChronoQueue! 🚀
