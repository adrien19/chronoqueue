# Service Management Scripts

This directory contains bash scripts for managing Interview Platform services.

## stop-service.sh

Universal service stopper script that safely terminates processes without affecting the parent make process or VS Code devcontainer port forwarding.

### Usage

```bash
./stop-service.sh <service-name>
```

### Supported Services

- `frontend` - Next.js development server
- `api` - Backend API server
- `workers` - Background worker processes
- `chronoqueue` - ChronoQueue server

### Why Use Scripts Instead of Direct pkill?

#### Problem with Direct pkill/xargs Approach

```bash
# ❌ This can kill the make process itself!
pkill -f "next-server" | xargs kill -9
```

When using `pkill` with `xargs` in a Makefile:

1. May kill the parent make process
2. May kill VS Code devcontainer port forwarding processes
3. Causes devcontainer to hang and require reload

#### Script-based Solution

```bash
# ✅ This safely iterates and kills only target processes
for pid in $(pgrep -f "next-server"); do
    if ps -p $pid -o cmd= | grep -q "next-server"; then
        kill -9 $pid 2>/dev/null || true
    fi
done
```

The script:

- Iterates through PIDs individually
- Verifies each PID matches the target process
- Avoids xargs which can kill the calling process
- Safe for use in VS Code devcontainers

### Benefits

1. **Consistent Behavior** - Same stopping logic for all services
2. **Devcontainer-Safe** - Won't break VS Code port forwarding
3. **Reusable** - Single script with service parameter
4. **Maintainable** - One place to update stopping logic
5. **Reliable** - Thoroughly kills all child processes

### Integration with Makefile

The script is called from Makefile targets:

```makefile
stop-frontend:
 @bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh frontend

stop-api:
 @bash $(WORKSPACE)/examples/interview-platform/scripts/stop-service.sh api
```

This provides:

- Clean `make stop-frontend`, `make stop-api`, etc. commands
- Consistent behavior across all services
- Easy testing: `./scripts/stop-service.sh frontend`
