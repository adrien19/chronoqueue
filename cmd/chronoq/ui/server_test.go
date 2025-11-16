package ui

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContextTimeout tests that context timeouts work correctly
func TestContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NotNil(t, ctx)

	// Verify timeout is set correctly
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.True(t, time.Until(deadline) > 0)
	assert.True(t, time.Until(deadline) <= 10*time.Second)
}

// TestHTTPRequest_WithCancellation tests request context cancellation
func TestHTTPRequest_WithCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	req := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	require.NotNil(t, req)
	assert.Error(t, req.Context().Err())
}

// Note: Full integration tests for UI endpoints are in tests/integration/ui_test.go
// Those tests use Testcontainers to start real Redis and ChronoQueue server instances.
