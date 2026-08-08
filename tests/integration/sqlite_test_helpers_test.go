//go:build integration && sqlite && cgo
// +build integration,sqlite,cgo

package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/internal/server"
)

type sqliteServerHandle struct {
	grpcTarget   string
	dbPath       string
	cancel       context.CancelFunc
	serverDone   chan error
	shutdownOnce sync.Once
}

func startSQLiteServer(t *testing.T, dbFilename string) *sqliteServerHandle {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, dbFilename)

	grpcPort := freePort(t)
	httpPort := freePort(t)
	grpcListenAddr := fmt.Sprintf(":%d", grpcPort)
	httpListenAddr := fmt.Sprintf(":%d", httpPort)
	httpProbeAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)

	config := server.DefaultConfig()
	config.StorageType = "sqlite"
	config.SQLiteDBPath = dbPath
	config.GRPCAddr = grpcListenAddr
	config.HTTPAddr = httpListenAddr
	config.IsDevelopment = true
	config.SchedulerIntervalMs = 300
	config.ReclaimIntervalMs = 2000

	srv, err := server.New(config)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	h := &sqliteServerHandle{
		grpcTarget: fmt.Sprintf("127.0.0.1:%d", grpcPort),
		dbPath:     dbPath,
		cancel:     cancel,
		serverDone: serverDone,
	}

	go func() {
		serverDone <- srv.Start(ctx)
	}()
	t.Cleanup(func() {
		h.shutdown(t)
	})

	require.NoError(t, waitForHTTPHealth(httpProbeAddr, 10*time.Second, serverDone))

	return h
}

func (h *sqliteServerHandle) shutdown(t *testing.T) {
	t.Helper()
	h.shutdownOnce.Do(func() {
		h.cancel()
		select {
		case err := <-h.serverDone:
			if err != nil {
				t.Logf("sqlite test server shutdown returned: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Log("sqlite test server shutdown still in progress; continuing teardown")
		}
	})
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		if closeErr := listener.Close(); closeErr != nil {
			t.Errorf("failed to close listener: %v", closeErr)
		}
	}()

	return listener.Addr().(*net.TCPAddr).Port
}

func waitForHTTPHealth(addr string, timeout time.Duration, serverDone <-chan error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	url := fmt.Sprintf("http://%s/health", addr)
	httpClient := &http.Client{}

	for {
		select {
		case err := <-serverDone:
			if err != nil {
				return fmt.Errorf("server exited before becoming ready: %w", err)
			}
			return fmt.Errorf("server exited before becoming ready")
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fmt.Errorf("failed to create health request: %w", err)
		}

		resp, err := httpClient.Do(req)
		if err == nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				return fmt.Errorf("failed to close health response body: %w", closeErr)
			}
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("HTTP health endpoint not reachable at %s within %s", url, timeout)
		case <-ticker.C:
		}
	}
}
