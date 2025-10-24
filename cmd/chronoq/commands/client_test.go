package commands

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
)

func TestGetClientOptions(t *testing.T) {
	tests := []struct {
		name          string
		setupFlags    func(*cobra.Command)
		expectedOpts  *ClientOptions
		expectedError bool
	}{
		{
			name: "default options",
			setupFlags: func(cmd *cobra.Command) {
				cmd.Flags().String("server", "localhost:9000", "")
				cmd.Flags().Bool("insecure", false, "")
				cmd.Flags().String("cert-file", "", "")
				cmd.Flags().String("key-file", "", "")
				cmd.Flags().String("ca-file", "", "")
				cmd.Flags().Duration("timeout", 30*time.Second, "")
				cmd.Flags().Bool("verbose", false, "")
			},
			expectedOpts: &ClientOptions{
				Server:   "localhost:9000",
				Insecure: false,
				CertFile: "",
				KeyFile:  "",
				CAFile:   "",
				Timeout:  30 * time.Second,
				Verbose:  false,
			},
			expectedError: false,
		},
		{
			name: "with TLS credentials",
			setupFlags: func(cmd *cobra.Command) {
				cmd.Flags().String("server", "localhost:9000", "")
				cmd.Flags().Bool("insecure", false, "")
				cmd.Flags().String("cert-file", "client.crt", "")
				cmd.Flags().String("key-file", "client.key", "")
				cmd.Flags().String("ca-file", "ca.crt", "")
				cmd.Flags().Duration("timeout", 60*time.Second, "")
				cmd.Flags().Bool("verbose", true, "")
			},
			expectedOpts: &ClientOptions{
				Server:   "localhost:9000",
				Insecure: false,
				CertFile: "client.crt",
				KeyFile:  "client.key",
				CAFile:   "ca.crt",
				Timeout:  60 * time.Second,
				Verbose:  true,
			},
			expectedError: false,
		},
		{
			name: "insecure connection",
			setupFlags: func(cmd *cobra.Command) {
				cmd.Flags().String("server", "localhost:9000", "")
				cmd.Flags().Bool("insecure", true, "")
				cmd.Flags().String("cert-file", "", "")
				cmd.Flags().String("key-file", "", "")
				cmd.Flags().String("ca-file", "", "")
				cmd.Flags().Duration("timeout", 0, "")
				cmd.Flags().Bool("verbose", false, "")
			},
			expectedOpts: &ClientOptions{
				Server:   "localhost:9000",
				Insecure: true,
				CertFile: "",
				KeyFile:  "",
				CAFile:   "",
				Timeout:  0,
				Verbose:  false,
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			tt.setupFlags(cmd)

			opts, err := GetClientOptions(cmd)

			if tt.expectedError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedOpts, opts)
		})
	}
}

func TestGetOutputFormat(t *testing.T) {
	tests := []struct {
		name           string
		outputFlag     string
		expectedFormat outputs.OutputFormat
	}{
		{
			name:           "json output",
			outputFlag:     "json",
			expectedFormat: outputs.OutputJSON,
		},
		{
			name:           "yaml output",
			outputFlag:     "yaml",
			expectedFormat: outputs.OutputYAML,
		},
		{
			name:           "table output",
			outputFlag:     "table",
			expectedFormat: outputs.OutputTable,
		},
		{
			name:           "default output (empty)",
			outputFlag:     "",
			expectedFormat: outputs.OutputTable,
		},
		{
			name:           "unknown output format defaults to table",
			outputFlag:     "xml",
			expectedFormat: outputs.OutputTable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().String("output", tt.outputFlag, "")

			format := GetOutputFormat(cmd)
			assert.Equal(t, tt.expectedFormat, format)
		})
	}
}

func TestCreateClient_InsecureConnection(t *testing.T) {
	opts := &ClientOptions{
		Server:   "localhost:9000",
		Insecure: true,
		Timeout:  30 * time.Second,
	}

	// This test just ensures the client creation logic doesn't panic
	// In a real test environment, you'd need a mock server
	client, err := CreateClient(opts)

	// We expect an error since there's no server running, but no panic
	if client != nil {
		client.Close()
	}

	// The error could be a connection error, which is expected in tests
	// The important thing is that the function doesn't panic
	t.Logf("Client creation result: client=%v, err=%v", client != nil, err)
}

func TestCreateClient_WithTLSCredentials(t *testing.T) {
	// Test case where both cert and key files are specified
	opts := &ClientOptions{
		Server:   "localhost:9000",
		Insecure: false,
		CertFile: "nonexistent.crt",
		KeyFile:  "nonexistent.key",
		Timeout:  30 * time.Second,
	}

	client, err := CreateClient(opts)

	// We expect an error because the cert files don't exist
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "failed to load client certificates")
}

func TestCreateClient_ServerOnlyTLS(t *testing.T) {
	// Test case for server-side TLS only (no client certificates)
	opts := &ClientOptions{
		Server:   "localhost:9000",
		Insecure: false,
		CertFile: "",
		KeyFile:  "",
		Timeout:  30 * time.Second,
	}

	// This should create a client with TLS credentials but without client certs
	client, err := CreateClient(opts)

	// We expect a connection error since there's no server, but the client should be created
	if client != nil {
		client.Close()
	}

	// The error could be a connection error, which is expected in tests
	t.Logf("Server-only TLS client creation result: client=%v, err=%v", client != nil, err)
}

func TestCreateContext(t *testing.T) {
	tests := []struct {
		name         string
		timeout      time.Duration
		expectCancel bool
	}{
		{
			name:         "with timeout",
			timeout:      30 * time.Second,
			expectCancel: true,
		},
		{
			name:         "without timeout",
			timeout:      0,
			expectCancel: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().Duration("timeout", tt.timeout, "")

			ctx, cancel := CreateContext(cmd)
			defer cancel()

			assert.NotNil(t, ctx)
			if tt.expectCancel {
				assert.NotNil(t, cancel)
			}

			// Test that context can be cancelled
			cancel()
			select {
			case <-ctx.Done():
				// Context was cancelled as expected
			case <-time.After(100 * time.Millisecond):
				t.Error("Context was not cancelled within expected time")
			}
		})
	}
}

func TestClientOptions_Validation(t *testing.T) {
	tests := []struct {
		name    string
		opts    *ClientOptions
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid insecure options",
			opts: &ClientOptions{
				Server:   "localhost:9000",
				Insecure: true,
			},
			wantErr: false,
		},
		{
			name: "cert file without key file",
			opts: &ClientOptions{
				Server:   "localhost:9000",
				Insecure: false,
				CertFile: "client.crt",
				KeyFile:  "",
			},
			// This should not error at validation time, but during client creation
			wantErr: false,
		},
		{
			name: "key file without cert file",
			opts: &ClientOptions{
				Server:   "localhost:9000",
				Insecure: false,
				CertFile: "",
				KeyFile:  "client.key",
			},
			// This should not error at validation time, but during client creation
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Since there's no explicit validation function, we just test
			// that the struct can be created with these values
			assert.NotNil(t, tt.opts)
			assert.Equal(t, "localhost:9000", tt.opts.Server)
		})
	}
}
