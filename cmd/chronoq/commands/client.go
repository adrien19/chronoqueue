package commands

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/credentials"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/cmd/chronoq/outputs"
)

// ClientOptions holds configuration for the ChronoQueue client
type ClientOptions struct {
	Server   string
	Insecure bool
	CertFile string
	KeyFile  string
	CAFile   string
	Timeout  time.Duration
	Verbose  bool
}

// GetClientOptions extracts client options from cobra command flags
func GetClientOptions(cmd *cobra.Command) (*ClientOptions, error) {
	server, _ := cmd.Flags().GetString("server")
	insecure, _ := cmd.Flags().GetBool("insecure")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	verbose, _ := cmd.Flags().GetBool("verbose")

	return &ClientOptions{
		Server:   server,
		Insecure: insecure,
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
		Timeout:  timeout,
		Verbose:  verbose,
	}, nil
}

// GetOutputFormat extracts output format from cobra command flags
func GetOutputFormat(cmd *cobra.Command) outputs.OutputFormat {
	output, _ := cmd.Flags().GetString("output")
	switch output {
	case "json":
		return outputs.OutputJSON
	case "yaml":
		return outputs.OutputYAML
	default:
		return outputs.OutputTable
	}
}

// CreateClient creates a new ChronoQueue client with the given options
func CreateClient(opts *ClientOptions) (*client.ChronoQueueClient, error) {
	// Create client options with default values
	clientOpts := client.ClientOptions{
		MaxRetries:          client.DefaultMaxRetries,
		InitialBackoff:      client.DefaultInitialBackoff,
		MaxBackoff:          client.DefaultMaxBackoff,
		MaxHeartBeatWorkers: client.DefaultMaxHeartBeatWorkers,
	}

	// Configure TLS credentials only if not using insecure connection
	if !opts.Insecure {
		var tlsConfig *tls.Config

		if opts.CertFile != "" && opts.KeyFile != "" {
			// mTLS with client certificates
			cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificates: %w", err)
			}

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
			}
		} else {
			// Server-side TLS only
			tlsConfig = &tls.Config{}
		}

		clientOpts.TLSCredentials = credentials.NewTLS(tlsConfig)
	}

	if opts.Timeout > 0 {
		clientOpts.DefaultRPCTimeout = opts.Timeout
	}
	log.Printf("Connecting to server at %s (insecure=%v)", opts.Server, opts.Insecure)
	// Create the client
	return client.NewChronoQueueClient(opts.Server, clientOpts)
}

// WithClient is a helper function that creates a client and passes it to a function
func WithClient(cmd *cobra.Command, fn func(*client.ChronoQueueClient) error) error {
	opts, err := GetClientOptions(cmd)
	if err != nil {
		return fmt.Errorf("failed to get client options: %w", err)
	}

	client, err := CreateClient(opts)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	return fn(client)
}

// CreateContext creates a context with timeout if specified
func CreateContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	timeout, _ := cmd.Flags().GetDuration("timeout")
	if timeout > 0 {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithCancel(context.Background())
}
