// go.mod for refactored ChronoQueue with gRPC-Ecosystem

module github.com/adrien19/chronoqueue

go 1.25

require (
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/go-redsync/redsync/v4 v4.9.4
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.18.0
	github.com/hashicorp/vault/api v1.10.0

	// Existing dependencies (keep)
	github.com/redis/go-redis/v9 v9.0.3
	github.com/robfig/cron/v3 v3.0.1
	github.com/sirupsen/logrus v1.9.3

	// CLI and Configuration
	github.com/spf13/pflag v1.0.9
	google.golang.org/genproto/googleapis/api v0.0.0-20251007200510-49b9836ed3ff
	// Core gRPC and HTTP Gateway
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.10

// Removed dependencies (go-kit related)
// github.com/go-kit/kit v0.12.0 - REMOVED
// github.com/oklog/oklog v0.3.2 - REMOVED (can be replaced with signal handling)
)

require (
	// Existing indirect dependencies
	github.com/alicebob/gopher-json v0.0.0-20230218143504-906a9b012302 // indirect
	github.com/cenkalti/backoff/v3 v3.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-jose/go-jose/v3 v3.0.0 // indirect
	github.com/golang/protobuf v1.5.4
	github.com/gomodule/redigo v1.8.9 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.6.6 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-secure-stdlib/parseutil v0.1.6 // indirect
	github.com/hashicorp/go-secure-stdlib/strutil v0.1.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/yuin/gopher-lua v1.1.0 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251002232023-7c0ddcbb5797 // indirect
)

require (
	github.com/prometheus/client_golang v1.23.2
	github.com/spf13/cobra v1.10.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/fatih/color v1.15.0 // indirect
	github.com/hashicorp/go-hclog v1.2.2 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
)
