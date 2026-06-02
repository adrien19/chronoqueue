module github.com/adrien19/chronoqueue/examples/interview-platform/backend

go 1.25.0

require (
	github.com/adrien19/chronoqueue v0.0.0-00010101000000-000000000000
	github.com/go-chi/chi/v5 v5.0.11
	github.com/go-chi/cors v1.2.1
	github.com/golang/protobuf v1.5.4
	github.com/google/uuid v1.6.0
	github.com/mattn/go-sqlite3 v1.14.32
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.2 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251007200510-49b9836ed3ff // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251002232023-7c0ddcbb5797 // indirect
	google.golang.org/grpc v1.75.0 // indirect
)

replace github.com/adrien19/chronoqueue => ../../..
