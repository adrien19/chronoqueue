package helpers

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const TestAPIKey = "chronoqueue-integration-test-key"

func AuthenticatedGRPCDialOption(apiKey string) grpc.DialOption {
	return grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "api-key", apiKey)
		return invoker(ctx, method, req, reply, cc, opts...)
	})
}
