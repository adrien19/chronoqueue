package gateway

import (
	"context"
	"net"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestAuthInterceptor(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	tests := []struct {
		name     string
		enabled  bool
		metadata metadata.MD
		wantCode codes.Code
	}{
		{name: "disabled", wantCode: codes.OK},
		{name: "missing credentials", enabled: true, wantCode: codes.Unauthenticated},
		{name: "invalid API key", enabled: true, metadata: metadata.Pairs("api-key", "wrong"), wantCode: codes.Unauthenticated},
		{name: "valid API key", enabled: true, metadata: metadata.Pairs("api-key", "secret"), wantCode: codes.OK},
		{name: "valid bearer token", enabled: true, metadata: metadata.Pairs("authorization", "Bearer secret"), wantCode: codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.metadata != nil {
				ctx = metadata.NewIncomingContext(ctx, tt.metadata)
			}
			called := false
			handler := func(context.Context, interface{}) (interface{}, error) {
				called = true
				return "ok", nil
			}

			_, err := AuthInterceptor(logger, tt.enabled, []string{"secret"})(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)
			assert.Equal(t, tt.wantCode, status.Code(err))
			assert.Equal(t, tt.wantCode == codes.OK, called)
		})
	}
}

func TestIncomingHeaderMatcher(t *testing.T) {
	key, ok := incomingHeaderMatcher("Api-Key")
	require.True(t, ok)
	assert.Equal(t, "api-key", key)

	key, ok = incomingHeaderMatcher("Authorization")
	require.True(t, ok)
	assert.Equal(t, "authorization", key)
}

func TestRateLimitingInterceptor(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	interceptor := RateLimitingInterceptor(logger, 1, 2, 100)
	ctx := context.WithValue(context.Background(), authenticatedPrincipalKey{}, credentialPrincipal("secret"))
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	handlerCalls := 0
	handler := func(context.Context, interface{}) (interface{}, error) {
		handlerCalls++
		return "ok", nil
	}

	_, err := interceptor(ctx, nil, info, handler)
	require.NoError(t, err)
	_, err = interceptor(ctx, nil, info, handler)
	require.NoError(t, err)
	_, err = interceptor(ctx, nil, info, handler)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, 2, handlerCalls)
}

func TestRateLimitingSeparatesAuthenticatedPrincipals(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	rateLimiter := RateLimitingInterceptor(logger, 1, 1, 100)
	auth := AuthInterceptor(logger, true, []string{"key-one", "key-two"})
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	handler := func(context.Context, interface{}) (interface{}, error) { return "ok", nil }

	invoke := func(apiKey string) error {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("api-key", apiKey))
		_, err := auth(ctx, nil, info, func(authenticatedContext context.Context, req interface{}) (interface{}, error) {
			return rateLimiter(authenticatedContext, req, info, handler)
		})
		return err
	}

	require.NoError(t, invoke("key-one"))
	assert.Equal(t, codes.ResourceExhausted, status.Code(invoke("key-one")))
	require.NoError(t, invoke("key-two"))
}

func TestRateLimitingRejectsNewPeerAtBucketCapacity(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	interceptor := RateLimitingInterceptor(logger, 1, 1, 2)
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	handlerCalls := 0
	handler := func(context.Context, interface{}) (interface{}, error) {
		handlerCalls++
		return "ok", nil
	}

	invoke := func(address string) error {
		ctx := peer.NewContext(context.Background(), &peer.Peer{
			Addr: &net.TCPAddr{IP: net.ParseIP(address), Port: 9000},
		})
		_, err := interceptor(ctx, nil, info, handler)
		return err
	}

	require.NoError(t, invoke("192.0.2.1"))
	require.NoError(t, invoke("192.0.2.2"))
	assert.Equal(t, codes.ResourceExhausted, status.Code(invoke("192.0.2.3")))
	assert.Equal(t, 2, handlerCalls)
}
