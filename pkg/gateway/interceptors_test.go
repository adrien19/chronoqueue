package gateway

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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
