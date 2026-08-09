package gateway

import (
	"context"
	"crypto/subtle"
	"crypto/x509"
	"errors"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/adrien19/chronoqueue/pkg/log"
	"github.com/adrien19/chronoqueue/pkg/metrics"
)

// LoggingInterceptor logs all gRPC requests and responses
func LoggingInterceptor(logger *log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// Extract peer information
		var peerAddr string
		if p, ok := peer.FromContext(ctx); ok {
			peerAddr = p.Addr.String()
		}

		logger.InfoWithFields(
			"gRPC request started",
			"method", info.FullMethod,
			"peer", peerAddr,
		)

		// Call the handler
		resp, err := handler(ctx, req)

		// Log the result
		duration := time.Since(start)
		if err != nil {
			logger.ErrorWithFields(
				"gRPC request failed",
				"method", info.FullMethod,
				"peer", peerAddr,
				"duration", duration.String(),
				"error", err,
			)
		} else {
			logger.InfoWithFields(
				"gRPC request completed",
				"method", info.FullMethod,
				"peer", peerAddr,
				"duration", duration.String(),
			)
		}

		return resp, err
	}
}

// AuthInterceptor handles authentication and authorization
func AuthInterceptor(logger *log.Logger, enabled bool, validAPIKeys []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !enabled {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing credentials")
		}

		credentials := append([]string(nil), md.Get("api-key")...)
		for _, authorization := range md.Get("authorization") {
			if scheme, token, found := strings.Cut(authorization, " "); found && strings.EqualFold(scheme, "bearer") {
				credentials = append(credentials, token)
			}
		}

		for _, credential := range credentials {
			for _, validAPIKey := range validAPIKeys {
				if validAPIKey != "" && len(credential) == len(validAPIKey) && subtle.ConstantTimeCompare([]byte(credential), []byte(validAPIKey)) == 1 {
					return handler(ctx, req)
				}
			}
		}

		logger.WarnWithFields("gRPC authentication failed", "method", info.FullMethod)
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
}

// RecoveryInterceptor recovers from panics and returns appropriate errors
func RecoveryInterceptor(logger *log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorWithFields(
					"gRPC handler panicked",
					"method", info.FullMethod,
					"panic", r,
				)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// MetricsInterceptor collects metrics for gRPC requests
func MetricsInterceptor(logger *log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start)

		// Record Prometheus metrics
		metrics.RecordGRPCMetrics(info.FullMethod, duration, err)

		// Keep the original logging behavior
		statusCode := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				statusCode = st.Code()
			} else {
				statusCode = codes.Unknown
			}
		}

		logger.InfoWithFields(
			"gRPC metrics",
			"method", info.FullMethod,
			"duration_ms", duration.Milliseconds(),
			"status_code", statusCode.String(),
		)

		return resp, err
	}
}

// RateLimitingInterceptor implements basic rate limiting
func RateLimitingInterceptor(logger *log.Logger, requestsPerSecond int) grpc.UnaryServerInterceptor {
	// In a production system, you would use a more sophisticated rate limiter
	// like golang.org/x/time/rate or a distributed rate limiter

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Extract client identifier (IP, API key, etc.)
		clientID := "default"
		if p, ok := peer.FromContext(ctx); ok {
			clientID = p.Addr.String()
		}

		// In production, implement actual rate limiting logic here
		logger.DebugWithFields("Rate limit check", "client", clientID, "method", info.FullMethod)

		return handler(ctx, req)
	}
}

// ValidationInterceptor validates incoming requests
func ValidationInterceptor(logger *log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Here you could implement request validation logic
		// For example, validating required fields, data formats, etc.

		// For ChronoQueue, you might want to validate:
		// - Queue names are valid
		// - Message payloads are within size limits
		// - Lease durations are reasonable
		// - Cron expressions are valid

		logger.DebugWithFields("Request validation", "method", info.FullMethod)

		return handler(ctx, req)
	}
}

// ClientCertMiddleware validates client certificates for HTTP requests
func ClientCertMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "No client certificate provided", http.StatusUnauthorized)
			return
		}

		seenDNs := make(map[string]bool)

		// Iterate through the chain of client's certificates
		for _, clientCert := range r.TLS.PeerCertificates {
			// 1. Check if the certificate is X.509v3
			if clientCert.Version != 3 {
				http.Error(w, "Certificate is not X.509v3", http.StatusForbidden)
				return
			}

			// 2. For client certificates, check key usage
			if (clientCert.KeyUsage & x509.KeyUsageDigitalSignature) == 0 {
				http.Error(w, "Client certificate key usage does not include digital signature", http.StatusForbidden)
				return
			}

			// 3. For CA certificates, check key usage
			if clientCert.IsCA && (clientCert.KeyUsage&x509.KeyUsageCertSign) == 0 {
				http.Error(w, "CA certificate key usage does not include certificate signing", http.StatusForbidden)
				return
			}

			// 4. Check weak signature algorithms
			if clientCert.SignatureAlgorithm == x509.SHA1WithRSA || clientCert.SignatureAlgorithm == x509.MD5WithRSA {
				http.Error(w, "Certificate uses a weak signature algorithm", http.StatusForbidden)
				return
			}

			// 5. Check Distinguished Name uniqueness
			dn := clientCert.Subject.String()
			if _, exists := seenDNs[dn]; exists {
				http.Error(w, "Multiple certificates in the chain have the same distinguished name", http.StatusForbidden)
				return
			}
			seenDNs[dn] = true
		}

		// If all checks pass, call the next handler
		next.ServeHTTP(w, r)
	})
}

// customVerifyPeerCertificate performs additional certificate validation
func customVerifyPeerCertificate(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
		return errors.New("could not obtain client certificate")
	}

	// Iterate through each presented chain
	for _, chain := range verifiedChains {
		for _, cert := range chain {
			// 1. Check if the certificate is X.509v3
			if cert.Version != 3 {
				return errors.New("certificate is not X.509v3")
			}

			// For CA certificates
			if cert.IsCA {
				// Check the key usage includes the required constraints
				if (cert.KeyUsage & x509.KeyUsageCertSign) == 0 {
					return errors.New("CA certificate key usage does not include certificate signing")
				}
			} else {
				// For client certificates
				// Check the key usage includes Digital Signature
				if (cert.KeyUsage & x509.KeyUsageDigitalSignature) == 0 {
					return errors.New("client certificate key usage does not include digital signature")
				}
			}

			// Check signature algorithms
			if cert.SignatureAlgorithm == x509.SHA1WithRSA || cert.SignatureAlgorithm == x509.MD5WithRSA {
				return errors.New("certificate uses weak signature algorithm")
			}

			// Check each certificate in the chain has a unique Distinguished Name
			for _, otherCert := range chain {
				if otherCert != cert && otherCert.Subject.String() == cert.Subject.String() {
					return errors.New("multiple certificates in the chain have the same distinguished name")
				}
			}
		}
	}

	return nil
}

// VerifyPeerCertificateInterceptor validates client certificates for gRPC requests
func VerifyPeerCertificateInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no peer found")
	}

	tlsAuth, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unexpected peer transport credentials")
	}

	if len(tlsAuth.State.VerifiedChains) == 0 || len(tlsAuth.State.VerifiedChains[0]) == 0 {
		return nil, status.Error(codes.Unauthenticated, "could not obtain client certificate")
	}

	err := customVerifyPeerCertificate(nil, tlsAuth.State.VerifiedChains)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	return handler(ctx, req)
}
