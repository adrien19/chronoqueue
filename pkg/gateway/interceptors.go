package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
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

type authenticatedPrincipalKey struct{}

const gatewayClientIDMetadataKey = "chronoqueue-gateway-client-id"

var gatewayClientIDSigningKey = func() [32]byte {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic("generate gateway client identity signing key: " + err.Error())
	}
	return key
}()

func signedGatewayClientID(host string) string {
	payload := "gateway:" + host
	signature := hmac.New(sha256.New, gatewayClientIDSigningKey[:])
	if _, err := signature.Write([]byte(payload)); err != nil {
		panic("sign gateway client identity: " + err.Error())
	}
	return payload + ":" + hex.EncodeToString(signature.Sum(nil))
}

func verifiedGatewayClientID(value string) (string, bool) {
	separator := strings.LastIndexByte(value, ':')
	if separator <= len("gateway:") {
		return "", false
	}
	payload := value[:separator]
	providedSignature, err := hex.DecodeString(value[separator+1:])
	if err != nil {
		return "", false
	}
	expectedSignature := hmac.New(sha256.New, gatewayClientIDSigningKey[:])
	if _, err := expectedSignature.Write([]byte(payload)); err != nil {
		return "", false
	}
	if !hmac.Equal(providedSignature, expectedSignature.Sum(nil)) {
		return "", false
	}
	return payload, true
}

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
					return handler(context.WithValue(ctx, authenticatedPrincipalKey{}, credentialPrincipal(credential)), req)
				}
			}
		}

		logger.WarnWithFields("gRPC authentication failed", "method", info.FullMethod)
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}
}

func credentialPrincipal(credential string) string {
	digest := sha256.Sum256([]byte(credential))
	return "api-key:" + hex.EncodeToString(digest[:])
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

type clientBucket struct {
	tokens   float64
	updated  time.Time
	lastSeen time.Time
}

type rateLimiter struct {
	mu                sync.Mutex
	requestsPerSecond float64
	burst             float64
	maxBuckets        int
	buckets           map[string]*clientBucket
	lastCleanup       time.Time
	staleAfter        time.Duration
}

// CONSIDER(distributed-rate-limiting): Share quotas across replicas when deployments require global limits.
func newRateLimiter(requestsPerSecond float64, burst, maxBuckets int) *rateLimiter {
	now := time.Now()
	return &rateLimiter{
		requestsPerSecond: requestsPerSecond,
		burst:             float64(burst),
		maxBuckets:        maxBuckets,
		buckets:           make(map[string]*clientBucket),
		lastCleanup:       now,
		staleAfter:        5 * time.Minute,
	}
}

func (l *rateLimiter) allow(clientID string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if now.Sub(l.lastCleanup) >= time.Minute {
		for id, bucket := range l.buckets {
			if now.Sub(bucket.lastSeen) >= l.staleAfter {
				delete(l.buckets, id)
			}
		}
		l.lastCleanup = now
	}

	bucket, ok := l.buckets[clientID]
	if !ok {
		if len(l.buckets) >= l.maxBuckets {
			return false
		}
		bucket = &clientBucket{tokens: l.burst, updated: now}
		l.buckets[clientID] = bucket
	}

	bucket.tokens += now.Sub(bucket.updated).Seconds() * l.requestsPerSecond
	if bucket.tokens > l.burst {
		bucket.tokens = l.burst
	}
	bucket.updated = now
	bucket.lastSeen = now
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

func rateLimitClientID(ctx context.Context) string {
	if principal, ok := ctx.Value(authenticatedPrincipalKey{}).(string); ok && principal != "" {
		return principal
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if clientIDs := md.Get(gatewayClientIDMetadataKey); len(clientIDs) == 1 && clientIDs[0] != "" {
			if clientID, valid := verifiedGatewayClientID(clientIDs[0]); valid {
				return clientID
			}
		}
	}
	if p, ok := peer.FromContext(ctx); ok {
		host, _, err := net.SplitHostPort(p.Addr.String())
		if err == nil && host != "" {
			return "peer:" + host
		}
		return "peer:" + p.Addr.String()
	}
	return "peer:unknown"
}

// RateLimitingInterceptor enforces a per-principal token bucket limit.
func RateLimitingInterceptor(logger *log.Logger, requestsPerSecond float64, burst, maxBuckets int) grpc.UnaryServerInterceptor {
	limiter := newRateLimiter(requestsPerSecond, burst, maxBuckets)

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		clientID := rateLimitClientID(ctx)
		if !limiter.allow(clientID, time.Now()) {
			logger.WarnWithFields("gRPC rate limit exceeded", "client", clientID, "method", info.FullMethod)
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}

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
