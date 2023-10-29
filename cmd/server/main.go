package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	pb_chronoqueue "github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal/encryption/keymanager"

	"github.com/adrien19/chronoqueue/pkg/chronoqueue"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/endpoints"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/repository"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/transport"
	kitgrpc "github.com/go-kit/kit/transport/grpc"
	"github.com/go-kit/log"
	"github.com/oklog/oklog/pkg/group"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	defaultGRPCPort  = "9000"
	defaultHTTPPort  = "9001"
	defaultHostname  = "0.0.0.0"
	defaultRedisHost = "0.0.0.0"
	defaultRedisPort = "6379"
	defaultRedisDB   = "0"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := initializeLogger()
	httpAddr, grpcAddr, redisConnectionString := getConfigsFromEnv()

	redisClient := initializeRedis(ctx, redisConnectionString, logger)

	encryptionKeyManager := initializeEncryptionKeyManager(logger)

	database := repository.NewQueueStorage(ctx, redisClient, encryptionKeyManager)
	service := chronoqueue.NewChronoqueueService(database)
	eps := endpoints.NewEndpointSet(service)
	httpHandler := transport.NewHTTPHandler(eps)
	grpcServer := transport.NewGRPCServer(eps)

	tlsConfig := getTLSConfigIfEnabled(logger) // This returns a tls.Config if mTLS is enabled, otherwise nil

	startServers(httpAddr, grpcAddr, httpHandler, grpcServer, tlsConfig, logger)
}

func initializeLogger() log.Logger {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	return log.With(logger, "ts", log.DefaultTimestampUTC)
}

func getConfigsFromEnv() (string, string, string) {
	httpAddr := net.JoinHostPort(envString("HOST", defaultHostname), envString("HTTP_PORT", defaultHTTPPort))
	grpcAddr := net.JoinHostPort(envString("HOST", defaultHostname), envString("GRPC_PORT", defaultGRPCPort))
	redisConnectionString := fmt.Sprintf("%s:%s", envString("REDIS_HOST", defaultRedisHost), envString("REDIS_PORT", defaultRedisPort))
	return httpAddr, grpcAddr, redisConnectionString
}

func initializeRedis(ctx context.Context, connectionString string, logger log.Logger) *redis.Client {
	dsn := connectionString
	if dsn == "" {
		dsn = "localhost:6379"
	}
	db, _ := strconv.ParseInt(envString("REDIS_DB", defaultRedisDB), 10, 0)
	redisClient := redis.NewClient(&redis.Options{
		Addr: dsn,
		DB:   int(db),
	})
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		logger.Log("could not connect to redis", err)
		os.Exit(1)
	}
	return redisClient
}

func initializeEncryptionKeyManager(logger log.Logger) *keymanager.EncryptionKeyManager {
	KeyManager, err := keymanager.NewEncryptionKeyManager()
	if err != nil {
		logger.Log("msg", "Failed to initialize encryption key manager", "err", err)
		os.Exit(1)
	}
	return KeyManager
}

func startServers(httpAddr, grpcAddr string, httpHandler http.Handler, grpcServer pb_chronoqueue.ChronoQueueServer, tlsConfig *tls.Config, logger log.Logger) {
	var g group.Group
	initHTTPServer(&g, httpAddr, httpHandler, tlsConfig, logger)
	initGRPCServer(&g, grpcAddr, grpcServer, tlsConfig, logger)
	waitForTermination(&g, logger)
}

func getTLSConfigIfEnabled(logger log.Logger) *tls.Config {
	tlsEnabled := envString("CHRONOQUEUE_TLS_ENABLED", "false") == "true"
	if !tlsEnabled {
		return nil
	}

	// Fetch paths from environment variables or use defaults
	serverCertPath := envString("SERVER_CERT_PATH", "server.crt")
	serverKeyPath := envString("SERVER_KEY_PATH", "server.key")
	caCertPath := envString("CA_CERT_PATH", "ca.crt")

	cert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		logger.Log("msg", "Failed to load server.crt/server.key", "err", err)
		os.Exit(1)
	}

	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		logger.Log("msg", "Failed to read ca.crt", "err", err)
		os.Exit(1)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
	}
}

func clientCertMiddleware(next http.Handler) http.Handler {
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

func initHTTPServer(g *group.Group, addr string, handler http.Handler, tlsConfig *tls.Config, logger log.Logger) {
	httpListener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log("transport", "HTTP", "during", "Listen", "err", err)
		os.Exit(1)
	}

	if tlsConfig != nil {
		httpListener = tls.NewListener(httpListener, tlsConfig)
		handler = clientCertMiddleware(handler)
	}

	g.Add(func() error {
		logger.Log("transport", "HTTP", "addr", addr)
		return http.Serve(httpListener, handler)
	}, func(error) {
		httpListener.Close()
	})
}

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

			// 3. For CA certificates
			if cert.IsCA {
				// Check the key usage includes the required constraints
				if (cert.KeyUsage & x509.KeyUsageCertSign) == 0 {
					return errors.New("CA certificate key usage does not include certificate signing")
				}
			} else {
				// 2. For client certificates
				// Check the key usage includes Digital Signature
				if (cert.KeyUsage & x509.KeyUsageDigitalSignature) == 0 {
					return errors.New("client certificate key usage does not include digital signature")
				}
			}

			// 4. & 5. Check signature algorithms
			if cert.SignatureAlgorithm == x509.SHA1WithRSA || cert.SignatureAlgorithm == x509.MD5WithRSA {
				return errors.New("certificate uses weak signature algorithm")
			}

			// 5. Check each certificate in the chain has a unique Distinguished Name (for end-entity certs)
			for _, otherCert := range chain {
				if otherCert != cert && otherCert.Subject.String() == cert.Subject.String() {
					return errors.New("multiple certificates in the chain have the same distinguished name")
				}
			}
		}
	}

	return nil
}

func verifyPeerCertificateInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no peer found")
	}

	tlsAuth, ok := peer.AuthInfo.(credentials.TLSInfo)
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

func initGRPCServer(g *group.Group, addr string, server pb_chronoqueue.ChronoQueueServer, tlsConfig *tls.Config, logger log.Logger) {
	grpcListener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log("transport", "gRPC", "during", "Listen", "err", err)
		os.Exit(1)
	}

	var opts []grpc.ServerOption
	opts = append(opts, grpc.ChainUnaryInterceptor(kitgrpc.Interceptor))
	if tlsConfig != nil {
		opts = append(opts, grpc.ChainUnaryInterceptor(verifyPeerCertificateInterceptor))
		creds := credentials.NewTLS(tlsConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	baseServer := grpc.NewServer(opts...)

	pb_chronoqueue.RegisterChronoQueueServer(baseServer, server)
	g.Add(func() error {
		logger.Log("transport", "gRPC", "addr", addr)
		return baseServer.Serve(grpcListener)
	}, func(error) {
		grpcListener.Close()
	})
}

func waitForTermination(g *group.Group, logger log.Logger) {
	cancelInterrupt := make(chan struct{})
	g.Add(func() error {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		select {
		case sig := <-c:
			return fmt.Errorf("received signal %s", sig)
		case <-cancelInterrupt:
			return nil
		}
	}, func(error) {
		close(cancelInterrupt)
	})
	logger.Log("exit", g.Run())
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
