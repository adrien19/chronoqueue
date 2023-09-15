package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	pb_chronoqueue "github.com/adrien19/chronoqueue/api/chronoqueue/v1"

	"github.com/adrien19/chronoqueue/pkg/chronoqueue"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/endpoints"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/repository"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/transport"
	kitgrpc "github.com/go-kit/kit/transport/grpc"
	"github.com/go-kit/log"
	"github.com/oklog/oklog/pkg/group"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	logger := initializeLogger()
	httpAddr, grpcAddr, redisConnectionString := getConfigsFromEnv()

	redisClient := initializeRedis(context.Background(), redisConnectionString, logger)

	database := repository.NewQueueStorage(redisClient)
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

	cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
	if err != nil {
		logger.Log("msg", "Failed to load server.crt/server.key", "err", err)
		os.Exit(1)
	}

	caCert, err := os.ReadFile("ca.crt")
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

func initHTTPServer(g *group.Group, addr string, handler http.Handler, tlsConfig *tls.Config, logger log.Logger) {
	httpListener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log("transport", "HTTP", "during", "Listen", "err", err)
		os.Exit(1)
	}

	if tlsConfig != nil {
		httpListener = tls.NewListener(httpListener, tlsConfig)
	}

	g.Add(func() error {
		logger.Log("transport", "HTTP", "addr", addr)
		return http.Serve(httpListener, handler)
	}, func(error) {
		httpListener.Close()
	})
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
