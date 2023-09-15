package main

import (
	"context"
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

	startServers(httpAddr, grpcAddr, httpHandler, grpcServer, logger)
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

func startServers(httpAddr, grpcAddr string, httpHandler http.Handler, grpcServer pb_chronoqueue.ChronoQueueServer, logger log.Logger) {
	var g group.Group
	initHTTPServer(&g, httpAddr, httpHandler, logger)
	initGRPCServer(&g, grpcAddr, grpcServer, logger)
	waitForTermination(&g, logger)
}

func initHTTPServer(g *group.Group, addr string, handler http.Handler, logger log.Logger) {
	httpListener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log("transport", "HTTP", "during", "Listen", "err", err)
		os.Exit(1)
	}
	g.Add(func() error {
		logger.Log("transport", "HTTP", "addr", addr)
		return http.Serve(httpListener, handler)
	}, func(error) {
		httpListener.Close()
	})
}

func initGRPCServer(g *group.Group, addr string, server pb_chronoqueue.ChronoQueueServer, logger log.Logger) {
	grpcListener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log("transport", "gRPC", "during", "Listen", "err", err)
		os.Exit(1)
	}
	g.Add(func() error {
		logger.Log("transport", "gRPC", "addr", addr)
		baseServer := grpc.NewServer(grpc.ChainUnaryInterceptor(kitgrpc.Interceptor))
		pb_chronoqueue.RegisterChronoQueueServer(baseServer, server)
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
