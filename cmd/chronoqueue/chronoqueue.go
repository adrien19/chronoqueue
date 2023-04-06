package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	pb_chronoqueue "github.com/adrien19/chronoqueue/api/chronoqueue/v1"
	"github.com/adrien19/chronoqueue/internal"

	// interceptors "github.com/adrien19/chronoqueue/internal/interceptors/grpc"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/endpoints"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/repository"
	"github.com/adrien19/chronoqueue/pkg/chronoqueue/transport"
	kitgrpc "github.com/go-kit/kit/transport/grpc"
	"github.com/go-kit/log"
	"github.com/oklog/oklog/pkg/group"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

const (
	defaultGRPCPort          = "9000"
	defaultHTTPPort          = "9001"
	defaultHostname          = "localhost"
	defaultAuthSvcHostname   = "0.0.0.0"
	defaultAuthSvcPort       = "5006"
	defaultActionSvcHostname = "0.0.0.0"
	defaultActionSvcPort     = "7000"
)

// func gRPCAccessibleRoles() map[string][]bool {
// 	const chronoqueueServicePath = "/pb.ChonoQueue/"

// 	return map[string][]bool{
// 		chronoqueueServicePath + "CreateQueue":        {true},
// 		chronoqueueServicePath + "DeleteQueue":        {true},
// 		chronoqueueServicePath + "PostMessage":        {true},
// 		chronoqueueServicePath + "GetNextMessage":     {true},
// 		chronoqueueServicePath + "AcknowledgeMessage": {true},
// 		chronoqueueServicePath + "RenewMessageLease":  {true},
// 		chronoqueueServicePath + "PeekQueueMessages":  {true},
// 		chronoqueueServicePath + "GetQueueState":      {true},
// 	}
// }

// func httpAccessibleRoles() map[string][]bool {
// 	const chronoqueueServicePath = "/"

// 	return map[string][]bool{
// 		chronoqueueServicePath + "queue/createqueue":        {true},
// 		chronoqueueServicePath + "queue/deletequeue":        {true},
// 		chronoqueueServicePath + "queue/postmessage":        {true},
// 		chronoqueueServicePath + "queue/getnextmessage":     {true},
// 		chronoqueueServicePath + "queue/acknowledgemessage": {true},
// 		chronoqueueServicePath + "queue/renewmessagelease":  {true},
// 		chronoqueueServicePath + "queue/peekqueuemessages":  {true},
// 		chronoqueueServicePath + "queue/getqueuestate":      {true},
// 	}
// }

func main() {
	var (
		logger   log.Logger
		httpAddr = net.JoinHostPort(envString("HOST", defaultHostname), envString("HTTP_PORT", defaultHTTPPort))
		grpcAddr = net.JoinHostPort(envString("HOST", defaultHostname), envString("GRPC_PORT", defaultGRPCPort))
	)

	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	// set up config variables
	configs, err := getConfigs("config/config.yml")
	if err != nil {
		panic(err)
	}

	// setup Redis store
	redisConnectionString := fmt.Sprintf("%s:%s", configs.Redis.Dns, configs.Redis.Port)
	var ctx = context.Background()

	redisClient, err := setupRedis(ctx, redisConnectionString)
	if err != nil {
		panic(err)
	}

	// // Set up a connection to Authentication server.
	// authSvcConnectionString := fmt.Sprintf("%s:%s", envString("AUTH_SERVICE_HOST", defaultAuthSvcHostname), envString("AUTH_SERVICE_PORT", defaultAuthSvcPort))
	// conn, err := grpc.Dial(authSvcConnectionString, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	// if err != nil {
	// 	panic(err)
	// }
	// defer conn.Close()
	// authClient := client.NewAuthClient(conn)

	var (
		database    = repository.NewQueueStorage(redisClient)
		service     = chronoqueue.NewChronoqueueService(database)
		eps         = endpoints.NewEndpointSet(service)
		httpHandler = transport.NewHTTPHandler(eps)
		grpcServer  = transport.NewGRPCServer(eps)
	)

	var g group.Group
	{
		// The HTTP listener mounts the Go kit HTTP handler we created.
		httpListener, err := net.Listen("tcp", httpAddr)
		if err != nil {
			logger.Log("transport", "HTTP", "during", "Listen", "err", err)
			os.Exit(1)
		}
		g.Add(func() error {
			logger.Log("transport", "HTTP", "addr", httpAddr)
			return http.Serve(httpListener, httpHandler)
		}, func(error) {
			httpListener.Close()
		})
	}
	{
		// The gRPC listener mounts the Go kit gRPC server we created.
		grpcListener, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			logger.Log("transport", "gRPC", "during", "Listen", "err", err)
			os.Exit(1)
		}
		g.Add(func() error {
			logger.Log("transport", "gRPC", "addr", grpcAddr)
			// we add the Go Kit gRPC Interceptor to our gRPC service as it is used by
			// the here demonstrated zipkin tracing middleware.
			// authInterceptor := interceptors.NewAuthInterceptor(authClient, gRPCAccessibleRoles())

			baseServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
				kitgrpc.Interceptor,
				// authInterceptor.Unary(),
			))

			// baseServer := grpc.NewServer(grpc.UnaryInterceptor(kitgrpc.Interceptor))
			pb_chronoqueue.RegisterChronoQueueServer(baseServer, grpcServer)
			return baseServer.Serve(grpcListener)
		}, func(error) {
			grpcListener.Close()
		})
	}
	{
		// This function just sits and waits for ctrl-C.
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
	}
	logger.Log("exit", g.Run())
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}

func setupRedis(ctx context.Context, connectionString string) (*redis.Client, error) {

	var redisClient *redis.Client

	//Initializing redis
	dsn := connectionString
	if len(dsn) == 0 {
		dsn = "localhost:6379"
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr: dsn, //redis port
		DB:   0,   // use default DB
	})
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}

	return redisClient, nil
}

func getConfigs(filePath string) (internal.Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return internal.Config{}, err
	}

	defer file.Close()

	var cfg internal.Config
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&cfg)
	if err != nil {
		return internal.Config{}, err
	}

	return cfg, nil
}
