#!/bin/bash

go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest

go get github.com/testcontainers/testcontainers-go
go get github.com/testcontainers/testcontainers-go/wait
go get github.com/stretchr/testify

# Create services required for the project
docker volume ls | grep -q "redisStoreData" || docker volume create redisStoreData

# Check if Redis container exists
if [ -z "$(docker container ls -a --filter name=^/redis$ --format '{{.Names}}')" ]; then
    echo "Pulling and running Redis"
    docker pull redis/redis-stack-server:latest
    docker run -d --name redis -p 6379:6379 -p 8379:8001 -e REDIS_ARGS="--requirepass mypassword" \
     -v redisStoreData:/data redis/redis-stack-server:latest
else
    echo "Starting redis"
    docker start redis
fi

# Validate that the services are running
docker container ls | grep -q "redis" && echo "Redis is running" || echo "Redis is not running"

# Install Node.js dependencies for the frontend

# Check if Node.js is installed
if ! command -v node --version &> /dev/null
then
    echo "Node.js could not be found, installing Node.js"
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo bash -
    sudo apt install nodejs -y
fi
# Now check node and npm versions
echo "Node.js version: $(node --version)"
echo "npm version: $(npm --version)"
# install pnpm if not installed
if ! command -v pnpm --version &> /dev/null
then
    echo "pnpm could not be found, installing pnpm"
    sudo npm install -g pnpm
fi
# Check pnpm version
echo "pnpm version: $(pnpm --version)"

