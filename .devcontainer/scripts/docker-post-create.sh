#!/bin/bash

go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest

go get github.com/testcontainers/testcontainers-go
go get github.com/testcontainers/testcontainers-go/wait
go get github.com/stretchr/testify
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Install Node.js dependencies for the frontend

# Check if Node.js is installed
if ! command -v node --version &> /dev/null
then
    echo "Node.js could not be found, installing Node.js"
    curl -fsSL https://deb.nodesource.com/setup_24.x | sudo bash -
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

