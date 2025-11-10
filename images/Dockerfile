# Build the cipher service binary
# FROM golang:1.19.7-stretch as base
# FROM golang:1.19.7-alpine3.17 as base
FROM golang:1.25.4-alpine3.22 as base

# add the working directory for the workflow service
WORKDIR /go/src/chronoqueuesvr

# Copy the server code
COPY api api
COPY cmd/chronoq cmd/chronoq
COPY client client
# COPY client client
COPY internal internal
COPY pkg pkg

COPY main.go ./
COPY go.mod go.sum ./
RUN go mod download
# && go mod verify

# Build arguments for version injection
ARG VERSION=1.0.0
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

# building server binary with version information
RUN go build -ldflags="-X 'github.com/adrien19/chronoqueue/pkg/version.Version=${VERSION}' \
    -X 'github.com/adrien19/chronoqueue/pkg/version.GitCommit=${GIT_COMMIT}' \
    -X 'github.com/adrien19/chronoqueue/pkg/version.BuildDate=${BUILD_DATE}'" \
    -o chronoqueue main.go

## Using the multi-stage image to just run the binary
FROM alpine:latest as final

# Init working directory to root /
WORKDIR /

# Copy just the binary from the base image
RUN mkdir ./config
RUN mkdir /secrets
COPY --from=base /go/src/chronoqueuesvr/chronoqueue .

# Copy entrypoint script
COPY images/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# just an indication that this port will be exposed by this container
EXPOSE 9000 8080

# command to run at the immediate start of the container
ENTRYPOINT ["/entrypoint.sh"]