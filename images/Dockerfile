# Build the cipher service binary
# FROM golang:1.19.7-stretch as base
FROM golang:1.19.7-alpine3.17 as base

# add the working directory for the workflow service
WORKDIR /go/src/chronoqueuesvc

# Copy the service code
COPY api api
COPY cmd/chronoqueue cmd/chronoqueue
# COPY client client
COPY internal/util internal/util
COPY pkg/chronoqueue pkg/chronoqueue

COPY go.mod go.sum ./
RUN go mod download
    # && go mod verify

# building service binary
RUN go build -o chronoqueue ./cmd/chronoqueue/chronoqueue.go

## Using the multi-stage image to just run the binary
FROM alpine:latest as final

# Init working directory to root /
WORKDIR /

# Copy just the binary from the base image
RUN mkdir ./config
RUN mkdir /secrets
COPY --from=base /go/src/chronoqueuesvc/chronoqueue .

# just an indication that this port will be exposed by this container
EXPOSE 9000 9001

# command to run at the immediate start of the container
ENTRYPOINT ["./chronoqueue"]