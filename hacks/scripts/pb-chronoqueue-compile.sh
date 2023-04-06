#!/usr/bin/env sh

# Install proto3 from source
#  brew install autoconf automake libtool
#  git clone https://github.com/google/protobuf
#  ./autogen.sh ; ./configure ; make ; make install
#
# Update protoc Go bindings via
#  go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
#
# See also
#  https://github.com/grpc/grpc-go/tree/master/examples

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
PB_PATH="${REPO_ROOT}/api/"
PROTO_FILE=${1:-"chronoqueue/v1/service.proto"}

echo "${REPO_ROOT}"
echo "${PB_PATH}"
echo "${PROTO_FILE}"


echo "Generating pb files for ${PROTO_FILE} service"

# Update your PATH so that the protoc compiler can find the plugins:
export PATH="$PATH:$(go env GOPATH)/bin"

# generate the messages
protoc --go_out="${PB_PATH}" -I="${PB_PATH}" --go_opt=paths=source_relative "${PROTO_FILE}"

# generate the services
protoc --go-grpc_out="${PB_PATH}" -I="${PB_PATH}" --go-grpc_opt=paths=source_relative "${PROTO_FILE}"