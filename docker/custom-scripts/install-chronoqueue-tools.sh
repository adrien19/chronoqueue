#!/usr/bin/env bash

# Source: Adapted from https://github.com/dapr/dapr/blob/6c87b4bd722bb43e58c78840a062d514a72ec934/docker/custom-scripts/install-dapr-tools.sh


# Copyright 2021 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Syntax: ./install-chronoqueue-tools.sh [USERNAME] [GOROOT] [GOPATH] [PROTOC_VERSION] [PROTOC_GEN_GO_VERSION] [PROTOC_GEN_GO_GRPC_VERSION] [GOLANGCI_LINT_VERSION]

USERNAME=${1:-"chronoqueue"}
GOROOT=${2:-"/usr/local/go"}
GOPATH=${3:-"/go"}
PROTOC_VERSION=${4:-"32.0"}
PROTOC_GEN_GO_VERSION=${5:-"1.36.9"}
PROTOC_GEN_GO_GRPC_VERSION=${6:-"1.5.1"}
GOLANGCI_LINT_VERSION=${7:-"2.5.0"}

set -e

if [ "$(id -u)" -ne 0 ]; then
    echo -e 'Script must be run as root. Use sudo, su, or add "USER root" to your Dockerfile before running this script.'
    exit 1
fi

# Install socat
apt-get install -y socat

# Install protoc compiler required by 'make gen-proto'
architecture="$(uname -m)"
case $architecture in
    x86_64) architecture="x86_64";;
    aarch64 | armv8*) architecture="aarch_64";;
    i?86) architecture="x86_32";;
    *) echo "(!) Architecture $architecture unsupported"; exit 1 ;;
esac

PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-${architecture}.zip
curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}"
unzip -o "${PROTOC_ZIP}" -d /usr/local bin/protoc
chmod -R 755 /usr/local/bin/protoc
unzip -o "${PROTOC_ZIP}" -d /usr/local 'include/*'
chmod -R 755 /usr/local/include/google/protobuf
rm -f "${PROTOC_ZIP}"

LOCAL_GOPATH="$(which go)"
LOCAL_GOVERSION="$(go version)"

echo "==>> ${LOCAL_GOPATH} ${GOPATH}/bin ${LOCAL_GOVERSION} DONE INSTALLING PROTO..."

# Install protoc-gen-go and protoc-gen-go-grpc
# Must be installed as the non-root user
export GOBIN="${GOPATH}/bin"
sudo -u ${USERNAME} --preserve-env=GOPATH,GOBIN,GOROOT \
    ${GOROOT}/bin/go install "google.golang.org/protobuf/cmd/protoc-gen-go@v${PROTOC_GEN_GO_VERSION}"
sudo -u ${USERNAME} --preserve-env=GOPATH,GOBIN,GOROOT \
     ${GOROOT}/bin/go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@v${PROTOC_GEN_GO_GRPC_VERSION}"

# Install golangci-lint using the recommended method (best to avoid using go install according to the docs)
# Must be installed as the non-root user
sudo -u ${USERNAME} --preserve-env=GOLANGCI_LINT_VERSION,GOPATH,GOBIN,GOROOT \
    sh -c 'curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "${GOBIN}" "v${GOLANGCI_LINT_VERSION}"'

