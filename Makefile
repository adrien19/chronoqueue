
################################################################################
# Variables                                                                    #
################################################################################

export GO111MODULE ?= on
export GOPROXY ?= https://proxy.golang.org
export GOSUMDB ?= sum.golang.org

GIT_COMMIT  = $(shell git rev-list -1 HEAD)
GIT_VERSION ?= $(shell git describe --always --abbrev=7 --dirty)
# By default, disable CGO_ENABLED. See the details on https://golang.org/cmd/cgo
CGO         ?= 0
BINARIES    ?= server

# Add latest tag if LATEST_RELEASE is true
LATEST_RELEASE ?=

PROTOC ?= protoc

# Version of "protoc" to use
# Must also specify a protobuf "suite" version from https://github.com/protocolbuffers/protobuf/releases
PROTOC_VERSION = 32.0
PROTOBUF_SUITE_VERSION = 32.0

# name of protoc-gen-go when protoc-gen-go --version is run.
PROTOC_GEN_GO_NAME = "protoc-gen-go"
ifdef REL_VERSION
	CHRONOQUEUE_VERSION := $(REL_VERSION)
else
	CHRONOQUEUE_VERSION := edge
endif

LOCAL_ARCH := $(shell uname -m)
ifeq ($(LOCAL_ARCH),x86_64)
	TARGET_ARCH_LOCAL=amd64
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 5),armv8)
	TARGET_ARCH_LOCAL=arm64
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 4),armv)
	TARGET_ARCH_LOCAL=arm
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 5),arm64)
	TARGET_ARCH_LOCAL=arm64
else ifeq ($(shell echo $(LOCAL_ARCH) | head -c 7),aarch64)
	TARGET_ARCH_LOCAL=arm64
else
	TARGET_ARCH_LOCAL=amd64
endif
export GOARCH ?= $(TARGET_ARCH_LOCAL)

ifeq ($(GOARCH),amd64)
	LATEST_TAG?=latest
else
	LATEST_TAG?=latest-$(GOARCH)
endif

LOCAL_OS := $(shell uname)
ifeq ($(LOCAL_OS),Linux)
   TARGET_OS_LOCAL = linux
else ifeq ($(LOCAL_OS),Darwin)
   TARGET_OS_LOCAL = darwin
else
   TARGET_OS_LOCAL = windows
   PROTOC_GEN_GO_NAME := "protoc-gen-go.exe"
endif
export GOOS ?= $(TARGET_OS_LOCAL)

PROTOC_GEN_GO_VERSION = v1.36.9
PROTOC_GEN_GO_GRPC_VERSION = 1.5.1

# Default docker container and e2e test targets.
TARGET_OS ?= linux
TARGET_ARCH ?= amd64
TEST_OUTPUT_FILE_PREFIX ?= ./test_report

GOLANGCI_LINT_TAGS=subtlecrypto
ifeq ($(GOOS),windows)
	BINARY_EXT_LOCAL:=.exe
	GOLANGCI_LINT:=golangci-lint.exe
	export ARCHIVE_EXT = .zip
else
	BINARY_EXT_LOCAL:=
	GOLANGCI_LINT:=golangci-lint
	export ARCHIVE_EXT = .tar.gz
endif

export BINARY_EXT ?= $(BINARY_EXT_LOCAL)

OUT_DIR := ./dist

# Helm template and install setting
HELM:=helm
RELEASE_NAME?=chronoqueue
CHRONOQUEUE_NAMESPACE?=chronoqueue-system
CHRONOQUEUE_MTLS_ENABLED?=true
HELM_CHART_ROOT:=./deploy/charts
HELM_CHART_DIR:=$(HELM_CHART_ROOT)/chronoqueue
HELM_OUT_DIR:=$(OUT_DIR)/install
HELM_MANIFEST_FILE:=$(HELM_OUT_DIR)/$(RELEASE_NAME).yaml
HELM_REGISTRY?=ghcr.io/chronoqueue


################################################################################
# Go build details                                                             #
################################################################################
BASE_PACKAGE_NAME := github.com/adrien19/chronoqueue

ifeq ($(origin DEBUG), undefined)
  BUILDTYPE_DIR:=release
else ifeq ($(DEBUG),0)
  BUILDTYPE_DIR:=release
else
  BUILDTYPE_DIR:=debug
  GCFLAGS:=-gcflags="all=-N -l"
  $(info Build with debugger information)
endif

CHRONOQUEUE_OUT_DIR := $(OUT_DIR)/$(GOOS)_$(GOARCH)/$(BUILDTYPE_DIR)
CHRONOQUEUE_LINUX_OUT_DIR := $(OUT_DIR)/linux_$(GOARCH)/$(BUILDTYPE_DIR)


################################################################################
# Target: build                                                                #
################################################################################
.PHONY: build
CHRONOQUEUE_BINS:=$(foreach ITEM,$(BINARIES),$(CHRONOQUEUE_OUT_DIR)/$(ITEM)$(BINARY_EXT))
build: $(CHRONOQUEUE_BINS)

# Generate builds for chronoqueue binaries for the target
# Params:
# $(1): the binary name for the target
# $(2): the binary main directory
# $(3): the target os
# $(4): the target arch
# $(5): the output directory
define genBinariesForTarget
.PHONY: $(5)/$(1)
$(5)/$(1):
	CGO_ENABLED=$(CGO) GOOS=$(3) GOARCH=$(4) go build $(GCFLAGS) -ldflags="" -tags=$(CHRONOQUEUE_GO_BUILD_TAGS) \
	-o $(5)/$(1) $(2)/;
endef

# Generate binary targets
$(foreach ITEM,$(BINARIES),$(eval $(call genBinariesForTarget,$(ITEM)$(BINARY_EXT),./cmd/$(ITEM),$(GOOS),$(GOARCH),$(CHRONOQUEUE_OUT_DIR))))


################################################################################
# Target: build-linux                                                          #
################################################################################
BUILD_LINUX_BINS:=$(foreach ITEM,$(BINARIES),$(CHRONOQUEUE_LINUX_OUT_DIR)/$(ITEM))
build-linux: $(BUILD_LINUX_BINS)

# Generate linux binaries targets to build linux docker image
ifneq ($(GOOS), linux)
$(foreach ITEM,$(BINARIES),$(eval $(call genBinariesForTarget,$(ITEM),./cmd/$(ITEM),linux,$(GOARCH),$(CHRONOQUEUE_LINUX_OUT_DIR))))
endif


################################################################################
# Target: check-gotestsum                                                      #
################################################################################
.PHONY: check-gotestsum
check-gotestsum:
	@which gotestsum > /dev/null || { \
		echo "Installing gotestsum..."; \
		go install gotest.tools/gotestsum@latest; \
	}

################################################################################
# Target: test                                                                 #
################################################################################
.PHONY: test
test: check-gotestsum
	CGO_ENABLED=$(CGO) \
		gotestsum \
			--jsonfile $(TEST_OUTPUT_FILE_PREFIX)_unit.json \
			--format pkgname-and-test-fails \
			-- \
				./pkg/... ./internal/... ./cmd/... ./client/...\
				$(COVERAGE_OPTS)


.PHONY: test-no-gotestsum
test-no-gotestsum:
.PHONY: test-no-gotestsum
test-no-gotestsum:
	CGO_ENABLED=$(CGO) \
		go test -v \
				./pkg/... ./internal/... ./cmd/... ./client/... \
				$(COVERAGE_OPTS)

.PHONY: test-stable
test-stable:
	CGO_ENABLED=$(CGO) \
		go test -v \
				./client ./pkg/chronoqueue ./pkg/gateway ./pkg/metrics ./cmd/server ./internal/util ./internal/encryption/... ./pkg/log \
				$(COVERAGE_OPTS)

.PHONY: test-stable-gotestsum
test-stable-gotestsum: check-gotestsum
	CGO_ENABLED=$(CGO) gotestsum \
		--jsonfile $(TEST_OUTPUT_FILE_PREFIX)_stable.json \
		--format pkgname-and-test-fails \
		-- \
		./client ./pkg/chronoqueue ./pkg/gateway ./pkg/metrics ./cmd/server ./internal/util ./internal/encryption/... ./pkg/log \
		$(COVERAGE_OPTS)

################################################################################
# Target: test-race                                                            #
################################################################################
.PHONY: test-race
test-race:
	CGO_ENABLED=1 ./pkg/... ./internal/... ./cmd/... ./client/... | xargs \
		go test -race

################################################################################
# Target: check-linter                                                         #
################################################################################
.PHONY: check-linter
check-linter:
	@which $(GOLANGCI_LINT) > /dev/null || { \
		echo "Installing golangci-lint..."; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.55.2; \
	}

################################################################################
# Target: lint                                                                 #
################################################################################
# Please use golangci-lint version v1.55.2 , otherwise you might encounter errors.
# You can download version v1.55.2 at https://github.com/golangci/golangci-lint/releases/tag/v1.55.2
.PHONY: lint
lint: check-linter
	$(GOLANGCI_LINT) run --build-tags=$(GOLANGCI_LINT_TAGS) --timeout=20m

################################################################################
# Target: modtidy-all                                                          #
################################################################################
MODFILES := $(shell find . -name go.mod)

define modtidy-target
.PHONY: modtidy-$(1)
modtidy-$(1):
	cd $(shell dirname $(1)); CGO_ENABLED=$(CGO) go mod tidy -compat=1.21; cd -
endef

# Generate modtidy target action for each go.mod file
$(foreach MODFILE,$(MODFILES),$(eval $(call modtidy-target,$(MODFILE))))

# Enumerate all generated modtidy targets
TIDY_MODFILES:=$(foreach ITEM,$(MODFILES),modtidy-$(ITEM))

# Define modtidy-all action trigger to run make on all generated modtidy targets
.PHONY: modtidy-all
modtidy-all: $(TIDY_MODFILES)

################################################################################
# Target: modtidy                                                              #
################################################################################
.PHONY: modtidy
modtidy:
	go mod tidy

################################################################################
# Target: format                                                               #
################################################################################
.PHONY: format
format: modtidy-all
	gofumpt -l -w . && goimports -local github.com/adrien19/ -w $(shell find ./pkg -type f -name '*.go' -not -path "./api/chronoqueue/v1/*")

################################################################################
# Target: check                                                                #
################################################################################
.PHONY: check
check: format test lint
	git status && [[ -z `git status -s` ]]


# Download Google API proto files (required for HTTP annotations)
################################################################################
# Target: get-googleapis                                                       #
################################################################################
.PHONY: get-googleapis
get-googleapis: ## Download Google API proto files for annotations
	@echo "Downloading Google API proto files..."
	@mkdir -p ./proto/google/api
	@curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto \
		> ./proto/google/api/annotations.proto
	@curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto \
		> ./proto/google/api/http.proto
	@curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/field_behavior.proto \
		> ./proto/google/api/field_behavior.proto
	@echo "Google API proto files downloaded!"


################################################################################
# Target: init-proto                                                           #
################################################################################
.PHONY: init-proto
init-proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v$(PROTOC_GEN_GO_GRPC_VERSION)
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
	@echo "init-proto completed!"


################################################################################
# Target: gen-proto                                                            #
################################################################################
PROTO_PREFIX:=github.com/adrien19/chronoqueue
GRPC_PROTOS:=$(shell ls proto)

# Generate archive files for each binary
# $(1): the binary name to be archived
define genProtoc
.PHONY: gen-proto-$(1)
gen-proto-$(1):
	$(PROTOC) --go_out=. --go_opt=module=$(PROTO_PREFIX) --go-grpc_out=. --go-grpc_opt=require_unimplemented_servers=false,module=$(PROTO_PREFIX) ./proto/$(1)/v1/*.proto
	# Generate gRPC-Gateway reverse proxy code (only for queueservice)
	@if [ "$(1)" = "queueservice" ]; then \
		mkdir -p docs/api && \
		$(PROTOC) --grpc-gateway_out=. \
			--grpc-gateway_opt=module=$(PROTO_PREFIX) \
			--grpc-gateway_opt=generate_unbound_methods=true \
			./proto/$(1)/v1/service.proto; \
	fi
	# Generate OpenAPI v2 documentation (only for queueservice)
	@if [ "$(1)" = "queueservice" ]; then \
		$(PROTOC) --openapiv2_out=docs/api \
			--openapiv2_opt=allow_merge=true,merge_file_name=chronoqueue \
			./proto/$(1)/v1/service.proto; \
	fi
endef

$(foreach ITEM,$(GRPC_PROTOS),$(eval $(call genProtoc,$(ITEM))))

GEN_PROTOS:=$(foreach ITEM,$(filter-out google,$(GRPC_PROTOS)),gen-proto-$(ITEM))

.PHONY: gen-proto
gen-proto: check-proto-version $(GEN_PROTOS) modtidy

################################################################################
# Target: check-diff                                                           #
################################################################################
.PHONY: check-diff
check-diff:
	git diff --exit-code ./go.mod # check no changes
	git diff --exit-code ./go.sum # check no changes

################################################################################
# Target: check-proto-version                                                  #
################################################################################
.PHONY: check-proto-version
check-proto-version: ## Checking the version of proto related tools
	@test "$(shell protoc --version)" = "libprotoc $(PROTOC_VERSION)" \
	|| { echo "please use protoc $(PROTOC_VERSION) (protobuf $(PROTOBUF_SUITE_VERSION)) to generate proto"; exit 1; }

	@test "$(shell protoc-gen-go-grpc --version)" = "protoc-gen-go-grpc $(PROTOC_GEN_GO_GRPC_VERSION)" \
	|| { echo "please use protoc-gen-go-grpc $(PROTOC_GEN_GO_GRPC_VERSION) to generate proto"; exit 1; }

	@test "$(shell protoc-gen-go --version 2>&1)" = "$(PROTOC_GEN_GO_NAME) $(PROTOC_GEN_GO_VERSION)" \
	|| { echo "please use protoc-gen-go $(PROTOC_GEN_GO_VERSION) to generate proto"; exit 1; }


################################################################################
# Target: check-proto-diff                                                     #
################################################################################
.PHONY: check-proto-diff
check-proto-diff:
	git diff --exit-code ./api/chronoqueue/v1/service.pb.go # check no changes
	git diff --exit-code ./api/chronoqueue/v1/service_grpc.pb.go # check no changes


################################################################################
# Target: docker                                                               #
################################################################################
include docker/docker.mk