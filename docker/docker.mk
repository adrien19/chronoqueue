# Docker image build setting
DOCKER:=docker
DOCKERFILE_DIR?=./docker
CHRONOQUEUE_IMAGE_NAME?=$(RELEASE_NAME)

# build docker image for linux
BIN_DIR:=$(OUT_DIR)/$(TARGET_OS)_$(TARGET_ARCH)

ifeq ($(TARGET_OS),windows)
	DOCKERFILE?=Dockerfile-windows
	BIN_DIR:=$(BIN_DIR)/release
else
	DOCKERFILE?=Dockerfile
	BIN_DIR:=$(BIN_DIR)/release
endif

ifeq ($(TARGET_ARCH),arm)
	DOCKER_IMAGE_PLATFORM:=$(TARGET_OS)/arm/v7
else ifeq ($(TARGET_ARCH),arm64)
	DOCKER_IMAGE_PLATFORM:=$(TARGET_OS)/arm64/v8
else
	DOCKER_IMAGE_PLATFORM:=$(TARGET_OS)/amd64
endif

# Supported docker image architecture
DOCKER_MULTI_ARCH?=linux-amd64 linux-arm linux-arm64 windows-1809-amd64 windows-ltsc2022-amd64
DEV_CONTAINER_MULTI_ARCH?=linux/amd64,linux/arm64

################################################################################
# Target: docker-build                                                         #
################################################################################

# If WINDOWS_VERSION is set, use it as the Windows version in the docker image tag.
# If unset, use a simple tag.
ifneq ($(WINDOWS_VERSION),)
	BUILD_ARGS=--build-arg WINDOWS_VERSION=$(WINDOWS_VERSION)
	DOCKER_IMAGE_VARIANT=$(TARGET_OS)-$(WINDOWS_VERSION)-$(TARGET_ARCH)
else
	DOCKER_IMAGE_VARIANT=$(TARGET_OS)-$(TARGET_ARCH)
endif

ifeq ($(MANIFEST_TAG),)
	MANIFEST_TAG=$(CHRONOQUEUE_TAG)
endif

ifeq ($(MANIFEST_LATEST_TAG),)
	MANIFEST_LATEST_TAG=$(CHRONOQUEUE_TAG)
endif

LINUX_BINS_OUT_DIR=$(OUT_DIR)/linux_$(GOARCH)
DOCKER_IMAGE=$(DOCKER_REGISTRY)/$(CHRONOQUEUE_IMAGE_NAME)
BUILD_TAG=$(CHRONOQUEUE_TAG)-$(CHRONOQUEUE_TAG)


check-docker-env:
ifeq ($(DOCKER_REGISTRY),)
	$(error DOCKER_REGISTRY environment variable must be set)
endif
ifeq ($(CHRONOQUEUE_TAG),)
	$(error CHRONOQUEUE_TAG environment variable must be set)
endif

check-arch:
ifeq ($(TARGET_OS),)
	$(error TARGET_OS environment variable must be set)
endif
ifeq ($(TARGET_ARCH),)
	$(error TARGET_ARCH environment variable must be set)
endif

docker-build: SHELL := $(shell which bash)
docker-build: check-docker-env check-arch
	$(info Building $(DOCKER_IMAGE):$(CHRONOQUEUE_TAG) docker image ...)
ifeq ($(TARGET_ARCH),$(TARGET_ARCH_LOCAL))
	$(DOCKER) build --build-arg PKG_FILES=* $(BUILD_ARGS) -f $(DOCKERFILE_DIR)/$(DOCKERFILE) $(BIN_DIR) -t $(DOCKER_IMAGE):$(BUILD_TAG)
else
	-$(DOCKER) buildx create --use --name chronoqueuebuild
	-$(DOCKER) run --rm --privileged multiarch/qemu-user-static --reset -p yes
	$(DOCKER) buildx build --build-arg PKG_FILES=* $(BUILD_ARGS) --platform $(DOCKER_IMAGE_PLATFORM) -f $(DOCKERFILE_DIR)/$(DOCKERFILE) $(BIN_DIR) -t $(DOCKER_IMAGE):$(BUILD_TAG) --provenance=false
endif


################################################################################
# Target: build-dev-container 					                               #
################################################################################

# Update whenever you upgrade dev container image
DEV_CONTAINER_VERSION_TAG?=latest

# ChronoQueue container image name
DEV_CONTAINER_IMAGE_NAME=chronoqueue-dev

DEV_CONTAINER_DOCKERFILE=Dockerfile-dev
DOCKERFILE_DIR=./docker

check-docker-env-for-dev-container:
ifeq ($(DOCKER_REGISTRY),)
	$(error DOCKER_REGISTRY environment variable must be set)
endif

build-dev-container:
ifeq ($(DOCKER_REGISTRY),)
	$(info DOCKER_REGISTRY environment variable not set, tagging image without registry prefix.)
	$(info `make tag-dev-container` should be run with DOCKER_REGISTRY before `make push-dev-container.)
	$(DOCKER) build -f $(DOCKERFILE_DIR)/$(DEV_CONTAINER_DOCKERFILE) $(DOCKERFILE_DIR)/. -t $(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG)
else
	$(DOCKER) build -f $(DOCKERFILE_DIR)/$(DEV_CONTAINER_DOCKERFILE) $(DOCKERFILE_DIR)/. -t $(DOCKER_REGISTRY)/$(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG)
endif

tag-dev-container: check-docker-env-for-dev-container
	$(DOCKER) tag $(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG) $(DOCKER_REGISTRY)/$(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG)

push-dev-container: check-docker-env-for-dev-container
	$(DOCKER) push $(DOCKER_REGISTRY)/$(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG)

build-dev-container-all-arch:
ifeq ($(DOCKER_REGISTRY),)
	$(info DOCKER_REGISTRY environment variable not set, tagging image without registry prefix.)
	$(DOCKER) buildx build --platform $(DEV_CONTAINER_MULTI_ARCH) \
		-f $(DOCKERFILE_DIR)/$(DEV_CONTAINER_DOCKERFILE) $(DOCKERFILE_DIR)/. \
		-t $(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG) \
		--provenance=false
else
	$(DOCKER) buildx build --platform $(DEV_CONTAINER_MULTI_ARCH) \
		-f $(DOCKERFILE_DIR)/$(DEV_CONTAINER_DOCKERFILE) $(DOCKERFILE_DIR)/. \
		-t $(DOCKER_REGISTRY)/$(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG) \
		--provenance=false
endif

push-dev-container-all-arch: check-docker-env-for-dev-container
	$(DOCKER) buildx build --platform $(DEV_CONTAINER_MULTI_ARCH) \
		-f $(DOCKERFILE_DIR)/$(DEV_CONTAINER_DOCKERFILE) $(DOCKERFILE_DIR)/. \
		-t $(DAPR_REGISTRY)/$(DEV_CONTAINER_IMAGE_NAME):$(DEV_CONTAINER_VERSION_TAG) \
		--push --provenance=false