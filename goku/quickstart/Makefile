.PHONY: check-artifactory-credential artifactory-login build pull help

GIT_REV=$(shell git log -1 --pretty=%h | tr '[:upper:]' '[:lower:]')
DOCKER_REGISTRY_URL := docker-dev-artifactory.workday.com
DOCKER_REPO := $(DOCKER_REGISTRY_URL)/omsopt/goku

IMG_NAME := goku-datahub-sample-data
IMG_TAG := $(GIT_REV)

.DEFAULT: help
help:
	@echo "make build"
	@echo "  build & push multi-architecture docker image"
	@echo "make pull"
	@echo "  run the latest image from artifactory"
	@echo "make run"
	@echo "  run the image"

# docker buildx create --name mybuilder --use
# docker buildx inspect --bootstrap
build: artifactory-login
	docker buildx build --no-cache --platform linux/amd64,linux/arm64 -t $(DOCKER_REPO)/$(IMG_NAME):$(IMG_TAG) -t $(DOCKER_REPO)/$(IMG_NAME):latest . --push

pull:
	docker pull $(DOCKER_REPO)/$(IMG_NAME):latest

run: pull
	docker run -e HOST_OS=$$(uname -s) $(DOCKER_REPO)/$(IMG_NAME):latest

check-artifactory-credential:
	@test "${AF_USER}" -a "${AF_PASSWORD}" || (echo 'AF_USER and AF_PASSWORD must be defined' && exit 1)

artifactory-login: check-artifactory-credential
	@echo "Login to $(DOCKER_REGISTRY_URL) ..."
	@echo ${AF_PASSWORD} | docker login ${DOCKER_REGISTRY_URL} --username ${AF_USER} --password-stdin
