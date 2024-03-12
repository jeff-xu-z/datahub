.PHONY: help check-artifactory-credential artifactory-login build-dependencies check-git-info run-dependencies \
build-datahub-builder \
local-build-datahub-upgrade local-build-datahub-gms local-build-datahub-frontend-react local-build-datahub dev-build-datahub-software \
build-datahub-software build-datahub-docker-images \
build test tag push \
promote-confluentinc-images-from-public-to-dev-artifactory \
promote-datahub-images-from-public-to-dev-artifactory \
promote-dev-images-from-public-to-dev-artifactory \
start-datahub-mac-m1 stop-datahub-mac-m1


GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]')
GIT_REV=$(shell git log -1 --pretty=%h | tr '[:upper:]' '[:lower:]')
USERNAME=$(shell whoami | tr '[:upper:]' '[:lower:]')

DOCKER_REGISTRY_URL=docker-dev-artifactory.workday.com

DATAHUB_UPGRADE_IMG := datahub-upgrade
DATAHUB_GMS_IMG := datahub-gms
DATAHUB_FRONTEND_REACT_IMG := datahub-frontend-react

LOCAL_SNAPSHOT_TAG := $(GIT_BRANCH)-SNAPSHOT
LOCAL_LATEST_TAG := head
ARTIFACTORY_DOCKER_IMG_GIT_TAG := $(GIT_BRANCH)-$(GIT_REV)
ARTIFACTORY_DOCKER_IMG_LATEST_TAG := $(LOCAL_LATEST_TAG)

DOCKER_REPO := $(DOCKER_REGISTRY_URL)/omsopt/goku/datahub/$(USERNAME)

.DEFAULT: help
help:
	@echo
	@echo "make local-build-datahub-gms (for developer)"
	@echo "  compile & build datahub gms (war.war only) from the current folder"
	@echo
	@echo "make local-build-datahub-frontend-react (for developer)"
	@echo "  compile & build datahub frontend (datahub-frontend.zip) from the current folder"
	@echo
	@echo "make local-build-datahub-upgrade (for developer)"
	@echo "  compile & build datahub upgrade (datahub-upgrade.jar) from the current folder"
	@echo
	@echo "make dev-build-datahub-software (for developer)"
	@echo "  compile & build datahub software from scratch (same as Jenkins)"
	@echo "  - Updated code must have been pushed to https://ghe.megaleo.com/dpoe/pharos-goku-datahub.git"
	@echo "  - GIT_USER, GIT_PASSCODE, GIT_REV must be supplied as environment variable"
	@echo
	@echo "make build-datahub-docker-images (for developer)"
	@echo "  build datahub docker images (gms, frontend and upgrade) by using"
	@echo "  war.war, datahub-upgrade.jar and datahub-frontend.zip from the"
	@echo "  current folder"
	@echo
	@echo
	@echo "make start-datahub-mac-m1 (for developer)"
	@echo "  launch datahub by using locally built docker images with docker-compose"
	@echo
	@echo "make stop-datahub-mac-m1 (for developer)"
	@echo "  stop currently running datahub"
	@echo
	@echo
	@echo "make build (for Jenkins)"
	@echo "  build datahub software & docker images"
	@echo
	@echo "make tag (for Jenkins)"
	@echo "  create local tags"
	@echo
	@echo "make push (for Jenkins)"
	@echo "  push images to the dev artifactory"
	@echo


check-artifactory-credential:
	@test "${AF_USER}" -a "${AF_PASSWORD}" || (echo 'AF_USER and AF_PASSWORD must be defined' && exit 1)

artifactory-login: check-artifactory-credential
	@echo "Login to $(DOCKER_REGISTRY_URL) ..."
	@echo ${AF_PASSWORD} | docker login ${DOCKER_REGISTRY_URL} --username ${AF_USER} --password-stdin

build-dependencies: check-artifactory-credential
	$(info user=$(USERNAME) branch=$(GIT_BRANCH), revision=$(GIT_REV))
	$(info DOCKER_REPO=$(DOCKER_REPO) LOCAL_SNAPSHOT_TAG=$(LOCAL_SNAPSHOT_TAG))

###############################################
#
# artifactory setup
#
###############################################

ALPINE_REPO_URL := https://${AF_USER}:${AF_PASSWORD}@artifactory.workday.com/artifactory/generic-alpine-remote/alpine
GITHUB_REPO_URL := https://${AF_USER}:${AF_PASSWORD}@artifactory.workday.com/artifactory/github-remote
MAVEN_CENTRAL_REPO_URL := https://${AF_USER}:${AF_PASSWORD}@artifactory.workday.com/artifactory/repo1
GOPROXY := https://${AF_USER}:${AF_PASSWORD}@artifactory.workday.com/artifactory/api/go/go

GOKU_DATAHUB_BUILDER_IMG := goku-datahub-builder
GOKU_DATAHUB_BUILDER_TAG := latest

#
# Build the image that builds datahub software
#
build-datahub-builder: build-dependencies
	@echo "build ${GOKU_DATAHUB_BUILDER_IMG}:${GOKU_DATAHUB_BUILDER_TAG} ..."
	@(cd ./goku/builder && docker build -t ${GOKU_DATAHUB_BUILDER_IMG}:${GOKU_DATAHUB_BUILDER_TAG} .)

###############################################
#
# local build targets: for developers only
#
###############################################

local-build-datahub-upgrade:
	@echo "Build datahub-upgrade ..."
	@./gradlew -PAF_USER=${AF_USER} -PAF_PASSWORD=${AF_PASSWORD} :datahub-upgrade:build -x test --parallel --info
	@cp ./datahub-upgrade/build/libs/datahub-upgrade.jar .

local-build-datahub-gms:
	@echo "Build datahub-gms ..."
	@./gradlew -PAF_USER=${AF_USER} -PAF_PASSWORD=${AF_PASSWORD} :metadata-service:war:build -x test --parallel --info
	@cp ./metadata-service/war/build/libs/war.war .

local-build-datahub-frontend-react:
	@echo "Build datahub-frontend-react ..."
	@./gradlew -PnodeDistBaseUrl=https://${AF_USER}:${AF_PASSWORD}@artifactory.workday.com/artifactory/nodejs.org-cache/dist \
		-PAF_USER=${AF_USER} -PAF_PASSWORD=${AF_PASSWORD} \
		:datahub-frontend:dist -x test -x yarnTest -x yarnLint --parallel --info
	@cp ./datahub-frontend/build/distributions/datahub-frontend-*.zip datahub-frontend.zip

local-build-datahub: local-build-datahub-upgrade, local-build-datahub-gms, local-build-datahub-frontend-react
	@echo

check-git-info:
	@test "${GIT_USER}" -a "${GIT_PASSCODE}" -a "${GIT_REV}" || (echo 'GIT_USER, GIT_PASSCODE and GIT_REV must be defined' && exit 1)

dev-build-datahub-software: check-git-info, build-datahub-builder
	@echo "Build datahub software ..."
	@docker run -e AF_USER=${AF_USER} -e AF_PASSWORD=${AF_PASSWORD} \
		-e GIT_USER=${GIT_USER} -e GIT_PASSCODE=${GIT_PASSCODE} \
		-v .:/artifacts \
		${GOKU_DATAHUB_BUILDER_IMG}:${GOKU_DATAHUB_BUILDER_TAG} \
		git-rev=${GIT_REV} \
		artifact-dir=/artifacts



###############################################
#
# Jenkins build targets
#
###############################################

#
# Build datahub core software (jars, frontend-react, etc.)
#
# - Jenkins already has freshly checked out code
build-datahub-software: build-datahub-builder
	@echo "Build datahub software ..."
	@docker run -e AF_USER=${AF_USER} -e AF_PASSWORD=${AF_PASSWORD} \
		-v .:/artifacts \
		-v .:/datahub \
		${GOKU_DATAHUB_BUILDER_IMG}:${GOKU_DATAHUB_BUILDER_TAG} \
		artifact-dir=/artifacts \
		datahub-dir=/datahub

# Assumes:
# - in datahub folder
# - datahub-frontend.zip, datahub-upgrade.jar and war.war are already built
build-datahub-docker-images: build-dependencies
	@echo "Build docker image of datahub-upgrade ..."
	@docker build -f ./docker/datahub-upgrade/Dockerfile \
		--build-arg ALPINE_REPO_URL=${ALPINE_REPO_URL} \
		--build-arg GITHUB_REPO_URL=${GITHUB_REPO_URL} \
		--build-arg MAVEN_CENTRAL_REPO_URL=${MAVEN_CENTRAL_REPO_URL} \
		--build-arg GOPROXY=${GOPROXY} \
		--progress=plain \
		--no-cache \
		-t ${DATAHUB_UPGRADE_IMG}:${LOCAL_SNAPSHOT_TAG} -t ${DATAHUB_UPGRADE_IMG}:${LOCAL_LATEST_TAG} .
	@echo "Build docker image of datahub-gms ..."
	@docker build -f ./docker/datahub-gms/Dockerfile \
		--build-arg ALPINE_REPO_URL=${ALPINE_REPO_URL} \
		--build-arg GITHUB_REPO_URL=${GITHUB_REPO_URL} \
		--build-arg MAVEN_CENTRAL_REPO_URL=${MAVEN_CENTRAL_REPO_URL} \
		--build-arg GOPROXY=${GOPROXY} \
		--progress=plain \
		--no-cache \
		-t ${DATAHUB_GMS_IMG}:${LOCAL_SNAPSHOT_TAG} -t ${DATAHUB_GMS_IMG}:${LOCAL_LATEST_TAG} .
	@echo "Build docker image of datahub-frontend-react ..."
	@docker build -f ./docker/datahub-frontend/Dockerfile \
		--build-arg ALPINE_REPO_URL=${ALPINE_REPO_URL} \
		--build-arg GITHUB_REPO_URL=${GITHUB_REPO_URL} \
		--build-arg MAVEN_CENTRAL_REPO_URL=${MAVEN_CENTRAL_REPO_URL} \
		--progress=plain \
		--no-cache \
		-t ${DATAHUB_FRONTEND_REACT_IMG}:${LOCAL_SNAPSHOT_TAG} -t ${DATAHUB_FRONTEND_REACT_IMG}:${LOCAL_LATEST_TAG} .

build: build-datahub-software \
       build-datahub-docker-images
	@echo

test: build
	@echo

# tag: test (choose not to do so)
tag:
	docker tag $(DATAHUB_UPGRADE_IMG):$(LOCAL_SNAPSHOT_TAG) $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(ARTIFACTORY_DOCKER_IMG_GIT_TAG)
	docker tag $(DATAHUB_UPGRADE_IMG):$(LOCAL_SNAPSHOT_TAG) $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(ARTIFACTORY_DOCKER_IMG_LATEST_TAG)
	docker tag $(DATAHUB_GMS_IMG):$(LOCAL_SNAPSHOT_TAG) $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(ARTIFACTORY_DOCKER_IMG_GIT_TAG)
	docker tag $(DATAHUB_GMS_IMG):$(LOCAL_SNAPSHOT_TAG) $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(ARTIFACTORY_DOCKER_IMG_LATEST_TAG)
	docker tag $(DATAHUB_FRONTEND_REACT_IMG):$(LOCAL_SNAPSHOT_TAG) $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(ARTIFACTORY_DOCKER_IMG_GIT_TAG)
	docker tag $(DATAHUB_FRONTEND_REACT_IMG):$(LOCAL_SNAPSHOT_TAG) $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(ARTIFACTORY_DOCKER_IMG_LATEST_TAG)

push: artifactory-login tag
	docker push $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(ARTIFACTORY_DOCKER_IMG_GIT_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(ARTIFACTORY_DOCKER_IMG_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(ARTIFACTORY_DOCKER_IMG_GIT_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(ARTIFACTORY_DOCKER_IMG_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(ARTIFACTORY_DOCKER_IMG_GIT_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(ARTIFACTORY_DOCKER_IMG_LATEST_TAG)


###############################################
#
# confluentinc images, needed for local dev
#
###############################################

# The default confluentinc version used by datahub
CONFLUENTINC_VERSION := 7.4.0

KAFKA_IMAGE := confluentinc/cp-kafka
KAFKA_VERSION := ${CONFLUENTINC_VERSION}
KAFKA_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${KAFKA_IMAGE}

# 7.4.0 can't pass XRay scans, so use 7.5.3
SCHEMA_REGISTRY_IMAGE := confluentinc/cp-schema-registry
SCHEMA_REGISTRY_VERSION := 7.5.3
SCHEMA_REGISTRY_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${SCHEMA_REGISTRY_IMAGE}

ZOOKEEPER_IMAGE := confluentinc/cp-zookeeper
ZOOKEEPER_VERSION := ${CONFLUENTINC_VERSION}
ZOOKEEPER_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${ZOOKEEPER_IMAGE}

# 7.10.1 can't pass XRay scans, so use 7.17.18
DATAHUB_SEARCH_IMAGE := elasticsearch/elasticsearch
DATAHUB_SEARCH_VERSION := 7.17.18
DATAHUB_SEARCH_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${DATAHUB_SEARCH_IMAGE}

promote-confluentinc-images-from-public-to-dev-artifactory: artifactory-login
	$(call promote_public_to_dev_artifactory,${KAFKA_IMAGE}:${KAFKA_VERSION})
	$(call promote_public_to_dev_artifactory,${SCHEMA_REGISTRY_IMAGE}:${SCHEMA_REGISTRY_VERSION})
	$(call promote_public_to_dev_artifactory,${ZOOKEEPER_IMAGE}:${ZOOKEEPER_VERSION})
	$(call promote_public_to_dev_artifactory,${DATAHUB_SEARCH_IMAGE}:${DATAHUB_SEARCH_VERSION})


###################################################
#
# datahub images, needed to deploy & run datahub
#
# acryldata/datahub-mysql-setup:v0.12.1
# linkedin/datahub-kafka-setup:v0.12.1
# linkedin/datahub-elasticsearch-setup:v0.12.1
###################################################

DATAHUB_VERSION := v0.13.0

DATAHUB_MYSQL_SETUP_IMG := acryldata/datahub-mysql-setup
DATAHUB_MYSQL_SETUP_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${DATAHUB_MYSQL_SETUP_IMG}
DATAHUB_KAFKA_SETUP_IMG := linkedin/datahub-kafka-setup
DATAHUB_KAFKA_SETUP_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${DATAHUB_KAFKA_SETUP_IMG}
DATAHUB_ELASTICSEARCH_SETUP_IMG := linkedin/datahub-elasticsearch-setup
DATAHUB_ELASTICSEARCH_SETUP_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${DATAHUB_ELASTICSEARCH_SETUP_IMG}

promote-datahub-images-from-public-to-dev-artifactory: artifactory-login
	$(call promote_public_to_dev_artifactory,${DATAHUB_MYSQL_SETUP_IMG}:${DATAHUB_VERSION})
	$(call promote_public_to_dev_artifactory,${DATAHUB_KAFKA_SETUP_IMG}:${DATAHUB_VERSION})
	$(call promote_public_to_dev_artifactory,${DATAHUB_ELASTICSEARCH_SETUP_IMG}:${DATAHUB_VERSION})


###################################################
#
# dev images, needed to build datahub
#
###################################################

promote-dev-images-from-public-to-dev-artifactory: artifactory-login
	$(call promote_public_to_dev_artifactory,library/alpine:3.18)
	$(call promote_public_to_dev_artifactory,library/golang:1-alpine3.18)
	$(call promote_public_to_dev_artifactory,library/python:3-alpine)
	$(call promote_public_to_dev_artifactory,library/ubuntu:24.04)


run-dependencies: artifactory-login
	@

# start-quickstart-mac-m1
#   - uses 3 datahub images from local build
#   - confluentinc, elasticsearch from dev artifactory
#   - mariadb from dockerhub
start-datahub-mac-m1: run-dependencies
	DATAHUB_FRONTEND_IMAGE=$(DATAHUB_FRONTEND_REACT_IMG) \
	DATAHUB_FRONTEND_VERSION=$(LOCAL_LATEST_TAG) \
	DATAHUB_GMS_IMAGE=$(DATAHUB_GMS_IMG) \
	DATAHUB_GMS_VERSION=$(LOCAL_LATEST_TAG) \
	DATAHUB_UPGRADE_IMAGE=$(DATAHUB_UPGRADE_IMG) \
	DATAHUB_UPGRADE_VERSION=$(LOCAL_LATEST_TAG) \
	KAFKA_IMAGE=$(KAFKA_IMAGE_IN_ARTIFACTORY) \
	KAFKA_VERSION=$(KAFKA_VERSION) \
	DATAHUB_SEARCH_IMAGE=$(DATAHUB_SEARCH_IMAGE_IN_ARTIFACTORY) \
	DATAHUB_SEARCH_TAG=$(DATAHUB_SEARCH_VERSION) \
	DATAHUB_VERSION=$(DATAHUB_VERSION) \
	DATAHUB_ELASTIC_SETUP_IMAGE=$(DATAHUB_ELASTICSEARCH_SETUP_IMAGE_IN_ARTIFACTORY) \
	DATAHUB_KAFKA_SETUP_IMAGE=$(DATAHUB_KAFKA_SETUP_IMAGE_IN_ARTIFACTORY) \
	DATAHUB_MYSQL_SETUP_IMAGE=$(DATAHUB_MYSQL_SETUP_IMAGE_IN_ARTIFACTORY) \
	SCHEMA_REGISTRY_IMAGE=$(SCHEMA_REGISTRY_IMAGE_IN_ARTIFACTORY) \
	SCHEMA_REGISTRY_VERSION=$(SCHEMA_REGISTRY_VERSION) \
	ZOOKEEPER_IMAGE=$(ZOOKEEPER_IMAGE_IN_ARTIFACTORY) \
	ZOOKEEPER_VERSION=$(ZOOKEEPER_VERSION) \
	docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml up -d
	@echo
	@echo "Go to http://localhost:9002/ ..."
	@echo

stop-datahub-mac-m1:
	docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml down
	@echo

# https://confluence.workday.com/display/DEVQA/Publishing%2C+Promoting+and+Installing+Docker+Content+in+Customer+Data+Environments+Using+Artifactory#Publishing,PromotingandInstallingDockerContentinCustomerDataEnvironmentsUsingArtifactory-PromotingThird-PartyContentfromdocker-public-repos(docker-public-artifactory.workday.com)toanotherRegistry
# https://artifactory.workday.com/ui/native/docker-public-repos/
# https://docker-dev-artifactory.workday.com/ui/native/docker-images-dev/
define promote_public_to_dev_artifactory
	@echo "promoting $(1) from public to dev artifactory ..."
	@docker run docker-dev-artifactory.workday.com/ambt/artifactory-client:latest docker promote-image3 \
		--source-registry=docker-public-repos \
		--source-image=$(1) \
		--target-registry=docker-images-dev \
		--target-image=$(1) \
		--artifactory-url=https://artifactory.workday.com/artifactory \
		--artifactory-user=${AF_USER} \
		--artifactory-password=${AF_PASSWORD} \
		--apply-changes
endef
