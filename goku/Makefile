.PHONY: help artifactory-login show-build-context build test tag push \
build-datahub-kafka-setup build-datahub-mysql-setup build-datahub-elasticsearch-setup build-datahub-upgrade \
build-datahub-gms build-datahub-frontend-react \
run-dependencies start-quickstart-mac-m1 stop-quickstart-mac-m1 \
promote-public-to-dev-artifactory

GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]')
GIT_REV=$(shell git log -1 --pretty=%h | tr '[:upper:]' '[:lower:]')
USERNAME=$(shell whoami | tr '[:upper:]' '[:lower:]')

DOCKER_REGISTRY_URL=docker-dev-artifactory.workday.com

DATAHUB_LATEST_TAG := head
DATAHUB_KAFKA_SETUP_IMG := datahub-kafka-setup
DATAHUB_MYSQL_SETUP_IMG := datahub-mysql-setup
DATAHUB_ELASTICSEARCH_SETUP_IMG := datahub-elasticsearch-setup
DATAHUB_UPGRADE_IMG := datahub-upgrade
DATAHUB_GMS_IMG := datahub-gms
DATAHUB_FRONTEND_REACT_IMG := datahub-frontend-react

DOCKER_IMG_TAG = $(GIT_BRANCH)-SNAPSHOT

can_push =
ifeq ($(USERNAME), jenkins)
    DOCKER_REPO := $(DOCKER_REGISTRY_URL)/omsopt/goku/datahub
    ifeq ($(GIT_BRANCH), master)
        can_push = yes
        DOCKER_IMG_TAG = $(GIT_BRANCH)-$(GIT_REV)
    endif
else
    can_push = yes
    DOCKER_REPO := $(DOCKER_REGISTRY_URL)/omsopt/$(USERNAME)/datahub
endif

.DEFAULT: help
help:
	@echo "make build"
	@echo "  build datahub docker images"
	@echo "make test"
	@echo "  build then run tests"
	@echo "make tag"
	@echo "  create local tags"
	@echo "make push"
	@echo "  push to the dev docker repository"
	@echo "make start-quickstart-mac-m1"
	@echo "  start datahub locally on Mac M1"
	@echo "make stop-quickstart-mac-m1"
	@echo "  stop datahub running locally on Mac M1"
	@echo "make run-ec2"
	@echo "  run datahub on ec2 with images from dev artifactory"
	@echo "make stop-ec2"
	@echo "  stop datahub running on ec2"

artifactory-login:
	@test "${AF_USER}" -a "${AF_PASSWORD}" || (echo 'AF_USER and AF_PASSWORD must be defined' && exit 1)
	@echo "Login to $(DOCKER_REGISTRY_URL) ..."

show-build-context:
	$(info user=$(USERNAME) branch=$(GIT_BRANCH), revision=$(GIT_REV))
	$(info DOCKER_REPO=$(DOCKER_REPO) DOCKER_IMG_TAG=$(DOCKER_IMG_TAG))

build-datahub-kafka-setup: show-build-context
	@echo "Build docker image of datahub-kafka-setup ..."
	@docker build -f docker/kafka-setup/Dockerfile --build-arg APACHE_DOWNLOAD_URL=https://archive.apache.org/dist/ -t $(DATAHUB_KAFKA_SETUP_IMG):${DATAHUB_LATEST_TAG} .

build-datahub-mysql-setup: show-build-context
	@echo "Build docker image of datahub-mysql-setup ..."
	@docker build -f docker/mysql-setup/Dockerfile -t $(DATAHUB_MYSQL_SETUP_IMG):${DATAHUB_LATEST_TAG} .

build-datahub-elasticsearch-setup: show-build-context
	@echo "Build docker image of datahub-elasticsearch-setup ..."
	@docker build -f docker/elasticsearch-setup/Dockerfile -t $(DATAHUB_ELASTICSEARCH_SETUP_IMG):${DATAHUB_LATEST_TAG} .

build-datahub-upgrade: show-build-context
	@echo "Build docker image of datahub-upgrade ..."
	@./gradlew :datahub-upgrade:build -x test --parallel
	@cp ./datahub-upgrade/build/libs/datahub-upgrade.jar .
	@docker build -f ./docker/datahub-upgrade/Dockerfile -t ${DATAHUB_UPGRADE_IMG}:${DATAHUB_LATEST_TAG} .

build-datahub-gms: show-build-context
	@echo "Build docker image of datahub-gms ..."
	@./gradlew :metadata-service:war:build -x test --parallel
	@cp ./metadata-service/war/build/libs/war.war .
	@docker build -f docker/datahub-gms/Dockerfile -t ${DATAHUB_GMS_IMG}:${DATAHUB_LATEST_TAG} .

build-datahub-frontend-react: show-build-context
	@echo "Build docker image of datahub-frontend-react ..."
	@./gradlew :datahub-frontend:dist -x test -x yarnTest -x yarnLint --parallel
	@cp ./datahub-frontend/build/distributions/datahub-frontend-*.zip datahub-frontend.zip
	@docker build -f docker/datahub-frontend/Dockerfile -t ${DATAHUB_FRONTEND_REACT_IMG}:${DATAHUB_LATEST_TAG} .

build: build-datahub-kafka-setup \
       build-datahub-mysql-setup \
       build-datahub-elasticsearch-setup \
       build-datahub-upgrade build-datahub-gms \
       build-datahub-frontend-react
	@echo

test: build
	@echo

tag: test
	docker tag $(DATAHUB_KAFKA_SETUP_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_KAFKA_SETUP_IMG):$(DATAHUB_LATEST_TAG)
	docker tag $(DATAHUB_KAFKA_SETUP_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_KAFKA_SETUP_IMG):$(DOCKER_IMG_TAG)
	docker tag $(DATAHUB_MYSQL_SETUP_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_MYSQL_SETUP_IMG):$(DATAHUB_LATEST_TAG)
	docker tag $(DATAHUB_MYSQL_SETUP_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_MYSQL_SETUP_IMG):$(DOCKER_IMG_TAG)
	docker tag $(DATAHUB_ELASTICSEARCH_SETUP_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_ELASTICSEARCH_SETUP_IMG):$(DATAHUB_LATEST_TAG)
	docker tag $(DATAHUB_ELASTICSEARCH_SETUP_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_ELASTICSEARCH_SETUP_IMG):$(DOCKER_IMG_TAG)
	docker tag $(DATAHUB_UPGRADE_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(DATAHUB_LATEST_TAG)
	docker tag $(DATAHUB_UPGRADE_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(DOCKER_IMG_TAG)
	docker tag $(DATAHUB_GMS_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(DATAHUB_LATEST_TAG)
	docker tag $(DATAHUB_GMS_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(DOCKER_IMG_TAG)
	docker tag $(DATAHUB_FRONTEND_REACT_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(DATAHUB_LATEST_TAG)
	docker tag $(DATAHUB_FRONTEND_REACT_IMG):$(DATAHUB_LATEST_TAG) $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(DOCKER_IMG_TAG)

ifdef can_push
push: artifactory-login, tag
	docker push $(DOCKER_REPO)/$(DATAHUB_KAFKA_SETUP_IMG):$(DATAHUB_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_KAFKA_SETUP_IMG):$(DOCKER_IMG_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_MYSQL_SETUP_IMG):$(DATAHUB_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_MYSQL_SETUP_IMG):$(DOCKER_IMG_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_ELASTICSEARCH_SETUP_IMG):$(DATAHUB_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_ELASTICSEARCH_SETUP_IMG):$(DOCKER_IMG_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(DATAHUB_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_UPGRADE_IMG):$(DOCKER_IMG_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(DATAHUB_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_GMS_IMG):$(DOCKER_IMG_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(DATAHUB_LATEST_TAG)
	docker push $(DOCKER_REPO)/$(DATAHUB_FRONTEND_REACT_IMG):$(DOCKER_IMG_TAG)
else
push: artifactory-login, tag
	@echo "Skipped; push only allowed on master branch by jenkins"
endif


# The default confluentinc version used by datahub
CONFLUENTINC_IMAGE_VERSION := 7.4.0

KAFKA_IMAGE := confluentinc/cp-kafka
KAFKA_VERSION := ${CONFLUENTINC_IMAGE_VERSION}
KAFKA_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${KAFKA_IMAGE}

# 7.4.0 can't pass XRay scans, so use 7.5.3
SCHEMA_REGISTRY_IMAGE := confluentinc/cp-schema-registry
SCHEMA_REGISTRY_VERSION := 7.5.3
SCHEMA_REGISTRY_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${SCHEMA_REGISTRY_IMAGE}

ZOOKEEPER_IMAGE := confluentinc/cp-zookeeper
ZOOKEEPER_VERSION := ${CONFLUENTINC_IMAGE_VERSION}
ZOOKEEPER_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${ZOOKEEPER_IMAGE}

# 7.10.1 can't pass XRay scans, so use 7.17.18
DATAHUB_SEARCH_IMAGE := elasticsearch/elasticsearch
DATAHUB_SEARCH_VERSION := 7.17.18
DATAHUB_SEARCH_IMAGE_IN_ARTIFACTORY := ${DOCKER_REGISTRY_URL}/${DATAHUB_SEARCH_IMAGE}

run-dependencies: artifactory-login
	@

# start-quickstart-mac-m1
#   - uses all datahub images from local build
#   - confluentinc, elasticsearch from dev artifactory
#   - mariadb from dockerhub
start-quickstart-mac-m1: run-dependencies
	DATAHUB_FRONTEND_IMAGE=$(DATAHUB_FRONTEND_REACT_IMG) \
	DATAHUB_GMS_IMAGE=$(DATAHUB_GMS_IMG) \
	DATAHUB_UPGRADE_IMAGE=$(DATAHUB_UPGRADE_IMG) \
	DATAHUB_ELASTIC_SETUP_IMAGE=$(DATAHUB_ELASTICSEARCH_SETUP_IMG) \
	DATAHUB_KAFKA_SETUP_IMAGE=$(DATAHUB_KAFKA_SETUP_IMG) \
	DATAHUB_MYSQL_SETUP_IMAGE=$(DATAHUB_MYSQL_SETUP_IMG) \
	KAFKA_IMAGE=$(KAFKA_IMAGE_IN_ARTIFACTORY) \
	KAFKA_VERSION=$(KAFKA_VERSION) \
	SCHEMA_REGISTRY_IMAGE=$(SCHEMA_REGISTRY_IMAGE_IN_ARTIFACTORY) \
	SCHEMA_REGISTRY_VERSION=$(SCHEMA_REGISTRY_VERSION) \
	ZOOKEEPER_IMAGE=$(ZOOKEEPER_IMAGE_IN_ARTIFACTORY) \
	ZOOKEEPER_VERSION=$(ZOOKEEPER_VERSION) \
	DATAHUB_SEARCH_IMAGE=$(DATAHUB_SEARCH_IMAGE_IN_ARTIFACTORY) \
	DATAHUB_SEARCH_TAG=$(DATAHUB_SEARCH_VERSION) \
	docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml up -d
	@echo
	@echo "Go to http://localhost:9002/ ..."
	@echo

stop-quickstart-mac-m1:
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

promote-public-to-dev-artifactory: artifactory-login
	$(call promote_public_to_dev_artifactory,${KAFKA_IMAGE}:${CONFLUENTINC_IMAGE_VERSION})
	$(call promote_public_to_dev_artifactory,${SCHEMA_REGISTRY_IMAGE}:${SCHEMA_REGISTRY_VERSION})
	$(call promote_public_to_dev_artifactory,${ZOOKEEPER_IMAGE}:${CONFLUENTINC_IMAGE_VERSION})
	$(call promote_public_to_dev_artifactory,${DATAHUB_SEARCH_IMAGE}:${DATAHUB_SEARCH_VERSION})
