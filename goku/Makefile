.PHONY: help build test tag push build-dependencies build-datahub-upgrade 

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


build-dependencies:
	$(info user=$(USERNAME) branch=$(GIT_BRANCH), revision=$(GIT_REV))
	$(info DOCKER_REPO=$(DOCKER_REPO) DOCKER_IMG_TAG=$(DOCKER_IMG_TAG))
	@test "${AF_USER}" -a "${AF_PASSWORD}" || (echo 'AF_USER and AF_PASSWORD must be defined' && exit 1)
	@echo "Login to $(DOCKER_REGISTRY_URL) ..."
	@echo ${AF_PASSWORD} | docker login $(DOCKER_REGISTRY_URL) --username ${AF_USER} --password-stdin

build-datahub-kafka-setup: build-dependencies
	@echo "Build docker image of datahub-kafka-setup ..."
	@docker build -f docker/kafka-setup/Dockerfile --build-arg APACHE_DOWNLOAD_URL=https://archive.apache.org/dist/ -t $(DATAHUB_KAFKA_SETUP_IMG):${DATAHUB_LATEST_TAG} .

build-datahub-mysql-setup: build-dependencies
	@echo "Build docker image of datahub-mysql-setup ..."
	@docker build -f docker/mysql-setup/Dockerfile -t $(DATAHUB_MYSQL_SETUP_IMG):${DATAHUB_LATEST_TAG} .

build-datahub-elasticsearch-setup: build-dependencies
	@echo "Build docker image of datahub-elasticsearch-setup ..."
	@docker build -f docker/elasticsearch-setup/Dockerfile -t $(DATAHUB_ELASTICSEARCH_SETUP_IMG):${DATAHUB_LATEST_TAG} .

build-datahub-upgrade: build-dependencies
	@echo "Build docker image of datahub-upgrade ..."
	@./gradlew :datahub-upgrade:build -x test --parallel
	@cp ./datahub-upgrade/build/libs/datahub-upgrade.jar .
	@docker build -f ./docker/datahub-upgrade/Dockerfile -t ${DATAHUB_UPGRADE_IMG}:${DATAHUB_LATEST_TAG} .

build-datahub-gms: build-dependencies
	@echo "Build docker image of datahub-gms ..."
	@./gradlew :metadata-service:war:build -x test --parallel
	@cp ./metadata-service/war/build/libs/war.war .
	@docker build -f docker/datahub-gms/Dockerfile -t ${DATAHUB_GMS_IMG}:${DATAHUB_LATEST_TAG} .

build-datahub-frontend-react: build-dependencies
	@echo "Build docker image of datahub-frontend-react ..."
	@./gradlew :datahub-frontend:dist -x test -x yarnTest -x yarnLint --parallel
	@cp ./datahub-frontend/build/distributions/datahub-frontend-*.zip datahub-frontend.zip
	@docker build -f docker/datahub-frontend/Dockerfile -t ${DATAHUB_FRONTEND_REACT_IMG}:${DATAHUB_LATEST_TAG} .

build: build-datahub-kafka-setup build-datahub-mysql-setup build-datahub-elasticsearch-setup build-datahub-upgrade build-datahub-gms build-datahub-frontend-react
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
push: tag
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
push: tag
	@echo "Skipped; push only allowed on master branch by jenkins"
endif

run-local-m1:
	DATAHUB_FRONTEND_IMAGE=$(DATAHUB_FRONTEND_REACT_IMG) \
	DATAHUB_GMS_IMAGE=$(DATAHUB_GMS_IMG) \
	DATAHUB_UPGRADE_IMAGE=$(DATAHUB_UPGRADE_IMG) \
	DATAHUB_ELASTIC_SETUP_IMAGE=$(DATAHUB_ELASTICSEARCH_SETUP_IMG) \
	DATAHUB_KAFKA_SETUP_IMAGE=$(DATAHUB_KAFKA_SETUP_IMG) \
	DATAHUB_MYSQL_SETUP_IMAGE=$(DATAHUB_MYSQL_SETUP_IMG) \
	docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml up -d
	@echo
	@echo "Go to http://localhost:9002/ ..."

stop-local-m1:
	DATAHUB_FRONTEND_IMAGE=$(DATAHUB_FRONTEND_REACT_IMG) \
	DATAHUB_GMS_IMAGE=$(DATAHUB_GMS_IMG) \
	DATAHUB_UPGRADE_IMAGE=$(DATAHUB_UPGRADE_IMG) \
	DATAHUB_ELASTIC_SETUP_IMAGE=$(DATAHUB_ELASTICSEARCH_SETUP_IMG) \
	DATAHUB_KAFKA_SETUP_IMAGE=$(DATAHUB_KAFKA_SETUP_IMG) \
	DATAHUB_MYSQL_SETUP_IMAGE=$(DATAHUB_MYSQL_SETUP_IMG) \
	docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml down
