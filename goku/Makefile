.PHONY: check-artifactory-credential artifactory-login \
start-datahub stop-datahub start-datahub-v0-13-0 \


GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD | tr '[:upper:]' '[:lower:]')
GIT_REV=$(shell git log -1 --pretty=%h | tr '[:upper:]' '[:lower:]')
USERNAME=$(shell whoami | tr '[:upper:]' '[:lower:]')

DOCKER_REGISTRY_URL=docker-dev-artifactory.workday.com
DATAHUB_V_0_13_0 := v0.13.0


start-datahub: start-datahub-v0-13-0
	@echo

stop-datahub:
	docker-compose -f docker/quickstart/docker-compose-without-neo4j.quickstart.yml -p datahub down

start-datahub-v0-13-0:
	@echo "Launching datahub v0.13.0 ..."
	@OS="`uname`"; \
	if [ "$$OS" = "Darwin" ]; then \
		export DATAHUB_MYSQL_IMAGE=mariadb; \
		export DATAHUB_MYSQL_VERSION=10.5.8; \
	else \
		export DATAHUB_MYSQL_IMAGE=${DOCKER_REGISTRY_URL}/mysql; \
		export DATAHUB_MYSQL_VERSION=5.7.35; \
	fi; \
	export DATAHUB_VERSION=${DATAHUB_V_0_13_0}; \
	export DATAHUB_FRONTEND_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-frontend-react; \
	export DATAHUB_GMS_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-gms; \
	export DATAHUB_UPGRADE_IMAGE=${DOCKER_REGISTRY_URL}/acryldata/datahub-upgrade; \
	export DATAHUB_ELASTIC_SETUP_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-elasticsearch-setup; \
	export DATAHUB_KAFKA_SETUP_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-kafka-setup; \
	export DATAHUB_MYSQL_SETUP_IMAGE=${DOCKER_REGISTRY_URL}/acryldata/datahub-mysql-setup; \
	export KAFKA_IMAGE=${DOCKER_REGISTRY_URL}/confluentinc/cp-kafka; \
	export KAFKA_VERSION=7.4.0; \
	export DATAHUB_SEARCH_IMAGE=${DOCKER_REGISTRY_URL}/library/elasticsearch; \
	export DATAHUB_SEARCH_TAG=8.12.2; \
	export SCHEMA_REGISTRY_IMAGE=${DOCKER_REGISTRY_URL}/confluentinc/cp-schema-registry; \
	export SCHEMA_REGISTRY_VERSION=7.5.3; \
	export ZOOKEEPER_IMAGE=${DOCKER_REGISTRY_URL}/confluentinc/cp-zookeeper; \
	export ZOOKEEPER_VERSION=7.4.0; \
	docker-compose -f docker/quickstart/docker-compose-without-neo4j.quickstart.yml -p datahub up -d
	@echo
	@echo "Go to http://localhost:9002/ ..."
	@echo

# one-time setup
promote-datahub-v0-13-0-images-from-public-to-dev-artifactory: artifactory-login
	$(call promote_public_to_dev_artifactory,linkedin/datahub-gms:v0.13.0)
	$(call promote_public_to_dev_artifactory,linkedin/datahub-frontend-react:v0.13.0)
	$(call promote_public_to_dev_artifactory,acryldata/datahub-upgrade:v0.13.0)
	$(call promote_public_to_dev_artifactory,linkedin/datahub-elasticsearch-setup:v0.13.0)
	$(call promote_public_to_dev_artifactory,linkedin/datahub-kafka-setup:v0.13.0)
	$(call promote_public_to_dev_artifactory,acryldata/datahub-mysql-setup:v0.13.0)
	$(call promote_public_to_dev_artifactory,confluentinc/cp-kafka:7.4.0)
	$(call promote_public_to_dev_artifactory,confluentinc/cp-zookeeper:7.4.0)
	$(call promote_public_to_dev_artifactory,confluentinc/cp-schema-registry:7.5.3)
	$(call promote_public_to_dev_artifactory,confluentinc/cp-kafka:7.4.0)
	$(call promote_public_to_dev_artifactory,library/elasticsearch:8.12.2)


check-artifactory-credential:
	@test "${AF_USER}" -a "${AF_PASSWORD}" || (echo 'AF_USER and AF_PASSWORD must be defined' && exit 1)

artifactory-login: check-artifactory-credential
	@echo "Login to $(DOCKER_REGISTRY_URL) ..."
	@echo ${AF_PASSWORD} | docker login ${DOCKER_REGISTRY_URL} --username ${AF_USER} --password-stdin


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
