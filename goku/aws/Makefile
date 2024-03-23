.PHONY: check-artifactory-credential artifactory-login \
start-datahub stop-datahub start-datahub-v0-13-0 \


#DOCKER_REGISTRY_URL=docker-dev-artifactory.workday.com
DATAHUB_V_0_13_0 := v0.13.0


start-datahub: start-datahub-v0-13-0
	@echo

stop-datahub:
	docker-compose -f datahub-rds-opensearch.yml -p datahub down

start-datahub-v0-13-0: artifactory-login
	@echo "Launching datahub v0.13.0 ..."
	export DATAHUB_VERSION=${DATAHUB_V_0_13_0}; \
	export DATAHUB_FRONTEND_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-frontend-react; \
	export DATAHUB_GMS_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-gms; \
	export DATAHUB_UPGRADE_IMAGE=${DOCKER_REGISTRY_URL}/acryldata/datahub-upgrade; \
	export DATAHUB_ELASTIC_SETUP_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-elasticsearch-setup; \
	export DATAHUB_KAFKA_SETUP_IMAGE=${DOCKER_REGISTRY_URL}/linkedin/datahub-kafka-setup; \
	export DATAHUB_MYSQL_SETUP_IMAGE=${DOCKER_REGISTRY_URL}/acryldata/datahub-mysql-setup; \
	export KAFKA_IMAGE=${DOCKER_REGISTRY_URL}/confluentinc/cp-kafka; \
	export KAFKA_VERSION=7.4.0; \
	export SCHEMA_REGISTRY_IMAGE=${DOCKER_REGISTRY_URL}/confluentinc/cp-schema-registry; \
	export SCHEMA_REGISTRY_VERSION=7.5.3; \
	export ZOOKEEPER_IMAGE=${DOCKER_REGISTRY_URL}/confluentinc/cp-zookeeper; \
	export ZOOKEEPER_VERSION=7.4.0; \
	docker-compose -f datahub-rds-opensearch.yml -p datahub up -d
	@echo
	@echo "Go to http://localhost:9002/ ..."
	@echo


check-artifactory-credential:
	@test "${AF_USER}" -a "${AF_PASSWORD}" || (echo 'AF_USER and AF_PASSWORD must be defined' && exit 1)

artifactory-login: check-artifactory-credential
	@echo "Login to $(DOCKER_REGISTRY_URL) ..."
	@echo ${AF_PASSWORD} | docker login ${DOCKER_REGISTRY_URL} --username ${AF_USER} --password-stdin
