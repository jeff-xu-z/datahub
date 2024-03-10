FROM docker-dev-artifactory.workday.com/library/ubuntu:24.04

LABEL MAINTAINER="goku.support@workday.com"

RUN apt-get -q update \
    && apt-get -yq install gnupg curl git openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

WORKDIR /app
COPY build-datahub.sh /app
RUN chmod +x /app/*.sh

WORKDIR /app/workspace
RUN chown ubuntu:ubuntu /app/workspace

USER ubuntu

ENTRYPOINT ["/app/build-datahub.sh"]
