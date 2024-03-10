#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

echo "$(date) $0 $@ ..."

for arg in "$@"
do
    case "$arg" in
        git-rev=*)          GIT_REV=$(echo $arg | cut -d= -f 2)
                            ;;
        artifact-dir=*)     ARTIFACT_DIR=$(echo $arg | cut -d= -f 2)
                            ;;
        datahub-dir=*)      DATAHUB_DIR=$(echo $arg | cut -d= -f 2)
                            ;;
    esac
done

if [ -z "$DATAHUB_DIR" ]; then
    echo -e "\nGOKU setup git repository ...\n"
    git clone https://${GIT_USER}:${GIT_PASSCODE}@ghe.megaleo.com/dpoe/pharos-goku-datahub.git
    cd pharos-goku-datahub
    git checkout $GIT_REV
else
    cd $DATAHUB_DIR
    echo -e "\nIn $DATAHUB_DIR ...\n"
fi

echo -e "\nGOKU build datahub-upgrade ...\n"
./gradlew -PAF_USER=${AF_USER} -PAF_PASSWORD=${AF_PASSWORD} :datahub-upgrade:build -x test --parallel --info
cp ./datahub-upgrade/build/libs/datahub-upgrade.jar $ARTIFACT_DIR

echo -e "\nGOKU build datahub gms ...\n"
./gradlew -PAF_USER=${AF_USER} -PAF_PASSWORD=${AF_PASSWORD} :metadata-service:war:build -x test --parallel --info
cp ./metadata-service/war/build/libs/war.war $ARTIFACT_DIR

# yarnrc with https proxy not resolved
echo -e "\nGOKU setup .npmrc ...\n"
# https://confluence.workday.com/display/DEVQA/Using+Artifactory+as+an+NPM+repository#UsingArtifactoryasanNPMrepository-Alternatively,howtomanuallyconfigure~/.npmrc
AUTH_OUTPUT=$(curl -X GET -u ${AF_USER}:${AF_PASSWORD} "https://artifactory.workday.com/artifactory/api/npm/auth")
echo "repository=https://artifactory.workday.com/artifactory/api/npm/npm-virtual" > ~/.npmrc
echo "$AUTH_OUTPUT" | grep -v ^_auth >> ~/.npmrc
echo "//artifactory.workday.com/artifactory/api/npm/:$(echo "$AUTH_OUTPUT" | grep ^_auth | tr -d ' ')" >> ~/.npmrc

echo -e "\nGOKU build datahub frontend ...\n"
./gradlew -PnodeDistBaseUrl=https://${AF_USER}:${AF_PASSWORD}@artifactory.workday.com/artifactory/nodejs.org-cache/dist \
          -PAF_USER=${AF_USER} -PAF_PASSWORD=${AF_PASSWORD} \
          :datahub-frontend:dist -x test -x yarnTest -x yarnLint --parallel --info
cp ./datahub-frontend/build/distributions/datahub-frontend-*.zip $ARTIFACT_DIR/datahub-frontend.zip          

echo "$(date) $0 completed"
