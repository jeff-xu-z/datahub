#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

if [[ "$HOST_OS" == "Darwin" ]]; then
    GMS=http://host.docker.internal:8080
else
    GMS=http://$(ip route show default | grep 'default via' | awk '{ print $3 }'):8080
fi

# CLI
export DATAHUB_GMS_URL=$GMS
export DATAHUB_TELEMETRY_ENABLED=false
echo "DATAHUB_GMS_URL=$DATAHUB_GMS_URL ..."

DIR=$(dirname $0)
cd ${DIR}

# Domains
echo "CREATING DOMAINS ..."
python3 create_domain.py --gms $GMS --domain Xpresso --desc "Workday Xpresso"

# Platforms
echo "CREATING DATA PLATFORMS ..."
datahub put platform --name SWH --display_name "SWH" --logo "https://pharos.inday.io/logo_hu38c25f701ae22c39d3ca6868a4a33532_29068_300x300_fit_catmullrom_2.png"

# Users

USERS_YAML_FILE=$(mktemp).yml
echo "" > $USERS_YAML_FILE
cat users | while read ln;
do
    u=$(echo $ln | base64 -d)
    echo "- id: $u" >> $USERS_YAML_FILE
    echo "  email: $u@workday.com" >> $USERS_YAML_FILE
done
echo "$USERS_YAML_FILE generated"
datahub user upsert -f $USERS_YAML_FILE
echo "$USERS_YAML_FILE loaded"
rm $USERS_YAML_FILE

# Tables
echo "CREATING DATASETS ..."
python3 ingest_tables.py --gms $GMS --file sfdc.csv --platform salesforce
python3 ingest_tables.py --gms $GMS --file xo.csv --platform swh --domain Xpresso
python3 ingest_tables.py --gms $GMS --file swh.csv --platform swh

# Nimbus/Superset
datahub ingest run -c nimbus-file.yml
