#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

DIR=$(dirname $0)

echo "Creating domains, data products ..."
${DIR}/create_base.py

for csv_f in sfdc.csv xo.csv swh.csv;
do
    echo "Loading ${csv_f} ..."
    ${DIR}/ingest_tables.py ${csv_f}
done

echo "Creating post ..."
${DIR}/create_post.py
