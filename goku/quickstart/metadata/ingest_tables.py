#!/usr/bin/env python3

# Imports for urn construction utility methods
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
    BooleanTypeClass,
    NumberTypeClass,
)

import sys
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def map_col_type_to_class(col_type):
    if col_type.startswith('decimal') or col_type in ('double', 'float'):
        return NumberTypeClass()
    if 'int' in col_type:
        return NumberTypeClass()
    return {
        "boolean": BooleanTypeClass(),
        "timestamp": TimeTypeClass(),
        "string": StringTypeClass(),
    }.get(col_type, StringTypeClass())

def map_schema_row_to_SchemaFieldClass(row):
    return SchemaFieldClass(
        fieldPath=row['col_name'],
        type=SchemaFieldDataTypeClass(type=map_col_type_to_class(row['col_type'].lower())),
        nativeDataType=row['col_type'],
        description=row['col_description'],
        lastModified=AuditStampClass(
            time=1640692800000, actor="urn:li:corpuser:ingestion"
        ),
        isPartitioningKey=bool(row['is_partition_col']),
    )

# xo.csv
# sfdc.csv
# swh.csv
def main(rest_emitter, graph, csv_file):
    if csv_file == "swh.csv":
        db_name = "swh"
        domain_urn = "urn:li:domain:swh"
        data_product_urn_map = {
            "oms": "urn:li:dataProduct:swh-oms",
            "prism": "urn:li:dataProduct:swh-prism",
            "ui": "urn:li:dataProduct:swh-ui",
            "apphub": "urn:li:dataProduct:swh-apphub",
        }
    elif csv_file == "sfdc.csv":
        db_name = "lookup_db"
        domain_urn = "urn:li:domain:salesforce"
        data_product_urn = None
    elif csv_file == "xo.csv":
        db_name = "lookup_db"
        domain_urn = "urn:li:domain:workday"
        data_product_urn = "urn:li:dataProduct:xo"
    else:
        raise Exception(f"Bad value: {csv_file}!")
    df = pd.read_csv(csv_file, keep_default_na=False, na_filter=False)
    for tbl_name in df['name'].unique().tolist():
        if db_name == "swh":
            data_product_urn = data_product_urn_map.get(tbl_name.split("_")[0], None)
        table_df = df[df['name'] == tbl_name]
        dataset_urn = make_dataset_urn(platform="trino", name=f"dw.{db_name}.{tbl_name}", env="PROD")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName="", # not used
                platform=make_data_platform_urn("trino"),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                lastModified=AuditStampClass(
                    time=1640692800000, actor="urn:li:corpuser:ingestion"
                ),
                fields=table_df.sort_values(by='col_sort_order').apply(map_schema_row_to_SchemaFieldClass, axis=1).tolist(),
            ),
        )
        logger.info(f"emit {tbl_name} ...")
        rest_emitter.emit(event)
        # set domain
        graph.execute_graphql(query = f"""
mutation setDomain {{
    setDomain(domainUrn: "{domain_urn}", entityUrn: "{dataset_urn}")
}}
""")
        # set data product
        if data_product_urn is not None:
            graph.execute_graphql(query = f"""
mutation batchSetDataProduct {{
    batchSetDataProduct(
        input: {{
            dataProductUrn: "{data_product_urn}",
            resourceUrns: ["{dataset_urn}"]
        }}
    )    
}}
""")                                  

'''
{
    "operationName": "batchSetDataProduct",
    "variables": {
        "input": {
            "resourceUrns": [
                "urn:li:dataset:(urn:li:dataPlatform:hive,swh.access,PROD)"
            ],
            "dataProductUrn": "urn:li:dataProduct:swh-ui"
        }
    },
    "query": "mutation batchSetDataProduct($input: BatchSetDataProductInput!) {\n  batchSetDataProduct(input: $input)\n}\n"
}
'''


if __name__ == "__main__":
    # Create rest emitter
    rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))
    main(rest_emitter, graph, sys.argv[1])
