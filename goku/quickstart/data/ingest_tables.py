#!/usr/bin/env python3

import argparse
import logging
import pandas as pd

# Imports for urn construction utility methods
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn, make_domain_urn
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

def main(rest_emitter, graph, csv_file, platform, domain):
    logger.info(f"processing {csv_file} ...")
    platform_urn = make_data_platform_urn(platform)
    df = pd.read_csv(csv_file, keep_default_na=False, na_filter=False)
    for tbl_name in df['name'].unique().tolist():
        logger.info(f"processing {csv_file} {tbl_name} ...")
        table_df = df[df['name'] == tbl_name]
        db_name = table_df['schema'].tolist()[0]
        dataset_urn = make_dataset_urn(platform=platform_urn, name=f"dw.{db_name}.{tbl_name}", env="PROD")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName="", # not used
                platform=platform_urn,
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
        if domain:
            domain_urn = make_domain_urn(domain.lower())
            graph.execute_graphql(query = f"""
    mutation setDomain {{
        setDomain(domainUrn: "{domain_urn}", entityUrn: "{dataset_urn}")
    }}
    """)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--gms", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--domain")
    args = parser.parse_args()
    rest_emitter = DatahubRestEmitter(gms_server=args.gms)
    graph = DataHubGraph(DatahubClientConfig(server=args.gms))
    main(rest_emitter, graph, args.file, args.platform, args.domain)
