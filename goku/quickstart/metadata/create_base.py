#!/usr/bin/env python3

import logging

from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ChangeTypeClass, DomainPropertiesClass
from datahub.api.entities.dataproduct.dataproduct import DataProduct
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_domain(emitter, domain_name, domain_desc):
    domain_urn = make_domain_urn(domain_name.lower())
    domain_properties_aspect = DomainPropertiesClass(
        name=domain_name, description=domain_desc
    )

    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityType="domain",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=domain_urn,
        aspect=domain_properties_aspect,
    )
    emitter.emit(event)
    log.info(f"Created domain {domain_urn}")
    return domain_urn

def create_data_product(graph, domain_urn, product_name, product_desc):
    data_product = DataProduct(
        id=product_name.lower(),
        display_name=product_name,
        domain=domain_urn,
        description=product_desc,
    )
    for mcp in data_product.generate_mcp(upsert=True):
        graph.emit(mcp)
    log.info(f"Created data product {data_product}")
    return domain_urn


def main():
    emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
    domain_name_urn_map = {}
    for domain_name, domain_desc in (
        ("SWH", "Stats Warehouse"),
        ("Workday", "Workday"),
        ("Salesforce", "Salesforce"),
    ):
        domain_urn = create_domain(emitter, domain_name, domain_desc)
        domain_name_urn_map[domain_name] = domain_urn

    graph = DataHubGraph(DatahubClientConfig(server="http://localhost:8080"))
    for domain_name, product_name, product_desc in (
        ("SWH", "SWH-apphub", "SWH AppHub tables"),
        ("SWH", "SWH-ui", "SWH UI tables"),
        ("SWH", "SWH-prism", "SWH Prism tables"),
        ("SWH", "SWH-oms", "SWH OMS tables"),
        ("Workday", "XO", "Workday XO tables"),
    ):
        create_data_product(graph, domain_name_urn_map[domain_name], product_name, product_desc)


if __name__ == "__main__":
    main()

