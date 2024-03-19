#!/usr/bin/env python3

import logging
import argparse

from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ChangeTypeClass, DomainPropertiesClass

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


def main(gms_server, domain_name, domain_desc):
    emitter = DatahubRestEmitter(gms_server=gms_server)
    domain_urn = create_domain(emitter, domain_name, domain_desc)
    logging.info(f"created {domain_urn}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--gms", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--desc", default='')
    args = parser.parse_args()
    main(args.gms, args.domain, args.desc)
