"""discover.py — Run OData $metadata-based discovery for SAP C4C.

Discovery runs in one phase:

Phase 1 — EDMX metadata discovery
    Parse the OData ``$metadata`` document to build schemas, Singer
    metadata, and stream definitions for every entity set.
"""

import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_sap_sales_service_cloud.metadata_discovery import \
    discover_dynamic_streams

LOGGER = singer.get_logger()


def discover(client=None) -> Catalog:
    """Run metadata-driven discovery and return a pruned Singer Catalog.

    Parameters
    ----------
    client:
        An authenticated
        :class:`~tap_sap_sales_service_cloud.client.SAPSalesServiceCloudClient`
        instance.

    Returns
    -------
    singer.catalog.Catalog
        Catalog containing a CatalogEntry for every reachable entity set
        exposed by the tenant's OData API.
    """
    # Phase 1 — EDMX discovery.
    LOGGER.info("Phase 1: Discovering entity sets from OData $metadata …")
    schemas, field_metadata, stream_defs = discover_dynamic_streams(client)
    LOGGER.info(
        "Phase 1 complete: %d entity sets discovered.", len(stream_defs)
    )

    # Build Catalog entries.
    entries = []
    for stream_name, stream_def in stream_defs.items():
        schema = schemas.get(stream_name, {})
        mdata = field_metadata.get(stream_name, [])
        key_properties = stream_def.get("key_properties", [])

        entry = CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            metadata=mdata
        )
        entries.append(entry)

    LOGGER.info(
        "Discovery complete: %d stream(s) in catalog.", len(entries)
    )
    return Catalog(entries)
