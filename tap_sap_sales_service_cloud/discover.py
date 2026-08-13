"""discover.py — Run OData $metadata-based discovery for SAP C4C.

Discovery runs in two phases:

Phase 1 — EDMX metadata discovery
    Parse the OData ``$metadata`` document to build schemas, Singer
    metadata, and stream definitions for every entity set.

Phase 2 — Stream probe-prune (delegated to :mod:`stream_probe`)
    Each entity set is probed with a sync-equivalent request.  Streams
    that return 4xx / 5xx are removed, and their children / ``$expand``
    dependents are cascade-excluded.  :func:`probe_all_streams` returns
    the complete set of names to drop.
"""

import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_sap_sales_service_cloud.metadata_discovery import \
    discover_dynamic_streams
from tap_sap_sales_service_cloud.stream_probe import probe_all_streams

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
    LOGGER.info(
        "Phase 1: Discovering entity sets from OData $metadata ..."
    )
    schemas, field_metadata, stream_defs = discover_dynamic_streams(client)
    LOGGER.info(
        "Phase 1 complete: %d entity sets discovered.", len(stream_defs)
    )

    # Phase 2 — Probe-prune (cascade-exclude handled inside probe module).
    LOGGER.info("Phase 2: Probing streams for accessibility ...")
    all_excluded = probe_all_streams(client, stream_defs)
    LOGGER.info(
        "Phase 2 complete: %d stream(s) excluded.", len(all_excluded)
    )

    # Build Catalog entries, skipping excluded streams.
    entries = []
    for stream_name, stream_def in stream_defs.items():
        if stream_name in all_excluded:
            continue
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
