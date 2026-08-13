"""sync.py — Sync orchestration for tap-sap-sales-service-cloud."""

from typing import Dict, Optional

import singer
from singer import metadata

from tap_sap_sales_service_cloud.schema import write_schema
from tap_sap_sales_service_cloud.metadata_discovery import MDATA_NS
from tap_sap_sales_service_cloud.streams.dynamic import DynamicStream

LOGGER = singer.get_logger()


def update_currently_syncing(
    state: Dict, stream_name: Optional[str]
) -> None:
    """Update the ``currently_syncing`` marker in state."""
    if not stream_name:
        state.pop("currently_syncing", None)
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config: Dict, catalog: singer.Catalog, state: Dict) -> None:
    """Sync all selected streams from *catalog*.

    Parameters
    ----------
    client:
        Authenticated
        :class:`~tap_sap_sales_service_cloud.client.SAPSalesServiceCloudClient`.
    config:
        Tap configuration dict (currently unused but kept for API parity).
    catalog:
        Singer Catalog produced by discovery.
    state:
        Singer state dict; bookmarks are read and written here.
    """
    del config  # Not used at sync time; kept for consistent function signature.

    # Build list of selected stream IDs.
    streams_to_sync = [
        stream.tap_stream_id
        for stream in catalog.streams
        if metadata.get(
            metadata.to_map(stream.metadata), (), "selected"
        )
    ]

    # Build child-stream map: {parent_stream_id: [child_stream_id, …]}
    child_map: Dict = {}
    for stream in catalog.streams:
        root_mdata = metadata.to_map(stream.metadata).get((), {})
        parent_stream = (
            root_mdata.get("parent-tap-stream-id")
            or root_mdata.get(f"{MDATA_NS}.parent-stream")
        )
        if parent_stream:
            child_map.setdefault(parent_stream, []).append(
                stream.tap_stream_id
            )

    LOGGER.info("Selected streams: %s", streams_to_sync)
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("Last / currently syncing stream: %s", last_stream)

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            catalog_stream = catalog.get_stream(stream_name)
            if catalog_stream is None:
                LOGGER.warning(
                    "Stream '%s' not found in catalog, skipping.",
                    stream_name,
                )
                continue

            if catalog_stream.schema is None:
                LOGGER.warning(
                    "Stream '%s' has no schema in catalog, skipping.",
                    stream_name,
                )
                continue

            stream = DynamicStream(
                client, catalog_stream, child_map=child_map
            )

            # If this stream is a child, ensure the parent will be synced.
            if stream.parent:
                if stream.parent not in streams_to_sync:
                    if catalog.get_stream(stream.parent) is not None:
                        streams_to_sync.append(stream.parent)
                    else:
                        LOGGER.warning(
                            "Stream '%s' declares parent '%s' which is not "
                            "in the catalog. Stream will be skipped.",
                            stream_name,
                            stream.parent,
                        )
                continue

            write_schema(
                stream,
                client,
                streams_to_sync,
                catalog,
                child_map=child_map,
            )

            LOGGER.info("START Syncing: %s", stream_name)
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(
                state=state, transformer=transformer
            )
            LOGGER.info(
                "FINISHED Syncing: %s, total_records: %s",
                stream_name,
                total_records,
            )
            update_currently_syncing(state, None)
