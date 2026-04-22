"""dynamic.py — Runtime stream built from OData $metadata discovery."""

from singer import metadata

from tap_sap_sales_service_cloud.metadata_discovery import MDATA_NS
from tap_sap_sales_service_cloud.streams.abstracts import BaseStream


class DynamicStream(BaseStream):
    """Runtime stream for entity sets discovered from OData $metadata.

    All stream properties are populated from the Singer CatalogEntry and its
    Singer metadata at construction time — no static class-level overrides
    are needed.
    """

    def __init__(self, client=None, catalog=None, child_map=None) -> None:
        super().__init__(client=client, catalog=catalog)
        child_map = child_map or {}

        root_metadata = metadata.to_map(catalog.metadata).get((), {})

        self.tap_stream_id = catalog.tap_stream_id
        self.key_properties = catalog.key_properties or []

        self.replication_keys = root_metadata.get("valid-replication-keys", [])
        self.replication_method = (
            root_metadata.get("forced-replication-method")
            or root_metadata.get("replication-method")
            or "FULL_TABLE"
        )

        # OData entity-set path (e.g. /sap/c4c/odata/v1/c4codataapi/AccountCollection)
        entity_set = root_metadata.get(f"{MDATA_NS}.entity-set", self.tap_stream_id)
        self.path = f"{client.odata_path}/{entity_set}"

        # Parent-child orchestration fields.
        self.parent = (
            root_metadata.get("parent-tap-stream-id")
            or root_metadata.get(f"{MDATA_NS}.parent-stream")
            or ""
        )
        self.parent_filter_field = (
            root_metadata.get(f"{MDATA_NS}.parent-filter-field") or ""
        )
        self.parent_key_field = (
            root_metadata.get(f"{MDATA_NS}.parent-key-field") or "ObjectID"
        )
        self.parent_secondary_filter_field = (
            root_metadata.get(f"{MDATA_NS}.parent-secondary-filter-field") or ""
        )
        self.parent_secondary_key_field = (
            root_metadata.get(f"{MDATA_NS}.parent-secondary-key-field") or ""
        )

        # OData $expand support for computed/read-only entity sets.
        self.expand_nav_property = (
            root_metadata.get(f"{MDATA_NS}.expand-nav-property") or ""
        )
        self.expand_parent_entity_set = (
            root_metadata.get(f"{MDATA_NS}.expand-parent-entity-set") or ""
        )
        self.orderby_field = (
            root_metadata.get(f"{MDATA_NS}.orderby-field") or ""
        )
        self.replication_key_edm_type = (
            root_metadata.get(f"{MDATA_NS}.replication-key-edm-type") or ""
        )

        if self.expand_nav_property and self.expand_parent_entity_set:
            # Override path to point at the parent entity set so that
            # _get_records_via_expand queries the right endpoint.
            self.path = (
                f"{client.odata_path}/{self.expand_parent_entity_set}"
            )

        self.children = child_map.get(self.tap_stream_id, [])
