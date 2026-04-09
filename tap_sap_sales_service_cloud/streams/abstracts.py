"""abstracts.py — Base stream class for tap-sap-sales-service-cloud.

Provides:
  - OData V2 record pagination via ``d.__next`` next-link.
  - Incremental replication with bookmark management.
  - ``/Date(ms±offset)/`` → RFC-3339 coercion that works on Windows
    for the full Python datetime range (avoids mktime() overflow).
  - Parent-child stream orchestration with filter injection.
  - OData ``$expand`` support for computed/read-only entity sets.
  - Singer SCHEMA / RECORD / STATE emission.
"""

import copy
import re
from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from singer import (Transformer, get_bookmark, get_logger, metadata, metrics,
                    write_bookmark, write_record, write_schema)
from singer.utils import strftime, strptime_with_tz

LOGGER = get_logger()

# Regex for SAP OData V2 /Date(milliseconds±offset)/ values.
ODATA_DATE_RE = re.compile(
    r"^/Date\((?P<millis>-?\d+)(?P<offset>[+-]\d{4})?\)/$"
)

# Reference epoch for /Date(ms)/ arithmetic.
# Using timedelta from this epoch avoids os.mktime() which on Windows only
# handles timestamps within 1970-01-01 – ~year 3001. SAP uses
# /Date(-2208988800000)/ (1900-01-01) as a sentinel min-date and
# /Date(253402214400000)/ (9999-12-31) as a sentinel max-date — both
# outside the Windows mktime range and cause OSError 22 without this fix.
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _format_odata_datetimeoffset(dt: datetime) -> str:
    """Format for ``Edm.DateTimeOffset`` filter literal.

    Produces: ``YYYY-MM-DDTHH:MM:SS.ffffffZ``
    """
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _format_odata_datetime(dt: datetime) -> str:
    """Format for ``Edm.DateTime`` filter literal.

    SAP C4C ``Edm.DateTime`` fields reject a timezone suffix.
    Produces: ``YYYY-MM-DDTHH:MM:SS.ffffff``
    """
    utc_dt = dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")


class BaseStream(ABC):
    """Base class for all SAP Sales and Service Cloud streams."""

    tap_stream_id: str = ""
    key_properties = []
    replication_method: str = "FULL_TABLE"
    replication_keys = []
    entity: str = ""
    parent: str = ""
    children = []
    data_key: str = "results"
    date_fields = []

    # Parent-child filter fields.
    parent_filter_field: str = ""
    parent_key_field: str = "ObjectID"
    parent_secondary_filter_field: str = ""
    parent_secondary_key_field: str = ""

    # OData $expand support for computed entities.
    expand_nav_property: str = ""
    expand_parent_entity_set: str = ""

    # $orderby field confirmed sortable via EDMX sap:sortable annotation.
    # Empty string means no $orderby is sent for this stream.
    orderby_field: str = ""

    # Raw Edm type of the replication key field.  Determines OData filter
    # keyword: 'Edm.DateTimeOffset' -> datetimeoffset'...' else datetime'...'
    replication_key_edm_type: str = ""

    def __init__(self, client=None, catalog=None) -> None:
        if catalog is None:
            raise ValueError(
                f"catalog is required when constructing stream "
                f"'{self.__class__.__name__}'. "
                "Run discovery and pass the matching CatalogEntry before "
                "constructing a stream instance."
            )
        if catalog.schema is None:
            raise ValueError(
                f"catalog entry '{catalog.tap_stream_id}' has no schema — "
                "the catalog may be malformed or the stream was not "
                "discovered correctly."
            )
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.effective_bookmark = None

    # ------------------------------------------------------------------
    # Catalog helpers
    # ------------------------------------------------------------------

    def is_selected(self) -> bool:
        """Return whether this stream is selected in catalog metadata."""
        return metadata.get(self.metadata, (), "selected")

    # ------------------------------------------------------------------
    # OData query parameter builders
    # ------------------------------------------------------------------

    def build_params(
        self, state: Dict, parent_obj: Optional[Dict] = None
    ) -> Dict:
        """Build OData query parameters for a single page fetch."""
        params: Dict[str, Any] = {}

        if self.replication_method == "INCREMENTAL" and self.replication_keys:
            key = self.replication_keys[0]
            bookmark = self.effective_bookmark or self.get_bookmark(
                state, self.tap_stream_id, key
            )
            lookback_days = int(
                self.client.config.get("lookback_window", 0)
            )
            bookmark_dt = strptime_with_tz(bookmark)
            effective_dt = bookmark_dt - timedelta(days=lookback_days)
            # Edm.DateTimeOffset -> datetimeoffset'...Z'
            # Edm.DateTime       -> datetime'...' (no tz suffix)
            if self.replication_key_edm_type == "Edm.DateTimeOffset":
                fval = _format_odata_datetimeoffset(effective_dt)
                params["$filter"] = (
                    f"{key} ge datetimeoffset'{fval}'"
                )
            else:
                fval = _format_odata_datetime(effective_dt)
                params["$filter"] = (
                    f"{key} ge datetime'{fval}'"
                )

        if parent_obj and self.parent_filter_field:
            parent_val = parent_obj[self.parent_key_field]
            clause = f"{self.parent_filter_field} eq '{parent_val}'"
            # Append optional secondary filter field.
            if (
                self.parent_secondary_filter_field
                and self.parent_secondary_key_field
            ):
                sec_val = parent_obj.get(self.parent_secondary_key_field, "")
                if sec_val is not None and sec_val != "":
                    clause += (
                        f" and {self.parent_secondary_filter_field} "
                        f"eq '{sec_val}'"
                    )
            params["$filter"] = (
                f"{params['$filter']} and {clause}"
                if "$filter" in params
                else clause
            )

        # $orderby ensures deterministic page order so that $skip-based
        # pagination always yields the same sequence across retries.
        # Only applied when confirmed sortable via EDMX sap:sortable
        # annotation (populated into self.orderby_field during discovery).
        if self.orderby_field:
            params["$orderby"] = self.orderby_field

        return params

    # ------------------------------------------------------------------
    # Bookmark helpers
    # ------------------------------------------------------------------

    def get_bookmark(
        self, state: Dict, stream: str, key: str = None
    ):
        """Wrapper around singer.get_bookmark."""
        return get_bookmark(
            state,
            stream,
            key or (self.replication_keys[0] if self.replication_keys else None),
            self.client.config["start_date"],
        )

    def write_bookmark(
        self,
        state: Dict,
        stream: str,
        key: str = None,
        value=None,
    ) -> None:
        """Wrapper around singer.write_bookmark.

        Writes this stream's own bookmark only when the stream is selected.
        Also writes the parent-alignment bookmark key on every child stream
        so that the next incremental run fetches children from the same
        window as the parent.
        """
        bookmark_key = key or (
            self.replication_keys[0] if self.replication_keys else None
        )
        if not bookmark_key:
            return
        if self.is_selected():
            write_bookmark(state, stream, bookmark_key, value)
        for child in self.child_to_sync:
            parent_bk_key = f"{self.tap_stream_id}_{bookmark_key}"
            write_bookmark(
                state, child.tap_stream_id, parent_bk_key, value
            )

    # ------------------------------------------------------------------
    # Record normalization
    # ------------------------------------------------------------------

    def parse_odata_records(self, payload: Dict) -> Iterable[Dict]:
        """Parse OData V2 payload and return the results list."""
        data = payload.get("d", {})
        return data.get("results", [])

    def append_times_to_dates(self, record: Dict) -> None:
        """Normalize explicit date_fields to Singer-compatible timestamps."""
        for date_field in self.date_fields:
            if record.get(date_field):
                try:
                    record[date_field] = strftime(
                        strptime_with_tz(record[date_field])
                    )
                except (TypeError, ValueError):
                    pass

    def _coerce_odata_datetime(self, value: Any) -> Any:
        """Convert SAP OData V2 ``/Date(ms±offset)/`` values to RFC 3339.

        Uses timedelta arithmetic from the Unix epoch to avoid mktime()
        overflow on Windows for sentinel dates like 1900-01-01 and 9999-12-31.

        SAP also returns all-zero sentinel strings (e.g. ``"00000000"``) for
        empty date fields.  These are coerced to ``None`` so that Singer's
        Transformer does not raise a schema-mismatch error.
        """
        if not isinstance(value, str):
            return value
        stripped = value.strip()
        if stripped and all(c == "0" for c in stripped):
            return None
        match = ODATA_DATE_RE.match(value)
        if not match:
            return value
        # The OData V2 ±hhmm offset is a display hint only; the millisecond
        # ticks are always UTC, so we only need the millis group.
        millis = int(match.group("millis"))
        try:
            dt = _UNIX_EPOCH + timedelta(milliseconds=millis)
        except (OverflowError, OSError, ValueError):
            # Millisecond value is out of Python datetime range
            # (e.g. .NET DateTime.MinValue = /Date(-62135769600000)/).
            # Treat as null rather than returning the raw unparseable string.
            return None
        try:
            return strftime(dt)
        except (ValueError, OSError):
            # strftime fails for very old dates (year < 1900) on some
            # platforms — treat as null.
            return None

    def _normalize_datetimes_with_schema(
        self, value: Any, schema: Dict
    ) -> Any:
        """Recursively normalize all date-time fields guided by JSON Schema."""
        if isinstance(value, dict):
            properties = schema.get("properties", {})
            result = {}
            for k, v in value.items():
                prop_schema = properties.get(k, {})
                result[k] = self._normalize_datetimes_with_schema(v, prop_schema)
            return result
        types = schema.get("type", [])
        if isinstance(types, str):
            types = [types]
        if schema.get("format") == "date-time":
            return self._coerce_odata_datetime(value)
        if isinstance(value, list):
            item_schema = schema.get("items", {})
            return [
                self._normalize_datetimes_with_schema(item, item_schema)
                for item in value
            ]
        return value

    def normalize_record_datetimes(self, record: Dict) -> Dict:
        """Normalize all date-time fields in a record before Singer transform."""
        return self._normalize_datetimes_with_schema(record, self.schema)

    def modify_object(
        self, record: Dict, _parent_record: Optional[Dict] = None
    ) -> Dict:
        """Enrich child records with parent-ID synthetic field."""
        if not _parent_record or not self.parent:
            return record
        parent_pk_field = self.parent_key_field
        if not parent_pk_field or parent_pk_field not in _parent_record:
            return record
        parent_pk_value = _parent_record[parent_pk_field]
        if self.parent_filter_field and self.parent_filter_field not in record:
            record[self.parent_filter_field] = parent_pk_value
        parent_id_field = f"__parent_{self.parent}_{parent_pk_field}"
        record[parent_id_field] = parent_pk_value
        return record

    # ------------------------------------------------------------------
    # Record fetching
    # ------------------------------------------------------------------

    def _get_records_via_expand(
        self, state: Dict, parent_obj: Optional[Dict]
    ) -> Iterable[Dict]:
        """Fetch records via OData ``$expand`` from a parent entity set."""
        expand_path = self.path  # Overridden in DynamicStream to parent set.
        params = self.build_params(state, parent_obj)
        params["$expand"] = self.expand_nav_property

        response = self.client.get(
            expand_path,
            params=params,
            headers={"Authorization": self.client.get_auth_header()},
        )
        if not response:
            return

        for parent_record in self.parse_odata_records(response):
            expanded = parent_record.get(self.expand_nav_property)
            if isinstance(expanded, dict):
                results = expanded.get("results", [])
            elif isinstance(expanded, list):
                results = expanded
            else:
                results = []
            yield from results

    def get_records(
        self, state: Dict, parent_obj: Optional[Dict] = None
    ) -> Iterable[Dict]:
        """Iterate records using client-controlled $skip/$top pagination.

        Each page is fetched with an explicit ``$skip`` offset and
        ``$top`` page-size so the tap — not the server — owns the
        pagination cursor.  ``$orderby`` (set in :meth:`build_params`)
        guarantees a stable result-set order across pages.
        """
        if self.expand_nav_property and self.expand_parent_entity_set:
            yield from self._get_records_via_expand(state, parent_obj)
            return

        page_size = int(
            self.client.config.get("page_size", 1000)
        )
        params = self.build_params(state, parent_obj)
        params["$top"] = page_size
        skip = 0

        while True:
            params["$skip"] = skip
            # Request total count only on the first page to avoid the
            # server-side overhead on every subsequent page request.
            if skip == 0:
                params["$inlinecount"] = "allpages"
            else:
                params.pop("$inlinecount", None)

            payload = self.client.get(
                self.path,
                params=params,
                headers={
                    "Authorization": self.client.get_auth_header()
                },
            )

            if not payload:
                break

            if skip == 0:
                total_count = payload.get("d", {}).get("__count")
                if total_count is not None:
                    LOGGER.info(
                        "Stream '%s' — total records available: %s",
                        self.tap_stream_id,
                        total_count,
                    )

            records = list(self.parse_odata_records(payload))
            yield from records

            # Fewer records than page_size means we are on the last page.
            if len(records) < page_size:
                break

            skip += page_size
            LOGGER.info(
                "Stream '%s' — fetched %s records so far (page_size=%s)",
                self.tap_stream_id,
                skip,
                page_size,
            )

    # ------------------------------------------------------------------
    # Singer emission
    # ------------------------------------------------------------------

    def write_schema(self) -> None:
        """Write Singer SCHEMA message."""
        write_schema(self.tap_stream_id, self.schema, self.key_properties)

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Optional[Dict] = None,
    ) -> int:
        """Sync stream records and emit Singer RECORD / STATE messages.

        Returns the total number of records emitted.
        """
        bookmark_key = (
            self.replication_keys[0] if self.replication_keys else None
        )
        bookmark = (
            self.get_bookmark(state, self.tap_stream_id)
            if bookmark_key
            else None
        )

        # Align child-stream bookmarks with parent bookmark so incremental
        # windows stay consistent.
        if bookmark_key and self.child_to_sync:
            parent_bookmark_key = f"{self.tap_stream_id}_{bookmark_key}"
            for child in self.child_to_sync:
                child_bookmark = self.get_bookmark(
                    state,
                    child.tap_stream_id,
                    key=parent_bookmark_key,
                )
                bookmark = (
                    min(bookmark, child_bookmark) if bookmark else child_bookmark
                )

        self.effective_bookmark = bookmark

        # Only advance bookmark from real records — never pre-seed with
        # start_date so that zero-record syncs never write a spurious bookmark.
        current_max_bookmark = None

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(
                state=state, parent_obj=parent_obj
            ):
                record = self.modify_object(record, parent_obj)
                record = self.normalize_record_datetimes(copy.deepcopy(record))
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                self.append_times_to_dates(transformed_record)

                record_passes_bookmark = True
                record_value = (
                    transformed_record.get(bookmark_key)
                    if bookmark_key
                    else None
                )
                if bookmark_key and bookmark:
                    record_passes_bookmark = bool(
                        record_value and record_value >= bookmark
                    )

                if record_passes_bookmark:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    if record_value:
                        current_max_bookmark = (
                            max(current_max_bookmark, record_value)
                            if current_max_bookmark
                            else record_value
                        )

                    # Sync child streams for this parent record.
                    # Intentionally outside is_selected(): an unselected
                    # parent still drives child syncs by supplying parent_obj
                    # without emitting its own SCHEMA/RECORD messages.
                    for child_stream in self.child_to_sync:
                        child_stream.sync(
                            state=state,
                            transformer=transformer,
                            parent_obj=transformed_record,
                        )

            if bookmark_key and current_max_bookmark:
                self.write_bookmark(
                    state,
                    self.tap_stream_id,
                    key=bookmark_key,
                    value=current_max_bookmark,
                )

            return counter.value
