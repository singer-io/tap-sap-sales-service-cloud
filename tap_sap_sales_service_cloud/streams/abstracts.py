"""abstracts.py — Base stream class for tap-sap-sales-service-cloud.

Provides:
  - OData V2 record pagination via ``d.__next`` next-link.
  - Incremental replication with bookmark management.
  - ``/Date(ms±offset)/`` → RFC-3339 coercion that works on Windows
    for the full Python datetime range (avoids mktime() overflow).
  - Parent-child stream orchestration with filter injection, batching
    eligible child requests across several parents with a single-property
    OR filter.
  - OData ``$expand`` support for computed/read-only entity sets.
  - Singer SCHEMA / RECORD / STATE emission.
"""

import copy
import re
from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from requests.exceptions import Timeout
from singer import (Transformer, get_bookmark, get_logger, metadata, metrics,
                    write_bookmark, write_record, write_schema)
from singer.utils import strftime, strptime_with_tz

from tap_sap_sales_service_cloud.exceptions import (
    SAPSalesServiceCloudBadRequestError,
    SAPSalesServiceCloudError,
    SAPSalesServiceCloudInternalServerError,
    SAPSalesServiceCloudServer5xxError,
)

LOGGER = get_logger()

# Child requests for several parents are combined into one
# '(FK eq 'a' or FK eq 'b' ...)' filter. SAP C4C only supports OR on a
# single property, so only children filtered by one FK field are batched.
CHILD_BATCH_MAX_PARENTS = 50
CHILD_BATCH_MAX_FILTER_CHARS = 3000
# Batched results are cross-checked against per-parent counts until a chunk
# with at least one child row matches (or this many chunks have matched).
CHILD_BATCH_VERIFY_MAX_CHUNKS = 3


class _ChildBatchRejected(Exception):
    """A batched child request failed before any record was received."""


def _is_batch_rejection(exc: Exception) -> bool:
    """Return True for errors a smaller batch could plausibly avoid."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status in (413, 414, 431) or isinstance(
        exc,
        (
            SAPSalesServiceCloudBadRequestError,
            SAPSalesServiceCloudServer5xxError,
            Timeout,
        ),
    )

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

# Distinguishes "bookmark not yet computed this run" from "computed as
# None" (no replication key) on BaseStream.effective_bookmark.
_BOOKMARK_UNSET = object()


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

    # True when replication_keys[0] is synthesized from the parent's own
    # replication key (no native replication key exists on this entity).
    # Such a field does not exist on SAP's side, so it must never be sent
    # as an OData $filter — only used for client-side bookmark comparison.
    synthetic_replication_key: bool = False
    parent_replication_key: str = ""

    path: str = ""

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
        # Resolved once per run (see _resolve_effective_bookmark) and reused
        # across every subsequent sync() invocation — child streams are
        # invoked once per parent record, so re-reading state each time
        # would let an out-of-order parent compare against a bookmark a
        # later-processed (but chronologically older) sibling already
        # advanced, silently dropping records.
        self.effective_bookmark = _BOOKMARK_UNSET
        # Some SAP C4C entities are backed by TREX/HANA search views (e.g.
        # Lead) whose query builder crashes on the '(key eq null or ...)'
        # construct. Set once get_records() detects that failure so later
        # pages/parent-scoped calls skip straight to the plain 'ge' filter.
        self._skip_null_filter = False
        self._batch_max_parents = CHILD_BATCH_MAX_PARENTS
        self._batching_disabled = False
        self._batch_verified = False
        self._batch_verify_attempts = 0

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
        self,
        state: Dict,
        parent_obj: Optional[Dict] = None,
        parent_objs: Optional[List[Dict]] = None,
    ) -> Dict:
        """Build OData query parameters for a single page fetch.

        *parent_objs* scopes the request to several parents at once with an
        OR filter on ``parent_filter_field`` (see :meth:`sync_parent_batch`).
        """
        params: Dict[str, Any] = {}

        if (
            self.replication_method == "INCREMENTAL"
            and self.replication_keys
            and not self.synthetic_replication_key
        ):
            key = self.replication_keys[0]
            has_effective_bookmark = (
                self.effective_bookmark
                and self.effective_bookmark is not _BOOKMARK_UNSET
            )
            bookmark = (
                self.effective_bookmark
                if has_effective_bookmark
                else self.get_bookmark(state, self.tap_stream_id, key)
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
                date_clause = f"{key} ge datetimeoffset'{fval}'"
            else:
                fval = _format_odata_datetime(effective_dt)
                date_clause = f"{key} ge datetime'{fval}'"
            if self._skip_null_filter:
                params["$filter"] = date_clause
            else:
                # SAP applies three-valued (SQL-style) logic to 'ge'
                # comparisons, so rows where the replication key is NULL
                # never satisfy the filter and are silently excluded by the
                # server — regardless of start_date. OR in an explicit
                # 'eq null' clause so those rows are always returned; they
                # can't be bookmarked incrementally, so they are re-fetched
                # on every run (see sync()). Reverted per-stream by
                # get_records() if the backend rejects this construct.
                params["$filter"] = f"({key} eq null or {date_clause})"

        clause = None
        if parent_objs and self.parent_filter_field:
            clause = self._batch_filter_clause(parent_objs)
        elif parent_obj and self.parent_filter_field:
            parent_val = parent_obj[self.parent_key_field]
            clause = self._parent_filter_term(parent_val)
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
        if clause:
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

    def _parent_filter_term(self, parent_val: Any) -> str:
        return f"{self.parent_filter_field} eq '{parent_val}'"

    def _batch_filter_clause(self, parent_objs: List[Dict]) -> str:
        terms = " or ".join(
            self._parent_filter_term(parent[self.parent_key_field])
            for parent in parent_objs
        )
        return f"({terms})"

    def _strip_null_inclusion_filter(self, filter_value: str) -> Optional[str]:
        """Remove the '(key eq null or ...)' wrapper from *filter_value*.

        Returns ``None`` when the wrapper isn't present (nothing to fall
        back from), otherwise the filter with only the plain 'ge' clause.
        """
        key = self.replication_keys[0] if self.replication_keys else None
        if not key:
            return None
        pattern = re.compile(
            r"\(" + re.escape(key) + r" eq null or (?P<rest>.+?)\)"
        )
        match = pattern.search(filter_value)
        if not match:
            return None
        return pattern.sub(match.group("rest"), filter_value, count=1)

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

    def _resolve_effective_bookmark(self, state: Dict, bookmark_key: str):
        """Compute the comparison bookmark, aligned with any child streams.

        Called once per run by sync() via the effective_bookmark cache.
        """
        bookmark = self.get_bookmark(state, self.tap_stream_id)
        if self.child_to_sync:
            parent_bookmark_key = f"{self.tap_stream_id}_{bookmark_key}"
            for child in self.child_to_sync:
                child_bookmark = self.get_bookmark(
                    state, child.tap_stream_id, key=parent_bookmark_key
                )
                bookmark = (
                    min(bookmark, child_bookmark) if bookmark else child_bookmark
                )
        return bookmark

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
            self._write_bookmark_max(state, stream, bookmark_key, value)
        for child in self.child_to_sync:
            parent_bk_key = f"{self.tap_stream_id}_{bookmark_key}"
            self._write_bookmark_max(
                state, child.tap_stream_id, parent_bk_key, value
            )

    @staticmethod
    def _write_bookmark_max(
        state: Dict, stream: str, key: str, value
    ) -> None:
        """Persist *value* only if it advances the existing bookmark.

        Child streams call write_bookmark() once per parent record, so a
        blind overwrite could regress the persisted bookmark below a value
        an earlier (but chronologically later) sibling already wrote.
        """
        existing = get_bookmark(state, stream, key, value)
        write_bookmark(state, stream, key, max(existing, value))

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

        if self.synthetic_replication_key and self.replication_keys:
            record[self.replication_keys[0]] = _parent_record.get(
                self.parent_replication_key
            )

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

        try:
            response = self.client.get(
                expand_path,
                params=params,
                headers={"Authorization": self.client.get_auth_header()},
            )
        except SAPSalesServiceCloudInternalServerError as exc:
            fallback_filter = self._strip_null_inclusion_filter(
                params.get("$filter", "")
            )
            if fallback_filter is None:
                raise
            LOGGER.warning(
                "Stream '%s' — backend rejected the NULL-inclusive "
                "$filter; retrying without it. Records with a NULL "
                "replication key will not be captured. Error: %s",
                self.tap_stream_id, exc,
            )

            self._skip_null_filter = True
            params["$filter"] = fallback_filter
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
        self,
        state: Dict,
        parent_obj: Optional[Dict] = None,
        parent_objs: Optional[List[Dict]] = None,
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
        params = self.build_params(state, parent_obj, parent_objs)
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

            try:
                payload = self.client.get(
                    self.path,
                    params=params,
                    headers={
                        "Authorization": self.client.get_auth_header()
                    },
                )
            except SAPSalesServiceCloudInternalServerError as exc:
                # A 500 on a batched request may be caused by the batch
                # itself; let sync_parent_batch() split it instead of
                # dropping the NULL-inclusive clause for the whole stream.
                if parent_objs:
                    raise
                fallback_filter = self._strip_null_inclusion_filter(
                    params.get("$filter", "")
                )
                if fallback_filter is None:
                    raise
                LOGGER.warning(
                    "Stream '%s' — backend rejected the NULL-inclusive "
                    "$filter (likely a TREX/HANA-view-backed entity); "
                    "retrying without it. Records with a NULL "
                    "replication key will not be captured for this "
                    "stream. Error: %s",
                    self.tap_stream_id, exc,
                )
                self._skip_null_filter = True
                params["$filter"] = fallback_filter
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
    # Batched child sync
    # ------------------------------------------------------------------

    @property
    def supports_batching(self) -> bool:
        """Whether child requests can be combined across parents.

        Only plain string foreign keys are eligible: SAP C4C ORs only work on
        a single property, and the parent of every returned row must be
        recovered exactly from the row's own foreign-key value.
        """
        fk_schema = self.schema.get("properties", {}).get(
            self.parent_filter_field, {}
        )
        fk_types = fk_schema.get("type", [])
        return bool(
            not self._batching_disabled
            and self.parent
            and self.parent_filter_field
            and not self.parent_secondary_filter_field
            and not self.parent_secondary_key_field
            and not (self.expand_nav_property and self.expand_parent_entity_set)
            and "string" in ([fk_types] if isinstance(fk_types, str) else fk_types)
            and fk_schema.get("format") != "date-time"
        )

    def batch_has_room(self, pending: List[Dict], parent: Dict) -> bool:
        """Whether *parent* can join the *pending* batch for this child."""
        if len(pending) >= self._batch_max_parents:
            return False
        key = str(parent[self.parent_key_field])
        if any(str(p[self.parent_key_field]) == key for p in pending):
            return False
        candidate = self._batch_filter_clause(pending + [parent])
        return len(candidate) <= CHILD_BATCH_MAX_FILTER_CHARS

    def _ensure_effective_bookmark(self, state: Dict) -> Optional[str]:
        """Resolve the run-wide comparison bookmark once; return its key."""
        bookmark_key = (
            self.replication_keys[0] if self.replication_keys else None
        )
        if not bookmark_key:
            self.effective_bookmark = None
        elif self.effective_bookmark is _BOOKMARK_UNSET:
            self.effective_bookmark = self._resolve_effective_bookmark(
                state, bookmark_key
            )
        return bookmark_key

    def _count_records(
        self,
        state: Dict,
        parent_obj: Optional[Dict] = None,
        parent_objs: Optional[List[Dict]] = None,
    ) -> Optional[int]:
        """Return the server-side ``$inlinecount`` for a child query."""
        params = self.build_params(state, parent_obj, parent_objs)
        params["$top"] = 1
        params["$inlinecount"] = "allpages"
        payload = self.client.get(
            self.path,
            params=params,
            headers={"Authorization": self.client.get_auth_header()},
        )
        try:
            return int((payload or {}).get("d", {}).get("__count"))
        except (TypeError, ValueError):
            return None

    def _disable_batching(self, reason: str) -> None:
        LOGGER.warning(
            "Stream '%s' — batched child requests disabled for this run; "
            "falling back to one request per parent. Reason: %s",
            self.tap_stream_id,
            reason,
        )
        self._batching_disabled = True

    def _verify_batch(self, state: Dict, parents: List[Dict]) -> bool:
        """Check that the batched filter returns exactly the per-parent rows.

        Guards against SAP evaluating the OR filter differently from the
        single-parent filter (which would otherwise silently drop rows).
        Returns False, and disables batching, when that cannot be confirmed.
        """
        self._ensure_effective_bookmark(state)
        try:
            batch_count = self._count_records(state, parent_objs=parents)
        except (SAPSalesServiceCloudError, Timeout) as exc:
            if _is_batch_rejection(exc):
                raise _ChildBatchRejected(str(exc)) from exc
            raise

        try:
            counts = [self._count_records(state, parent_obj=p) for p in parents]
        except (SAPSalesServiceCloudError, Timeout) as exc:
            self._disable_batching(f"per-parent count check failed: {exc}")
            return False
        if batch_count is None or None in counts or batch_count != sum(counts):
            self._disable_batching(
                f"batched count {batch_count} != per-parent counts {counts}"
            )
            return False

        self._batch_verify_attempts += 1
        if sum(counts) > 0 or (
            self._batch_verify_attempts >= CHILD_BATCH_VERIFY_MAX_CHUNKS
        ):
            self._batch_verified = True
            LOGGER.info(
                "Stream '%s' — batched child requests verified "
                "(%s parents, %s records).",
                self.tap_stream_id,
                len(parents),
                batch_count,
            )
        return True

    def sync_parent_batch(
        self, state: Dict, transformer: Transformer, parents: List[Dict]
    ) -> None:
        """Sync this child for several parents, batching when possible.

        Falls back to the per-parent path (one request per parent) when the
        stream is ineligible or batching cannot be verified, and splits the
        batch when SAP rejects it before any record was received.
        """
        if len(parents) > 1 and self.supports_batching:
            try:
                if self._batch_verified or self._verify_batch(state, parents):
                    self.sync(state, transformer, parent_objs=parents)
                    return
            except _ChildBatchRejected as exc:
                mid = len(parents) // 2
                self._batch_max_parents = mid
                LOGGER.warning(
                    "Stream '%s' — batched request for %s parents rejected; "
                    "retrying in batches of %s. Error: %s",
                    self.tap_stream_id, len(parents), mid, exc,
                )
                self.sync_parent_batch(state, transformer, parents[:mid])
                self.sync_parent_batch(state, transformer, parents[mid:])
                return
        for parent in parents:
            self.sync(state, transformer, parent_obj=parent)

    @staticmethod
    def _reject_unfetched_batch(records: Iterable[Dict]) -> Iterable[Dict]:
        """Turn a failure before the first batched record into a rejection.

        Once any record has been received the error is re-raised unchanged
        (fail fast), as splitting would re-emit records.
        """
        fetched_any = False
        try:
            for record in records:
                fetched_any = True
                yield record
        except (SAPSalesServiceCloudError, Timeout) as exc:
            if not fetched_any and _is_batch_rejection(exc):
                raise _ChildBatchRejected(str(exc)) from exc
            raise

    def _batch_parent_matcher(self, parent_objs: List[Dict]):
        """Build a function mapping a batched child row to its parent."""
        by_key = {str(p[self.parent_key_field]): p for p in parent_objs}
        # Case-insensitive fallback; ambiguous keys must match exactly.
        by_folded: Dict[str, Optional[Dict]] = {}
        for key, parent in by_key.items():
            by_folded[key.casefold()] = (
                None if key.casefold() in by_folded else parent
            )

        def match(record: Dict) -> Dict:
            fk_value = record.get(self.parent_filter_field)
            if fk_value is not None:
                fk_value = str(fk_value)
                parent = by_key.get(fk_value) or by_folded.get(
                    fk_value.casefold()
                )
                if parent is not None:
                    return parent
            raise SAPSalesServiceCloudError(
                f"Stream '{self.tap_stream_id}' — batched child record has "
                f"{self.parent_filter_field}={fk_value!r}, which matches no "
                "parent in its batch. Aborting rather than misattributing "
                "the record."
            )

        return match

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
        parent_objs: Optional[List[Dict]] = None,
    ) -> int:
        """Sync stream records and emit Singer RECORD / STATE messages.

        *parent_objs* syncs this child for several parents in one batched
        request; each record is attributed to its parent by foreign key.

        Returns the total number of records emitted.
        """
        bookmark_key = self._ensure_effective_bookmark(state)

        bookmark = self.effective_bookmark

        # Only advance bookmark from real records — never pre-seed with
        # start_date so that zero-record syncs never write a spurious bookmark.
        current_max_bookmark = None

        records = self.get_records(
            state=state, parent_obj=parent_obj, parent_objs=parent_objs
        )
        match_parent = None
        if parent_objs:
            records = self._reject_unfetched_batch(records)
            match_parent = self._batch_parent_matcher(parent_objs)

        batch_children = [
            child for child in self.child_to_sync if child.supports_batching
        ]
        inline_children = [
            child for child in self.child_to_sync
            if child not in batch_children
        ]
        pending_parents: List[Dict] = []

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in records:
                source_parent = (
                    match_parent(record) if match_parent else parent_obj
                )
                record = self.modify_object(record, source_parent)
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
                # A NULL replication-key value can never be compared against
                # the bookmark. Such rows are only fetched via the $filter's
                # 'eq null' clause, so pass them through here rather than
                # dropping them a second time; they simply won't advance
                # current_max_bookmark below.
                if bookmark_key and bookmark and record_value:
                    record_passes_bookmark = record_value >= bookmark

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
                    for child_stream in inline_children:
                        child_stream.sync(
                            state=state,
                            transformer=transformer,
                            parent_obj=transformed_record,
                        )
                    if pending_parents and not all(
                        child.batch_has_room(pending_parents, transformed_record)
                        for child in batch_children
                    ):
                        for child_stream in batch_children:
                            child_stream.sync_parent_batch(
                                state, transformer, pending_parents
                            )
                        pending_parents = []
                    if batch_children:
                        pending_parents.append(transformed_record)

            for child_stream in batch_children if pending_parents else []:
                child_stream.sync_parent_batch(
                    state, transformer, pending_parents
                )

            if bookmark_key and current_max_bookmark:
                self.write_bookmark(
                    state,
                    self.tap_stream_id,
                    key=bookmark_key,
                    value=current_max_bookmark,
                )

            return counter.value
