"""Unit tests for tap_sap_sales_service_cloud.streams.abstracts."""

import unittest
from unittest.mock import MagicMock, patch

from parameterized import parameterized
from singer import get_bookmark
from singer import metadata as md

from tap_sap_sales_service_cloud.metadata_discovery import MDATA_NS
from tap_sap_sales_service_cloud.streams.abstracts import ODATA_DATE_RE

# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _make_catalog(
    stream_id="test_stream",
    replication_method="INCREMENTAL",
    replication_keys=None,
    selected=True,
    edm_type="Edm.DateTimeOffset",
):
    """Build a minimal Singer CatalogEntry mock."""
    replication_keys = replication_keys or ["ChangedOn"]
    catalog = MagicMock()
    catalog.tap_stream_id = stream_id
    catalog.schema = MagicMock()
    catalog.schema.to_dict.return_value = {
        "type": "object",
        "properties": {
            "ObjectID": {"type": ["string", "null"]},
            "ChangedOn": {
                "format": "date-time",
                "type": ["string", "null"],
            },
            "Name": {"type": ["string", "null"]},
        },
    }
    mdata = md.new()
    mdata = md.write(mdata, (), "selected", selected)
    mdata = md.write(mdata, (), "replication-method", replication_method)
    mdata = md.write(
        mdata, (), "valid-replication-keys", replication_keys
    )
    mdata = md.write(
        mdata, (), f"{MDATA_NS}.replication-key-edm-type", edm_type
    )
    catalog.metadata = md.to_list(mdata)
    return catalog


def _make_client(start_date="2024-01-01T00:00:00Z", page_size=1000):
    client = MagicMock()
    client.config = {
        "start_date": start_date,
        "page_size": page_size,
        "lookback_window": 0,
    }
    return client


def _make_stream(
    stream_id="test_stream",
    replication_method="INCREMENTAL",
    replication_keys=None,
    selected=True,
    edm_type="Edm.DateTimeOffset",
    start_date="2024-01-01T00:00:00Z",
):
    """Instantiate a DynamicStream with a minimal catalog."""
    from tap_sap_sales_service_cloud.streams.dynamic import DynamicStream

    catalog = _make_catalog(
        stream_id=stream_id,
        replication_method=replication_method,
        replication_keys=replication_keys,
        selected=selected,
        edm_type=edm_type,
    )
    client = _make_client(start_date=start_date)
    return DynamicStream(client, catalog)


def _make_odata_page(n, offset=0, total_count=None):
    """Build an OData V2 ``{"d": {"results": [...]}}`` payload.

    Defined at module level so it is accessible inside
    ``@parameterized.expand`` decorators, which are evaluated at
    class-definition time (before any instance methods exist).
    """
    results = [
        {
            "ObjectID": str(offset + i),
            "ChangedOn": "2024-01-01T00:00:00.000000Z",
        }
        for i in range(n)
    ]
    d = {"results": results}
    if total_count is not None:
        d["__count"] = str(total_count)
    return {"d": d}


# ---------------------------------------------------------------------------
# OData regex
# ---------------------------------------------------------------------------

class TestODataDateRe(unittest.TestCase):

    @parameterized.expand(
        [
            ("positive_millis", "/Date(1609459200000)/", "1609459200000", None),
            ("negative_millis", "/Date(-62135769600000)/", "-62135769600000", None),
            ("positive_offset", "/Date(1609459200000+0530)/", "1609459200000", "+0530"),
            ("negative_offset", "/Date(1609459200000-0800)/", "1609459200000", "-0800"),
        ]
    )
    def test_matches(self, _, value, exp_millis, exp_offset):
        m = ODATA_DATE_RE.match(value)
        self.assertIsNotNone(m)
        self.assertEqual(m.group("millis"), exp_millis)
        self.assertEqual(m.group("offset"), exp_offset)

    @parameterized.expand(
        [
            ("plain_iso_string", "2024-01-01T00:00:00Z"),
            ("empty_string", ""),
        ]
    )
    def test_no_match(self, _, value):
        self.assertIsNone(ODATA_DATE_RE.match(value))


# ---------------------------------------------------------------------------
# _coerce_odata_datetime
# ---------------------------------------------------------------------------

class TestCoerceOdataDatetime(unittest.TestCase):

    def setUp(self):
        self.stream = _make_stream()

    @parameterized.expand(
        [
            ("unix_epoch", "/Date(0)/", "1970-01-01T00:00:00.000000Z"),
            ("known_date", "/Date(1609459200000)/", "2021-01-01"),
            ("sap_max_sentinel_9999", "/Date(253402214400000)/", "9999"),
            ("offset_applied", "/Date(0+0530)/", "T"),
            ("non_date_string", "some-random-string", "some-random-string"),
            ("rfc3339_passthrough", "2024-01-01T00:00:00Z", "2024-01-01"),
        ]
    )
    def test_returns_valid_string_containing(self, _, value, expected_substr):
        """Coercion returns a non-None string containing *expected_substr*."""
        result = self.stream._coerce_odata_datetime(value)
        self.assertIsNotNone(result)
        self.assertIn(expected_substr, result)

    @parameterized.expand(
        [
            ("dot_net_min_value", "/Date(-62135769600000)/"),
            ("all_zeros_sentinel", "00000000"),
        ]
    )
    def test_returns_none(self, _, value):
        """Sentinel / out-of-range values are coerced to None."""
        self.assertIsNone(self.stream._coerce_odata_datetime(value))

    def test_non_string_passthrough(self):
        """Non-string values are returned unchanged."""
        self.assertEqual(self.stream._coerce_odata_datetime(42), 42)
        self.assertIsNone(self.stream._coerce_odata_datetime(None))


# ---------------------------------------------------------------------------
# _normalize_datetimes_with_schema
# ---------------------------------------------------------------------------

class TestNormalizeDatetimesWithSchema(unittest.TestCase):

    def setUp(self):
        self.stream = _make_stream()

    def _normalize(self, record, schema):
        return self.stream._normalize_datetimes_with_schema(
            record, schema
        )

    def test_date_field_coerced(self):
        schema = {
            "type": "object",
            "properties": {
                "ChangedOn": {
                    "format": "date-time",
                    "type": ["string", "null"],
                },
            },
        }
        result = self._normalize(
            {"ChangedOn": "/Date(1609459200000)/"}, schema
        )
        self.assertIn("2021-01-01", result["ChangedOn"])

    def test_non_date_string_unchanged(self):
        schema = {
            "type": "object",
            "properties": {"Name": {"type": ["string", "null"]}},
        }
        result = self._normalize({"Name": "Alice"}, schema)
        self.assertEqual(result["Name"], "Alice")

    def test_null_date_value(self):
        schema = {
            "type": "object",
            "properties": {
                "ChangedOn": {
                    "format": "date-time",
                    "type": ["string", "null"],
                },
            },
        }
        self.assertIsNone(
            self._normalize({"ChangedOn": None}, schema)["ChangedOn"]
        )

    def test_nested_dict(self):
        schema = {
            "type": "object",
            "properties": {
                "Inner": {
                    "type": "object",
                    "properties": {
                        "CreatedOn": {
                            "format": "date-time",
                            "type": ["string", "null"],
                        },
                    },
                },
            },
        }
        result = self._normalize(
            {"Inner": {"CreatedOn": "/Date(1609459200000)/"}}, schema
        )
        self.assertIn("2021-01-01", result["Inner"]["CreatedOn"])

    def test_list_of_date_items(self):
        schema = {
            "type": "array",
            "items": {
                "format": "date-time",
                "type": ["string", "null"],
            },
        }
        result = self._normalize(
            ["/Date(1609459200000)/", "/Date(0)/"], schema
        )
        self.assertIn("2021-01-01", result[0])
        self.assertIn("1970-01-01", result[1])


# ---------------------------------------------------------------------------
# build_params
# ---------------------------------------------------------------------------

class TestBuildParams(unittest.TestCase):

    @parameterized.expand(
        [
            ("datetimeoffset", "Edm.DateTimeOffset", "ChangedOn", "datetimeoffset'", None),
            ("datetime", "Edm.DateTime", "LastUpdatedOn", "datetime'", "datetimeoffset"),
        ]
    )
    def test_incremental_filter_keyword(
        self, _, edm_type, rep_key, exp_in, exp_not_in
    ):
        """Correct OData keyword emitted for each Edm type."""
        stream = _make_stream(edm_type=edm_type)
        stream.replication_keys = [rep_key]
        stream.replication_method = "INCREMENTAL"
        stream.replication_key_edm_type = edm_type
        stream.effective_bookmark = "2024-06-01T00:00:00Z"
        params = stream.build_params(state={})
        self.assertIn("$filter", params)
        self.assertIn(exp_in, params["$filter"])
        if exp_not_in:
            self.assertNotIn(exp_not_in, params["$filter"])

    def test_full_table_no_filter(self):
        stream = _make_stream(
            replication_method="FULL_TABLE", replication_keys=[]
        )
        stream.replication_method = "FULL_TABLE"
        stream.replication_keys = []
        self.assertNotIn("$filter", stream.build_params(state={}))

    def test_orderby_added_when_set(self):
        stream = _make_stream()
        stream.effective_bookmark = "2024-01-01T00:00:00Z"
        stream.orderby_field = "ChangedOn"
        self.assertEqual(
            stream.build_params(state={}).get("$orderby"), "ChangedOn"
        )

    def test_parent_filter_appended(self):
        stream = _make_stream(
            replication_method="FULL_TABLE", replication_keys=[]
        )
        stream.replication_method = "FULL_TABLE"
        stream.replication_keys = []
        stream.parent_filter_field = "ParentID"
        stream.parent_key_field = "ObjectID"
        params = stream.build_params(
            state={}, parent_obj={"ObjectID": "ABC123"}
        )
        self.assertIn("ParentID eq 'ABC123'", params["$filter"])

    def test_parent_filter_combined_with_replication_filter(self):
        stream = _make_stream(edm_type="Edm.DateTimeOffset")
        stream.replication_keys = ["ChangedOn"]
        stream.replication_method = "INCREMENTAL"
        stream.replication_key_edm_type = "Edm.DateTimeOffset"
        stream.effective_bookmark = "2024-06-01T00:00:00Z"
        stream.parent_filter_field = "ParentID"
        stream.parent_key_field = "ObjectID"
        params = stream.build_params(
            state={}, parent_obj={"ObjectID": "XYZ"}
        )
        self.assertIn("datetimeoffset'", params["$filter"])
        self.assertIn("ParentID eq 'XYZ'", params["$filter"])


# ---------------------------------------------------------------------------
# is_selected
# ---------------------------------------------------------------------------

class TestIsSelected(unittest.TestCase):

    @parameterized.expand(
        [
            ("true", True, True),
            ("false", False, False),
        ]
    )
    def test_is_selected(self, _, selected_flag, expected):
        self.assertEqual(
            _make_stream(selected=selected_flag).is_selected(), expected
        )


# ---------------------------------------------------------------------------
# get_bookmark / write_bookmark
# ---------------------------------------------------------------------------

class TestBookmarkHelpers(unittest.TestCase):

    def test_get_bookmark_returns_start_date_when_no_bookmark(self):
        stream = _make_stream(start_date="2024-01-01T00:00:00Z")
        result = stream.get_bookmark({"bookmarks": {}}, "test_stream")
        self.assertEqual(result, "2024-01-01T00:00:00Z")

    def test_get_bookmark_returns_stored_value(self):
        stream = _make_stream()
        state = {
            "bookmarks": {
                "test_stream": {"ChangedOn": "2024-06-01T00:00:00Z"},
            }
        }
        self.assertEqual(
            stream.get_bookmark(state, "test_stream"),
            "2024-06-01T00:00:00Z",
        )

    @parameterized.expand(
        [
            (
                "advances_forward",
                "2024-01-01T00:00:00Z",
                "2024-06-01T00:00:00Z",
                "2024-06-01T00:00:00Z",
            ),
        ]
    )
    def test_write_bookmark(self, _, existing, new_value, expected):
        stream = _make_stream()
        state = {
            "bookmarks": {"test_stream": {"ChangedOn": existing}}
        }
        stream.write_bookmark(state, "test_stream", value=new_value)
        self.assertEqual(
            get_bookmark(state, "test_stream", "ChangedOn"), expected
        )

    def test_write_bookmark_writes_selected_own_key(self):
        """Selected stream: own bookmark key is written."""
        stream = _make_stream(selected=True)
        state = {"bookmarks": {}}
        stream.write_bookmark(
            state, "test_stream", value="2024-06-01T00:00:00Z"
        )
        self.assertEqual(
            get_bookmark(state, "test_stream", "ChangedOn"),
            "2024-06-01T00:00:00Z",
        )

    def test_write_bookmark_skips_own_key_when_not_selected(self):
        """Unselected stream (parent added to drive children):
        own bookmark key must NOT be written."""
        stream = _make_stream(selected=False)
        state = {"bookmarks": {}}
        stream.write_bookmark(
            state, "test_stream", value="2024-06-01T00:00:00Z"
        )
        self.assertIsNone(
            get_bookmark(state, "test_stream", "ChangedOn", None)
        )

    def test_write_bookmark_writes_child_alignment_key(self):
        """Parent alignment key is written on each child regardless of
        whether the parent itself is selected."""
        from tap_sap_sales_service_cloud.streams.dynamic import DynamicStream
        parent = _make_stream(selected=True)
        child_catalog = _make_catalog(stream_id="child_stream")
        child = DynamicStream(
            client=_make_client(), catalog=child_catalog
        )
        parent.child_to_sync = [child]
        state = {"bookmarks": {}}
        parent.write_bookmark(
            state, "test_stream", value="2024-06-01T00:00:00Z"
        )
        self.assertEqual(
            get_bookmark(
                state,
                "child_stream",
                "test_stream_ChangedOn",
                None,
            ),
            "2024-06-01T00:00:00Z",
        )


# ---------------------------------------------------------------------------
# sync() â€” bookmark and is_selected behaviour
# ---------------------------------------------------------------------------

_WR = "tap_sap_sales_service_cloud.streams.abstracts.write_record"


class TestSync(unittest.TestCase):

    def _make_sync_stream(
        self, records, selected=True,
        start_date="2000-01-01T00:00:00Z",
    ):
        stream = _make_stream(selected=selected, start_date=start_date)
        stream.get_records = MagicMock(return_value=iter(records))
        stream.path = "/TestCollection"
        return stream

    @staticmethod
    def _transformer():
        t = MagicMock()
        t.transform.side_effect = lambda r, s, m: r
        return t

    def test_unselected_stream_emits_no_records(self):
        stream = self._make_sync_stream(
            [{"ObjectID": "1",
              "ChangedOn": "2024-06-01T00:00:00.000000Z",
              "Name": "A"}],
            selected=False,
        )
        with patch(_WR) as mock_write:
            stream.sync(state={}, transformer=self._transformer())
        mock_write.assert_not_called()

    def test_records_below_bookmark_not_emitted(self):
        stream = self._make_sync_stream([
            {"ObjectID": "1",
             "ChangedOn": "2023-01-01T00:00:00.000000Z",
             "Name": "old"},
            {"ObjectID": "2",
             "ChangedOn": "2025-01-01T00:00:00.000000Z",
             "Name": "new"},
        ])
        state = {
            "bookmarks": {
                "test_stream": {"ChangedOn": "2024-01-01T00:00:00Z"},
            }
        }
        written = []
        with patch(_WR, side_effect=lambda s, r: written.append(r)):
            stream.sync(state=state, transformer=self._transformer())
        self.assertEqual(len(written), 1)
        self.assertEqual(written[0]["ObjectID"], "2")

    def test_bookmark_advances_to_max_record_value(self):
        stream = self._make_sync_stream([
            {"ObjectID": "1",
             "ChangedOn": "2024-03-01T00:00:00.000000Z",
             "Name": "A"},
            {"ObjectID": "2",
             "ChangedOn": "2024-06-01T00:00:00.000000Z",
             "Name": "B"},
        ])
        state = {}
        with patch(_WR):
            stream.sync(state=state, transformer=self._transformer())
        self.assertEqual(
            get_bookmark(state, "test_stream", "ChangedOn"),
            "2024-06-01T00:00:00.000000Z",
        )

    def test_zero_records_does_not_write_bookmark(self):
        stream = self._make_sync_stream([])
        state = {}
        with patch(_WR):
            stream.sync(state=state, transformer=self._transformer())
        self.assertNotIn("bookmarks", state)

    def test_child_bookmark_written_after_sync(self):
        stream = self._make_sync_stream([
            {"ObjectID": "P1",
             "ChangedOn": "2024-06-01T00:00:00.000000Z",
             "Name": "Parent"},
        ])
        child = MagicMock()
        child.tap_stream_id = "child_stream"
        child.sync.return_value = 0
        stream.child_to_sync = [child]
        state = {}
        with patch(_WR):
            stream.sync(state=state, transformer=self._transformer())
        self.assertEqual(
            get_bookmark(state, "child_stream", "test_stream_ChangedOn"),
            "2024-06-01T00:00:00.000000Z",
        )

    @parameterized.expand(
        [
            ("zero_records", [], 0),
            (
                "one_record",
                [{"ObjectID": "1", "ChangedOn": "2024-06-01T00:00:00.000000Z", "Name": "A"}],
                1,
            ),
            (
                "two_records",
                [
                    {"ObjectID": "1", "ChangedOn": "2024-06-01T00:00:00.000000Z", "Name": "A"},
                    {"ObjectID": "2", "ChangedOn": "2024-07-01T00:00:00.000000Z", "Name": "B"},
                ],
                2,
            ),
        ]
    )
    def test_sync_returns_record_count(self, _, records, expected_count):
        stream = self._make_sync_stream(records)
        with patch(_WR):
            count = stream.sync(
                state={}, transformer=self._transformer()
            )
        self.assertEqual(count, expected_count)


# ---------------------------------------------------------------------------
# get_records() â€” $skip/$top pagination
# ---------------------------------------------------------------------------

class TestGetRecords(unittest.TestCase):
    """Tests for BaseStream.get_records() OData $skip/$top pagination."""

    # ------------------------------------------------------------------
    # Class-level helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _ft_stream(page_size=1000):
        """Return a FULL_TABLE DynamicStream with no replication keys."""
        stream = _make_stream(
            replication_method="FULL_TABLE",
            replication_keys=[],
        )
        stream.client.config["page_size"] = page_size
        return stream

    def _params_of_call(self, stream, call_index=0):
        """Return the ``params`` kwarg from the nth ``client.get`` call."""
        return stream.client.get.call_args_list[
            call_index
        ].kwargs["params"]

    @staticmethod
    def _capturing_side_effect(pages):
        """Return (side_effect_fn, snapshots) that copies params at call time.

        ``params`` is a single dict mutated in-place between pages
        ($skip increments, $inlinecount is popped), so call_args_list
        always reflects the *final* state.  This helper snapshots the
        dict at the moment of each call.
        """
        snapshots = []
        page_iter = iter(pages)

        def _get(path, params=None, headers=None):
            snapshots.append(dict(params or {}))
            return next(page_iter)

        return _get, snapshots

    # ------------------------------------------------------------------
    # Termination conditions (parameterized)
    # ------------------------------------------------------------------

    @parameterized.expand(
        [
            ("partial_first_page", [_make_odata_page(3)], 100, 3, 1),
            ("empty_first_page", [_make_odata_page(0)], 1000, 0, 1),
            ("none_payload_first_page", [None], 1000, 0, 1),
            ("two_pages_full_then_partial", [_make_odata_page(3, offset=0), _make_odata_page(1, offset=3)], 3, 4, 2),
            (
                "three_pages_full_then_empty",
                [_make_odata_page(2, offset=0), _make_odata_page(2, offset=2), _make_odata_page(0)],
                2, 4, 3,
            ),
            ("none_on_second_page", [_make_odata_page(2, offset=0), None], 2, 2, 2),
        ]
    )
    def test_termination(
        self, _, pages, page_size, exp_records, exp_calls
    ):
        """Loop exits correctly under each stopping condition."""
        stream = self._ft_stream(page_size=page_size)
        stream.client.get.side_effect = pages
        records = list(stream.get_records(state={}))
        self.assertEqual(len(records), exp_records)
        self.assertEqual(stream.client.get.call_count, exp_calls)

    # ------------------------------------------------------------------
    # Query parameter correctness
    # ------------------------------------------------------------------

    def test_skip_starts_at_zero(self):
        """First request always sends $skip=0."""
        stream = self._ft_stream()
        stream.client.get.return_value = _make_odata_page(1)
        list(stream.get_records(state={}))
        self.assertEqual(self._params_of_call(stream, 0)["$skip"], 0)

    def test_skip_increments_by_page_size(self):
        """$skip advances by page_size on each subsequent request."""
        stream = self._ft_stream(page_size=5)
        get_fn, snapshots = self._capturing_side_effect([
            _make_odata_page(5, offset=0),
            _make_odata_page(5, offset=5),
            _make_odata_page(2, offset=10),
        ])
        stream.client.get.side_effect = get_fn
        list(stream.get_records(state={}))
        self.assertEqual(
            [p["$skip"] for p in snapshots], [0, 5, 10]
        )

    @parameterized.expand(
        [
            ("page_size_42", 42),
            ("page_size_500", 500),
            ("page_size_1000", 1000),
        ]
    )
    def test_top_matches_page_size(self, _, page_size):
        """$top equals the configured page_size."""
        stream = self._ft_stream(page_size=page_size)
        stream.client.get.return_value = _make_odata_page(1)
        list(stream.get_records(state={}))
        self.assertEqual(
            self._params_of_call(stream, 0)["$top"], page_size
        )

    def test_inlinecount_only_on_first_page(self):
        """$inlinecount=allpages on page 1 only; absent on subsequent pages."""
        stream = self._ft_stream(page_size=2)
        get_fn, snapshots = self._capturing_side_effect([
            _make_odata_page(2, offset=0),
            _make_odata_page(1, offset=2),
        ])
        stream.client.get.side_effect = get_fn
        list(stream.get_records(state={}))
        self.assertEqual(snapshots[0].get("$inlinecount"), "allpages")
        self.assertNotIn("$inlinecount", snapshots[1])

    def test_total_count_in_first_page_does_not_break(self):
        """A __count field in the first page payload is consumed silently."""
        stream = self._ft_stream()
        stream.client.get.return_value = _make_odata_page(
            3, total_count=999
        )
        self.assertEqual(
            len(list(stream.get_records(state={}))), 3
        )

    @parameterized.expand(
        [
            ("present_when_set", "ObjectID", True),
            ("absent_when_empty", "", False),
        ]
    )
    def test_orderby_in_params(self, _, orderby_field, should_be_present):
        """$orderby present iff orderby_field is non-empty."""
        stream = self._ft_stream()
        stream.orderby_field = orderby_field
        stream.client.get.return_value = _make_odata_page(1)
        list(stream.get_records(state={}))
        params = self._params_of_call(stream, 0)
        if should_be_present:
            self.assertEqual(params.get("$orderby"), orderby_field)
        else:
            self.assertNotIn("$orderby", params)

    # ------------------------------------------------------------------
    # INCREMENTAL filter (parameterized)
    # ------------------------------------------------------------------

    @parameterized.expand(
        [
            ("datetimeoffset", "Edm.DateTimeOffset", "ChangedOn ge datetimeoffset'", True),
            ("datetime", "Edm.DateTime", "ChangedOn ge datetime'", False),
        ]
    )
    def test_incremental_filter_format(
        self, _, edm_type, exp_in_filter, ends_with_z
    ):
        """Correct OData literal keyword used for each Edm type."""
        stream = _make_stream(
            replication_method="INCREMENTAL",
            replication_keys=["ChangedOn"],
            edm_type=edm_type,
            start_date="2024-06-01T00:00:00Z",
        )
        stream.client.get.return_value = _make_odata_page(1)
        list(stream.get_records(state={}))
        flt = self._params_of_call(stream, 0)["$filter"]
        self.assertIn(exp_in_filter, flt)
        if ends_with_z:
            self.assertTrue(flt.endswith("Z'"))
        else:
            self.assertNotIn("Z'", flt)

    def test_incremental_bookmark_takes_precedence_over_start_date(self):
        """Stored bookmark is used in $filter instead of start_date."""
        stream = _make_stream(
            replication_method="INCREMENTAL",
            replication_keys=["ChangedOn"],
            edm_type="Edm.DateTimeOffset",
            start_date="2020-01-01T00:00:00Z",
        )
        stream.client.get.return_value = _make_odata_page(1)
        state = {
            "bookmarks": {
                "test_stream": {"ChangedOn": "2025-03-15T00:00:00Z"},
            }
        }
        list(stream.get_records(state=state))
        flt = self._params_of_call(stream, 0)["$filter"]
        self.assertIn("2025-03-15", flt)

    def test_full_table_has_no_filter(self):
        """FULL_TABLE streams send no $filter parameter."""
        stream = self._ft_stream()
        stream.client.get.return_value = _make_odata_page(1)
        list(stream.get_records(state={}))
        self.assertNotIn("$filter", self._params_of_call(stream, 0))

    # ------------------------------------------------------------------
    # Parent-child FK injection
    # ------------------------------------------------------------------

    def test_parent_filter_field_injected(self):
        """parent_obj ObjectID becomes a $filter clause on child streams."""
        stream = self._ft_stream()
        stream.parent_filter_field = "ParentID"
        stream.parent_key_field = "ObjectID"
        stream.client.get.return_value = _make_odata_page(1)
        list(
            stream.get_records(state={}, parent_obj={"ObjectID": "ABC"})
        )
        params = self._params_of_call(stream, 0)
        self.assertIn("$filter", params)
        self.assertIn("ParentID eq 'ABC'", params["$filter"])

    def test_parent_filter_combined_with_incremental_filter(self):
        """Parent FK and incremental $filter are ANDed together."""
        stream = _make_stream(
            replication_method="INCREMENTAL",
            replication_keys=["ChangedOn"],
            edm_type="Edm.DateTimeOffset",
            start_date="2024-01-01T00:00:00Z",
        )
        stream.parent_filter_field = "ParentID"
        stream.parent_key_field = "ObjectID"
        stream.client.get.return_value = _make_odata_page(1)
        list(
            stream.get_records(
                state={}, parent_obj={"ObjectID": "XYZ"}
            )
        )
        flt = self._params_of_call(stream, 0)["$filter"]
        self.assertIn("ChangedOn ge datetimeoffset'", flt)
        self.assertIn("ParentID eq 'XYZ'", flt)
        self.assertIn(" and ", flt)

    # ------------------------------------------------------------------
    # $expand delegation
    # ------------------------------------------------------------------

    def test_expand_delegates_to_expand_method(self):
        """expand_nav_property + parent set â†’ _get_records_via_expand."""
        stream = self._ft_stream()
        stream.expand_nav_property = "Items"
        stream.expand_parent_entity_set = "OrderCollection"
        expected = [{"ItemID": "X"}]
        with patch.object(
            stream,
            "_get_records_via_expand",
            return_value=iter(expected),
        ) as mock_expand:
            records = list(
                stream.get_records(
                    state={}, parent_obj={"ObjectID": "P1"}
                )
            )
        mock_expand.assert_called_once_with({}, {"ObjectID": "P1"})
        self.assertEqual(records, expected)
        stream.client.get.assert_not_called()

    def test_no_expand_when_only_nav_property_set(self):
        """Only expand_nav_property, no parent set normal pagination."""
        stream = self._ft_stream()
        stream.expand_nav_property = "Items"
        stream.expand_parent_entity_set = ""
        stream.client.get.return_value = _make_odata_page(2)
        records = list(stream.get_records(state={}))
        self.assertEqual(len(records), 2)
