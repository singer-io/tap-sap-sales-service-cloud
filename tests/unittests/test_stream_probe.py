"""Unit tests for tap_sap_sales_service_cloud.stream_probe."""

import unittest
from unittest.mock import MagicMock, patch

from tap_sap_sales_service_cloud.stream_probe import (_cascade_exclude,
                                                      _cascade_exclude_children,
                                                      _cascade_exclude_expand,
                                                      _probe_filter_value,
                                                      _replication_filter,
                                                      probe_stream)

# ---------------------------------------------------------------------------
# _probe_filter_value
# ---------------------------------------------------------------------------


class TestProbeFilterValue(unittest.TestCase):

    def test_string_type_returns_sentinel(self):
        schema = {"type": ["string", "null"]}
        self.assertEqual(_probe_filter_value(schema), "eq '__probe__'")

    def test_integer_type_returns_zero(self):
        schema = {"type": ["integer", "null"]}
        self.assertEqual(_probe_filter_value(schema), "eq 0")

    def test_number_type_returns_zero(self):
        schema = {"type": ["number", "null"]}
        self.assertEqual(_probe_filter_value(schema), "eq 0")

    def test_datetime_format_edm_datetimeoffset(self):
        schema = {"format": "date-time", "x-edm-type": "Edm.DateTimeOffset",
                  "type": ["string", "null"]}
        result = _probe_filter_value(schema)
        self.assertTrue(result.startswith("ge datetimeoffset'"))
        self.assertIn("Z'", result)

    def test_datetime_format_edm_datetime(self):
        schema = {"format": "date-time", "x-edm-type": "Edm.DateTime",
                  "type": ["string", "null"]}
        result = _probe_filter_value(schema)
        self.assertTrue(result.startswith("ge datetime'"))
        self.assertNotIn("Z'", result)

    def test_datetime_format_no_edm_type_defaults_to_datetime(self):
        schema = {"format": "date-time", "type": ["string", "null"]}
        result = _probe_filter_value(schema)
        self.assertTrue(result.startswith("ge datetime'"))

    def test_empty_schema_returns_sentinel(self):
        self.assertEqual(_probe_filter_value({}), "eq '__probe__'")

    def test_single_type_string_not_list(self):
        schema = {"type": "integer"}
        self.assertEqual(_probe_filter_value(schema), "eq 0")


# ---------------------------------------------------------------------------
# _replication_filter
# ---------------------------------------------------------------------------

class TestReplicationFilter(unittest.TestCase):

    def test_datetimeoffset_filter(self):
        result = _replication_filter(
            "ChangedOn", "Edm.DateTimeOffset", "2024-01-01T00:00:00Z"
        )
        self.assertIn("datetimeoffset'", result)
        self.assertIn("ChangedOn ge", result)
        self.assertIn("Z'", result)

    def test_datetime_filter(self):
        result = _replication_filter(
            "LastUpdatedOn", "Edm.DateTime", "2024-01-01T00:00:00Z"
        )
        self.assertIn("datetime'", result)
        self.assertIn("LastUpdatedOn ge", result)
        self.assertNotIn("Z'", result)

    def test_invalid_start_date_falls_back_to_epoch(self):
        result = _replication_filter("ChangedOn", "Edm.DateTimeOffset", "not-a-date")
        self.assertIn("2000-01-01", result)

    def test_filter_value_contains_formatted_date(self):
        result = _replication_filter(
            "ChangedOn", "Edm.DateTimeOffset", "2024-06-15T12:30:00Z"
        )
        self.assertIn("2024-06-15", result)


# ---------------------------------------------------------------------------
# _cascade_exclude
# ---------------------------------------------------------------------------

class TestCascadeExclude(unittest.TestCase):

    def test_child_excluded_when_parent_failed(self):
        stream_defs = {
            "parent_stream": {"parent_stream": None, "expand_info": None},
            "child_stream": {"parent_stream": "parent_stream", "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, {"parent_stream"})
        self.assertIn("child_stream", result)
        self.assertNotIn("parent_stream", result)

    def test_child_not_excluded_when_parent_ok(self):
        stream_defs = {
            "parent_stream": {"parent_stream": None, "expand_info": None},
            "child_stream": {"parent_stream": "parent_stream", "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, set())
        self.assertNotIn("child_stream", result)

    def test_already_failed_stream_not_re_added(self):
        stream_defs = {
            "parent_stream": {"parent_stream": None, "expand_info": None},
            "child_stream": {"parent_stream": "parent_stream", "expand_info": None},
        }
        # child_stream already in directly_failed
        result = _cascade_exclude(stream_defs, {"parent_stream", "child_stream"})
        self.assertNotIn("child_stream", result)

    def test_expand_stream_excluded_when_expand_parent_failed(self):
        stream_defs = {
            "parent_stream": {"parent_stream": None, "expand_info": None},
            "expand_stream": {
                "parent_stream": None,
                "expand_info": {"expand-parent-entity-set": "ParentStream"},
            },
        }
        result = _cascade_exclude(stream_defs, {"parent_stream"})
        self.assertIn("expand_stream", result)

    def test_no_exclusions_when_nothing_failed(self):
        stream_defs = {
            "s1": {"parent_stream": None, "expand_info": None},
            "s2": {"parent_stream": None, "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, set())
        self.assertEqual(result, set())

    # -- dangling parent (parent absent from stream_defs entirely) ----------

    def test_child_excluded_when_parent_dangling(self):
        """Child whose parent_stream is not in stream_defs must be excluded."""
        stream_defs = {
            "orphan_child": {"parent_stream": "ghost_parent", "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, set())
        self.assertIn("orphan_child", result)

    def test_dangling_parent_does_not_include_unrelated_stream(self):
        """Streams without a parent are unaffected by a dangling sibling."""
        stream_defs = {
            "orphan_child": {"parent_stream": "ghost_parent", "expand_info": None},
            "standalone":   {"parent_stream": None,           "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, set())
        self.assertNotIn("standalone", result)

    # -- multi-level chain propagation --------------------------------------

    def test_two_level_chain_grandchild_excluded(self):
        """grandparent failed → child cascade-excluded → grandchild cascade-excluded."""
        stream_defs = {
            "grandparent": {"parent_stream": None,          "expand_info": None},
            "child":       {"parent_stream": "grandparent", "expand_info": None},
            "grandchild":  {"parent_stream": "child",       "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, {"grandparent"})
        self.assertIn("child",      result)
        self.assertIn("grandchild", result)
        self.assertNotIn("grandparent", result)

    def test_four_level_chain_fully_propagated(self):
        """a(failed) → b → c → d: all three dependents must be excluded."""
        stream_defs = {
            "a": {"parent_stream": None,  "expand_info": None},
            "b": {"parent_stream": "a",   "expand_info": None},
            "c": {"parent_stream": "b",   "expand_info": None},
            "d": {"parent_stream": "c",   "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, {"a"})
        self.assertEqual(result, {"b", "c", "d"})

    def test_unrelated_branch_not_excluded(self):
        """A separate chain whose root did not fail must not be excluded."""
        stream_defs = {
            "bad_root":  {"parent_stream": None,       "expand_info": None},
            "bad_child": {"parent_stream": "bad_root", "expand_info": None},
            "ok_root":   {"parent_stream": None,       "expand_info": None},
            "ok_child":  {"parent_stream": "ok_root",  "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, {"bad_root"})
        self.assertIn("bad_child", result)
        self.assertNotIn("ok_root",  result)
        self.assertNotIn("ok_child", result)

    def test_multi_level_with_dangling_grandparent(self):
        """child has a dangling parent → grandchild of that child also excluded."""
        stream_defs = {
            # 'ghost_root' is intentionally absent
            "child":      {"parent_stream": "ghost_root", "expand_info": None},
            "grandchild": {"parent_stream": "child",      "expand_info": None},
        }
        result = _cascade_exclude(stream_defs, set())
        self.assertIn("child",      result)
        self.assertIn("grandchild", result)


# ---------------------------------------------------------------------------
# _cascade_exclude_children
# ---------------------------------------------------------------------------

class TestCascadeExcludeChildren(unittest.TestCase):
    """Unit tests for the dedicated child-chain propagation helper."""

    def _run(self, stream_defs, excluded):
        """Call helper and return (newly_excluded, mutated_excluded)."""
        working = set(excluded)
        newly = _cascade_exclude_children(stream_defs, working)
        return newly, working

    def test_single_level_parent_failed(self):
        stream_defs = {
            "parent": {},
            "child":  {"parent_stream": "parent"},
        }
        newly, working = self._run(stream_defs, {"parent"})
        self.assertIn("child", newly)
        self.assertIn("child", working)   # mutated in-place

    def test_multi_level_chain_propagated(self):
        stream_defs = {
            "gp":  {},
            "p":   {"parent_stream": "gp"},
            "c":   {"parent_stream": "p"},
            "gc":  {"parent_stream": "c"},
        }
        newly, working = self._run(stream_defs, {"gp"})
        self.assertEqual(newly, {"p", "c", "gc"})
        self.assertTrue({"gp", "p", "c", "gc"}.issubset(working))

    def test_dangling_parent_excluded(self):
        stream_defs = {
            "orphan": {"parent_stream": "does_not_exist"},
        }
        newly, _ = self._run(stream_defs, set())
        self.assertIn("orphan", newly)

    def test_dangling_grandparent_cascade(self):
        """Dangling root → child excluded in round 1 → grandchild in round 2."""
        stream_defs = {
            # 'ghost' intentionally absent
            "child":      {"parent_stream": "ghost"},
            "grandchild": {"parent_stream": "child"},
        }
        newly, _ = self._run(stream_defs, set())
        self.assertIn("child",      newly)
        self.assertIn("grandchild", newly)

    def test_already_excluded_stream_not_returned(self):
        stream_defs = {
            "parent": {},
            "child":  {"parent_stream": "parent"},
        }
        newly, _ = self._run(stream_defs, {"parent", "child"})
        self.assertNotIn("child", newly)

    def test_stream_without_parent_not_affected(self):
        stream_defs = {
            "failed":      {},
            "no_parent":   {"parent_stream": None},
            "empty_sdef":  {},
        }
        newly, _ = self._run(stream_defs, {"failed"})
        self.assertNotIn("no_parent",  newly)
        self.assertNotIn("empty_sdef", newly)

    def test_mutates_working_set_in_place(self):
        """The working set passed by the caller must be updated in-place."""
        stream_defs = {
            "a": {},
            "b": {"parent_stream": "a"},
        }
        working = {"a"}
        _cascade_exclude_children(stream_defs, working)
        self.assertIn("b", working)

    def test_returns_only_newly_added_streams(self):
        """Returned set must not include streams already in excluded on entry."""
        stream_defs = {
            "root":  {},
            "child": {"parent_stream": "root"},
        }
        newly, _ = self._run(stream_defs, {"root", "child"})
        self.assertEqual(newly, set())


# ---------------------------------------------------------------------------
# _cascade_exclude_expand
# ---------------------------------------------------------------------------

class TestCascadeExcludeExpand(unittest.TestCase):
    """Unit tests for the $expand cascade helper."""

    def test_expand_stream_excluded_when_parent_failed(self):
        stream_defs = {
            "parent_stream": {"parent_stream": None, "expand_info": None},
            "expand_stream": {
                "parent_stream": None,
                "expand_info": {"expand-parent-entity-set": "ParentStream"},
            },
        }
        excluded = {"parent_stream"}
        result = _cascade_exclude_expand(stream_defs, excluded)
        self.assertIn("expand_stream", result)

    def test_expand_stream_not_excluded_when_parent_ok(self):
        stream_defs = {
            "parent_stream": {"parent_stream": None, "expand_info": None},
            "expand_stream": {
                "parent_stream": None,
                "expand_info": {"expand-parent-entity-set": "ParentStream"},
            },
        }
        result = _cascade_exclude_expand(stream_defs, set())
        self.assertNotIn("expand_stream", result)

    def test_already_excluded_expand_stream_not_returned(self):
        stream_defs = {
            "parent_stream": {"parent_stream": None, "expand_info": None},
            "expand_stream": {
                "parent_stream": None,
                "expand_info": {"expand-parent-entity-set": "ParentStream"},
            },
        }
        excluded = {"parent_stream", "expand_stream"}
        result = _cascade_exclude_expand(stream_defs, excluded)
        self.assertNotIn("expand_stream", result)

    def test_no_expand_info_not_excluded(self):
        stream_defs = {
            "failed":   {"expand_info": None},
            "no_expand": {"expand_info": None},
        }
        result = _cascade_exclude_expand(stream_defs, {"failed"})
        self.assertNotIn("no_expand", result)

    def test_expand_info_missing_key_not_excluded(self):
        """expand_info present but without expand-parent-entity-set key."""
        stream_defs = {
            "failed": {"expand_info": None},
            "stream": {"expand_info": {}},
        }
        result = _cascade_exclude_expand(stream_defs, {"failed"})
        self.assertNotIn("stream", result)


# ---------------------------------------------------------------------------
# probe_stream
# ---------------------------------------------------------------------------

class TestProbeStream(unittest.TestCase):

    @patch("tap_sap_sales_service_cloud.stream_probe.requests.get")
    def test_returns_ok_on_200(self, mock_get):
        resp = MagicMock()
        resp.status_code = 200
        mock_get.return_value = resp
        result = probe_stream("my_stream", "https://example.com", "/MyStream",
                              "Basic abc")
        self.assertEqual(result["status"], 200)
        self.assertIsNone(result["error"])
        self.assertEqual(result["stream"], "my_stream")

    @patch("tap_sap_sales_service_cloud.stream_probe.requests.get")
    def test_returns_status_on_403(self, mock_get):
        resp = MagicMock()
        resp.status_code = 403
        resp.text = '{"error": "Not Authorized"}'
        mock_get.return_value = resp
        result = probe_stream("my_stream", "https://example.com", "/MyStream",
                              "Basic abc")
        self.assertEqual(result["status"], 403)
        self.assertIsNotNone(result["error"])

    @patch("tap_sap_sales_service_cloud.stream_probe.requests.get")
    def test_returns_status_on_400(self, mock_get):
        resp = MagicMock()
        resp.status_code = 400
        resp.text = '{"error": "Bad Request"}'
        mock_get.return_value = resp
        result = probe_stream("my_stream", "https://example.com", "/MyStream",
                              "Basic abc")
        self.assertEqual(result["status"], 400)

    @patch("tap_sap_sales_service_cloud.stream_probe.requests.get")
    def test_network_error_returns_none_status(self, mock_get):
        import requests as req
        mock_get.side_effect = req.exceptions.ConnectionError("refused")
        result = probe_stream("my_stream", "https://example.com", "/MyStream",
                              "Basic abc")
        self.assertIsNone(result["status"])
        self.assertIsNotNone(result["error"])

    @patch("tap_sap_sales_service_cloud.stream_probe.requests.get")
    def test_extra_params_passed_to_request(self, mock_get):
        resp = MagicMock()
        resp.status_code = 200
        mock_get.return_value = resp
        probe_stream("s", "https://example.com", "/S", "Basic x",
                     extra_params={"$filter": "ChangedOn ge datetimeoffset'2024-01-01T00:00:00Z'"})
        _, kwargs = mock_get.call_args
        self.assertIn("$filter", kwargs["params"])
