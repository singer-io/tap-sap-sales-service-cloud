"""Unit tests for tap_sap_sales_service_cloud.stream_probe."""

import unittest
from unittest.mock import MagicMock, patch

from tap_sap_sales_service_cloud.stream_probe import (_cascade_exclude,
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
