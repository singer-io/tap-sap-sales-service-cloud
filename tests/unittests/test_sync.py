"""Unit tests for tap_sap_sales_service_cloud.sync."""

import unittest
from unittest import mock
from unittest.mock import MagicMock

from singer import metadata as md

from tap_sap_sales_service_cloud.sync import update_currently_syncing


class TestUpdateCurrentlySyncing(unittest.TestCase):
    """Tests for update_currently_syncing."""

    @mock.patch("tap_sap_sales_service_cloud.sync.singer")
    def test_sets_stream_name_in_state(self, mock_singer):
        mock_singer.get_currently_syncing = MagicMock(return_value=None)
        mock_singer.set_currently_syncing = MagicMock()
        mock_singer.write_state = MagicMock()
        state = {}
        update_currently_syncing(state, "account_collection")
        mock_singer.set_currently_syncing.assert_called_once_with(
            state, "account_collection"
        )
        mock_singer.write_state.assert_called_once_with(state)

    @mock.patch("tap_sap_sales_service_cloud.sync.singer")
    def test_clears_stream_name_from_state(self, mock_singer):
        mock_singer.write_state = MagicMock()
        state = {"currently_syncing": "account_collection"}
        update_currently_syncing(state, None)
        self.assertNotIn("currently_syncing", state)
        mock_singer.write_state.assert_called_once_with(state)


class TestStreamSelection(unittest.TestCase):
    """Tests for stream selection logic in sync()."""

    def _make_stream(self, stream_id, selected):
        stream = MagicMock()
        stream.tap_stream_id = stream_id
        stream.schema = MagicMock()
        raw = md.to_map(md.new())
        raw[()] = {"selected": selected}
        stream.metadata = md.to_list(raw)
        return stream

    def test_only_selected_streams_synced(self):
        """Streams not marked selected should not appear in sync list."""
        catalog = MagicMock()
        catalog.streams = [
            self._make_stream("account_collection", True),
            self._make_stream("contact_collection", False),
        ]
        selected_ids = [
            s.tap_stream_id
            for s in catalog.streams
            if md.get(md.to_map(s.metadata), (), "selected")
        ]
        self.assertIn("account_collection", selected_ids)
        self.assertNotIn("contact_collection", selected_ids)
