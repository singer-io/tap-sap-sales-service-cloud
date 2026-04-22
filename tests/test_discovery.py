"""Test tap discovery mode and metadata."""
from base import SAPSalesServiceCloudBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest
from tap_tester import menagerie
import re


class SAPSalesServiceCloudDiscoveryTest(DiscoveryTest, SAPSalesServiceCloudBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""
    orphan_streams = {}

    @staticmethod
    def name():
        return "tap_tester_sap_sales_service_cloud_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names() - self.streams_to_exclude()

    # ---------------------------------------------------------------------- #
    # Custom test: parent-child relationship metadata (SAP Sales Service specific)       #
    # ---------------------------------------------------------------------- #

    def test_parent_stream_metadata(self):
        """Verify that child streams have the correct ``parent-tap-stream-id``
        written into their catalog root-level metadata.

        The value is set by ``metadata_discovery.py`` and stored under the
        ``parent-tap-stream-id`` key in the root breadcrumb metadata entry.
        Only streams listed in ``expected_metadata()`` with a
        ``PARENT_TAP_STREAM_ID`` entry are validated.
        """
        for stream in self.streams_to_test():
            expected = self.expected_metadata().get(stream, {})
            expected_parent = expected.get(self.PARENT_TAP_STREAM_ID)

            if expected_parent is None:
                # Top-level stream — no parent expected; skip silently.
                continue

            with self.subTest(stream=stream):
                catalog = [
                    c for c in self.found_catalogs
                    if c["stream_name"] == stream
                ][0]
                metadata = menagerie.get_annotated_schema(
                    self.conn_id, catalog["stream_id"]
                )["metadata"]

                stream_root = [
                    item for item in metadata if item.get("breadcrumb") == []
                ]
                self.assertTrue(
                    len(stream_root) > 0,
                    msg=f"No root-level metadata entry found for stream '{stream}'",
                )

                actual_parent = (
                    stream_root[0]
                    .get("metadata", {})
                    .get(self.PARENT_TAP_STREAM_ID)
                )
                self.assertEqual(
                    expected_parent,
                    actual_parent,
                    msg=(
                        f"Stream '{stream}': expected parent-tap-stream-id "
                        f"'{expected_parent}', got '{actual_parent}'"
                    ),
                )
    # ---------------------------------------------------------------------- #
    # Override: stream naming (allow digits for SAP Sales Service version numbers)       #
    # ---------------------------------------------------------------------- #

    def test_stream_naming(self):
        """Verify stream names use only lowercase letters, digits, and underscores.

        The tap-tester base implementation uses ``[a-z_]+``, which rejects
        digits.  SAP Sales Service OData entity sets legitimately include
        version numbers in their names (e.g. ``ONB2ActivityNudgeDetails`` →
        ``onb2_activity_nudge_details``).  We broaden the allowed charset to
        ``[a-z0-9_]+`` while keeping all other assertions from the base class.
        """
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                match = re.fullmatch(r"[a-z0-9_]+", stream)
                self.assertIsNotNone(
                    match,
                    msg=(
                        f"Stream name '{stream}' does not conform to the Singer "
                        f"naming convention [a-z0-9_]+"
                    ),
                )
                self.assertEqual(
                    match.group(0),
                    stream,
                    msg=f"Stream name '{stream}' contains invalid characters",
                )
