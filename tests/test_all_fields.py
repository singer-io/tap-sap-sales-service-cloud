"""test_all_fields.py — Verify all schema fields are replicated.

Ensures that running the tap with all streams and all fields selected
produces records that contain every field declared in the SCHEMA message
for that stream.
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import SAPSalesServiceCloudBaseTest


class SAPSalesServiceCloudTestAllFields(AllFieldsTest, SAPSalesServiceCloudBaseTest):
    """Ensure running the tap with all streams and fields selected results
    in the replication of all fields."""

    start_date = "2024-01-01T00:00:00Z"

    # Fields that the API documents in its EDMX metadata but never returns
    # in practice (e.g. write-only or tenant-disabled fields).  Add entries
    # here when a field is consistently absent from the API response so the
    # test does not fail spuriously.
    MISSING_FIELDS = {}

    @staticmethod
    def name():
        return "tap_tester_sap_sales_service_cloud_all_fields"

    def streams_to_test(self):
        return self.expected_stream_names() - self.streams_to_exclude()
