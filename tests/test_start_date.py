"""test_start_date.py — Verify the tap respects the configured start date.

Runs two syncs with different start dates and asserts that the second sync
(with a later start date) returns fewer or equal records than the first.
"""

from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import SAPSalesServiceCloudBaseTest


class SAPSalesServiceCloudStartDateTest(StartDateTest, SAPSalesServiceCloudBaseTest):
    """Verify that changing the start_date config reduces the record window."""

    @staticmethod
    def name():
        return "tap_tester_sap_sales_service_cloud_start_date"

    def streams_to_test(self):
        # The number of streams are very large,
        # so we are only testing the main stream with replication key
        return {
            "lead_collection",
            "activity_collection"
        }

    @property
    def start_date_1(self):
        """Earlier start date — should produce more records."""
        return "2024-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        """Later start date — should produce fewer or equal records."""
        return "2026-03-18T23:35:33.796000Z"
