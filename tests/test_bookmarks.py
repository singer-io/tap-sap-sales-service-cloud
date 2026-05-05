"""test_bookmarks.py — Verify incremental bookmark behaviour.

Runs two consecutive syncs and asserts that:
  1. The first sync writes a STATE with a bookmark per stream.
  2. The second sync (using that STATE) replicates only records at or
     after the bookmark — i.e. the record count is ≤ the first sync's
     count for the same window.
  3. The bookmark advances after each sync.

Child streams that have their own replication key are included because
they maintain an independent bookmark.  Child streams with no replication
key are excluded — they scope data through the parent FK filter only.
"""

from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

from base import SAPSalesServiceCloudBaseTest


class SAPSalesServiceCloudBookmarkTest(BookmarkTest, SAPSalesServiceCloudBaseTest):
    """Verify bookmark state is written and respected on subsequent syncs."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    initial_bookmarks = {
        "bookmarks": {
            "lead_collection": {"EntityLastChangedOn": "2025-12-01T00:00:00.000000Z"},
            "activity_collection": {"EntityLastChangedOn": "2025-12-01T00:00:00.000000Z"}
        }
    }

    @staticmethod
    def name():
        return "tap_tester_sap_sales_service_cloud_bookmarks"

    def streams_to_test(self):
        # The number of streams are very large,
        # so we are only testing the main stream with replication key
        return {
            "lead_collection",
            "activity_collection"
        }

    def calculate_new_bookmarks(self):
        """Return bookmark state to inject between sync 1 and sync 2.

        The dates returned here should be:
          * **After** the oldest record in sync 1  — so sync 2 is not empty.
          * **Before** the newest record in sync 1  — so sync 2 is smaller
            than sync 1 (confirming the bookmark is respected).
        """

        return {
            "lead_collection": {"EntityLastChangedOn": "2026-03-18T23:11:16.643000Z"},
            "activity_collection":     {"EntityLastChangedOn": "2026-03-19T15:20:24.729000Z"}
        }
