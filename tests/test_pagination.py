"""Test tap can replicate multiple pages of data for paginated streams."""
from base import SAPSalesServiceCloudBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


class SAPSalesServiceCloudPaginationTest(PaginationTest, SAPSalesServiceCloudBaseTest):
    """Ensure tap can replicate multiple pages of data for streams that use
    pagination.
    """

    @staticmethod
    def name():
        """Unique test name used by the tap-tester framework."""
        return "tap_tester_sap_sales_service_cloud_pagination_test"

    def streams_to_test(self):
        """Return streams known to span at least two OData pages.

        Only streams confirmed to hold more than the default page size
        (1000 records) in the reference environment are included.  Add
        additional stream names here when the test account is populated
        with more data.
        """
        return {
            # Testing specific stream having paginated data
            "competitor_po_box_deviating_state_code_collection"
        }
