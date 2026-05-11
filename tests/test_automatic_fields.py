"""test_automatic_fields.py — Verify automatic (key + replication) fields.

Ensures that running the tap with only automatic fields selected still
replicates the primary key(s) and replication key for every stream.
Automatic fields are those the tap marks as ``inclusion: automatic`` in
the catalog metadata — they cannot be de-selected.
"""

from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import SAPSalesServiceCloudBaseTest


class SAPSalesServiceCloudAutomaticFieldsTest(MinimumSelectionTest, SAPSalesServiceCloudBaseTest):
    """Ensure key and replication-key fields are always replicated."""

    # NOTE: For some streams, Automatic tests are failing as API is returning records with similar primary
    # key values in different records.
    # EG: For this stream: EmployeeUserSubscriptionAssignmentUserSubscriptionTypeCodeCollection, we are
    # getting multiple records with same value for primary key field "Code".
    # This is causing the test to fail as tap-tester expects unique values for primary key fields.
    # Endpoint to test: https://myxxxxx.crm.ondemand.com/sap/c4c/odata/v1/c4codataapi/EmployeeUserSubscriptionAssignmentUserSubscriptionTypeCodeCollection?$format=json&$filter=Code eq '8003186'

    @staticmethod
    def name():
        return "tap_tester_sap_sales_service_cloud_automatic_fields"

    def streams_having_duplicated_records(self):
        """
            Returns the set of streams having few identical records with same primary key values.
            This causes our fix to have all keys as composite keys to fail as tap-tester
            expects unique values for primary key fields.
        """

        return {
            "contract_item_party_state_code_collection",
            "lead_individual_customer_address_state_code_collection",
            "contract_item_party_po_box_deviating_state_code_collection"
        }

    def streams_to_test(self):
        return self.expected_stream_names() - self.streams_to_exclude() - self.streams_having_duplicated_records()
