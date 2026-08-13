"""Unit tests for tap_sap_sales_service_cloud.exceptions."""

import unittest

from tap_sap_sales_service_cloud.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING, SAPSalesServiceCloudBadRequestError,
    SAPSalesServiceCloudError, SAPSalesServiceCloudForbiddenError,
    SAPSalesServiceCloudNotFoundError, SAPSalesServiceCloudRateLimitError,
    SAPSalesServiceCloudServer5xxError, SAPSalesServiceCloudUnauthorizedError)


class TestExceptionHierarchy(unittest.TestCase):
    """All custom exceptions should inherit from the base error class."""

    def test_bad_request_is_base_error(self):
        self.assertTrue(
            issubclass(
                SAPSalesServiceCloudBadRequestError,
                SAPSalesServiceCloudError,
            )
        )

    def test_unauthorized_is_base_error(self):
        self.assertTrue(
            issubclass(
                SAPSalesServiceCloudUnauthorizedError,
                SAPSalesServiceCloudError,
            )
        )

    def test_forbidden_is_base_error(self):
        self.assertTrue(
            issubclass(
                SAPSalesServiceCloudForbiddenError,
                SAPSalesServiceCloudError,
            )
        )

    def test_not_found_is_base_error(self):
        self.assertTrue(
            issubclass(
                SAPSalesServiceCloudNotFoundError,
                SAPSalesServiceCloudError,
            )
        )

    def test_rate_limit_is_base_error(self):
        self.assertTrue(
            issubclass(
                SAPSalesServiceCloudRateLimitError,
                SAPSalesServiceCloudError,
            )
        )

    def test_5xx_is_base_error(self):
        self.assertTrue(
            issubclass(
                SAPSalesServiceCloudServer5xxError,
                SAPSalesServiceCloudError,
            )
        )


class TestErrorCodeMapping(unittest.TestCase):
    """ERROR_CODE_EXCEPTION_MAPPING should cover key HTTP status codes."""

    def test_400_maps_to_bad_request(self):
        self.assertIs(
            ERROR_CODE_EXCEPTION_MAPPING[400]["raise_exception"],
            SAPSalesServiceCloudBadRequestError,
        )

    def test_401_maps_to_unauthorized(self):
        self.assertIs(
            ERROR_CODE_EXCEPTION_MAPPING[401]["raise_exception"],
            SAPSalesServiceCloudUnauthorizedError,
        )

    def test_429_maps_to_rate_limit(self):
        self.assertIs(
            ERROR_CODE_EXCEPTION_MAPPING[429]["raise_exception"],
            SAPSalesServiceCloudRateLimitError,
        )

    def test_all_entries_have_message(self):
        for code, entry in ERROR_CODE_EXCEPTION_MAPPING.items():
            self.assertIn(
                "message",
                entry,
                msg=f"Status {code} mapping is missing a 'message' key.",
            )


class TestBaseErrorAttributes(unittest.TestCase):
    """SAPSalesServiceCloudError should store message and response."""

    def test_message_stored(self):
        err = SAPSalesServiceCloudError(message="oops")
        self.assertEqual(err.message, "oops")

    def test_response_stored(self):
        mock_resp = object()
        err = SAPSalesServiceCloudError(message="oops", response=mock_resp)
        self.assertIs(err.response, mock_resp)

    def test_str_representation(self):
        err = SAPSalesServiceCloudError(message="test error")
        self.assertIn("test error", str(err))
