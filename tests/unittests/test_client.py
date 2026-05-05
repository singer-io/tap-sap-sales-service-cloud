"""Unit tests for tap_sap_sales_service_cloud.client."""

import unittest
from unittest import mock
from unittest.mock import MagicMock

from parameterized import parameterized

from tap_sap_sales_service_cloud.client import (DEFAULT_ODATA_PATH,
                                                REQUEST_TIMEOUT,
                                                SAPSalesServiceCloudClient,
                                                raise_for_error)
from tap_sap_sales_service_cloud.exceptions import (
    SAPSalesServiceCloudBadRequestError, SAPSalesServiceCloudRateLimitError,
    SAPSalesServiceCloudUnauthorizedError)


def _make_config(**kwargs):
    base = {
        "api_server": "https://my123456.crm.ondemand.com",
        "username": "admin@tenant",
        "password": "secret",
        "start_date": "2024-01-01T00:00:00Z",
    }
    base.update(kwargs)
    return base


def _ok_response():
    resp = MagicMock()
    resp.status_code = 200
    resp.content = b'{"d": {}}'
    resp.json.return_value = {"d": {}}
    return resp


def _rate_limit_response():
    resp = MagicMock()
    resp.status_code = 429
    resp.json.return_value = {}
    return resp


def _server_error_response():
    resp = MagicMock()
    resp.status_code = 500
    resp.json.return_value = {}
    return resp


class TestClientInit(unittest.TestCase):
    """Tests for SAPSalesServiceCloudClient construction."""

    def test_base_url_trailing_slash_stripped(self):
        config = _make_config(
            api_server="https://my123456.crm.ondemand.com/"
        )
        client = SAPSalesServiceCloudClient(config)
        self.assertFalse(client.base_url.endswith("/"))

    def test_default_odata_path(self):
        client = SAPSalesServiceCloudClient(_make_config())
        self.assertEqual(client.odata_path, DEFAULT_ODATA_PATH)

    def test_custom_odata_path(self):
        config = _make_config(odata_path="/sap/c4c/odata/v1/custom")
        client = SAPSalesServiceCloudClient(config)
        self.assertEqual(client.odata_path, "/sap/c4c/odata/v1/custom")

    def test_default_request_timeout(self):
        client = SAPSalesServiceCloudClient(_make_config())
        self.assertEqual(client.request_timeout, REQUEST_TIMEOUT)

    def test_basic_auth_header_built_when_credentials_present(self):
        client = SAPSalesServiceCloudClient(_make_config())
        self.assertIsNotNone(client._basic_auth_header)
        self.assertTrue(client._basic_auth_header.startswith("Basic "))

    def test_no_basic_auth_when_only_token(self):
        config = _make_config()
        del config["username"]
        del config["password"]
        config["access_token"] = "mytoken"
        client = SAPSalesServiceCloudClient(config)
        self.assertIsNone(client._basic_auth_header)


class TestGetAuthHeader(unittest.TestCase):
    """Tests for SAPSalesServiceCloudClient.get_auth_header."""

    def test_returns_basic_when_credentials_present(self):
        client = SAPSalesServiceCloudClient(_make_config())
        self.assertTrue(client.get_auth_header().startswith("Basic "))

    def test_returns_bearer_for_static_token(self):
        config = _make_config()
        del config["username"]
        del config["password"]
        config["access_token"] = "mytoken123"
        client = SAPSalesServiceCloudClient(config)
        self.assertEqual(client.get_auth_header(), "Bearer mytoken123")


class TestRaiseForError(unittest.TestCase):
    """Tests for the raise_for_error helper."""

    def _make_response(self, status_code, json_body=None, text=""):
        resp = MagicMock()
        resp.status_code = status_code
        resp.text = text
        if json_body is not None:
            resp.json.return_value = json_body
        else:
            resp.json.side_effect = ValueError("no json")
        return resp

    def test_200_does_not_raise(self):
        resp = self._make_response(200, {})
        raise_for_error(resp)  # should not raise

    def test_400_raises_bad_request(self):
        resp = self._make_response(
            400, {"error": {"message": {"value": "bad"}}}
        )
        with self.assertRaises(SAPSalesServiceCloudBadRequestError):
            raise_for_error(resp)

    def test_401_raises_unauthorized(self):
        resp = self._make_response(401, {})
        with self.assertRaises(SAPSalesServiceCloudUnauthorizedError):
            raise_for_error(resp)

    def test_429_raises_rate_limit(self):
        resp = self._make_response(429, {})
        with self.assertRaises(SAPSalesServiceCloudRateLimitError):
            raise_for_error(resp)

    def test_sap_nested_error_message_extracted(self):
        resp = self._make_response(
            400,
            {
                "error": {
                    "code": "ABC",
                    "message": {"lang": "en", "value": "Invalid filter."},
                }
            },
        )
        with self.assertRaises(SAPSalesServiceCloudBadRequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("Invalid filter.", str(ctx.exception))


class TestMakeRequestBackoff(unittest.TestCase):
    """Verify _make_request retries on 429 and 5xx via exponential backoff."""

    @parameterized.expand([
        ("429", _rate_limit_response),
        ("5xx", _server_error_response),
    ])
    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_retries_once_and_succeeds(
            self, _name, error_response_fn, mock_request, _mock_sleep):
        """Retries after a transient error; succeeds on second attempt."""
        mock_request.side_effect = [error_response_fn(), _ok_response()]
        client = SAPSalesServiceCloudClient(_make_config())
        result = client._make_request(
            "GET", "https://my123456.crm.ondemand.com/test"
        )
        self.assertEqual(result, {"d": {}})
        self.assertEqual(mock_request.call_count, 2)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request")
    def test_429_exhausts_retries_raises(self, mock_request, _mock_sleep):
        """Raises SAPSalesServiceCloudRateLimitError after max_tries=5."""
        mock_request.return_value = _rate_limit_response()
        client = SAPSalesServiceCloudClient(_make_config())
        with self.assertRaises(SAPSalesServiceCloudRateLimitError):
            client._make_request(
                "GET", "https://my123456.crm.ondemand.com/test"
            )
        self.assertEqual(mock_request.call_count, 5)
