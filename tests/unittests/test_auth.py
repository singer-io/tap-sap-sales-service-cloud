"""Unit tests for tap_sap_sales_service_cloud.auth."""

import base64
import unittest

from tap_sap_sales_service_cloud.auth import (build_basic_auth_header,
                                              build_token_request)


class TestBuildBasicAuthHeader(unittest.TestCase):
    """Tests for build_basic_auth_header."""

    def test_returns_basic_header_when_credentials_present(self):
        config = {"username": "user@tenant", "password": "s3cret"}
        header = build_basic_auth_header(config)
        self.assertIsNotNone(header)
        self.assertTrue(header.startswith("Basic "))

    def test_base64_encoding_is_correct(self):
        config = {"username": "alice", "password": "pass123"}
        header = build_basic_auth_header(config)
        encoded = header.split(" ", 1)[1]
        decoded = base64.b64decode(encoded).decode()
        self.assertEqual(decoded, "alice:pass123")

    def test_returns_none_when_username_missing(self):
        config = {"password": "s3cret"}
        self.assertIsNone(build_basic_auth_header(config))

    def test_returns_none_when_password_missing(self):
        config = {"username": "user@tenant"}
        self.assertIsNone(build_basic_auth_header(config))

    def test_returns_none_when_both_missing(self):
        self.assertIsNone(build_basic_auth_header({}))


class TestBuildTokenRequest(unittest.TestCase):
    """Tests for build_token_request."""

    def test_returns_empty_dict_for_static_token(self):
        config = {"access_token": "mytoken"}
        self.assertEqual(build_token_request(config), {})

    def test_saml_bearer_flow(self):
        config = {"client_id": "my_client", "assertion": "base64saml"}
        payload = build_token_request(config)
        self.assertEqual(
            payload["grant_type"],
            "urn:ietf:params:oauth:grant-type:saml2-bearer",
        )
        self.assertEqual(payload["client_id"], "my_client")
        self.assertEqual(payload["assertion"], "base64saml")

    def test_refresh_token_flow(self):
        config = {"client_id": "my_client", "refresh_token": "myrefresh"}
        payload = build_token_request(config)
        self.assertEqual(payload["grant_type"], "refresh_token")
        self.assertEqual(payload["refresh_token"], "myrefresh")

    def test_none_values_stripped(self):
        config = {"client_id": "cid", "assertion": None}
        payload = build_token_request(config)
        self.assertNotIn("assertion", payload)

    def test_saml_assertion_alias(self):
        """``saml_assertion`` should be treated as ``assertion``."""
        config = {"client_id": "cid", "saml_assertion": "b64"}
        payload = build_token_request(config)
        self.assertEqual(payload.get("assertion"), "b64")
