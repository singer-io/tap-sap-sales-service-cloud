"""Authentication utilities for tap-sap-sales-service-cloud.

SAP Sales and Service Cloud (C4C) supports:
  1. HTTP Basic Authentication  — ``username`` + ``password`` in config.
  2. OAuth 2.0 SAML Bearer flow — ``client_id`` + ``assertion`` (base64 SAML).
  3. Static access token         — ``access_token`` in config.

Reference:
  https://help.sap.com/docs/sap-cloud-for-customer/odata-services/sap-cloud-for-customer-odata-api#authentication
"""

import base64
from typing import Dict, Optional

from singer import get_logger

LOGGER = get_logger()


def build_basic_auth_header(config: Dict) -> Optional[str]:
    """Return a ``Basic <base64>`` authorization header value when the config
    contains ``username`` and ``password``.

    The encoded credential is ``base64(username:password)`` per RFC 7617.
    Returns ``None`` when the required keys are absent so callers can fall
    back to the OAuth / SAML bearer flow.
    """
    username = config.get("username")
    password = config.get("password")
    if username and password:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        LOGGER.info(
            "Using HTTP Basic authentication with the provided username and password."
        )
        return f"Basic {token}"
    return None


def build_token_request(config: Dict) -> Dict:
    """Build the OAuth 2.0 token request payload.

    NOTE: This may be reworked depending on the requirement

    Supported flows:
      - SAML 2.0 Bearer Assertion — requires ``client_id`` + ``assertion``.
      - Refresh token             — requires ``client_id`` + ``refresh_token``.

    Returns an empty dict when ``access_token`` is supplied (static token mode).
    """
    if config.get("access_token"):
        # Static token — no OAuth exchange needed.
        return {}

    if config.get("refresh_token"):
        LOGGER.info("Using OAuth 2.0 refresh-token flow.")
        payload = {
            "client_id": config.get("client_id"),
            "grant_type": "refresh_token",
            "refresh_token": config.get("refresh_token"),
        }
    else:
        LOGGER.info("Using OAuth 2.0 SAML 2.0 Bearer Assertion flow.")
        payload = {
            "client_id": config.get("client_id"),
            "grant_type": "urn:ietf:params:oauth:grant-type:saml2-bearer",
            "assertion": config.get("assertion") or config.get("saml_assertion"),
        }

    # Strip None values so the form-encoded body stays clean.
    return {k: v for k, v in payload.items() if v is not None}
