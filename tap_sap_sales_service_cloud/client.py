"""HTTP client for SAP Sales and Service Cloud OData v2 API.

Handles:
    - HTTP Basic Auth and OAuth 2.0 SAML Bearer authentication.
    - Automatic token refresh before expiry.
    - Exponential-backoff retry for 5xx, 429, and connection errors.
    - JSON response parsing and error mapping.

NOTE: OAuth 2 SAML workflow may need to be reworked depending on the requirement

Reference:
    https://help.sap.com/docs/sap-cloud-for-customer/odata-services/sap-cloud-for-customer-odata-api
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout
from singer import get_logger, metrics

from tap_sap_sales_service_cloud.auth import (build_basic_auth_header,
                                              build_token_request)
from tap_sap_sales_service_cloud.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING, SAPSalesServiceCloudError,
    SAPSalesServiceCloudRateLimitError, SAPSalesServiceCloudServer5xxError)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300

# Default OData service path for the main SAP C4C CRM data API.
DEFAULT_ODATA_PATH = "/sap/c4c/odata/v1/c4codataapi"


def raise_for_error(response: requests.Response) -> None:
    """Raise a mapped exception for non-2xx responses."""
    if response.status_code in (200, 201, 204):
        return

    try:
        response_json = response.json()
    except ValueError:
        response_json = {}

    mapped = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {})
    if not mapped and 500 <= response.status_code < 600:
        exc_cls = SAPSalesServiceCloudServer5xxError
        default_message = f"Server side error ({response.status_code})."
    else:
        exc_cls = mapped.get("raise_exception", SAPSalesServiceCloudError)
        default_message = mapped.get("message", "Unknown API error.")

    # SAP C4C error responses may nest the message under several keys.
    error_node = response_json.get("error") or {}
    if isinstance(error_node, dict):
        inner_msg = (
            error_node.get("message", {}).get("value")
            if isinstance(error_node.get("message"), dict)
            else error_node.get("message")
        )
    else:
        inner_msg = str(error_node) if error_node else None

    error_msg = (
        inner_msg
        or response_json.get("error_description")
        or response_json.get("message")
        or default_message
    )

    raise exc_cls(
        f"HTTP {response.status_code}: {error_msg}",
        response=response,
    )


class SAPSalesServiceCloudClient:
    """Authenticated HTTP client for SAP Sales and Service Cloud OData API."""

    def __init__(self, config: Dict) -> None:
        self.config = config
        self.base_url = config["api_server"].rstrip("/")
        self.odata_path = config.get("odata_path", DEFAULT_ODATA_PATH).rstrip("/")
        self.request_timeout = int(config.get("request_timeout", REQUEST_TIMEOUT))

        self._session = requests.Session()
        self._access_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

        # Build Basic-auth header once; if absent we fall through to OAuth.
        self._basic_auth_header: Optional[str] = build_basic_auth_header(config)

    def __enter__(self):
        self.refresh_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def refresh_access_token(self) -> None:
        """Obtain or refresh the OAuth access token.

        For Basic-auth mode no token exchange is needed — every request
        carries the ``Authorization: Basic <base64>`` header built at
        construction time.
        """
        if self._basic_auth_header:
            return  # Basic auth — no token needed.

        if self.config.get("access_token"):
            self._access_token = self.config["access_token"]
            return  # Static token — no exchange needed.

        payload = build_token_request(self.config)
        token_url = self.base_url + "/sap/bc/sec/oauth2/token"
        response = self._session.post(
            token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.request_timeout,
        )
        raise_for_error(response)
        response_json = response.json()
        self._access_token = response_json.get("access_token")
        expires_in_seconds = int(response_json.get("expires_in", 3600))
        self._expires_at = datetime.now(tz=timezone.utc) + timedelta(
            seconds=expires_in_seconds
        )

    def get_access_token(self) -> str:
        """Return a valid access token, refreshing if necessary."""
        if self._access_token and self._expires_at:
            if self._expires_at > datetime.now(tz=timezone.utc):
                return self._access_token
        if self._access_token and self.config.get("access_token"):
            return self._access_token
        self.refresh_access_token()
        return self._access_token

    def get_auth_header(self) -> str:
        """Return the full ``Authorization`` header value.

        - **Basic auth** (``username`` + ``password``): ``Basic <base64>``
        - **OAuth / SAML bearer** (all other modes): ``Bearer <token>``
        """
        if self._basic_auth_header:
            return self._basic_auth_header
        return f"Bearer {self.get_access_token()}"

    def authenticate(
        self, headers: Dict, params: Dict
    ) -> Tuple[Dict, Dict]:
        """Inject auth headers and default OData query parameters."""
        headers["Authorization"] = self.get_auth_header()
        headers["Accept"] = "application/json"
        params["$format"] = "json"
        return headers, params

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def get(
        self,
        path: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> Any:
        """Perform an authenticated GET request and return parsed JSON."""
        params = dict(params or {})
        headers = dict(headers or {})
        headers, params = self.authenticate(headers, params)
        endpoint = f"{self.base_url}{path}"
        return self._make_request("GET", endpoint, headers=headers, params=params)

    def request_raw(
        self, method: str, endpoint: str, **kwargs
    ) -> requests.Response:
        """Perform a raw HTTP request without JSON parsing.

        Useful for fetching XML metadata (``$metadata`` endpoint).
        """
        return self._make_request(method, endpoint, parse_json=False, **kwargs)

    # SAP C4C OData API docs make no mention of rate limiting, HTTP 429, or a
    # Retry-After header (verified: help.sap.com + SAP-archive/C4CODATAAPIDEVGUIDE).
    # 429 is therefore treated identically to 5xx: exponential back-off, factor=2.
    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            SAPSalesServiceCloudRateLimitError,
            SAPSalesServiceCloudServer5xxError,
            ConnectionResetError,
            RequestsConnectionError,
            ChunkedEncodingError,
            Timeout,
        ),
        max_tries=5,
        factor=2,
    )
    def _make_request(
        self,
        method: str,
        endpoint: str,
        parse_json: bool = True,
        **kwargs,
    ) -> Optional[Mapping[Any, Any]]:
        """Execute an HTTP request; back-off decorators handle retries."""
        kwargs.setdefault("timeout", self.request_timeout)
        with metrics.http_request_timer(endpoint):
            response = self._session.request(method, endpoint, **kwargs)

        raise_for_error(response)

        if not parse_json:
            return response

        if not response.content.strip():
            return None
        return response.json()
