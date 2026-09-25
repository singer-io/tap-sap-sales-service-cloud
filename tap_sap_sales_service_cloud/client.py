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
import re
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urlsplit

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
API_SERVER_PATTERN = re.compile(
    r"^https://[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?\.crm\.ondemand\.com/?$"
)


def validate_api_server(api_server: str) -> None:
    """Reject API servers outside the SAP Sales and Service Cloud domain."""
    if not isinstance(api_server, str) or not API_SERVER_PATTERN.fullmatch(api_server):
        raise ValueError(
            "api_server must be an HTTPS SAP tenant URL ending in "
            ".crm.ondemand.com"
        )


def validate_odata_path(odata_path: str) -> None:
    """Reject OData paths that can alter the validated API authority."""
    if not isinstance(odata_path, str):
        raise ValueError("odata_path must be a relative URL path")

    parsed = urlsplit(odata_path)
    if (
        not odata_path.startswith("/")
        or odata_path.startswith("//")
        or parsed.scheme
        or parsed.netloc
    ):
        raise ValueError("odata_path must be a relative URL path")


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


# TREX raises this deterministically whenever a '<field> eq null' filter
# reaches certain HANA-view-backed entities (e.g. Lead) — retrying the same
# request is guaranteed to fail identically, so back-off should give up
# immediately and let the caller fall back to a non-null-inclusive filter.
def _is_non_retryable_null_filter_error(exc: Exception) -> bool:
    """ Checks for a nullptr message or TREX-specific 70023000 error
        indicating a non-retryable null filter."""
    msg = getattr(exc, "message", "") or str(exc)
    return "rootWhere == nullptr" in msg or "70023000" in msg


class SAPSalesServiceCloudClient:
    """Authenticated HTTP client for SAP Sales and Service Cloud OData API."""

    def __init__(self, config: Dict) -> None:
        self.config = config
        validate_api_server(config["api_server"])
        odata_path = config.get("odata_path", DEFAULT_ODATA_PATH)
        validate_odata_path(odata_path)
        self.base_url = config["api_server"].rstrip("/")
        self.odata_path = odata_path.rstrip("/")
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
            allow_redirects=False,
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
        giveup=_is_non_retryable_null_filter_error,
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
        kwargs.setdefault("allow_redirects", False)
        with metrics.http_request_timer(endpoint):
            response = self._session.request(method, endpoint, **kwargs)

        raise_for_error(response)

        if not parse_json:
            return response

        if not response.content.strip():
            return None
        return response.json()
