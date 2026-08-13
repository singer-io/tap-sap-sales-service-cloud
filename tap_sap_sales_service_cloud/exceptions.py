"""Custom exception classes for tap-sap-sales-service-cloud."""


class SAPSalesServiceCloudError(Exception):
    """Generic SAP Sales and Service Cloud API error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class SAPSalesServiceCloudBadRequestError(SAPSalesServiceCloudError):
    """400 Bad Request — the request body or query parameters are invalid."""


class SAPSalesServiceCloudUnauthorizedError(SAPSalesServiceCloudError):
    """401 Unauthorized — invalid or missing credentials."""


class SAPSalesServiceCloudForbiddenError(SAPSalesServiceCloudError):
    """403 Forbidden — the authenticated user lacks the required permissions."""


class SAPSalesServiceCloudNotFoundError(SAPSalesServiceCloudError):
    """404 Not Found — the requested resource does not exist."""


class SAPSalesServiceCloudMethodNotAllowedError(SAPSalesServiceCloudError):
    """405 Method Not Allowed."""


class SAPSalesServiceCloudConflictError(SAPSalesServiceCloudError):
    """409 Conflict."""


class SAPSalesServiceCloudRateLimitError(SAPSalesServiceCloudError):
    """429 Too Many Requests — honour the Retry-After response header."""


class SAPSalesServiceCloudServer5xxError(SAPSalesServiceCloudError):
    """Base class for HTTP 5xx server errors."""


class SAPSalesServiceCloudInternalServerError(SAPSalesServiceCloudServer5xxError):
    """500 Internal Server Error."""


class SAPSalesServiceCloudServiceUnavailableError(SAPSalesServiceCloudServer5xxError):
    """503 Service Unavailable."""


# ---------------------------------------------------------------------------
# Mapping of HTTP status codes → exception classes + default messages
# ---------------------------------------------------------------------------
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": SAPSalesServiceCloudBadRequestError,
        "message": "Bad Request — the request body or parameters are invalid.",
    },
    401: {
        "raise_exception": SAPSalesServiceCloudUnauthorizedError,
        "message": "Unauthorized — invalid or missing credentials.",
    },
    403: {
        "raise_exception": SAPSalesServiceCloudForbiddenError,
        "message": "Forbidden — insufficient permissions to access this resource.",
    },
    404: {
        "raise_exception": SAPSalesServiceCloudNotFoundError,
        "message": "Not Found — the requested resource does not exist.",
    },
    405: {
        "raise_exception": SAPSalesServiceCloudMethodNotAllowedError,
        "message": "Method Not Allowed.",
    },
    409: {
        "raise_exception": SAPSalesServiceCloudConflictError,
        "message": "Conflict — the request conflicts with the current state.",
    },
    429: {
        "raise_exception": SAPSalesServiceCloudRateLimitError,
        "message": "Too Many Requests — API rate limit exceeded.",
    },
    500: {
        "raise_exception": SAPSalesServiceCloudInternalServerError,
        "message": "Internal Server Error.",
    },
    503: {
        "raise_exception": SAPSalesServiceCloudServiceUnavailableError,
        "message": "Service Unavailable.",
    },
}
