"""stream_probe.py — Discovery-time endpoint accessibility check.

Each entity set is probed before the catalog is emitted.  A probe that
returns 4xx / 5xx causes the stream (and any cascade-dependent children)
to be excluded from the catalog so they are never attempted during sync.

Probe strategy
--------------
Every probe mirrors actual sync-time query parameters:

* ``$top=1`` — minimal data transfer.
* ``$filter`` — replication key ``ge <start_date>`` using the correct OData
  literal for the field's Edm type:

  - ``Edm.DateTimeOffset`` → ``datetimeoffset'YYYY-MM-DDTHH:MM:SS.ffffffZ'``
  - ``Edm.DateTime``       → ``datetime'YYYY-MM-DDTHH:MM:SS.ffffff'``

  FULL_TABLE streams are probed without a filter.

* ``$orderby`` — appended when the replication-key field is EDMX-sortable.
* Child streams combine the parent FK filter with the replication-key filter
  so SAP evaluates the full permission chain.

Reliability
-----------
* Transient network errors (``Timeout``, ``ConnectionError``) are retried up
  to ``_MAX_PROBE_RETRIES`` times with exponential back-off.
* HTTP 4xx / 5xx is **not** retried — those are definitive denials.
* ``$expand``-only streams are skipped entirely; they can only be queried
  via ``$expand`` on their parent and would always 400 if probed directly.

Concurrency
-----------
Probes run in a ``ThreadPoolExecutor`` capped at ``_MAX_PROBE_WORKERS``
threads.  Each worker issues its own ``requests.get`` call so sessions are
never shared across threads.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import backoff
import requests
from singer import get_logger

from tap_sap_sales_service_cloud.metadata_discovery import _to_snake_case

LOGGER = get_logger()

_PROBE_TIMEOUT = 30      # seconds per request
_MAX_PROBE_WORKERS = 20  # max concurrent probe threads
_MAX_PROBE_RETRIES = 3   # max retries on transient network errors

# Type alias for a probe task tuple passed to probe_stream().
_ProbeTask = Tuple[str, str, str, str, Optional[Dict]]


# ---------------------------------------------------------------------------
# OData datetime literal helpers
# ---------------------------------------------------------------------------

def _fmt_datetimeoffset(dt: datetime) -> str:
    """Format *dt* as an ``Edm.DateTimeOffset`` literal (UTC, with Z)."""
    return dt.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def _fmt_datetime(dt: datetime) -> str:
    """Format *dt* as an ``Edm.DateTime`` literal (no timezone suffix)."""
    return dt.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )


def _replication_filter(
    key: str, edm_type: str, start_date: str
) -> str:
    """Return a ``$filter`` clause mirroring sync-time behaviour.

    The correct OData keyword is chosen based on *edm_type*:
    ``datetimeoffset'...'`` for ``Edm.DateTimeOffset``, ``datetime'...'``
    for everything else (``Edm.DateTime`` and any unknown types).
    """
    try:
        dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        dt = datetime(2000, 1, 1, tzinfo=timezone.utc)

    if edm_type == "Edm.DateTimeOffset":
        return f"{key} ge datetimeoffset'{_fmt_datetimeoffset(dt)}'"
    return f"{key} ge datetime'{_fmt_datetime(dt)}'"


def _probe_filter_value(schema: Dict) -> str:
    """Return a safe OData ``<op> <value>`` fragment for a child FK field.

    Branches on the field's JSON Schema (enriched with ``x-edm-type`` by
    :func:`metadata_discovery._pff_schema`) so the SAP query evaluator
    sees a type-correct filter and can make a genuine authorization
    decision rather than rejecting with HTTP 400.

    * ``format: date-time`` + ``x-edm-type: Edm.DateTimeOffset``
      → ``datetimeoffset'2000-01-01T00:00:00.000000Z'``
    * ``format: date-time`` (any other Edm.DateTime variant)
      → ``datetime'2000-01-01T00:00:00.000000'``
    * ``type: integer / number`` → ``eq 0``
    * Everything else (string, unknown) → ``eq '__probe__'``
    """
    if schema.get("format") == "date-time":
        if schema.get("x-edm-type") == "Edm.DateTimeOffset":
            return "ge datetimeoffset'2000-01-01T00:00:00.000000Z'"
        return "ge datetime'2000-01-01T00:00:00.000000'"
    types = schema.get("type", [])
    if not isinstance(types, list):
        types = [types]
    if "integer" in types or "number" in types:
        return "eq 0"
    return "eq '__probe__'"


# ---------------------------------------------------------------------------
# Single-stream probe
# ---------------------------------------------------------------------------

def _on_backoff(details: Dict) -> None:
    """Backoff hook — log each retry attempt."""
    LOGGER.warning(
        "Probe transient error — retrying "
        "(attempt %d, wait %.1fs).",
        details["tries"],
        details["wait"],
    )


def probe_stream(
    stream_name: str,
    base_url: str,
    path: str,
    auth_header: str,
    extra_params: Optional[Dict] = None,
) -> Dict:
    """Probe *path* with ``$top=1`` and return a status dict.

    Returns a ``dict`` with keys ``stream``, ``status`` (HTTP code or
    ``None`` on network failure), and ``error`` (response snippet or
    exception message).

    Transient network errors are retried up to ``_MAX_PROBE_RETRIES``
    times.  HTTP errors are returned immediately without retrying.
    """
    url = f"{base_url}{path}"
    headers = {
        "Authorization": auth_header,
        "Accept": "application/json",
    }
    params: Dict = {"$top": "1", "$format": "json"}
    if extra_params:
        params.update(extra_params)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        max_tries=_MAX_PROBE_RETRIES,
        on_backoff=_on_backoff,
        jitter=backoff.random_jitter,
    )
    def _get() -> requests.Response:
        return requests.get(
            url, headers=headers, params=params, timeout=_PROBE_TIMEOUT
        )

    try:
        resp = _get()
    except (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
    ) as exc:
        LOGGER.warning(
            "Probe abandoned '%s' after %d retries: %s",
            stream_name, _MAX_PROBE_RETRIES, exc,
        )
        return {"stream": stream_name, "status": None, "error": str(exc)}
    except requests.exceptions.RequestException as exc:
        LOGGER.warning("Probe network error '%s': %s", stream_name, exc)
        return {"stream": stream_name, "status": None, "error": str(exc)}

    if resp.status_code >= 400:
        snippet = (resp.text or "")[:300].strip()
        return {
            "stream": stream_name,
            "status": resp.status_code,
            "error": snippet,
        }

    return {
        "stream": stream_name,
        "status": resp.status_code,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Probe-task builders
# ---------------------------------------------------------------------------

def _root_task(
    stream_name: str,
    sdef: Dict,
    base_url: str,
    auth_header: str,
    start_date: str,
) -> _ProbeTask:
    """Build a probe task for a root (non-child, non-expand) stream."""
    rep_keys: List[str] = sdef.get("replication_keys") or []
    edm_type: str = sdef.get("replication_key_edm_type") or ""
    orderby: str = sdef.get("orderby_field") or ""

    extra: Optional[Dict] = None
    if rep_keys and edm_type:
        extra = {
            "$filter": _replication_filter(
                rep_keys[0], edm_type, start_date
            )
        }
        if orderby:
            extra["$orderby"] = f"{orderby}"

    return (stream_name, base_url, sdef["path"], auth_header, extra)


def _child_task(
    stream_name: str,
    sdef: Dict,
    base_url: str,
    auth_header: str,
    start_date: str,
) -> _ProbeTask:
    """Build a probe task for a child stream.

    Combines the parent FK filter with the replication-key filter so SAP
    evaluates the full permission chain on the child endpoint.
    """
    clauses: List[str] = []

    fk_field: str = sdef.get("parent_filter_field") or ""
    if fk_field:
        pff_schema = sdef.get("parent_filter_field_schema", {})
        filter_clause = _probe_filter_value(pff_schema)
        clauses.append(f"{fk_field} {filter_clause}")

    rep_keys: List[str] = sdef.get("replication_keys") or []
    edm_type: str = sdef.get("replication_key_edm_type") or ""
    if rep_keys and edm_type:
        clauses.append(
            _replication_filter(rep_keys[0], edm_type, start_date)
        )

    extra = {"$filter": " and ".join(clauses)} if clauses else None
    return (stream_name, base_url, sdef["path"], auth_header, extra)


# ---------------------------------------------------------------------------
# Cascade-exclude helper
# ---------------------------------------------------------------------------

def _cascade_exclude(
    stream_defs: Dict,
    directly_failed: Set[str],
) -> Set[str]:
    """Return dependent streams to exclude because their parent was pruned.

    Covers child streams (``parent_stream`` key) and ``$expand``-only
    streams whose expand-parent appears in *directly_failed*.
    """
    cascade: Set[str] = set()
    for stream_name, sdef in stream_defs.items():
        if stream_name in directly_failed:
            continue

        parent = sdef.get("parent_stream") or ""
        if parent and parent in directly_failed:
            LOGGER.info(
                "Cascade-excluding child '%s' — parent '%s' pruned.",
                stream_name, parent,
            )
            cascade.add(stream_name)
            continue

        expand_parent_raw = (
            (sdef.get("expand_info") or {}).get(
                "expand-parent-entity-set", ""
            )
        )
        if expand_parent_raw:
            expand_parent = _to_snake_case(expand_parent_raw)
            if expand_parent in directly_failed:
                LOGGER.info(
                    "Cascade-excluding $expand stream '%s' "
                    "— expand-parent '%s' pruned.",
                    stream_name, expand_parent,
                )
                cascade.add(stream_name)

    return cascade


# ---------------------------------------------------------------------------
# Public orchestrator
# ---------------------------------------------------------------------------

def probe_all_streams(
    client,
    stream_defs: Dict,
    max_workers: int = _MAX_PROBE_WORKERS,
) -> Set[str]:
    """Probe every stream; return the complete set to exclude from catalog.

    The returned set includes both directly-failed streams (HTTP 4xx/5xx)
    and cascade-excluded dependents (children / ``$expand`` streams whose
    parent was pruned).

    Parameters
    ----------
    client:
        Authenticated SAP client exposing ``base_url``,
        ``get_auth_header()``, and ``config``.
    stream_defs:
        ``{stream_name: stream_def_dict}`` mapping from
        :func:`~tap_sap_sales_service_cloud.metadata_discovery\
.discover_dynamic_streams`.
    max_workers:
        Concurrent probe threads (default ``_MAX_PROBE_WORKERS``).
    """
    auth_header = client.get_auth_header()
    base_url = client.base_url
    start_date = client.config.get("start_date", "2000-01-01T00:00:00Z")

    # Classify each stream into one of three buckets.
    root_streams: Dict[str, Dict] = {}
    child_streams: Dict[str, Dict] = {}
    skip_streams: Set[str] = set()

    for name, sdef in stream_defs.items():
        if sdef.get("expand_info"):
            skip_streams.add(name)
        elif sdef.get("parent_stream") and sdef.get("parent_filter_field"):
            child_streams[name] = sdef
        else:
            root_streams[name] = sdef

    LOGGER.info(
        "Stream probe: %d root, %d child, %d skipped ($expand-only). "
        "max_workers=%d, timeout=%ds per probe.",
        len(root_streams), len(child_streams), len(skip_streams),
        max_workers, _PROBE_TIMEOUT,
    )

    # Build all probe tasks up front, tagging each with is_child.
    root_tasks: List[Tuple[_ProbeTask, bool]] = [
        (_root_task(n, s, base_url, auth_header, start_date), False)
        for n, s in root_streams.items()
    ]
    child_tasks: List[Tuple[_ProbeTask, bool]] = [
        (_child_task(n, s, base_url, auth_header, start_date), True)
        for n, s in child_streams.items()
    ]
    all_tasks = root_tasks + child_tasks
    total = len(all_tasks)

    # Execute concurrently; collect direct failures.
    directly_failed: Set[str] = set()
    done_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(probe_stream, *task): (task[0], is_child)
            for task, is_child in all_tasks
        }
        for future in as_completed(futures):
            stream_name, is_child = futures[future]
            done_count += 1
            try:
                result = future.result()
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.warning(
                    "[Probe] Unexpected error '%s': %s", stream_name, exc
                )
                if done_count % 50 == 0 or done_count == total:
                    LOGGER.info(
                        "[Probe] Progress: %d/%d probes completed "
                        "(%d excluded so far)",
                        done_count, total, len(directly_failed),
                    )
                continue

            status = result["status"]
            is_excluded = status is not None and 400 <= status <= 599

            if is_excluded:
                LOGGER.warning(
                    "[Probe] Excluding%s stream '%s' — HTTP %s: %s",
                    " child" if is_child else "",
                    result["stream"],
                    result["status"],
                    result["error"],
                )
                directly_failed.add(result["stream"])
            elif status is None:
                LOGGER.warning(
                    "[Probe] Stream '%s' probe failed (network/timeout) — "
                    "keeping in catalog. Error: %s",
                    result["stream"],
                    result["error"],
                )
            else:
                LOGGER.debug(
                    "[Probe] Stream '%s' → HTTP %s",
                    result["stream"],
                    result["status"],
                )

            if done_count % 50 == 0 or done_count == total:
                LOGGER.info(
                    "[Probe] Progress: %d/%d probes completed "
                    "(%d excluded so far)",
                    done_count, total, len(directly_failed),
                )

    cascade = _cascade_exclude(stream_defs, directly_failed)
    all_excluded = directly_failed | cascade

    if all_excluded:
        LOGGER.info(
            "Probe excluded %d stream(s) total "
            "(%d direct, %d cascade)",
            len(all_excluded), len(directly_failed), len(cascade)
        )

    return all_excluded
