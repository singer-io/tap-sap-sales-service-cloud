"""metadata_discovery.py — OData $metadata-driven stream discovery for SAP C4C.

Parses the tenant's OData EDMX document and builds Singer schemas, metadata,
and stream definitions for every entity set exposed by the API.

Key differences from SAP SuccessFactors:
  - SAP annotation namespace: ``http://www.sap.com/Protocols/SAPData``
  - Replication-key candidates: ``ChangedOn``, ``ChangeDateTime``,
    ``LastModifiedOn``, ``UpdatedOn``
  - Date-time types: ``Edm.DateTime``, ``Edm.DateTimeOffset``
  - C4C entity-set names typically end in ``Collection``
    (e.g. ``AccountCollection``, ``OpportunityCollection``).

Reference:
  https://help.sap.com/doc/d0f9ba822c08405da7d88174b304df84/CLOUD/en-US/index.html
"""

import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple

import singer
from singer import metadata

LOGGER = singer.get_logger()

# ---------------------------------------------------------------------------
# SAP C4C annotation namespace
# ---------------------------------------------------------------------------
SAP_DATA_NS = "{http://www.sap.com/Protocols/SAPData}"

# Fallback pattern used when the namespace prefix differs across API versions.
_SAP_NS_RE = re.compile(r"\{[^}]*sap[^}]*\}")

# ---------------------------------------------------------------------------
# OData primitive → JSON Schema type mapping
# ---------------------------------------------------------------------------
DATE_TIME_TYPES = {
    "Edm.DateTime",
    "Edm.DateTimeOffset",
}

ODATA_TO_JSON_TYPE: Dict[str, object] = {
    "Edm.Boolean": {"type": ["boolean", "null"]},
    "Edm.Byte": {"type": ["integer", "null"]},
    "Edm.SByte": {"type": ["integer", "null"]},
    "Edm.Int16": {"type": ["integer", "null"]},
    "Edm.Int32": {"type": ["integer", "null"]},
    "Edm.Int64": {"type": ["integer", "null"]},
    "Edm.Single": {"type": ["number", "null"]},
    "Edm.Double": {"type": ["number", "null"]},
    "Edm.Decimal": {"type": ["number", "null"]},
    "Edm.Guid": {"type": ["string", "null"]},
    "Edm.String": {"type": ["string", "null"]},
    "Edm.Binary": {"type": ["string", "null"]},
    "Edm.DateTime": {"type": ["string", "null"], "format": "date-time"},
    "Edm.DateTimeOffset": {"type": ["string", "null"], "format": "date-time"},
    "Edm.Time": {"type": ["string", "null"]},
}

# ---------------------------------------------------------------------------
# Ordered list of candidate replication-key field names for C4C entities.
# The first match that is also marked as filterable wins.
# ---------------------------------------------------------------------------
REPLICATION_KEY_CANDIDATES = [
    "ChangedOn",
    "ChangeDateTime",
    "LastModifiedOn",
    "UpdatedOn",
    "LastChangeDateTime",
    "ModifiedOn",
    # SAP C4C relationship/has-entity change timestamps (e.g.
    # CorporateAccountHasContactPersonCollection).  Added after the
    # primary candidates so they only win when no primary field exists.
    "EntityLastChangedOn",  # SAP "Change Timestamp" on has-entities
    "LastChangedOn",        # variant of ChangedOn used on some entities
    "LastUpdatedOn",        # variant of UpdatedOn used on some entities
]

# ---------------------------------------------------------------------------
# Known parent-child overrides for C4C entity relationships not always
# represented cleanly in EDMX association constraints.
#
# Format:
#   "<child_stream_name>": {
#       "parent-stream": "<parent_stream_name>",       # snake_case stream id
#       "parent-filter-field": "<child_filter_field>", # OData filter field
#       "parent-key-field": "<parent_pk_field>",       # parent entity PK
#   }
# ---------------------------------------------------------------------------
KNOWN_PARENT_OVERRIDES: Dict[str, Dict] = {
    # Example: Opportunity items belong to an Opportunity.
    # "opportunity_item_collection": {
    #     "parent-stream": "opportunity_collection",
    #     "parent-filter-field": "OpportunityID",
    #     "parent-key-field": "ObjectID",
    # },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_snake_case(name: str) -> str:
    """Convert CamelCase / PascalCase to snake_case stream id."""
    # Insert underscore before uppercase letters that follow lowercase or digits.
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert underscore before uppercase sequences followed by lowercase.
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


def _local_name(tag: str) -> str:
    """Return the local (non-namespace) part of an ElementTree tag string.

    ElementTree represents namespaced tags as ``{namespace_uri}localname``.
    Tags without a namespace are returned unchanged.
    """
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _find_children(node: ET.Element, local_name: str) -> List[ET.Element]:
    """Return all direct children whose local name matches *local_name*."""
    return [
        child
        for child in node
        if _local_name(child.tag) == local_name
    ]


def _find_child(node: ET.Element, local_name: str) -> Optional[ET.Element]:
    """Return the first direct child whose local name matches *local_name*."""
    for child in node:
        if _local_name(child.tag) == local_name:
            return child
    return None


def _get_sap_attrib(prop: ET.Element, attrib_name: str) -> Optional[str]:
    """Return the SAP annotation attribute value with namespace fallback.

    Tries the canonical ``SAP_DATA_NS`` prefix first; if absent, falls back
    to a regex scan of all namespaced attributes so the tap keeps working
    when SAP changes the prefix URI across API versions.
    """
    # Step 1: canonical namespace.
    value = prop.attrib.get(f"{SAP_DATA_NS}{attrib_name}")
    if value is not None:
        return value
    # Step 2: namespace-agnostic scan.
    for key, val in prop.attrib.items():
        match = _SAP_NS_RE.match(key)
        if match:
            local = key[match.end():]
            if local == attrib_name:
                return val
    return None


def _is_filterable(prop: ET.Element) -> bool:
    """Return True when the property is marked filterable (default: True).

    SAP C4C marks non-filterable properties with ``sap:filterable="false"``.
    """
    val = _get_sap_attrib(prop, "filterable")
    return val != "false"


def _is_sortable(prop: ET.Element) -> bool:
    """Return True when the property is marked sortable (default: True)."""
    val = _get_sap_attrib(prop, "sortable")
    return val != "false"


def _is_creatable(prop: ET.Element) -> bool:
    """Return True when the property is marked creatable (default: True)."""
    val = _get_sap_attrib(prop, "creatable")
    return val != "false"


def _build_association_map(
    schema_nodes: List[ET.Element],
) -> Dict[str, Dict]:
    """Build a map of fully-qualified association name → association metadata.

    Each entry records the two End elements and any ReferentialConstraint.
    """
    assoc_map: Dict[str, Dict] = {}
    for schema_node in schema_nodes:
        namespace = schema_node.attrib.get("Namespace", "")
        for assoc in _find_children(schema_node, "Association"):
            name = assoc.attrib.get("Name", "")
            fq_name = f"{namespace}.{name}" if namespace else name
            ends: Dict[str, Dict] = {}
            for end in _find_children(assoc, "End"):
                role = end.attrib.get("Role", "")
                ends[role] = {
                    "type": end.attrib.get("Type", ""),
                    "multiplicity": end.attrib.get("Multiplicity", ""),
                }
            dependent_role = None
            principal_role = None
            dependent_keys: List[str] = []
            principal_keys: List[str] = []
            constraint = _find_child(assoc, "ReferentialConstraint")
            if constraint is not None:
                dep_node = _find_child(constraint, "Dependent")
                prin_node = _find_child(constraint, "Principal")
                if dep_node is not None:
                    dependent_role = dep_node.attrib.get("Role")
                    dependent_keys = [
                        pr.attrib.get("Name", "")
                        for pr in _find_children(dep_node, "PropertyRef")
                    ]
                if prin_node is not None:
                    principal_role = prin_node.attrib.get("Role")
                    principal_keys = [
                        pr.attrib.get("Name", "")
                        for pr in _find_children(prin_node, "PropertyRef")
                    ]
            assoc_map[fq_name] = {
                "ends": ends,
                "dependent_role": dependent_role,
                "principal_role": principal_role,
                "dependent_keys": dependent_keys,
                "principal_keys": principal_keys,
            }
    return assoc_map


def _infer_dependent_field(
    dependent_properties: Dict,
    principal_keys: List[str],
    nav_name: Optional[str] = None,
) -> Optional[str]:
    """Infer the FK field on the dependent entity when no constraint is given."""
    if not principal_keys:
        return None
    dep_fields = set(dependent_properties.keys())
    # Best case: principal key name matches a dependent field directly.
    for key in principal_keys:
        if key in dep_fields:
            return key
    # Convention: <navName><Key>, <navName>Id, <navName>_id.
    nav_base = nav_name or ""
    if nav_base:
        for key in principal_keys:
            candidates = [
                f"{nav_base}{key[:1].upper()}{key[1:]}",
                f"{nav_base}Id",
                f"{nav_base}_id",
            ]
            for candidate in candidates:
                if candidate in dep_fields:
                    return candidate
    return None


def _rescue_type_mismatched_filter_field(
    nav_name: Optional[str],
    par_is_dt: bool,
    dependent_properties: Dict,
) -> Optional[str]:
    """Find a type-compatible child filter field when the inferred one mismatches.

    Called when ``_infer_dependent_field`` picks a child field whose
    JSON-Schema type (date-time or not) differs from the parent key field
    it must be equated to in an OData ``$filter`` clause.

    Strategy
    --------
    Derive a candidate FK field name from the navigation property name
    (e.g. ``countryNav`` → ``country``, ``jobCountryNav`` → ``jobCountry``),
    then verify it exists on the child entity *and* shares the same
    date-time-ness as the parent key field.

    Example
    -------
    EntityX.countryNav → Country.  Country PK = (code:String,
    effectiveStartDate:DateTime).  ``_infer_dependent_field`` wrongly picks
    ``effectiveStartDate`` (DateTime) because it shares the name with a PK
    component.  This function strips ``Nav`` from ``countryNav`` → ``country``
    and finds ``country`` (String) — type-compatible with ``code`` (String).

    Parameters
    ----------
    nav_name:
        Name of the NavigationProperty, e.g. ``'countryNav'``.
    par_is_dt:
        Whether the parent key field has ``format: date-time`` (True) or not.
    dependent_properties:
        JSON-Schema property map of the child entity.

    Returns
    -------
    str | None
        Name of a type-compatible child field, or ``None`` if none found.
    """
    base = nav_name or ""
    # Strip recognised NavigationProperty suffixes to get the FK base name.
    for suffix in ("Nav", "nav"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break

    candidates: List[str] = []
    if base:
        candidates.extend([base, f"{base}Id", f"{base}Code"])

    for candidate in candidates:
        if candidate not in dependent_properties:
            continue
        cand_is_dt = (
            dependent_properties[candidate].get("format") == "date-time"
        )
        if cand_is_dt == par_is_dt:
            return candidate

    return None


def _infer_parent_relationship(
    entity_type: str,
    nav: Dict,
    assoc: Dict,
    entity_type_to_set: Dict[str, str],
    entity_types: Dict[str, Dict],
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    """Infer parent stream, filter field, and key field for a navigation.

    Returns:
        (parent_stream, parent_filter_field, parent_key_field,
         relationship_name, inference_mode)
    or a tuple of Nones when no valid relationship is found.
    """
    ends = assoc.get("ends", {})
    dependent_role = assoc.get("dependent_role")
    principal_role = assoc.get("principal_role")

    # Path 1 — authoritative ReferentialConstraint.
    if dependent_role and principal_role:
        dependent_end = ends.get(dependent_role, {})
        principal_end = ends.get(principal_role, {})
        if dependent_end.get("type") != entity_type:
            return None, None, None, None, None
        principal_type = principal_end.get("type")
        principal_set = entity_type_to_set.get(principal_type)
        if not principal_set:
            return None, None, None, None, None
        dependent_keys = assoc.get("dependent_keys") or []
        principal_keys = assoc.get("principal_keys") or []
        if not dependent_keys or not principal_keys:
            return None, None, None, None, None
        return (
            _to_snake_case(principal_set),
            dependent_keys[0],
            principal_keys[0],
            nav.get("relationship"),
            "referential_constraint",
        )

    # Path 2 — multiplicity heuristic (* → 1 / 0..1).
    from_role = nav.get("from_role")
    to_role = nav.get("to_role")
    if not from_role or not to_role:
        return None, None, None, None, None

    current_end = ends.get(from_role, {})
    target_end = ends.get(to_role, {})
    current_mult = current_end.get("multiplicity", "")
    target_mult = target_end.get("multiplicity", "")
    if not (current_mult == "*" and target_mult in {"1", "0..1"}):
        return None, None, None, None, None

    target_type = target_end.get("type", "")
    parent_set = entity_type_to_set.get(target_type)
    if not parent_set:
        return None, None, None, None, None

    parent_entity_data = entity_types.get(target_type, {})
    principal_keys = parent_entity_data.get("keys", [])
    if not principal_keys:
        return None, None, None, None, None

    dependent_properties = entity_types.get(entity_type, {}).get("properties", {})
    dependent_field = _infer_dependent_field(
        dependent_properties,
        principal_keys,
        nav_name=nav.get("name"),
    )
    if not dependent_field:
        return None, None, None, None, None

    return (
        _to_snake_case(parent_set),
        dependent_field,
        principal_keys[0],
        nav.get("relationship"),
        "multiplicity_heuristic",
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def discover_dynamic_streams(client) -> Tuple[Dict, Dict, Dict]:
    """Build schemas + Singer metadata for all entity sets from OData $metadata.

    Returns:
        schemas        — ``{stream_name: json_schema_dict}``
        field_metadata — ``{stream_name: singer_metadata_list}``
        stream_defs    — ``{stream_name: stream_definition_dict}``
    """
    endpoint = f"{client.base_url}{client.odata_path}/$metadata"
    LOGGER.info("Fetching OData metadata from %s", endpoint)
    response = client.request_raw(
        "GET",
        endpoint,
        headers={
            "Authorization": client.get_auth_header(),
            "Accept": "application/xml",
        },
    )
    root = ET.fromstring(response.text)

    # Collect all Schema nodes under DataServices.
    schema_nodes: List[ET.Element] = []
    for data_services in _find_children(root, "DataServices"):
        schema_nodes.extend(_find_children(data_services, "Schema"))

    entity_types: Dict[str, Dict] = {}          # fq_type → {keys, properties, …}
    entity_sets: Dict[str, str] = {}            # set_name → fq_entity_type
    entity_navigations: Dict[str, List[Dict]] = {}  # fq_type → [nav_dicts]

    associations = _build_association_map(schema_nodes)

    for schema_node in schema_nodes:
        namespace = schema_node.attrib.get("Namespace", "")
        for entity_type in _find_children(schema_node, "EntityType"):
            type_name = entity_type.attrib.get("Name", "")
            fq_name = f"{namespace}.{type_name}" if namespace else type_name

            # Key properties.
            key_names: List[str] = []
            key_node = _find_child(entity_type, "Key")
            if key_node is not None:
                for key_ref in _find_children(key_node, "PropertyRef"):
                    ref_name = key_ref.attrib.get("Name")
                    if ref_name:
                        key_names.append(ref_name)

            # Scalar properties.
            properties: Dict[str, Dict] = {}
            filterable_props: set = set()

            prop_elements = _find_children(entity_type, "Property")

            # Detect SAP "expand-only" entities: ALL properties are
            # non-filterable, non-sortable, non-creatable AND there are no
            # outbound NavigationProperties.  SAP rejects direct queries on
            # these with COE0025 / COE0018.
            all_non_queryable = bool(prop_elements) and all(
                not _is_filterable(p)
                and not _is_sortable(p)
                and not _is_creatable(p)
                for p in prop_elements
            )
            has_nav = bool(_find_children(entity_type, "NavigationProperty"))
            is_expand_only = all_non_queryable and not has_nav

            for prop in prop_elements:
                prop_name = prop.attrib.get("Name", "")
                prop_type = prop.attrib.get("Type", "Edm.String")
                # Strip Collection() wrapper if present.
                if prop_type.startswith("Collection("):
                    inner = prop_type[len("Collection("):-1]
                    json_prop = {
                        "type": ["array", "null"],
                        "items": ODATA_TO_JSON_TYPE.get(
                            inner, {"type": ["string", "null"]}
                        ),
                    }
                else:
                    json_prop = dict(
                        ODATA_TO_JSON_TYPE.get(
                            prop_type, {"type": ["string", "null"]}
                        )
                    )
                properties[prop_name] = json_prop
                if _is_filterable(prop):
                    filterable_props.add(prop_name)

            # NavigationProperty definitions (for relationship inference).
            navigations: List[Dict] = []
            for nav in _find_children(entity_type, "NavigationProperty"):
                relationship = nav.attrib.get("Relationship")
                if relationship:
                    navigations.append(
                        {
                            "name": nav.attrib.get("Name"),
                            "relationship": relationship,
                            "from_role": nav.attrib.get("FromRole"),
                            "to_role": nav.attrib.get("ToRole"),
                        }
                    )

            entity_types[fq_name] = {
                "keys": key_names,
                "properties": properties,
                "filterable_props": filterable_props,
                "is_expand_only": is_expand_only,
            }
            entity_navigations[fq_name] = navigations

        # EntityContainer → EntitySet mappings.
        for container in _find_children(schema_node, "EntityContainer"):
            for entity_set in _find_children(container, "EntitySet"):
                set_name = entity_set.attrib.get("Name", "")
                entity_type_ref = entity_set.attrib.get("EntityType", "")
                if set_name and entity_type_ref:
                    entity_sets[set_name] = entity_type_ref

    # Build reverse maps.
    entity_type_to_set: Dict[str, str] = {v: k for k, v in entity_sets.items()}

    # snake_case stream name → fully-qualified entity type.
    # Used by the type-compatibility guard to look up parent property schemas.
    stream_to_entity_type: Dict[str, str] = {
        _to_snake_case(sn): fq for sn, fq in entity_sets.items()
    }

    # Build reverse navigation map: target_type → (source_type, nav_name)
    # Used to detect $expand-only entities.
    reverse_nav: Dict[str, Tuple[str, str]] = {}
    for src_type, navs in entity_navigations.items():
        for nav in navs:
            assoc = associations.get(nav.get("relationship", ""))
            if not assoc:
                continue
            to_role = nav.get("to_role")
            target_type = assoc.get("ends", {}).get(to_role, {}).get("type")
            if target_type and target_type not in reverse_nav:
                reverse_nav[target_type] = (src_type, nav.get("name"))

    # Stream-name set for parent-existence validation.
    discovered_stream_names: set = {
        _to_snake_case(s) for s in entity_sets
    }

    # ---------------------------------------------------------------------------
    # Build per-entity-set schemas and Singer metadata.
    # ---------------------------------------------------------------------------
    schemas: Dict[str, Dict] = {}
    field_metadata: Dict[str, object] = {}
    stream_defs: Dict[str, Dict] = {}

    for set_name, entity_type in entity_sets.items():
        stream_name = _to_snake_case(set_name)
        entity_data = entity_types.get(entity_type, {})
        properties = entity_data.get("properties", {})
        key_names = entity_data.get("keys", [])
        entity_filterable = entity_data.get("filterable_props", set(properties.keys()))

        key_properties = key_names or []

        # Replication key: first candidate that exists and is filterable.
        replication_keys: List[str] = []
        for candidate in REPLICATION_KEY_CANDIDATES:
            if candidate in properties and candidate in entity_filterable:
                replication_keys = [candidate]
                break

        replication_method = "INCREMENTAL" if replication_keys else "FULL_TABLE"

        # -------------------------------------------------------------------
        # Parent-child relationship inference.
        # -------------------------------------------------------------------
        parent_stream = None
        parent_filter_field = None
        parent_key_field = None
        relationship_name = None
        relationship_inference = None
        parent_secondary_filter_field = None
        parent_secondary_key_field = None
        expand_info: Optional[Dict] = None
        # Name of the NavigationProperty that produced the accepted
        # relationship; used by the type-mismatch rescue stage.
        matched_nav_name: Optional[str] = None

        for nav in entity_navigations.get(entity_type, []):
            assoc = associations.get(nav.get("relationship", ""))
            if not assoc:
                continue
            (
                parent_stream,
                parent_filter_field,
                parent_key_field,
                relationship_name,
                relationship_inference,
            ) = _infer_parent_relationship(
                entity_type,
                nav,
                assoc,
                entity_type_to_set,
                entity_types,
            )
            if parent_stream:
                matched_nav_name = nav.get("name")
                break  # Use the first valid relationship found.

        # Apply manual overrides from KNOWN_PARENT_OVERRIDES.
        override = KNOWN_PARENT_OVERRIDES.get(stream_name)
        if override:
            declared_parent = override.get("parent-stream")
            if declared_parent not in discovered_stream_names:
                LOGGER.warning(
                    "KNOWN_PARENT_OVERRIDES entry for '%s' references parent "
                    "'%s' which was not discovered in this EDMX instance. "
                    "Stream will be attempted as FULL_TABLE direct query.",
                    stream_name,
                    declared_parent,
                )
            else:
                parent_stream = declared_parent
                parent_filter_field = override["parent-filter-field"]
                parent_key_field = override["parent-key-field"]
                parent_secondary_filter_field = override.get(
                    "parent-secondary-filter-field"
                )
                parent_secondary_key_field = override.get(
                    "parent-secondary-key-field"
                )
                relationship_name = None
                relationship_inference = "manual_override"
                LOGGER.info(
                    "Applied KNOWN_PARENT_OVERRIDES for %s → parent=%s",
                    stream_name,
                    parent_stream,
                )

        # Validate that the inferred parent actually exists.
        if parent_stream and parent_stream not in discovered_stream_names:
            LOGGER.warning(
                "Dropping parent '%s' for stream '%s': not discoverable "
                "in this EDMX instance (inferred via %s). "
                "Stream will be synced as FULL_TABLE direct query.",
                parent_stream,
                stream_name,
                relationship_inference,
            )
            parent_stream = None
            parent_filter_field = None
            parent_key_field = None

        # -------------------------------------------------------------------
        # Type-compatibility guard.
        #
        # When the inferred parent_filter_field (child FK) and
        # parent_key_field (parent PK) have incompatible JSON Schema types,
        # the OData $filter clause SAP would receive is invalid and SAP
        # returns HTTP 400.
        #
        # Classic false-positive (multiplicity heuristic, no constraint):
        #   EntityX.countryNav → Country  (PKs: code:String,
        #                                       effectiveStartDate:DateTime)
        #   _infer_dependent_field finds effectiveStartDate on EntityX
        #   → parent_filter_field='effectiveStartDate' (date-time)
        #   → parent_key_field='code'           (string)
        #   → $filter: effectiveStartDate eq 'BTN'  ← SAP HTTP 400
        #
        # Two-stage rescue before dropping the relationship:
        #
        #   Stage 1 — parent PK swap
        #     Scan the remaining PKs of the parent entity for one whose
        #     type matches the child FK type.  Covers parents whose PK
        #     list is ordered DateTime-first.
        #
        #   Stage 2 — child FK rescue via nav-name stripping
        #     Strip the "Nav" suffix from the NavigationProperty name
        #     (countryNav → country) and look for that base name on the
        #     child entity.  Accepted only when type-compatible.
        #
        #   If neither stage succeeds the relationship is dropped.
        # -------------------------------------------------------------------
        if parent_stream and parent_filter_field and parent_key_field:
            child_fld = properties.get(parent_filter_field, {})
            _par_et = stream_to_entity_type.get(parent_stream)
            _par_props = (
                entity_types.get(_par_et, {}).get("properties", {})
                if _par_et else {}
            )
            par_fld = _par_props.get(parent_key_field, {})
            child_is_dt = child_fld.get("format") == "date-time"
            par_is_dt = par_fld.get("format") == "date-time"

            if child_is_dt != par_is_dt:
                # Stage 1 — find a parent PK that matches the child type.
                _par_keys = (
                    entity_types.get(_par_et, {}).get("keys", [])
                    if _par_et else []
                )
                alt_par_key: Optional[str] = next(
                    (
                        pk for pk in _par_keys
                        if pk != parent_key_field
                        and (
                            _par_props.get(pk, {}).get("format")
                            == "date-time"
                        ) == child_is_dt
                    ),
                    None,
                )

                rescued_filter: Optional[str] = None
                if not alt_par_key:
                    # Stage 2 — strip Nav suffix; find matching child field.
                    rescued_filter = _rescue_type_mismatched_filter_field(
                        matched_nav_name,
                        par_is_dt,
                        properties,
                    )

                if alt_par_key:
                    LOGGER.info(
                        "Type-compat rescue '%s'→'%s': replaced "
                        "parent_key_field '%s' (dt=%s) with alt PK "
                        "'%s' (compatible with filter_field '%s')",
                        stream_name, parent_stream,
                        parent_key_field, par_is_dt,
                        alt_par_key, parent_filter_field,
                    )
                    parent_key_field = alt_par_key
                elif rescued_filter:
                    LOGGER.info(
                        "Type-compat rescue '%s'→'%s': replaced "
                        "parent_filter_field '%s' (dt=%s) with '%s' "
                        "(compatible with parent_key_field '%s')",
                        stream_name, parent_stream,
                        parent_filter_field, child_is_dt,
                        rescued_filter, parent_key_field,
                    )
                    parent_filter_field = rescued_filter
                else:
                    LOGGER.warning(
                        "Dropping parent '%s' for stream '%s': "
                        "type mismatch filter_field '%s' (dt=%s) vs "
                        "key_field '%s' (dt=%s) — no rescue found. "
                        "Stream will be synced as FULL_TABLE.",
                        parent_stream, stream_name,
                        parent_filter_field, child_is_dt,
                        parent_key_field, par_is_dt,
                    )
                    parent_stream = None
                    parent_filter_field = None
                    parent_key_field = None
                    parent_secondary_filter_field = None
                    parent_secondary_key_field = None
                    relationship_name = None
                    relationship_inference = None

        # -------------------------------------------------------------------
        # Auto-detect $expand-only entities.
        # -------------------------------------------------------------------
        if entity_data.get("is_expand_only", False):
            src_type, nav_name = reverse_nav.get(entity_type, (None, None))
            parent_set_name = (
                entity_type_to_set.get(src_type) if src_type else None
            )
            if parent_set_name and nav_name:
                expand_info = {
                    "expand-nav-property": nav_name,
                    "expand-parent-entity-set": parent_set_name,
                }
                LOGGER.debug(
                    "Stream '%s' marked as $expand-only via '%s' from '%s'.",
                    stream_name,
                    nav_name,
                    parent_set_name,
                )

        # -------------------------------------------------------------------
        # Inject synthetic parent-ID field into schema.
        # -------------------------------------------------------------------
        parent_id_field = None
        if parent_stream and parent_key_field:
            parent_entity_type = entity_sets.get(
                # Reverse-map snake_case stream name to entity set name.
                next(
                    (k for k in entity_sets if _to_snake_case(k) == parent_stream),
                    "",
                ),
                "",
            )
            parent_entity_data = entity_types.get(parent_entity_type, {})
            parent_props = parent_entity_data.get("properties", {})
            parent_pk_schema = dict(
                parent_props.get(
                    parent_key_field,
                    {"type": ["string", "null"]},
                )
            )
            if parent_filter_field and parent_filter_field not in properties:
                properties[parent_filter_field] = dict(parent_pk_schema)
            parent_id_field = (
                f"__parent_{parent_stream}_{parent_key_field}"
            )
            properties[parent_id_field] = dict(parent_pk_schema)

        schemas[stream_name] = {
            "type": "object",
            "additionalProperties": True,
            "properties": properties,
        }

        # Build Singer metadata.
        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schemas[stream_name],
            key_properties=key_properties,
            valid_replication_keys=replication_keys,
            replication_method=replication_method,
        )
        mdata = metadata.to_map(mdata)
        # Singer's get_standard_metadata marks only key_properties as
        # inclusion=automatic.  Replication-key fields must also be
        # automatic so that target systems always receive the timestamp
        # needed to resume incremental syncs.
        for rep_key in (replication_keys or []):
            if rep_key in schemas[stream_name].get("properties", {}):
                mdata = metadata.write(
                    mdata, ("properties", rep_key), "inclusion", "automatic"
                )
        mdata = metadata.write(mdata, (), "entity-set", set_name)

        if parent_stream:
            mdata = metadata.write(
                mdata, (), "parent-tap-stream-id", parent_stream
            )
            mdata = metadata.write(
                mdata, (), "parent-stream", parent_stream
            )
            mdata = metadata.write(
                mdata, (), "parent-filter-field", parent_filter_field
            )
            mdata = metadata.write(
                mdata, (), "parent-key-field", parent_key_field
            )
            if parent_secondary_filter_field:
                mdata = metadata.write(
                    mdata,
                    (),
                    "parent-secondary-filter-field",
                    parent_secondary_filter_field,
                )
            if parent_secondary_key_field:
                mdata = metadata.write(
                    mdata,
                    (),
                    "parent-secondary-key-field",
                    parent_secondary_key_field,
                )
            if parent_id_field:
                mdata = metadata.write(
                    mdata, (), "parent-id-field", parent_id_field
                )

        if expand_info:
            mdata = metadata.write(
                mdata, (), "expand-nav-property",
                expand_info["expand-nav-property"]
            )
            mdata = metadata.write(
                mdata, (), "expand-parent-entity-set",
                expand_info["expand-parent-entity-set"]
            )

        field_metadata[stream_name] = metadata.to_list(mdata)

        # OData path for this entity set.
        stream_path = f"{client.odata_path}/{set_name}"

        stream_defs[stream_name] = {
            "tap_stream_id": stream_name,
            "key_properties": key_properties,
            "replication_method": replication_method,
            "replication_keys": replication_keys,
            "path": stream_path,
            "entity_set": set_name,
            "parent_stream": parent_stream,
            "parent_filter_field": parent_filter_field,
            "parent_key_field": parent_key_field,
            "parent_secondary_filter_field": parent_secondary_filter_field,
            "parent_secondary_key_field": parent_secondary_key_field,
            "expand_info": expand_info,
            "relationship_name": relationship_name,
            "relationship_inference": relationship_inference,
        }

    LOGGER.info(
        "Discovered %d entity sets from OData $metadata.", len(stream_defs)
    )
    return schemas, field_metadata, stream_defs
