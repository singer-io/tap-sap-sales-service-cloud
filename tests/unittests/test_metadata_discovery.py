"""Unit tests for tap_sap_sales_service_cloud.metadata_discovery."""

import unittest
import xml.etree.ElementTree as ET
from unittest import mock

from singer import metadata

from tap_sap_sales_service_cloud.metadata_discovery import (
    KNOWN_PARENT_OVERRIDES, REPLICATION_KEY_CANDIDATES, _find_child,
    _find_children, _get_sap_attrib, _is_filterable, _to_snake_case,
    discover_dynamic_streams)


class _FakeResponse:
    def __init__(self, text):
        self.text = text


class _FakeClient:
    def __init__(self, metadata_xml):
        self.base_url = "https://example.crm.ondemand.com"
        self.odata_path = "/sap/c4c/odata/v1/c4codataapi"
        self._metadata_xml = metadata_xml

    def get_auth_header(self):
        return "Basic dGVzdDp0ZXN0"

    def request_raw(self, method, endpoint, headers=None):
        self.last_request = {
            "method": method,
            "endpoint": endpoint,
            "headers": headers or {},
        }
        return _FakeResponse(self._metadata_xml)


class TestToSnakeCase(unittest.TestCase):
    """Tests for _to_snake_case helper."""

    def test_pascal_to_snake(self):
        self.assertEqual(
            _to_snake_case("AccountCollection"), "account_collection"
        )

    def test_camel_to_snake(self):
        self.assertEqual(
            _to_snake_case("serviceRequest"), "service_request"
        )

    def test_all_caps_acronym(self):
        self.assertEqual(
            _to_snake_case("CRMOpportunity"), "crm_opportunity"
        )

    def test_already_snake(self):
        self.assertEqual(
            _to_snake_case("already_snake"), "already_snake"
        )

    def test_single_word(self):
        self.assertEqual(_to_snake_case("Lead"), "lead")


class TestFindChildrenHelpers(unittest.TestCase):
    """Tests for _find_child / _find_children XML helpers."""

    def _make_root(self, xml_string):
        return ET.fromstring(xml_string)

    def test_find_children_by_local_name(self):
        root = self._make_root(
            "<Root><Child name='a'/><Child name='b'/><Other/></Root>"
        )
        children = _find_children(root, "Child")
        self.assertEqual(len(children), 2)

    def test_find_child_returns_first(self):
        root = self._make_root(
            "<Root><Child name='a'/><Child name='b'/></Root>"
        )
        child = _find_child(root, "Child")
        self.assertIsNotNone(child)
        self.assertEqual(child.attrib["name"], "a")

    def test_find_child_returns_none_when_missing(self):
        root = self._make_root("<Root/>")
        self.assertIsNone(_find_child(root, "Missing"))

    def test_find_children_with_namespace(self):
        root = self._make_root(
            "<Root xmlns:m='http://example.com'>"
            "<m:Child/><m:Child/></Root>"
        )
        children = _find_children(root, "Child")
        self.assertEqual(len(children), 2)


class TestGetSapAttrib(unittest.TestCase):
    """Tests for _get_sap_attrib namespace-aware attribute lookup."""

    _SAP_NS = "http://www.sap.com/Protocols/SAPData"

    def _make_prop(self, attrib_dict):
        elem = ET.Element("Property")
        elem.attrib.update(attrib_dict)
        return elem

    def test_canonical_namespace(self):
        prop = self._make_prop(
            {f"{{{self._SAP_NS}}}filterable": "false"}
        )
        self.assertEqual(_get_sap_attrib(prop, "filterable"), "false")

    def test_returns_none_when_absent(self):
        prop = self._make_prop({})
        self.assertIsNone(_get_sap_attrib(prop, "filterable"))


class TestIsFilterable(unittest.TestCase):
    """Tests for _is_filterable helper."""

    _SAP_NS = "http://www.sap.com/Protocols/SAPData"

    def _make_prop(self, filterable_value=None):
        elem = ET.Element("Property")
        if filterable_value is not None:
            elem.attrib[
                f"{{{self._SAP_NS}}}filterable"
            ] = filterable_value
        return elem

    def test_default_is_filterable(self):
        self.assertTrue(_is_filterable(self._make_prop()))

    def test_explicit_true(self):
        self.assertTrue(_is_filterable(self._make_prop("true")))

    def test_explicit_false(self):
        self.assertFalse(_is_filterable(self._make_prop("false")))


class TestReplicationKeyCandidates(unittest.TestCase):
    """Verify that expected replication key names are present."""

    def test_changed_on_is_candidate(self):
        self.assertIn("ChangedOn", REPLICATION_KEY_CANDIDATES)

    def test_change_date_time_is_candidate(self):
        self.assertIn("ChangeDateTime", REPLICATION_KEY_CANDIDATES)

    def test_last_modified_on_is_candidate(self):
        self.assertIn("LastModifiedOn", REPLICATION_KEY_CANDIDATES)

    def test_entity_last_changed_on_is_candidate(self):
        self.assertIn("EntityLastChangedOn", REPLICATION_KEY_CANDIDATES)

    def test_last_changed_on_is_candidate(self):
        self.assertIn("LastChangedOn", REPLICATION_KEY_CANDIDATES)

    def test_last_updated_on_is_candidate(self):
        self.assertIn("LastUpdatedOn", REPLICATION_KEY_CANDIDATES)

    def test_candidates_are_ordered_list(self):
        self.assertIsInstance(REPLICATION_KEY_CANDIDATES, list)
        self.assertGreater(len(REPLICATION_KEY_CANDIDATES), 0)

    def test_primary_candidates_precede_secondary(self):
        """ChangedOn must come before EntityLastChangedOn in priority."""
        idx_primary = REPLICATION_KEY_CANDIDATES.index("ChangedOn")
        idx_secondary = REPLICATION_KEY_CANDIDATES.index(
            "EntityLastChangedOn"
        )
        self.assertLess(idx_primary, idx_secondary)


class TestParentChildDiscovery(unittest.TestCase):
    """Verify parent-child relationship discovery from EDMX metadata."""

    def _discover(self, metadata_xml):
        client = _FakeClient(metadata_xml)
        return discover_dynamic_streams(client)

    def test_referential_constraint_relationship_discovered(self):
        metadata_xml = """
        <Edmx>
          <DataServices>
            <Schema Namespace="TestModel"
                    xmlns:sap="http://www.sap.com/Protocols/SAPData">
              <EntityType Name="Account">
                <Key><PropertyRef Name="ID" /></Key>
                <Property Name="ID" Type="Edm.String"
                          sap:filterable="true" />
              </EntityType>
              <EntityType Name="Address">
                <Key><PropertyRef Name="AddressID" /></Key>
                <Property Name="AddressID" Type="Edm.String"
                          sap:filterable="true" />
                <Property Name="AccountID" Type="Edm.String"
                          sap:filterable="true" />
                <NavigationProperty Name="AccountNav"
                    Relationship="TestModel.Account_Address"
                    FromRole="Address" ToRole="Account" />
              </EntityType>
              <Association Name="Account_Address">
                <End Role="Account" Type="TestModel.Account"
                     Multiplicity="1" />
                <End Role="Address" Type="TestModel.Address"
                     Multiplicity="*" />
                <ReferentialConstraint>
                  <Principal Role="Account">
                    <PropertyRef Name="ID" />
                  </Principal>
                  <Dependent Role="Address">
                    <PropertyRef Name="AccountID" />
                  </Dependent>
                </ReferentialConstraint>
              </Association>
              <EntityContainer Name="DefaultContainer">
                <EntitySet
                    Name="AccountCollection"
                    EntityType="TestModel.Account" />
                <EntitySet
                    Name="AddressCollection"
                    EntityType="TestModel.Address" />
              </EntityContainer>
            </Schema>
          </DataServices>
        </Edmx>
        """

        schemas, field_metadata, stream_defs = self._discover(metadata_xml)

        child_def = stream_defs["address_collection"]
        self.assertEqual(child_def["parent_stream"], "account_collection")
        self.assertEqual(child_def["parent_filter_field"], "AccountID")
        self.assertEqual(child_def["parent_key_field"], "ID")
        self.assertEqual(
            child_def["relationship_name"],
            "TestModel.Account_Address",
        )
        self.assertEqual(
            child_def["relationship_inference"],
            "referential_constraint",
        )
        self.assertIn(
            "__parent_account_collection_ID",
            schemas["address_collection"]["properties"],
        )

        root_metadata = metadata.to_map(
            field_metadata["address_collection"]
        ).get((), {})
        self.assertEqual(
            root_metadata["parent-stream"],
            "account_collection",
        )
        self.assertEqual(root_metadata["parent-filter-field"], "AccountID")
        self.assertEqual(root_metadata["parent-key-field"], "ID")

    def test_multiplicity_heuristic_relationship_discovered(self):
        metadata_xml = """
        <Edmx>
          <DataServices>
            <Schema Namespace="TestModel"
                    xmlns:sap="http://www.sap.com/Protocols/SAPData">
              <EntityType Name="Opportunity">
                <Key><PropertyRef Name="ObjectID" /></Key>
                <Property Name="ObjectID" Type="Edm.String"
                          sap:filterable="true" />
              </EntityType>
              <EntityType Name="OpportunityItem">
                <Key><PropertyRef Name="ItemID" /></Key>
                <Property Name="ItemID" Type="Edm.String"
                          sap:filterable="true" />
                <Property Name="OpportunityObjectID"
                          Type="Edm.String"
                          sap:filterable="true" />
                <NavigationProperty Name="Opportunity"
                    Relationship="TestModel.Opportunity_Item"
                    FromRole="OpportunityItem"
                    ToRole="Opportunity" />
              </EntityType>
              <Association Name="Opportunity_Item">
                <End Role="Opportunity"
                     Type="TestModel.Opportunity"
                     Multiplicity="1" />
                <End Role="OpportunityItem"
                     Type="TestModel.OpportunityItem"
                     Multiplicity="*" />
              </Association>
              <EntityContainer Name="DefaultContainer">
                <EntitySet
                    Name="OpportunityCollection"
                    EntityType="TestModel.Opportunity" />
                <EntitySet
                    Name="OpportunityItemCollection"
                    EntityType="TestModel.OpportunityItem" />
              </EntityContainer>
            </Schema>
          </DataServices>
        </Edmx>
        """

        _schemas, _field_metadata, stream_defs = self._discover(metadata_xml)

        child_def = stream_defs["opportunity_item_collection"]
        self.assertEqual(
            child_def["parent_stream"],
            "opportunity_collection",
        )
        self.assertEqual(
            child_def["parent_filter_field"],
            "OpportunityObjectID",
        )
        self.assertEqual(child_def["parent_key_field"], "ObjectID")
        self.assertEqual(
            child_def["relationship_name"],
            "TestModel.Opportunity_Item",
        )
        self.assertEqual(
            child_def["relationship_inference"],
            "multiplicity_heuristic",
        )

    @mock.patch.dict(
        KNOWN_PARENT_OVERRIDES,
        {
            "opportunity_item_collection": {
                "parent-stream": "opportunity_collection",
                "parent-filter-field": "OpportunityID",
                "parent-key-field": "ObjectID",
            }
        },
        clear=True,
    )
    def test_manual_parent_override_applied(self):
        metadata_xml = """
        <Edmx>
          <DataServices>
            <Schema Namespace="TestModel"
                    xmlns:sap="http://www.sap.com/Protocols/SAPData">
              <EntityType Name="Opportunity">
                <Key><PropertyRef Name="ObjectID" /></Key>
                <Property Name="ObjectID" Type="Edm.String"
                          sap:filterable="true" />
              </EntityType>
              <EntityType Name="OpportunityItem">
                <Key><PropertyRef Name="ItemID" /></Key>
                <Property Name="ItemID" Type="Edm.String"
                          sap:filterable="true" />
                <Property Name="OpportunityID" Type="Edm.String"
                          sap:filterable="true" />
              </EntityType>
              <EntityContainer Name="DefaultContainer">
                <EntitySet
                    Name="OpportunityCollection"
                    EntityType="TestModel.Opportunity" />
                <EntitySet
                    Name="OpportunityItemCollection"
                    EntityType="TestModel.OpportunityItem" />
              </EntityContainer>
            </Schema>
          </DataServices>
        </Edmx>
        """

        schemas, field_metadata, stream_defs = self._discover(metadata_xml)

        child_def = stream_defs["opportunity_item_collection"]
        self.assertEqual(
            child_def["parent_stream"],
            "opportunity_collection",
        )
        self.assertEqual(child_def["parent_filter_field"], "OpportunityID")
        self.assertEqual(child_def["parent_key_field"], "ObjectID")
        self.assertIsNone(child_def["relationship_name"])
        self.assertEqual(
            child_def["relationship_inference"],
            "manual_override",
        )
        self.assertIn(
            "__parent_opportunity_collection_ObjectID",
            schemas["opportunity_item_collection"]["properties"],
        )

        root_metadata = metadata.to_map(
            field_metadata["opportunity_item_collection"]
        ).get((), {})
        self.assertEqual(
            root_metadata["parent-tap-stream-id"],
            "opportunity_collection",
        )

    def test_entity_last_changed_on_used_as_replication_key(self):
        """EntityLastChangedOn is picked when no primary candidate exists.

        Simulates a has-entity (e.g. CorporateAccountHasContactPerson)
        that carries EntityLastChangedOn but none of the primary
        candidates (ChangedOn, ChangeDateTime, …).
        """
        metadata_xml = """
        <Edmx>
          <DataServices>
            <Schema Namespace="TestModel"
                    xmlns:sap="http://www.sap.com/Protocols/SAPData">
              <EntityType Name="AccountHasContact">
                <Key><PropertyRef Name="ObjectID" /></Key>
                <Property Name="ObjectID" Type="Edm.String"
                          sap:filterable="true" />
                <Property Name="AccountID" Type="Edm.String"
                          sap:filterable="true" />
                <Property Name="EntityLastChangedOn"
                          Type="Edm.DateTime"
                          sap:filterable="true" />
              </EntityType>
              <EntityContainer Name="DefaultContainer">
                <EntitySet
                    Name="AccountHasContactCollection"
                    EntityType="TestModel.AccountHasContact" />
              </EntityContainer>
            </Schema>
          </DataServices>
        </Edmx>
        """
        _schemas, _fmeta, stream_defs = self._discover(metadata_xml)
        stream = stream_defs["account_has_contact_collection"]
        self.assertEqual(stream["replication_method"], "INCREMENTAL")
        self.assertEqual(
            stream["replication_keys"],
            ["EntityLastChangedOn"],
        )
