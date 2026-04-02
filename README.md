# tap-sap-sales-service-cloud

A [Singer](https://singer.io/) tap that extracts data from the **SAP Sales and Service Cloud** OData v2 API and emits Singer `SCHEMA`, `RECORD`,
and `STATE` messages.

---

## Overview

`tap-sap-sales-service-cloud` is a **dynamic** tap — it discovers every entity set exposed
by your tenant via the OData `$metadata` document and builds the catalog/schema at runtime.

Entity sets commonly available include `AccountCollection`, `OpportunityCollection`,
`ContactCollection`, `LeadCollection`, `ServiceRequestCollection`, `TicketCollection`, and
many more (see the
[SAP C4C OData v2 Reference](https://help.sap.com/doc/d0f9ba822c08405da7d88174b304df84/CLOUD/en-US/index.html)).

---

## Stream Blueprint (Dynamic)

For each discovered entity set:

| Property            | Value                                                                   |
|---------------------|-------------------------------------------------------------------------|
| Endpoint            | `/sap/c4c/odata/v1/c4codataapi/<EntitySet>`                             |
| Primary key(s)      | Inferred from `$metadata` entity key definitions                        |
| Replication key     | `ChangedOn` / `ChangeDateTime` / `LastModifiedOn` / `EntityLastChangedOn` / `LastUpdatedOn` when present & filterable |
| Replication method  | `INCREMENTAL` when replication key found, else `FULL_TABLE`             |
| Pagination          | OData `d.__next` traversal                                              |

---

## Authentication

SAP Sales and Service Cloud supports two authentication modes.

### Option 1 — HTTP Basic (recommended for development)

```json
{
  "api_server": "https://myXXXXXX.crm.ondemand.com",
  "username": "your_user@tenant",
  "password": "your_password",
  "start_date": "2024-01-01T00:00:00Z"
}
```

### Option 2 — OAuth 2.0 SAML Bearer

```json
{
  "api_server": "https://myXXXXXX.crm.ondemand.com",
  "client_id": "your_oauth_client_id",
  "assertion": "base64_encoded_saml_assertion",
  "start_date": "2024-01-01T00:00:00Z"
}
```

See [SAP Authentication docs](https://help.sap.com/docs/sap-cloud-for-customer/odata-services/sap-cloud-for-customer-odata-api#authentication)
for full details.

---

## Quick Start

```bash
python3 -m venv /usr/local/share/virtualenvs/tap-sap-sales-service-cloud
source /usr/local/share/virtualenvs/tap-sap-sales-service-cloud/bin/activate
cd /opt/code/tap-sap-sales-service-cloud
pip install -U pip
pip install -r requirements.txt
pip install -e .
```

**Discover:**

```bash
tap-sap-sales-service-cloud --config /tmp/tap_config.json --discover > /tmp/catalog.json
```

**Sync:**

```bash
tap-sap-sales-service-cloud --config /tmp/tap_config.json \
    --catalog /tmp/catalog.json > /tmp/sync_output.json 2>/tmp/sync_errors.log
tail -1 /tmp/sync_output.json > /tmp/state.json
```

**Sync with state:**

```bash
tap-sap-sales-service-cloud --config /tmp/tap_config.json \
    --catalog /tmp/catalog.json --state /tmp/state.json \
    > /tmp/sync_output_2.json 2>/tmp/sync_errors_2.log
```

---

## Bookmarking

Incremental streams store bookmarks using each stream's replication key:

```json
{
  "bookmarks": {
    "account_collection": {
      "ChangedOn": "2024-10-10T00:00:00.000000Z"
    }
  }
}
```

---

## Rate Limiting

The tap retries HTTP `429` and `5xx` responses using exponential back-off and the
`Retry-After` header when available.

---

## Pagination

Pagination uses OData v2 next-link (`d.__next`) traversal until exhaustion.

---

## Development

**Run tests:**

```bash
pytest tests/unittests --verbose --cov=tap_sap_sales_service_cloud --cov-report=term-missing
```

**Run lint:**

```bash
pylint tap_sap_sales_service_cloud
flake8 tap_sap_sales_service_cloud --max-line-length=120
```

---

## Supported Entities (Sample)

The tap dynamically discovers all entities from `$metadata`. Common ones include:

- `AccountCollection` — Accounts / Business Partners
- `OpportunityCollection` — Sales Opportunities
- `ContactCollection` — Contacts
- `LeadCollection` — Leads
- `ServiceRequestCollection` — Service Requests
- `TicketCollection` — Customer Tickets
- `ActivityCollection` — Activities
- `AppointmentCollection` — Appointments
- `TaskCollection` — Tasks
- `CampaignCollection` — Marketing Campaigns
- `QuoteCollection` — Sales Quotes
- `SalesOrderCollection` — Sales Orders
- `ContractCollection` — Contracts

---

## References

- [SAP C4C OData API Overview](https://help.sap.com/docs/sap-cloud-for-customer/odata-services/sap-cloud-for-customer-odata-api)
- [SAP C4C OData V2 Entity Reference](https://help.sap.com/doc/d0f9ba822c08405da7d88174b304df84/CLOUD/en-US/index.html)
- [Singer Specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
