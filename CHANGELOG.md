# Changelog

## 1.0.2
- Prune OData `$select` to fields Singer will actually emit, cutting network/parse overhead on wide entities. Builds on the parent-child batching in #9.[#10](https://github.com/singer-io/tap-sap-sales-service-cloud/pull/10)

## 1.0.1
- Remove `replication-method` from the root-metadata.[#8](https://github.com/singer-io/tap-sap-sales-service-cloud/pull/8)

## 1.0.0
- Updated child streams from Full Table to Incremental replication when the parent uses Incremental replication.[#7](https://github.com/singer-io/tap-sap-sales-service-cloud/pull/7)

## 0.0.3
- Hardened SSRF protections for OData paths and credentialed request redirects.[#6](https://github.com/singer-io/tap-sap-sales-service-cloud/pull/6)

## 0.0.2
- Add support for `null` values in replication key fields. [#4](https://github.com/singer-io/tap-sap-sales-service-cloud/pull/4)

## 0.0.1
- Initial production-oriented implementation of `tap-sap-sales-service-cloud`.
