# Flight SQL DDL rough plan

This document records the rough implementation plan for adding Flight SQL table
definition support to `mangrobe-db`.

## Summary

Add mangrobe-backed Flight SQL table definition support through the existing
`CommandStatementQuery` ticket flow.

- `get_flight_info_statement` stays side-effect free and only creates a ticket
  from SQL.
- `do_get_statement` parses the SQL and executes `CREATE EXTERNAL TABLE`,
  `SHOW CREATE TABLE`, or the existing SELECT path.
- `CommandGetTables` returns mangrobe table metadata.
- No tests are required for this change.

## Key changes

- Extend `CatalogPort` with data-definition operations:
  - `create_external_table(...)`
  - `get_table(...)`
  - `list_tables(...)`
- Implement those methods in `MangrobeCatalog` by calling:
  - `mangrobe.data_definition().create_external_table`
  - `mangrobe.data_definition().get_table`
  - `mangrobe.data_definition().list_tables`
- Keep `MockCatalog` compiling with equivalent in-memory behavior or minimal
  explicit support.

## Flight SQL behavior

- Keep `CommandStatementQuery` / `get_flight_info_statement` behavior
  structurally the same:
  - do not parse for side effects
  - do not call mangrobe APIs
  - return a ticket containing the SQL
- In `do_get_statement`, parse the SQL from the ticket:
  - `CREATE EXTERNAL TABLE`: call `create_external_table`, then return an empty
    successful result stream.
  - `SHOW CREATE TABLE table_name`: call `get_table`, then return detailed table
    metadata.
  - Other SQL: pass through to the current query execution path.
- Implement `get_flight_info_tables` and `do_get_tables` for
  `CommandGetTables`, using Arrow Flight SQL `GetTablesBuilder` and mangrobe
  `list_tables`.

## SQL to mangrobe mapping

- Table name is always one identifier supplied by the user.
  - Reject qualified names like `catalog.schema.table`.
  - Server fills catalog/schema internally, defaulting to `mangrobe_db.default`.
- Location:
  - Parse `s3://bucket/prefix` with the `url` crate.
  - `bucket` comes from the URL host.
  - `prefix` comes from the URL path without the leading `/`.
  - Non-`s3` locations return `InvalidArgument`.
- Options:
  - `s3.endpoint` maps to `ExternalLocation.endpoint`.
  - `s3.region` maps to `ExternalLocation.region`.
- Format:
  - Accept only `STORED AS VORTEX`.
  - Reject all other formats with a TODO-style unsupported-format error.
- Columns:
  - Map only mangrobe-supported types: bool, int64, float64, string, date, and
    supported time units.
  - Reject unsupported column types with `InvalidArgument`.
- Partitions:
  - Support DataFusion `PARTITIONED BY (col, ...)`.
  - Map each partition column to an identity `PartitionField`.
  - Reject partition columns not present in the declared schema.
- `IF NOT EXISTS` maps to `skip_if_exists`.
- Reject `OR REPLACE`, `TEMPORARY`, `UNBOUNDED`, `WITH ORDER`, constraints, and
  column defaults for this iteration.

## Result shapes

- `CREATE EXTERNAL TABLE`: empty successful Flight stream/result.
- `SHOW CREATE TABLE`: detailed mangrobe table metadata, including identifier,
  location, format, columns, nullable flags, comments, and partition fields.
- `CommandGetTables`: standard Flight SQL table listing rows.

## Assumptions

- No tests are added.
- `SHOW CREATE TABLE` returns mangrobe-specific metadata, not a literal SQL
  reconstruction string.
- This intentionally supports CREATE through `CommandStatementQuery` because the
  client sends normal SQL through the existing query path.
