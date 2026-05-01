# mangrobe-db initial reader notes

This document contains implementation details for the reader/query side of the
initial MVP.

The product-level behavior and goals remain in `rough.md`. Shared development
details live in `development.md`. Writer/import details live in `writer.md`.

## Query Flow

The server accepts SQL queries through Arrow Flight RPC:

```text
Query(sql)
```

The initial MVP must support these query shapes:

```sql
select * from dummy_table
where user = 'foo'
```

```sql
select count(distinct user) from dummy_table
where posted_at between timestamp '2026-04-30 00:00:00'
                    and timestamp '2026-04-30 23:59:59'
```

The initial SQL surface is intentionally narrow.

Allowed:

- `SELECT`.
- A single table, `dummy_table`.
- `WHERE`.
- Equality filters such as `user = 'foo'`.
- `posted_at between ...`.
- `count(distinct user)`.

Rejected:

- `JOIN`.
- `WITH` / CTE.
- Subquery.
- DDL.
- `INSERT`.
- `UPDATE`.
- `DELETE`.
- Multi-statement SQL.
- Direct references to `__mangrobe__*` columns.

Query processing:

```text
Query(sql)
  -> parse SQL
  -> reject unsupported SQL
  -> reject __mangrobe__ column references
  -> resolve dummy_table
  -> extract posted_at predicate
  -> derive __mangrobe__partition_time hour range
  -> mock GetCurrentState(dummy_table, stream_id=0)
  -> partition pruning
  -> mock GetFileInfo(candidate files, min/max stats)
  -> file pruning
  -> read selected Vortex files
  -> align each file to the unified in-memory schema
  -> fill missing columns with NULL
  -> execute with DataFusion
  -> hide internal columns
  -> return Arrow results over Flight
```

## Pruning

The initial implementation performs pruning in two stages.

First, partition pruning:

- Extract a `posted_at` range from SQL when possible.
- Convert it to the corresponding hourly
  `__mangrobe__partition_time` range.
- Use that range to reduce candidate partitions/files.

Second, file statistics pruning:

- Fetch min/max statistics from the mock mangrobe metadata.
- Use numeric and timestamp min/max statistics to skip files.

String predicates such as `user = 'foo'` are not used for file-level pruning in
the initial MVP. They are still applied by DataFusion during query execution.

## Visibility

Queries only see files whose catalog registration has succeeded. Files that are
currently being flushed, uploaded, or not yet registered are invisible to
queries.

Uploaded but unregistered objects remain ignored because visibility is defined
only by catalog registration.

## Schema Visibility

DataFusion receives a unified schema based on the in-memory table definition.

If a selected file does not contain a column from the unified schema, that
column is read as `NULL` for that file.

Internal `__mangrobe__` columns are hidden from user results.

Direct references to `__mangrobe__*` columns are rejected.

## Mangrobe Protocol Usage

The final design uses the mangrobe protocol for metadata operations.

For the initial MVP, these calls are represented by a mock:

- `GetCurrentState(table_name, stream_id=0)`
- `GetFileInfo(file_ids, included_column_statistics_types, included_file_metadata_types)`

When reading files, table-relative file paths from the catalog are resolved
against the table storage prefix to produce object-storage keys.

Compaction-related methods and lock-related methods are out of scope for the
initial MVP.

## Open Details

These details are intentionally not fixed yet:

- Exact Arrow Flight action or RPC encoding for `Query(sql)`.
- Exact DataFusion table provider design.
- Exact SQL validation implementation.
- Exact Vortex read API signatures.
- Exact mock catalog read/query data structures.

## Known Decisions

- Query pruning uses catalog statistics, not Vortex file metadata.
- Query visibility is based on successful `AddFiles` registration.
- Files being flushed, uploaded, or not yet registered are invisible to queries.
- Query results are returned as Arrow over Flight.
- Query execution uses DataFusion.
- Query reads selected Vortex files.
- Join, CTE, subquery, DDL, and SQL insert are out of scope.
