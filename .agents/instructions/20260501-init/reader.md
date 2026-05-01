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

The initial Query RPC uses Arrow Flight `DoGet` directly.

```text
Ticket contains a query request with:
  sql
```

The MVP does not require `GetFlightInfo` or `PollFlightInfo` for query
execution. Queries are expected to be short enough that the server can execute
the query while handling `DoGet` and stream the Arrow result batches directly
from that call.

The `Ticket` is a query request for the initial MVP, not a server-issued opaque
result handle. A future version may add `GetFlightInfo` or `PollFlightInfo` and
change `Ticket` into an opaque handle for a planned or running query.

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

The initial SQL surface is intentionally narrower than full SQL, but it should
not reject DataFusion-supported projections, filter expressions, or aggregate
functions merely because they are not needed for mangrobe-specific pruning.

Allowed:

- A single `SELECT` statement that DataFusion can plan.
- A single table, `dummy_table`.
- DataFusion-supported projections over `dummy_table`.
- DataFusion-supported `WHERE` expressions over `dummy_table`.
- DataFusion-supported aggregate functions over `dummy_table`.

Rejected:

- Non-`SELECT` statements.
- `JOIN`.
- `WITH` / CTE.
- Subquery.
- DDL.
- `INSERT`.
- `UPDATE`.
- `DELETE`.
- Multi-statement SQL.
- Tables other than `dummy_table`.
- Direct references to `__mangrobe__*` columns.

Query processing:

```text
Query(sql)
  -> create DataFusion logical plan
  -> reject unsupported SQL from the DataFusion plan
  -> reject __mangrobe__ column references
  -> resolve dummy_table through the mangrobe table provider
  -> mangrobe table provider receives DataFusion scan filters
  -> extract posted_at predicate from DataFusion expressions
  -> derive __mangrobe__partition_time hour range
  -> mock GetCurrentState(dummy_table, stream_id=0, partition_times)
  -> mock GetFileInfo(candidate files, min/max stats)
  -> file pruning
  -> hand selected Vortex files to vortex-datafusion
  -> vortex-datafusion reads selected Vortex files during execution
  -> hide internal columns
  -> return Arrow results over Flight
```

## Pruning

The initial implementation performs pruning in two stages.

First, partition pruning:

- Extract a `posted_at` range from DataFusion filter expressions when possible.
- Convert it to the corresponding hourly
  `__mangrobe__partition_time` range.
- Pass the derived partition times to `GetCurrentState` so the mock catalog
  returns only matching partitions when a partition range is available.
- If no partition range can be derived, call `GetCurrentState` without
  `partition_times` and scan all visible partitions for the stream.

Second, file statistics pruning:

- Fetch min/max statistics from the mock mangrobe metadata.
- Use numeric and timestamp min/max statistics to skip files.

Filter expressions that cannot be used for mangrobe catalog pruning are still
valid when DataFusion can plan and execute them. They are applied by DataFusion
or pushed down by `vortex-datafusion` when supported.

String predicates such as `user = 'foo'` are not used by mangrobe catalog
statistics pruning in the initial MVP.

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

## DataFusion Integration

The reader integrates with DataFusion through a mangrobe-owned table provider
for `dummy_table`.

The mangrobe table provider is catalog-aware. Its `scan` operation receives the
projection, filters, and limit information that DataFusion derived from the SQL
query. It uses those DataFusion expressions for catalog-aware file selection.

The mangrobe table provider is responsible for:

- exposing the unified in-memory table schema to DataFusion;
- rejecting direct references to internal `__mangrobe__*` columns;
- extracting `posted_at` predicates from DataFusion filter expressions when
  possible;
- deriving candidate `__mangrobe__partition_time` hours;
- calling the mock catalog for current state and file information;
- applying partition pruning and catalog-statistics pruning;
- resolving selected table-relative catalog paths against the table storage
  prefix;
- constructing a DataFusion file scan over the selected Vortex files.

The mangrobe table provider must not implement its own Vortex row scanner.
After catalog-aware file selection, selected files are handed to the
`vortex-datafusion` integration.

`vortex-datafusion` owns:

- reading selected Vortex files;
- projection pushdown into Vortex scans;
- any Vortex-supported filter pushdown;
- producing Arrow batches for DataFusion execution.

The mangrobe table provider may use DataFusion file-scan facilities such as a
Vortex `FileFormat`/`FileSource`, rather than implementing a full custom
physical scanner.

The table provider's `scan` operation may perform mock catalog metadata lookup
and pruning. It must not read Vortex file bodies during planning; file body I/O
belongs to the execution plan produced by DataFusion and `vortex-datafusion`.

The reader side does not define a separate `VortexReader` abstraction for
returning Arrow `RecordBatch` values. Vortex file body reads are delegated to
`vortex-datafusion`.

The mangrobe table provider's `scan` implementation converts selected catalog
files into the DataFusion file-scan inputs required by `vortex-datafusion`. This
conversion includes resolving table-relative catalog paths against the table
storage prefix to produce object-storage keys. The exact helper structs or
function names for this conversion are implementation details.

## SQL Validation

SQL validation is performed after DataFusion parses and plans the SQL. The
initial implementation should inspect the DataFusion logical plan and
expressions rather than implementing a separate SQL parser for mangrobe-specific
validation.

The validator should reject only SQL features that affect mangrobe's table
resolution, catalog visibility, or initial MVP scope:

- multiple statements;
- non-`SELECT` statements;
- tables other than `dummy_table`;
- joins;
- CTEs;
- subqueries;
- direct references to internal `__mangrobe__*` columns.

The validator should allow DataFusion-supported projections, filter
expressions, and aggregate functions over `dummy_table`. Unsupported functions,
operators, casts, or type combinations may fail during DataFusion planning or
execution and do not need separate mangrobe-specific rejection rules unless
they reference hidden internal columns or another table.

## Mangrobe Protocol Usage

The final design uses the mangrobe protocol for metadata operations.

For the initial MVP, these calls are represented by a mock:

- `GetCurrentState(table_name, stream_id=0, partition_times)`
- `GetFileInfo(file_ids, included_column_statistics_types, included_file_metadata_types)`

`GetCurrentStateRequest.partition_times` is optional by convention. If the
repeated field is empty, the mock returns all visible partitions for the stream.
If it contains values, the mock returns only those partition times.

`GetCurrentStateResponse` groups visible files by partition. The reader must not
derive partition membership by parsing file paths.

When reading files, table-relative file paths from the catalog are resolved
against the table storage prefix to produce object-storage keys.

Compaction-related methods and lock-related methods are out of scope for the
initial MVP.

## Open Details

These details are intentionally not fixed yet:

- Exact Rust internal data structures for the mock catalog read model and
  persistence.

## Known Decisions

- Query pruning uses catalog statistics, not Vortex file metadata.
- `GetCurrentState` is partition-aware and can be limited by requested
  partition times.
- The reader uses partition information from `GetCurrentStateResponse`, not
  file path parsing.
- Timestamp min/max statistics may be used for pruning when their stored
  statistic values can be compared with the query predicate values.
- Query visibility is based on successful `AddFiles` registration.
- Files being flushed, uploaded, or not yet registered are invisible to queries.
- Query results are returned as Arrow over Flight.
- Query uses Arrow Flight `DoGet` directly in the initial MVP.
- Query execution uses DataFusion.
- Query uses a mangrobe-owned, catalog-aware DataFusion table provider for
  `dummy_table`.
- Query reads selected Vortex files.
- Selected Vortex file scanning is delegated to `vortex-datafusion`.
- The reader side does not define a separate `VortexReader` abstraction.
- SQL validation is based on DataFusion logical plan inspection.
- DataFusion-supported projections, filters, and aggregate functions are
  allowed when they reference only `dummy_table` user-visible columns.
- Join, CTE, subquery, DDL, and SQL insert are out of scope.
