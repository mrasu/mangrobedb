# mangrobe-db rough specification

This document summarizes the initial rough specification for `mangrobe-db`.
It intentionally stays at the framework, component-boundary, and internal
behavior level, before choosing detailed Rust APIs or implementation internals.

This document should be sufficient context for continuing the specification
discussion later, even without the original conversation history. If work is
resumed with a request such as "continue from rough.md", start from the
"Continuation Notes" section at the end of this document.

## Goal

`mangrobe-db` is a schema-less OLAP database for AI or streaming workloads.

The initial MVP uses:

- Apache DataFusion as the query engine.
- Apache Arrow as the internal data representation.
- Arrow data when passing data to DataFusion.
- Arrow data when returning query results.
- Arrow Flight RPC as the query/import server protocol.
- Vortex files as the persistent data file format.
- S3-compatible object storage as the file storage backend.
- The [mangrobe protocol](mangrobe-api.proto) as the metadata/catalog protocol.

For the initial implementation, the mangrobe protocol is not implemented in
this repository. `mangrobe-db` calls it as an external service in the final
design, but uses a mock replacement for now.

The MVP is considered complete when Arrow Flight import can accept Arrow
batches, the five-second background buffer can flush those rows into Vortex
files, the mock catalog can register those files, and Arrow Flight query can
return DataFusion query results from the registered files.

## Initial Scope

In scope:

- `Query(sql)` over Arrow Flight RPC.
- `Import(arrow)` over Arrow Flight RPC.
- DataFusion execution over selected Vortex files.
- Five-second import buffering before writing files.
- File registration through a mock mangrobe protocol client.
- Predicate and partition pruning for the supported query shapes.
- A single mock table, `dummy_table`.

Out of scope for the initial MVP:

- OTel ingest.
- Compaction.
- Real mangrobe protocol server implementation.
- Real schema evolution through mangrobe protocol.
- SQL DDL.
- SQL `INSERT`.
- `JOIN`.
- CTE.
- Subquery.
- String min/max statistics.
- Non-zero `stream_id`.
- Reconstructing catalog or schema state by scanning existing object-store
  files.
- Cross-process concurrent access to the same mock catalog or schema
  persistence files.

## Initial Table

The initial user-visible table is `dummy_table`.

The table has user-visible event data columns and internal columns used for
physical partitioning. Detailed table and schema behavior is specified in
`development.md`.

## Schema Handling

The initial system is schema-less at import time for new user columns, while
still rejecting incompatible existing columns. Detailed schema behavior is
specified in `development.md`, with writer-specific and reader-specific effects
in `writer.md` and `reader.md`.

## Stream ID

The initial MVP only supports stream ID `0`. Detailed stream mapping and import
validation behavior is specified in `development.md` and `writer.md`.

## Partition Time

The initial MVP partitions data by the hour derived from event time. Detailed
partition mapping behavior is specified in `development.md`.

## Object Storage Path

Data is stored in S3-compatible object storage as Vortex files. Catalog metadata
uses table-relative file paths, while object storage uses table-specific storage
prefixes. Detailed path and immutability behavior is specified in
`development.md` and `writer.md`.

## Import

The server accepts Arrow data imports through Arrow Flight RPC. Imported rows
are buffered briefly, flushed to Vortex files, uploaded to object storage, and
registered in the mock catalog. Detailed writer/import behavior is specified in
`writer.md`.

## File Write and Statistics

Imported rows are eventually written as Vortex files. The writer computes
numeric and timestamp min/max statistics for pruning. String min/max statistics
are out of scope for the initial MVP.

Detailed writer behavior is specified in `writer.md`.

## Query

The server accepts SQL queries through Arrow Flight RPC.

The initial MVP must support at least these query shapes:

```sql
select * from dummy_table
where user = 'foo'
```

```sql
select count(distinct user) from dummy_table
where posted_at between timestamp '2026-04-30 00:00:00'
                    and timestamp '2026-04-30 23:59:59'
```

The initial SQL surface is narrower than full SQL, but DataFusion-supported
projections, filter expressions, and aggregate functions over `dummy_table` are
allowed unless they use an explicitly rejected SQL feature.

Detailed supported and rejected SQL behavior is specified in `reader.md`.

## Pruning

The initial implementation performs partition pruning and file statistics
pruning when supported predicates can be extracted. String predicates such as
`user = 'foo'` are not used for mangrobe catalog statistics pruning in the
initial MVP. They are still applied by DataFusion or pushed down by
`vortex-datafusion` when supported.

Detailed pruning behavior is specified in `reader.md`.

## Mangrobe Protocol Usage

The final design uses the mangrobe protocol for metadata operations.

For the initial MVP, these calls are represented by a mock:

- Current-state lookup.
- File-info lookup.
- File registration.

Compaction-related methods and lock-related methods are out of scope for the
initial MVP.

Catalog registration is the visibility and commit point for imported files.
Uploading a Vortex file to object storage does not make it visible to queries.
A file becomes visible only after `AddFiles(...)` succeeds.

Mock schema and catalog state are persisted so the server can resume from the
previous mock state. If the mock persistence file does not exist, the server
starts with the initial `dummy_table` schema and an empty catalog. The
development details for this mock persistence live in `development.md`.

Existing object-store files are not scanned at startup to reconstruct the mock
catalog or schema. Uploaded but unregistered objects remain ignored because
visibility is defined only by catalog registration.

Detailed mock catalog persistence, writer-side failure behavior, and reader-side
visibility behavior are specified in `development.md`, `writer.md`, and
`reader.md`.

## Implementation Notes

Implementation and development details are split into separate documents:

- `development.md` for shared development setup, mock persistence, component
  boundaries, and server lifecycle notes.
- `writer.md` for import, buffering, flushing, Vortex writing, statistics, and
  object-store write behavior.
- `reader.md` for query execution, pruning, Vortex reading, and query
  visibility behavior.

## Continuation Notes

Implementation and development details are intentionally split out of this
document.

If work is resumed with no conversation context, open `rough.md`,
`development.md`, `writer.md`, and `reader.md`.

`development.md`, `writer.md`, and `reader.md` have enough initial decisions
for now. Treat them as temporarily complete unless the user explicitly wants to
revisit those topics. The next natural step is implementation planning or
implementation work.

Use these files as the continuation context:

- `rough.md`: product goal, user-visible behavior, and high-level MVP scope.
- `development.md`: shared development setup, mock persistence, component
  boundaries, and server lifecycle notes.
- `writer.md`: writer/import/flush/Vortex-write details.
- `reader.md`: reader/query/pruning/Vortex-read details.

When continuing the specification or starting implementation, do not silently
choose implementation details. Ask for explicit decisions before fixing details
such as public structs and traits, public functions, error types, test cases,
exact Rust signatures, mock catalog storage model, object storage API
signatures, or helper names for the DataFusion/Vortex handoff.

However, do not reopen already-decided `development.md` topics unless needed.
Shared development decisions already made include domain value construction,
mock persistence, application ports, error model, and initial test direction.

Known decisions so far:

- The MVP is complete when Flight import, five-second flush, Vortex write, mock
  catalog registration, DataFusion query execution, and Flight query results
  work together.
- Initial table is `dummy_table`.
- `posted_at` is event time.
- The initial MVP only supports stream ID `0`.
- Data files are partitioned by an internal stream ID and an internal
  event-time hour.
- Internal columns are hidden from users.
- Import buffers flush after 5 seconds.
- Catalog file paths are table-relative and do not include the table storage
  prefix.
- Existing object-store files are not scanned to reconstruct catalog or schema
  state at startup.
- Query pruning uses catalog statistics, not Vortex file metadata.
- Catalog registration is the visibility/commit point.
- Actual Vortex data files are immutable once written and must not be
  overwritten.
- Multiple imports, multiple queries, and background flushes may run
  concurrently within one server process.
- Query results are returned as Arrow over Flight.
- Query uses Arrow Flight `DoGet` directly in the initial MVP.
- Query execution uses DataFusion.
- Query uses a mangrobe-owned, catalog-aware DataFusion table provider for
  `dummy_table`.
- The mangrobe table provider uses DataFusion scan filters for catalog-aware
  file selection.
- Selected Vortex file scanning is delegated to `vortex-datafusion`; the reader
  side does not define a separate `VortexReader` abstraction.
- `GetCurrentState` is partition-aware and can be limited by requested
  partition times.
- SQL validation is based on DataFusion logical plan inspection.
- DataFusion-supported projections, filters, and aggregate functions are
  allowed when they reference only `dummy_table` user-visible columns and avoid
  explicitly rejected SQL features.
- Join, CTE, subquery, DDL, and SQL insert are out of scope.
- OTel ingest and compaction are out of scope.
- `development.md` is temporarily complete as the shared development
  specification.
- `writer.md` is temporarily complete as the writer/import specification.
- Import uses Arrow Flight `DoPut` with descriptor path `["import",
  table_name]`.
- Import synchronously validates and updates mock schema/catalog state before
  buffering rows.
- Import success means rows were accepted into the in-memory import buffer, not
  that they are query-visible.
- Imported rows become query-visible only after asynchronous Vortex write,
  object storage upload, and `AddFiles` registration succeed.
- One import request may contain rows from multiple partition hours. Buffering
  and flushing split those rows by flush unit.
- One background flush tick registers its produced files with one `AddFiles`
  request when possible. Files in that request become visible together.
- Object storage upload accepts the table definition, table-relative file path,
  and local temporary file path. The object storage implementation resolves the
  full object key internally.
- Mock catalog file registration uses table-relative paths.
- Initial mock catalog file registration includes path, size, and column
  statistics, but not row count.
- Mock schema and catalog persistence uses one directly overwritten JSON file:
  `./data/mock/state.json`.
- Domain value objects use `TryFrom`; the domain layer defines `DomainError`.
- Application services and infrastructure ports return `anyhow::Result` for the
  initial MVP.
- Application ports use Rust `async fn in trait`; `async_trait` is not used for
  now.
- Initial tests should start with domain value objects, table schema, table
  mapping, import service validation, and buffer/flush units.

Open or intentionally undecided details:

- Exact Rust signatures for application ports.
- Exact import error model.
- Exact helper structs or function names for the reader-side
  object-store/Vortex/DataFusion file handoff.
