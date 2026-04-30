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
- The mangrobe protocol as the metadata/catalog protocol.

For the initial implementation, the mangrobe protocol is not implemented in
this repository. `mangrobe-db` calls it as an external service in the final
design, but uses a mock replacement for now.

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

## Initial Table

The initial user-visible table is:

```sql
create table dummy_table (
  id int,
  stream_id int,
  message text,
  user text,
  posted_at timestamp
)
```

Internally, the system adds:

```text
__mangrobe__stream_id int
__mangrobe__partition_time timestamp
```

These internal columns are not visible to users.

- They are not returned by `select *`.
- SQL must not be allowed to reference them.
- Any user-provided column whose name starts with `__mangrobe__` is rejected.
- If a user provides a column named `partition_time`, it is treated as an
  ordinary user column, not as the internal partition column.
- The user-visible `stream_id` column is accepted, but partitioning uses
  `__mangrobe__stream_id`.

## Schema Handling

The initial implementation keeps the current table definition in memory.

Files may have different schemas.

Import behavior:

- Unknown user columns are added to the in-memory table definition.
- Known columns with the same type are accepted.
- Known columns with a different type cause import failure.
- Columns with the `__mangrobe__` prefix cause import failure.

Query behavior:

- DataFusion receives a unified schema based on the in-memory table definition.
- If a selected file does not contain a column from the unified schema, that
  column is read as `NULL` for that file.
- Internal `__mangrobe__` columns are hidden from user results.

Future schema evolution is expected to use a mangrobe protocol method such as
`ModifyColumn(column_name, type)`. That future method should add unknown columns
and reject incompatible existing column types. Type widening, such as `int32` to
`int64`, may be allowed in the future, but is not part of the initial MVP.

## Stream ID

`stream_id` represents a partition-like concept in the protocol, but the initial
implementation only supports:

```text
stream_id = 0
```

The input Arrow batch is expected to contain a user-visible `stream_id` column.
For the initial MVP:

- The `stream_id` column is stored as a normal user-visible column.
- Every imported row must have `stream_id = 0`.
- If any row has a non-zero `stream_id`, import fails.
- The internal `__mangrobe__stream_id` column is set from the user-visible
  `stream_id` value.
- The stream ID passed to the mangrobe protocol mock is
  `__mangrobe__stream_id`, which is currently always `0`.

The internal stream column exists so the physical partitioning value can diverge
from the user-visible `stream_id` in the future. For example, a future version
may derive `__mangrobe__stream_id` from something like `hash(partition_name)`.

## Partition Time

The internal partition time is derived from `posted_at`.

```text
__mangrobe__partition_time = hour(posted_at)
```

`posted_at` is event time.

The mangrobe protocol `partition_time` fields use
`__mangrobe__partition_time`, not any user-visible `partition_time` column.

## Object Storage Path

Each table definition has a storage prefix. For the initial table:

```text
mangrobe-db/dummy_table
```

The table storage prefix is kept in memory together with the table definition.
It is not part of the file path registered in the catalog.

Files use this table-relative path shape:

```text
stream_id={__mangrobe__stream_id}/partition_time=YYYYMMDD_HHMMSS/{file_id}.vortex
```

Example:

```text
stream_id=0/partition_time=20260430_100000/{file_id}.vortex
```

The actual object storage key is produced by joining the table storage prefix
and the table-relative file path.

```text
mangrobe-db/dummy_table/stream_id=0/partition_time=20260430_100000/{file_id}.vortex
```

The catalog registers the table-relative file path, not the full object storage
key. The table storage prefix is resolved from the table definition when reading
or writing object storage.

Because partitioning is hourly, minutes and seconds are expected to be `0000`.

## Import

The server accepts imports through Arrow Flight RPC:

```text
Import(table_name, Arrow batches)
```

The import may contain any number of Arrow RecordBatches. All RecordBatches in
one import request must have the same schema. If schemas differ within one
import request, import fails.

Required validation:

- `table_name` must be `dummy_table`.
- `posted_at` must exist.
- `stream_id` must exist.
- All `stream_id` values must be `0`.
- No user column may start with `__mangrobe__`.
- Known columns must have compatible types.

Import RPC processing:

```text
Import
  -> validate batches
  -> add internal __mangrobe__stream_id = stream_id
  -> add internal __mangrobe__partition_time = hour(posted_at)
  -> buffer by (table_name, __mangrobe__stream_id=0, __mangrobe__partition_time)
  -> return after buffering succeeds
```

Flush processing is asynchronous and is owned by the background flusher:

```text
Background flusher
  -> take buffered rows after 5 seconds
  -> split rows by flush unit
  -> write Vortex files to temporary local files and compute min/max statistics
  -> upload files to object storage
  -> register files through mock AddFiles
```

The flush unit is:

```text
(table_name, __mangrobe__stream_id=0, __mangrobe__partition_time hour)
```

If a five-second buffer contains rows from multiple hour partitions, those rows
are written into separate files.

## File Write and Statistics

Vortex files are written to temporary local files, not held as in-memory byte
buffers. The initial Rust implementation should use `NamedTempFile` for the
Vortex write result.

The Vortex writer computes min/max statistics from Arrow RecordBatches at write
time.

Statistics are collected for:

- Numeric columns.
- Timestamp columns.

Statistics are not collected for:

- String columns.

Statistics are written into the Vortex file and also registered in the mock
mangrobe protocol metadata.

The system does not use Vortex file metadata as the normal source of statistics
for query planning. Query pruning uses statistics stored in the catalog. Vortex
file metadata is file-local metadata. A future repair or recovery job may read
statistics from Vortex files to reconstruct catalog metadata, but normal query
planning should not open Vortex files just to fetch pruning statistics.

The Vortex write operation returns both:

- The temporary Vortex file.
- The statistics computed while writing the file.

The flush service passes those returned statistics to catalog registration.

## Query

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

## Mangrobe Protocol Usage

The final design uses the mangrobe protocol for metadata operations.

For the initial MVP, these calls are represented by a mock:

- `GetCurrentState(table_name, stream_id=0)`
- `GetFileInfo(file_ids, included_column_statistics_types, included_file_metadata_types)`
- `AddFiles(...)`

When registering files, protocol `partition_time` values are derived from the
internal `__mangrobe__partition_time` column.

Compaction-related methods and lock-related methods are out of scope for the
initial MVP.

Catalog registration is the visibility and commit point for imported files.
Uploading a Vortex file to object storage does not make it visible to queries.
A file becomes visible only after `AddFiles(...)` succeeds.

Failure behavior:

- If Vortex writing fails, no object is uploaded and no catalog entry is added.
- If object storage upload fails, no catalog entry is added.
- If object storage upload succeeds but catalog registration fails, the uploaded
  object becomes an orphan object.
- Orphan objects are ignored by queries because they are not registered in the
  catalog.

A cleanup job is required in the full design to remove orphan objects, but that
cleanup job is out of scope for the initial implementation.

## Component Boundaries

The initial implementation should use a DDD-oriented directory structure.

```text
src/
  domain/
    mod.rs
    table.rs
    schema.rs
    column.rs
    table_mapping.rs
    partition.rs
    file.rs
    statistics.rs
    error.rs

  application/
    mod.rs
    import_service.rs
    query_service.rs
    flush_service.rs
    buffer.rs
    ports.rs
    datafusion/
      mod.rs
      sql_validator.rs
      predicate.rs
      table_provider.rs
      execution.rs

  infrastructure/
    mod.rs
    catalog/
      mod.rs
      mock.rs
    object_store/
      mod.rs
      s3.rs
      memory.rs
    vortex/
      mod.rs
      reader.rs
      writer.rs
    clock.rs
    file_id.rs

  server/
    mod.rs
    flight/
      mod.rs
      server.rs
      import.rs
      query.rs
    background/
      mod.rs
      flusher.rs

  main.rs
```

No `server/otel` module should be created for the initial implementation. OTel
ingest is out of scope.

### Domain Layer

The domain layer may depend on Apache Arrow types. Arrow is the internal data
representation of `mangrobe-db`.

The domain layer owns stable database concepts and rules, including:

- Table definition.
- Table schema.
- Column names and column visibility.
- Internal column prefix restrictions.
- Table mapping.
- Partition time and stream ID domain values.
- File ID and table-relative file path.
- Object storage prefix and object key value types.
- File statistics domain values.

Temporary MVP-only restrictions should not be modeled as permanent domain
rules. For example, `stream_id = 0` is an initial implementation restriction and
belongs in `application/import_service.rs`, not in the domain mapping rule.

The table definition shape is:

```text
TableDefinition
  name: TableName
  schema: TableSchema
  storage_prefix: ObjectStoragePrefix
  mapping: TableMapping
```

`TableDefinition.schema` includes both user-visible columns and internal
columns. Internal columns are hidden from mangrobe-db users, but the catalog
sees them as ordinary registered columns. Therefore each column definition must
record whether the column is user-visible or internal.

```text
TableSchema
  columns: Vec<ColumnDefinition>

ColumnDefinition
  name: ColumnName
  data_type: Arrow DataType
  kind: ColumnKind

ColumnKind
  User
  Internal
```

The initial `dummy_table` schema includes:

```text
id: int                                  User
stream_id: int                           User
message: text                            User
user: text                               User
posted_at: timestamp                     User
__mangrobe__stream_id: int               Internal
__mangrobe__partition_time: timestamp    Internal
```

Unknown imported user columns are added to `TableSchema` as `ColumnKind::User`.
The internal columns remain fixed for the initial implementation.

Domain values should be represented with explicit domain types where practical,
rather than raw `String` or primitive values. Examples:

```text
TableName
ColumnName
FileId
FilePath
ObjectStoragePrefix
ObjectKey
StreamId
PartitionTime
RowCount
```

`domain/file.rs` owns file naming and path construction. It builds the
table-relative file path:

```text
FilePath::for_partition(stream_id, partition_time, file_id)
  -> stream_id={stream_id}/partition_time=YYYYMMDD_HHMMSS/{file_id}.vortex
```

`ObjectStoragePrefix::join(FilePath)` produces the actual `ObjectKey` used by
object storage.

`domain/table_mapping.rs` owns the fixed initial mapping from user columns to
internal columns:

```text
stream_id -> __mangrobe__stream_id
posted_at -> __mangrobe__partition_time
```

The mapping derives internal columns from imported Arrow RecordBatches and
checks that the source columns required by the mapping exist. The `stream_id =
0` check is not part of table mapping; it belongs to import application logic.

### Application Layer

The application layer owns use-case flow.

`application/import_service.rs` owns import-specific processing, including:

- Initial table restriction to `dummy_table`.
- Import request batch schema consistency.
- Initial `stream_id = 0` restriction.
- Calling domain table mapping to derive internal columns.
- Adding rows to the import buffer.

`application/buffer.rs` owns the import buffer. The buffer groups rows by:

```text
(table_name, __mangrobe__stream_id, __mangrobe__partition_time hour)
```

`application/flush_service.rs` owns the actual flush use case:

```text
FlushService
  -> take flush units from the import buffer
  -> generate a FileId
  -> build a table-relative FilePath from domain/file.rs
  -> join TableDefinition.storage_prefix and FilePath into ObjectKey
  -> write a Vortex temporary file and compute statistics
  -> upload the temporary file to object storage
  -> register the table-relative FilePath and statistics in the catalog
```

`application/query_service.rs` owns query flow. DataFusion integration belongs
under `application/datafusion/`, not under `infrastructure/`, because DataFusion
is treated as part of the query application logic for this project.

`application/ports.rs` defines ports for external dependencies:

```text
CatalogPort
ObjectStorePort
VortexPort
ClockPort
FileIdGeneratorPort
```

There is no `QueryEnginePort` in the initial design. Query execution uses
DataFusion directly through `application/datafusion/`.

`CatalogPort` abstracts the mock mangrobe protocol client and the future real
mangrobe protocol client.

`ObjectStorePort` abstracts object storage operations. It uploads local files
using object keys produced from table definitions and file paths.

`VortexPort` abstracts Vortex file writing and reading. Vortex and object store
ports are separate because Vortex owns file format behavior and statistics
computation, while object storage owns file placement.

The Vortex write result contains:

```text
VortexWriteResult
  temp_file: NamedTempFile
  statistics: FileStatistics
  row_count: RowCount
```

`ClockPort` and `FileIdGeneratorPort` are ports so tests can mock time and file
ID generation.

### Server Layer

`server/flight` owns Arrow Flight RPC handling:

- Flight request decoding.
- Mapping `Import(table_name, Arrow batches)` to `ImportService`.
- Mapping `Query(sql)` to `QueryService`.
- Flight response encoding.

The background flusher is not owned by the Flight server.

`server/background/flusher.rs` owns the background flusher loop:

- Start the flusher when the server process starts.
- Run the periodic five-second interval.
- Handle shutdown.
- Call `FlushService`.

The background flusher is an internal server background task, not an
independently runnable worker process. It is closely tied to import buffering
and the server lifecycle. It should run when the server process accepts Flight
imports, and it should also run when future OTel ingest is enabled.

The process startup shape is:

```text
fn main() {
  start_flusher(...)
  start_flight(...)
}
```

The exact Rust async/threading API is not decided yet.

## Continuation Notes

If work is resumed with only this document as context, assume the component
boundaries described above are accepted decisions unless the user explicitly
reopens them.

The next planned topic is the detailed API shape for the components and ports
described above. Start by asking the user which API surface to specify first.
A reasonable order is:

1. Domain value types and table/schema APIs.
2. `application/ports.rs` trait signatures.
3. `application/import_service.rs`, `application/flush_service.rs`, and
   `application/query_service.rs` input/output types.
4. Mock catalog storage model.
5. DataFusion table provider and SQL validation details.
6. Arrow Flight RPC method mapping.
7. Tests to write first.

When continuing the specification, do not silently choose implementation
details. Ask for explicit decisions before fixing details such as:

- Public structs and traits.
- Public functions.
- Error types.
- Test cases.
- Exact DataFusion table provider implementation.
- Exact Vortex read/write API signatures.
- Mock catalog storage model.
- Exact object storage API signatures.
- Arrow Flight RPC method mapping.

Known decisions so far:

- Initial table is `dummy_table`.
- Initial protocol stream ID is always `0`.
- Non-zero imported `stream_id` values are rejected.
- Internal stream ID column is `__mangrobe__stream_id`.
- `__mangrobe__stream_id` is currently copied from the user-visible `stream_id`.
- `posted_at` is event time.
- Internal partition time is `hour(posted_at)`.
- Internal partition column is `__mangrobe__partition_time`.
- User-provided `__mangrobe__*` columns are rejected.
- User-visible missing columns are read as `NULL`.
- Import buffers flush after 5 seconds.
- Flush unit is
  `(table_name, __mangrobe__stream_id=0, __mangrobe__partition_time hour)`.
- The background flusher lives in `server/background/flusher.rs`, separate from
  `server/flight`.
- The background flusher is an internal server background task, not an
  independently runnable worker process.
- The actual flush use case lives in `application/flush_service.rs`.
- Table definitions include `storage_prefix`.
- Catalog file paths are table-relative and do not include the table storage
  prefix.
- Actual object keys are produced by joining `storage_prefix` and table-relative
  file path.
- File paths use `stream_id={stream_id}/partition_time=YYYYMMDD_HHMMSS`.
- RecordBatches in one import request must have the same schema.
- Min/max stats are computed from Arrow batches at write time.
- Stats are collected for numeric and timestamp columns only.
- Vortex writes use temporary local files, represented by `NamedTempFile`.
- Vortex write returns the computed statistics so catalog registration does not
  need to parse Vortex metadata.
- Query pruning uses catalog statistics, not Vortex file metadata.
- Catalog registration is the visibility/commit point.
- Uploaded but unregistered objects are orphan objects and are ignored by
  queries.
- Orphan cleanup is required in the full design but out of scope for the initial
  implementation.
- Query results are returned as Arrow over Flight.
- Query execution uses DataFusion.
- DataFusion integration lives under `application/datafusion/`.
- Query reads selected Vortex files.
- The initial directory structure uses `domain`, `application`,
  `infrastructure`, `server/flight`, and `server/background`.
- No `server/otel` module is created for the initial implementation.
- The domain layer may depend on Arrow types.
- `TableDefinition.schema` includes both user-visible and internal columns.
- Columns are tagged as user-visible or internal.
- `TableMapping` is fixed initially and derives
  `__mangrobe__stream_id` from `stream_id` and
  `__mangrobe__partition_time` from `posted_at`.
- The `stream_id = 0` check is application import logic, not a domain mapping
  rule.
- `CatalogPort`, `ObjectStorePort`, `VortexPort`, `ClockPort`, and
  `FileIdGeneratorPort` are application ports.
- There is no `QueryEnginePort` in the initial design.
- Vortex and object storage ports are separate.
- Join, CTE, subquery, DDL, and SQL insert are out of scope.
- OTel ingest and compaction are out of scope.

Open or intentionally undecided details:

- Exact Arrow Flight action or RPC encoding for `Query(sql)` and
  `Import(arrow)`.
- Exact DataFusion table provider design.
- Exact SQL validation implementation.
- Exact mock mangrobe catalog data structures.
- Exact Rust signatures for application ports.
- Exact import error model.
- Exact tests to write first.
