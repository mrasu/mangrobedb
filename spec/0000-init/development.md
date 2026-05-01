# mangrobe-db initial development notes

This document contains implementation and local-development details that are
shared by both the writer/import side and the reader/query side.

The product-level behavior and goals remain in `rough.md`. Writer-specific
details live in `writer.md`. Reader-specific details live in `reader.md`.

## Document Status

This shared development specification is temporarily complete. If work resumes
without conversation context, do not continue expanding this document by
default. The next natural specification step is `writer.md`, because
writer/import behavior depends directly on the domain schema, table mapping,
import buffer, flush unit, file statistics, object storage upload, and catalog
registration decisions captured here.

The remaining open details in this document are intentionally left for
implementation-time discovery or writer/reader-specific specification work.

## Local Mock Environment

The mock development environment uses Docker Compose.

For the initial mock setup, Docker Compose starts RustFS as the S3-compatible
object storage service. Other mock behavior, including mock catalog state and
mock schema state, runs inside the Rust process.

The initial mock object storage bucket is:

```text
dummy_bucket
```

The bucket must be created by a person before starting `mangrobe-db`. The
server does not create buckets. If `dummy_bucket` does not exist, startup or
object-storage initialization fails.

The initial mock setup does not run a real mangrobe protocol service in Docker
Compose.

## Mock Persistence

Mock catalog state and mock schema state are persisted under:

```text
./data/mock
```

The initial mock persistence file is:

```text
./data/mock/state.json
```

This single JSON file contains both mock schema state and mock catalog state.

This persistence is only for the mock implementation. It must not be stored in
the same object-storage location as real table data.

If `state.json` does not exist, the server starts from the initial
`dummy_table` schema and an empty catalog.

The mock state file may be directly overwritten when the schema or catalog
changes. The initial mock implementation does not need atomic temp-file and
rename behavior.

If `state.json` exists but cannot be read or parsed, server startup fails. The
server only falls back to the initial schema and empty catalog when the file does
not exist.

Existing object-store files are not scanned at startup to reconstruct the mock
catalog or schema.

The mock catalog/schema persistence does not need to support multiple server
processes using the same persistence files concurrently.

## Initial Table And Schema

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

The initial implementation keeps the current table definition in memory.

Files may have different schemas.

Import schema behavior:

- Unknown user columns are added to the in-memory table definition.
- Known columns with the same type are accepted.
- Known columns with a different type cause import failure.
- Columns with the `__mangrobe__` prefix cause import failure.

Query schema behavior:

- DataFusion receives a unified schema based on the in-memory table definition.
- If a selected file does not contain a column from the unified schema, that
  column is read as `NULL` for that file.
- Internal `__mangrobe__` columns are hidden from user results.

Future schema evolution is expected to use a mangrobe protocol method such as
`ModifyColumn(column_name, type)`. That future method should add unknown columns
and reject incompatible existing column types. Type widening, such as `int32` to
`int64`, may be allowed in the future, but is not part of the initial MVP.

## Stream And Partition Mapping

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

The internal partition time is derived from `posted_at`.

```text
__mangrobe__partition_time = hour(posted_at)
```

`posted_at` is event time.

The mangrobe protocol `partition_time` fields use
`__mangrobe__partition_time`, not any user-visible `partition_time` column.

## Object Storage Paths

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

## Domain Layer

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
  column_index: HashMap<ColumnName, usize>

ColumnDefinition
  name: ColumnName
  data_type: Arrow DataType
  kind: ColumnKind

ColumnKind
  User
  Internal
```

`TableSchema.columns` is the authoritative column order. This order is used for
schema presentation such as `select *`. The initial order is the initial table
schema order, and unknown imported user columns are appended in discovery order.
`TableSchema.column_index` is maintained for lookup by column name and points
into `columns`.

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

Validated domain value objects are constructed through `TryFrom`
implementations. Raw public tuple constructors should not be exposed for
validated domain values. External input, mock persistence input, and generated
values must pass through `TryFrom` before entering domain state.

Examples:

```text
TableName::try_from(String)
ColumnName::try_from(String)
FileId::try_from(String)
FilePath::try_from(String)
ObjectStoragePrefix::try_from(String)
ObjectKey::try_from(String)
```

Domain validation owns stable invariants such as empty-name rejection, invalid
path rejection, and reserved internal prefix helpers. Application services own
temporary MVP restrictions such as `dummy_table`-only imports and `stream_id =
0`.

`StreamId` is represented as an `i64` domain value, matching the expected
gRPC/protobuf integer shape unless the future mangrobe protocol fixes a
different type. `PartitionTime` is represented as `chrono::DateTime<Utc>`.

`FileId` accepts a string through `TryFrom`. The initial implementation may
generate UUID-based file IDs, but generation is owned by
`FileIdGeneratorPort`, not by domain path construction.

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

`domain/statistics.rs` owns file statistics values. Statistics preserve their
Arrow data type information and are only cast or normalized at the point where a
consumer needs comparison or pruning behavior.

```text
FileStatistics
  columns: Vec<ColumnStatistics>

ColumnStatistics
  column_name: ColumnName
  data_type: Arrow DataType
  min: StatisticsValue
  max: StatisticsValue

StatisticsValue
  Int8(i8)
  Int16(i16)
  Int32(i32)
  Int64(i64)
  UInt8(u8)
  UInt16(u16)
  UInt32(u32)
  UInt64(u64)
  Float32(f32)
  Float64(f64)
  TimestampSecond(i64)
  TimestampMillisecond(i64)
  TimestampMicrosecond(i64)
  TimestampNanosecond(i64)
```

The initial MVP only collects statistics for numeric and timestamp columns.
String min/max statistics remain out of scope.

## Application Layer

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

`ClockPort` and `FileIdGeneratorPort` are ports so tests can mock time and file
ID generation.

The initial port traits use Rust `async fn in trait` directly for operations
that need asynchronous I/O. The initial design does not use `async_trait`.
If trait-object usage, mocking, or service composition later makes this
awkward, the implementation can revisit this decision.

`CatalogPort`, `ObjectStorePort`, and `VortexPort` use async methods only where
they actually perform I/O. `ClockPort` and `FileIdGeneratorPort` are ordinary
synchronous traits.

Port methods return `anyhow::Result` in the initial implementation. Detailed
application-specific error types are intentionally deferred and will be refined
later.

The reader-side boundary between object storage, Vortex, and DataFusion is not
fixed yet. If Vortex/DataFusion integration can naturally produce the needed
`RecordBatch` values without exposing local paths, the reader should use that
shape. If the Vortex library requires file paths or local files, selected
objects may be downloaded to temporary local files before reading. This must be
decided during implementation after checking the actual library APIs.

## Error Model

The domain layer defines a concrete `DomainError` type. `TryFrom`
implementations for validated domain values return `DomainError`.

The initial domain error cases include:

```text
DomainError
  EmptyValue { type_name }
  InvalidName { type_name, value, reason }
  InvalidPath { type_name, value, reason }
  InvalidTimestamp { reason }
  DuplicateColumn { column_name }
  UnknownColumn { column_name }
  IncompatibleColumnType { column_name, expected, actual }
```

The application layer returns `anyhow::Result` for the initial MVP. This keeps
the use-case code and tests simple while detailed application-specific error
types are deferred.

The infrastructure port traits also return `anyhow::Result` for the initial
MVP.

The Flight/server layer converts failures to RPC errors. It should inspect the
error chain for `DomainError`. Domain errors caused by user input or user-owned
schema/query/import data are returned as `invalid_argument`. Errors that are not
user-actionable, or that do not contain a known user-input `DomainError`, are
returned as `internal`.

Detailed structured application error types are deferred until the initial
import/query paths make the needed distinctions clear.

Examples:

```text
invalid user column name          -> invalid_argument
reserved __mangrobe__ user column -> invalid_argument
incompatible imported column type -> invalid_argument
unsupported SQL shape             -> invalid_argument when detected explicitly
object storage failure            -> internal
Vortex read/write failure         -> internal
mock catalog persistence failure  -> internal
```

Validation error messages should be deterministic enough to test.

## Test Direction

The initial test order should avoid Flight, DataFusion, object storage, and
Vortex integration until the domain and application rules are stable.

Write tests in this order:

1. Domain value object tests.
2. Table schema tests.
3. Table mapping tests.
4. Import service validation tests.
5. Buffer and flush unit tests.

Domain value object tests should cover:

- Empty names are rejected.
- Valid names are accepted.
- `__mangrobe__` prefix detection works for column names.
- `FilePath::for_partition` builds the expected table-relative path.
- `ObjectStoragePrefix::join` builds the expected object key.

Table schema tests should cover:

- Initial schema order.
- `column_index` lookup.
- Unknown imported user columns are appended.
- Duplicate columns are rejected.
- Incompatible known column types are rejected.
- User-visible and internal column distinction.

Table mapping tests should cover:

- `stream_id` derives `__mangrobe__stream_id`.
- `posted_at` derives `__mangrobe__partition_time`.
- `posted_at` is truncated to the hour.
- Required source columns are rejected when missing.

Import service validation tests should cover:

- Only `dummy_table` is accepted.
- `stream_id` is required.
- `posted_at` is required.
- Non-zero `stream_id` is rejected.
- RecordBatches with different schemas in one import request are rejected.
- User-provided `__mangrobe__` columns are rejected.

Buffer and flush unit tests should cover:

- Rows are grouped by `(table_name, __mangrobe__stream_id,
  __mangrobe__partition_time hour)`.
- Rows spanning multiple hours become separate flush units.

## Server Layer

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

The initial server may handle multiple imports, multiple queries, and background
flushes concurrently within one process. Shared in-memory state such as the
import buffer, table schema, and mock catalog must be protected by appropriate
single-process synchronization.

The import buffer is in-memory only during normal execution. Buffered rows may
be lost on an unsafe process stop. On a safe process stop such as Ctrl-C, the
server should try to flush buffered rows before exiting. Shutdown flush has a
ten-second timeout. While waiting for shutdown flush, the process should print a
message such as `stopping...` to stdout.

The process startup shape is:

```text
fn main() {
  start_flusher(...)
  start_flight(...)
}
```

The exact Rust async/threading API is not decided yet.

## Open Details

These details are intentionally not fixed yet:

- Public structs and traits.
- Public functions.
- Exact Rust signatures for application ports.
- Reader-side object-store/Vortex/DataFusion file handoff shape.
- Exact import error model.

## Known Decisions

- The mock development environment uses Docker Compose.
- Docker Compose starts RustFS as the S3-compatible object storage service for
  the initial mock setup.
- The initial mock object storage bucket is `dummy_bucket`.
- `dummy_bucket` must be created by a person before starting `mangrobe-db`.
- The server does not create buckets; missing `dummy_bucket` is an error.
- Other mock behavior, including mock catalog and mock schema behavior, runs
  inside the Rust process.
- Mock schema and catalog state are stored together in
  `./data/mock/state.json`.
- If `state.json` is missing, the server starts from the initial `dummy_table`
  schema and an empty catalog.
- The mock state file may be directly overwritten; atomic temp-file and rename
  behavior is not required for the initial mock implementation.
- If `state.json` exists but cannot be read or parsed, startup fails.
- Mock persistence is separate from real object storage.
- Existing object-store files are not scanned to reconstruct catalog or schema
  state at startup.
- Table definitions include `storage_prefix`.
- Domain table, schema, column, file, path, partition, and statistics concepts
  use explicit newtype-style domain values where practical.
- Validated domain values are constructed through `TryFrom`.
- `TableSchema.columns` preserves schema order, and `TableSchema.column_index`
  provides lookup by column name.
- The domain layer may store Apache Arrow `DataType` values directly.
- `StreamId` is represented as `i64`.
- `PartitionTime` is represented as `chrono::DateTime<Utc>`.
- `FileId` accepts strings, while UUID-based generation belongs behind
  `FileIdGeneratorPort`.
- File statistics preserve Arrow type information and use a domain-owned
  `StatisticsValue` enum for numeric and timestamp min/max values.
- Application ports use Rust `async fn in trait` directly where asynchronous
  I/O is needed; the initial design does not use `async_trait`.
- `ClockPort` and `FileIdGeneratorPort` are synchronous traits.
- Initial port methods return `anyhow::Result` until the detailed error model is
  decided.
- The domain layer defines `DomainError`, and domain `TryFrom` implementations
  return it.
- Application services and infrastructure ports return `anyhow::Result` for the
  initial MVP.
- Flight error mapping inspects `DomainError`: user-input/domain validation
  problems become `invalid_argument`, while other failures become `internal`.
- Initial tests start with domain value objects, table schema, table mapping,
  import service validation, and buffer/flush units before Flight/DataFusion
  integration tests.
- Catalog file paths are table-relative and do not include the table storage
  prefix.
- Actual object keys are produced by joining `storage_prefix` and table-relative
  file path.
- File paths use `stream_id={stream_id}/partition_time=YYYYMMDD_HHMMSS`.
- Cross-process concurrent use of the same mock catalog/schema persistence
  files is out of scope.
- Multiple imports, multiple queries, and background flushes may run
  concurrently within one server process.
- On safe process stop, such as Ctrl-C, the server tries to flush buffered rows
  before exiting.
- Shutdown flush has a ten-second timeout and prints a `stopping...`-style
  message to stdout while waiting.
