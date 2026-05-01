# mangrobe-db initial writer notes

This document contains implementation details for the writer/import side of the
initial MVP.

The product-level behavior and goals remain in `rough.md`. Shared development
details live in `development.md`. Reader/query details live in `reader.md`.

## Import Flow

The server accepts imports through Arrow Flight RPC:

```text
Import(table_name, Arrow batches)
```

The initial Import RPC uses Arrow Flight `DoPut`.

```text
FlightDescriptor.path[0] = "import"
FlightDescriptor.path[1] = table_name
```

The `DoPut` stream carries Arrow RecordBatches for the import request.

The import may contain any number of Arrow RecordBatches. All RecordBatches in
one import request must have the same schema. If schemas differ within one
import request, import fails.

One import request may contain rows from multiple
`__mangrobe__partition_time` hours. Mixed-hour imports are accepted when all
other validation succeeds. Internal partition columns are derived per row, and
buffering groups rows by flush unit.

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
  -> synchronously validate and update mock schema/catalog state
  -> add internal __mangrobe__stream_id = stream_id
  -> add internal __mangrobe__partition_time = hour(posted_at)
  -> buffer by (table_name, __mangrobe__stream_id=0, __mangrobe__partition_time)
  -> return after buffering succeeds
```

Import schema/catalog changes are synchronous. During Import, before rows are
accepted into the buffer:

- The incoming Arrow schema is validated against the current mock schema/catalog
  state.
- Existing columns with incompatible types cause Import failure.
- Unknown user columns are added to the mock schema/catalog state.
- If the mock schema/catalog update fails, Import fails and rows are not
  buffered.
- Schema/catalog update is performed for the whole import request before any
  rows from that request are buffered.
- The import request is all-or-nothing with respect to buffering. If validation
  or schema/catalog update fails, no rows from any RecordBatch in the request
  are buffered.

Data file writing, object storage upload, and `AddFiles` catalog registration
remain asynchronous and are owned by the background flusher.

Import RPC success means that:

- Arrow RecordBatch validation succeeded.
- Mock schema/catalog state validation and synchronous update succeeded.
- Internal columns were derived successfully.
- Rows were accepted into the in-memory import buffer.

Import RPC success does not mean that the imported rows are query-visible yet.
Rows become query-visible only after the background flusher writes Vortex files,
uploads them to object storage, and successfully registers them with `AddFiles`.

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

For one background flush tick, the flusher should register the files it produced
with one `AddFiles` request when possible. The request may contain multiple
`partition_time` entries and multiple files. All files in that `AddFiles`
request become visible together only after `AddFiles` succeeds.

The object storage upload boundary accepts:

- The table definition.
- The table-relative file path.
- The local temporary file path produced by the Vortex writer.

The object storage implementation resolves the full object storage key by
joining the table definition's storage prefix with the table-relative file path.
The catalog still registers only the table-relative file path.

Mock catalog file registration follows the mangrobe protocol shape. The writer
registers files by table-relative path only.

The mock `AddFiles` registration input contains:

```text
AddFiles
  table_name
  stream_id
  entries

AddFilesEntry
  partition_time
  files

AddFile
  path
  size
  column_statistics
```

The registration data does not include row count in the initial MVP.

## Stream ID

The initial protocol stream ID is always `0`.

The input Arrow batch is expected to contain a user-visible `stream_id` column.
For the initial MVP:

- The `stream_id` column is stored as a normal user-visible column.
- Every imported row must have `stream_id = 0`.
- If any row has a non-zero `stream_id`, import fails.
- The internal `__mangrobe__stream_id` column is set from the user-visible
  `stream_id` value.
- The stream ID passed to the mangrobe protocol mock is
  `__mangrobe__stream_id`, which is currently always `0`.

The `stream_id = 0` check is import application logic, not a domain mapping
rule.

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

The Vortex writer API boundary accepts one flush unit worth of Arrow
RecordBatches. The RecordBatches passed to the Vortex writer already include
internal columns and belong to a single
`(table_name, __mangrobe__stream_id, __mangrobe__partition_time hour)` flush
unit.

The Vortex writer writes a temporary local Vortex file and computes file
statistics. It does not upload to object storage and does not register files in
the catalog.

The Vortex write result contains:

```text
VortexWriteResult
  temp_file: NamedTempFile
  statistics: FileStatistics
  row_count: RowCount
```

## Data File Immutability

Actual data files are immutable once written. The implementation must not
overwrite an existing data file path. New flushed data is always written to a
new Vortex file with a new unique file path component.

## Failure Behavior

Catalog registration is the visibility and commit point for imported files.
Uploading a Vortex file to object storage does not make it visible to queries.
A file becomes visible only after `AddFiles(...)` succeeds.

- If Vortex writing fails, no object is uploaded and no catalog entry is added.
- If object storage upload fails, no catalog entry is added.
- If object storage upload succeeds but catalog registration fails, the uploaded
  object becomes an orphan object.
- Orphan objects are ignored by queries because they are not registered in the
  catalog.
- When one flush tick registers multiple uploaded files in a single `AddFiles`
  request and that request fails, none of those files become query-visible.

A cleanup job is required in the full design to remove orphan objects, but that
cleanup job is out of scope for the initial implementation.

## Open Details

No writer-specific open details remain in this document.

## Known Decisions

- Import buffers flush after 5 seconds.
- Import uses Arrow Flight `DoPut` with descriptor path `["import",
  table_name]`.
- Import synchronously validates and updates mock schema/catalog state before
  buffering rows.
- Import schema/catalog update is performed once for the whole import request
  before any rows from that request are buffered.
- Import success means rows were accepted into the in-memory import buffer after
  synchronous validation and schema/catalog update.
- Imported rows become query-visible only after asynchronous Vortex write,
  object storage upload, and `AddFiles` registration succeed.
- Flush unit is
  `(table_name, __mangrobe__stream_id=0, __mangrobe__partition_time hour)`.
- RecordBatches in one import request must have the same schema.
- One import request may contain rows from multiple partition hours. Buffering
  and flushing split those rows by flush unit.
- Min/max stats are computed from Arrow batches at write time.
- Stats are collected for numeric and timestamp columns only.
- Vortex writes use temporary local files, represented by `NamedTempFile`.
- The Vortex writer accepts one flush unit worth of Arrow RecordBatches that
  already include internal columns.
- The Vortex writer does not upload to object storage and does not register
  files in the catalog.
- Vortex write returns the computed statistics so catalog registration does not
  need to parse Vortex metadata.
- Catalog registration is the visibility/commit point.
- Uploaded but unregistered objects are orphan objects and are ignored by
  queries.
- Actual Vortex data files are immutable once written and must not be
  overwritten.
- New flushed data is written to a new Vortex file with a new unique file path
  component.
- One background flush tick registers its produced files with one `AddFiles`
  request when possible. Files in that request become visible together.
- Object storage upload accepts the table definition, table-relative file path,
  and local temporary file path. The object storage implementation resolves the
  full object storage key internally.
- Mock catalog file registration uses table-relative paths.
- Initial mock catalog file registration includes path, size, and column
  statistics, but not row count.
- Orphan cleanup is required in the full design but out of scope for the initial
  implementation.
