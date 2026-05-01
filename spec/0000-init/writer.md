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
new Vortex file with a new file ID.

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

A cleanup job is required in the full design to remove orphan objects, but that
cleanup job is out of scope for the initial implementation.

## Open Details

These details are intentionally not fixed yet:

- Exact Arrow Flight action or RPC encoding for `Import(arrow)`.
- Exact Vortex write API signatures.
- Exact object storage API signatures.
- Exact mock catalog registration data structures.

## Known Decisions

- Import buffers flush after 5 seconds.
- Flush unit is
  `(table_name, __mangrobe__stream_id=0, __mangrobe__partition_time hour)`.
- RecordBatches in one import request must have the same schema.
- Min/max stats are computed from Arrow batches at write time.
- Stats are collected for numeric and timestamp columns only.
- Vortex writes use temporary local files, represented by `NamedTempFile`.
- Vortex write returns the computed statistics so catalog registration does not
  need to parse Vortex metadata.
- Catalog registration is the visibility/commit point.
- Uploaded but unregistered objects are orphan objects and are ignored by
  queries.
- Actual Vortex data files are immutable once written and must not be
  overwritten.
- New flushed data is written to a new Vortex file with a new file ID.
- Orphan cleanup is required in the full design but out of scope for the initial
  implementation.
