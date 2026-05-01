# Writer implementation checklist

## Resume context

This file is intended to be enough context to resume writer implementation after
clearing the chat context. If asked to continue from this file, first read:

- `rough.md`
- `development.md`
- `writer.md`
- this `impl-writer.md`

Current implemented code is intentionally minimal. Do not recreate the full
planned directory structure up front. Add files only when the next checklist
step needs them.

Important implementation rule from `AGENTS.md`:

- Do not silently invent non-trivial implementation choices.
- Ask before choosing public structs, traits, functions, test cases, or behavior
  that requires judgment.
- Prefer small implementation increments that can be manually checked.

The next natural step is the first unchecked item below.

## Checklist

- [x] create crate
- [x] dummy server
- [x] dummy Flight import client
- [x] keep `cargo run -- --addr 127.0.0.1:50051` as the server entrypoint
- [x] keep `cargo run --bin flight_import_client -- --addr 127.0.0.1:50051` as the first manual import check
- [ ] add the initial domain value objects needed by writer code
- [ ] add `TableName`, `ColumnName`, `StreamId`, `PartitionTime`, and `RowCount`
- [ ] add `FileId`, `FilePath`, `ObjectStoragePrefix`, and `ObjectKey`
- [ ] add domain validation through `TryFrom` for external strings
- [ ] add the initial `dummy_table` table definition
- [ ] add user/internal column visibility to the table schema model
- [ ] add the fixed table mapping from `stream_id` to `__mangrobe__stream_id`
- [ ] add the fixed table mapping from `posted_at` to `__mangrobe__partition_time`
- [ ] add a small domain-level check or example that builds the initial table definition
- [ ] run `cargo check`
- [ ] add `ImportService` as the writer application entrypoint behind Flight `DoPut`
- [ ] move table descriptor parsing out of the Flight handler into request construction logic
- [ ] keep the Flight layer responsible only for protocol decode and response mapping
- [ ] make `DoPut` call `ImportService` with table name and decoded `RecordBatch` values
- [ ] keep `ImportService` behavior dummy at first: print the accepted request summary
- [ ] manually confirm the client still sends data and the server prints from `ImportService`
- [ ] run `cargo check`
- [ ] add import request validation for `FlightDescriptor.path = ["import", table_name]`
- [ ] add application validation that `table_name` is `dummy_table`
- [ ] add validation that all batches in one import request have the same schema
- [ ] add validation that `posted_at` exists
- [ ] add validation that `stream_id` exists
- [ ] add validation that no user column starts with `__mangrobe__`
- [ ] add validation that every `stream_id` value is `0`
- [ ] make the dummy client able to send an invalid table name for manual error checks
- [ ] manually confirm valid import succeeds and invalid table name fails
- [ ] run `cargo check`
- [ ] add the mock schema/catalog state model in memory
- [ ] add initial `dummy_table` schema state
- [ ] add known-column compatible type validation
- [ ] add unknown user column discovery and append it to the in-memory schema
- [ ] ensure schema update happens before rows are accepted by the buffer
- [ ] keep the mock schema/catalog persistence file out of scope until the in-memory path works
- [ ] manually confirm a batch with a new user column is accepted once and then treated as known
- [ ] manually confirm a batch with an incompatible known column type fails
- [ ] run `cargo check`
- [ ] add internal column derivation for imported batches
- [ ] derive `__mangrobe__stream_id` from user-visible `stream_id`
- [ ] derive `__mangrobe__partition_time` from `posted_at` truncated to hour
- [ ] keep the original user-visible `stream_id` column in the batch
- [ ] print the derived internal columns in the dummy path before buffering
- [ ] manually confirm the server prints original and derived columns
- [ ] run `cargo check`
- [ ] add the in-memory import buffer
- [ ] group buffered batches by `(table_name, __mangrobe__stream_id, __mangrobe__partition_time)`
- [ ] make `ImportService` add derived batches to the buffer instead of only printing
- [ ] add a debug method or temporary log that shows buffer groups and row counts
- [ ] manually confirm mixed-hour input creates separate buffer groups
- [ ] run `cargo check`
- [ ] add the background flusher skeleton
- [ ] start the flusher from server startup
- [ ] make the flusher tick every five seconds
- [ ] make the flusher take ready buffer groups and print what would be flushed
- [ ] manually confirm imported rows are printed by the flusher after five seconds
- [ ] run `cargo check`
- [ ] add writer-side port traits in `application/ports.rs`
- [ ] add `CatalogPort` methods needed for schema update and `AddFiles`
- [ ] add `ObjectStorePort` upload method
- [ ] add `VortexPort` write method
- [ ] add `ClockPort` and `UuidGeneratorPort`
- [ ] keep concrete implementations minimal until each port is used
- [ ] run `cargo check`
- [ ] add file path construction in `domain/file.rs`
- [ ] generate table-relative paths in the form `stream_id=0/partition_time=YYYYMMDD_HHMMSS/{file_id}.vortex`
- [ ] add `ObjectStoragePrefix::join(FilePath)` for object keys
- [ ] add deterministic UUID generator support for later tests or manual checks
- [ ] manually confirm the flusher prints the generated table-relative file path
- [ ] run `cargo check`
- [ ] add `FileStatistics` domain values
- [ ] add numeric min/max statistics collection
- [ ] add timestamp min/max statistics collection
- [ ] skip string min/max statistics
- [ ] print computed statistics from the dummy flush path
- [ ] manually confirm stats are printed for `id`, `stream_id`, `posted_at`, and internal columns
- [ ] run `cargo check`
- [ ] add the Vortex writer infrastructure implementation
- [ ] write one flush unit worth of Arrow batches to a `NamedTempFile`
- [ ] return `VortexWriteResult` with temp file, statistics, and row count
- [ ] keep object storage upload and catalog registration disabled for the first Vortex check
- [ ] manually confirm a temporary Vortex file is produced during flush
- [ ] run `cargo check`
- [ ] add local memory object store implementation
- [ ] upload the Vortex temp file through `ObjectStorePort`
- [ ] resolve object keys by joining table storage prefix and table-relative file path
- [ ] keep S3/RustFS implementation out of the first writer path unless explicitly requested
- [ ] manually confirm a flushed import creates an object in the memory object store
- [ ] run `cargo check`
- [ ] add mock catalog `AddFiles`
- [ ] register table-relative file paths only
- [ ] register stream id, partition time, file size, and column statistics
- [ ] make catalog registration the visibility/commit point
- [ ] manually confirm a flushed import appears in mock catalog state after `AddFiles`
- [ ] run `cargo check`
- [ ] add mock state persistence under `./data/mock/state.json`
- [ ] persist schema state and catalog state together
- [ ] start with initial schema and empty catalog when `state.json` is missing
- [ ] fail startup when `state.json` exists but cannot be read or parsed
- [ ] manually confirm restart preserves discovered schema and registered file metadata
- [ ] run `cargo check`
- [ ] replace temporary print-only checks with focused tests where behavior is stable
- [ ] add tests for descriptor parsing and table restriction
- [ ] add tests for schema compatibility and unknown column append
- [ ] add tests for stream id validation
- [ ] add tests for partition-time derivation
- [ ] add tests for buffer grouping
- [ ] add tests for file path construction
- [ ] add tests for statistics collection
- [ ] add tests for flush failure boundaries
- [ ] run `cargo test`
- [ ] add RustFS/S3 object store implementation
- [ ] read endpoint, bucket, and credentials from configuration
- [ ] fail startup if the configured bucket does not exist
- [ ] upload Vortex files to `dummy_bucket`
- [ ] manually confirm flushed files land under `mangrobe-db/dummy_table/...`
- [ ] run `cargo check`
- [ ] remove temporary debug prints that are no longer useful
- [ ] keep concise operational logs for import accepted, flush started, flush succeeded, and flush failed
- [ ] manually run server and client end-to-end
- [ ] confirm import returns after buffering, before flush visibility
- [ ] confirm background flush writes Vortex, uploads object storage, and registers catalog metadata
- [ ] run `cargo test`
