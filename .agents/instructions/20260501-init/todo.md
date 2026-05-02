# TODO

Before implementing a section, read the listed files and ask for explicit user decisions on APIs, files, structs, functions, tests, or behavior that are not already fixed.

- `Get Writer Working`: read `rough.md`, `development.md`, and `writer.md`.
  This section intentionally writes files synchronously first, before adding
  buffering or the background flusher.
- `Get Reader Working`: read `rough.md`, `development.md`, and `reader.md`.
  Query execution must use DataFusion and selected Vortex files; do not build a
  separate Vortex row scanner.
- `Complete Writer Semantics`: read `writer.md` and the server lifecycle notes
  in `development.md`. This section changes import success back to buffer
  acceptance and moves file visibility to background flush + `AddFiles`.

## 1. Get Writer Working

- [x] Make the Flight server runnable enough to accept import requests.
- [x] Transform imported Flight `RecordBatch` values into the `RecordBatch` values that should be written.
- [x] Mock mangrobe API: Validate/update mock state
- [ ] Split RecordBatch by flush unit, generate immutable file paths.
- [ ] Build statistics + Vortex writing: write each flush unit to a `NamedTempFile` and return numeric/timestamp min/max statistics, row count, and file size.
- [ ] Build upload + `AddFiles`: upload written files, register table-relative paths and statistics.

## 2. Get Reader Working

- [ ] Add Flight `DoGet` query handling: decode SQL from the ticket, execute it, and stream Arrow result batches.
- [ ] Build query planning/validation: use DataFusion, allow supported `SELECT` over `dummy_table`, and reject out-of-scope SQL and `__mangrobe__*` references.
- [ ] Add the catalog-aware DataFusion table provider for `dummy_table`, exposing the unified user-visible schema.
- [ ] Implement reader catalog selection: derive partition hours from `posted_at`, call mock `GetCurrentState`/`GetFileInfo`, and prune by partition and numeric/timestamp statistics.
- [ ] Hand selected Vortex files to `vortex-datafusion`, resolve table-relative paths through the storage prefix, and keep query visibility based on successful `AddFiles`.

## 3. Complete Writer Semantics

- [ ] Add in-memory import buffering: group rows by flush unit and make Import return after buffer acceptance.
- [ ] Add background flushing: every five seconds drain buffered rows, write/upload/register files, and batch one flush tick into one `AddFiles` request when possible.
