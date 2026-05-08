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
- `Use Mangrobe API`: read `rough.md`, `development.md`, and
  `mangrobe-integration.md`. This section replaces file metadata operations
  with real Mangrobe API calls while keeping table schema metadata in
  `half_mocked_state.json`.

## 1. Get Writer Working

- [x] Make the Flight server runnable enough to accept import requests.
- [x] Transform imported Flight `RecordBatch` values into the `RecordBatch` values that should be written.
- [x] Mock mangrobe API: Validate/update mock state
- [x] Split RecordBatch by flush unit, generate immutable file paths.
- [x] Build statistics + Vortex writing: write each flush unit to a `NamedTempFile` and return numeric/timestamp min/max statistics, row count, and file size.
- [x] Build upload + `AddFiles`: upload written files, register table-relative paths and statistics.

## 2. Get Reader Working

- [x] Add Flight `DoGet` query handling: decode SQL from the ticket and stream Arrow result batches.
- [x] Make `vortex-datafusion` work minimally: assume `select * from dummy_table;`, read only the first registered file, and return its data without error handling.
- [x] Add the catalog-aware DataFusion table provider for `dummy_table`, exposing the unified user-visible schema and completing partition pruning.
- [x] Build query planning/validation: use DataFusion, allow supported `SELECT` over `dummy_table`, and reject out-of-scope SQL and `__mangrobe__*` references.
- [x] Implement reader catalog statistics pruning: call mock `GetFileInfo` and prune by numeric/timestamp statistics after partition pruning.

## 3. Use Mangrobe API

- [x] Add the Mangrobe integration spec and split this section into implementation steps.
- [x] Add the half-mocked `MangrobeCatalog`: load/save table schema metadata from `./data/mock/half_mocked_state.json` without touching `./data/mock/state.json`.
- [x] Wire real Mangrobe API calls for `GetCurrentState`, `GetFileInfo`, and `AddFiles`, including partition-time and statistics conversions.
- [x] Add configuration for the Mangrobe API database URL from YAML and environment variables.

## 4. Complete Writer Semantics

- [x] Add in-memory import buffering: group rows by flush unit and make Import return after buffer acceptance.
- [x] Add background flushing: every five seconds drain buffered rows, write/upload/register files, and batch one flush tick into one `AddFiles` request when possible.
