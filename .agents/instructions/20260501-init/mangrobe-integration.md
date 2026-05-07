# Mangrobe API Integration

## Goal

Replace mock file metadata operations with real Mangrobe API calls while keeping
table schema metadata local.

This step introduces a half-mocked catalog:

- File visibility and file metadata operations use the real Mangrobe API.
- Table schema operations still use a local JSON catalog because table
  definition support is not ready yet.

## Catalog Behavior

Add a `MangrobeCatalog` implementation of `CatalogPort`.

`MangrobeCatalog` must handle catalog methods as follows:

- `get_table_schema`: read table schema from the local half-mocked JSON state.
- `update_table_schema`: update table schema in the local half-mocked JSON
  state.
- `get_current_state`: call the real Mangrobe API.
- `get_file_info`: call the real Mangrobe API.
- `add_files`: call the real Mangrobe API.

The local JSON state is only for table schema metadata. Real Mangrobe owns file
visibility, file IDs, file statistics, and file metadata for this catalog.

## Persistence

Use this state path for the half-mocked catalog:

```text
./data/mock/half_mocked_state.json
```

Use the same persisted JSON format as the existing mock catalog state.

Never overwrite or migrate this file from the existing mock state path:

```text
./data/mock/state.json
```

If `half_mocked_state.json` does not exist, start from the same initial
`dummy_table` schema that the mock catalog uses.

## Configuration

Read the Mangrobe API database URL from `AppConfig`.

Support both YAML and environment variable configuration:

- YAML key: `mangrobe_api.database_url`
- Environment variable: `MANGROBE_DB__MANGROBE_API__DATABASE_URL`

The development YAML may use the same local PostgreSQL URL currently used by
the temporary `src/main.rs::aa` connection probe.

## Server Wiring

Keep both catalog construction paths available in the Flight server code:

- `MockCatalog`
- `MangrobeCatalog`

The active path is selected by editing/commenting the code. Do not add a runtime
catalog mode switch in this step.

Remove or stop calling the temporary `src/main.rs::aa` startup probe so the
Flight server can run normally.

## Conversion Rules

Partition times remain `i64` microseconds inside `mangrobe-db`.

At the real Mangrobe API boundary:

- Convert partition times from microseconds to `chrono::DateTime<Utc>` when
  building real API parameters.
- Convert partition times from `chrono::DateTime<Utc>` to microseconds when
  building `CatalogFile` values.
- Use real Mangrobe file IDs as returned by the API. Do not add the mock
  `id:` prefix.

The real Mangrobe API accepts and returns file statistics as `f64`.

- Convert local `Int32`, `Int64`, `Float64`, and timestamp-microsecond
  statistics to `f64` when calling `AddFiles`.
- Convert real API statistics back to `StatisticValue::Float64`.
- Adjust statistics pruning so `Float64` catalog statistics can compare
  correctly with numeric query literals.

## Verification

Do not add tests for this step.

Verify the implementation with:

```text
cargo check
```
