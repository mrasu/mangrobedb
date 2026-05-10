# TODO

Before implementing a section, read `rough.md` and ask for explicit user
decisions on APIs, files, structs, functions, tests, or behavior that are not
already fixed.

- `Create Table`: read `rough.md`. Implement `CREATE EXTERNAL TABLE` through the
  existing `CommandStatementQuery` ticket flow, executing it in
  `do_get_statement`.
- `List Table`: read `rough.md`. Implement Flight SQL `CommandGetTables` using
  mangrobe `list_tables`.
- `Show Table`: read `rough.md`. Implement `SHOW CREATE TABLE table_name` using
  mangrobe `get_table`.

- [x] Create Table
   - Add catalog data-definition support needed for `create_external_table`.
   - Parse `CREATE EXTERNAL TABLE` in `do_get_statement`.
   - Convert DataFusion DDL fields to mangrobe table-definition request fields.
   - Return an empty successful Flight stream/result after creation.
- [x] List Table
   - Add catalog data-definition support needed for `list_tables`.
   - Implement `get_flight_info_tables`.
   - Implement `do_get_tables`.
   - Return standard Flight SQL table listing rows.
- [x] Show Table
   - Add catalog data-definition support needed for `get_table`.
   - Parse `SHOW CREATE TABLE table_name` in `do_get_statement`.
   - Convert mangrobe table metadata to the agreed detailed result shape.
   - Return the metadata as a Flight result stream.
