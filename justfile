set dotenv-load := true

run:
    cargo run -- --config mangrobedb.dev.yml

client-ingest:
    cargo run --bin client_ingest

default_sql := "select * from hello_table where stream = 0"

client-query sql=default_sql:
    cargo run --bin client_query -- --sql "{{sql}}"

client-create-table:
    cargo run --bin client_query -- --sql "CREATE TABLE hello_table(stream INT64, posted_at TIMESTAMP(6), posted_at_hour TIMESTAMP(6)) LOCATION 's3://mangrobedb-development/bar' WITH(stream_column = 'stream', partition_column = 'posted_at_hour', format = 'VORTEX')"

client-show-table:
    cargo run --bin client_query -- --sql "SHOW CREATE TABLE hello_table"

client-list-tables:
    cargo run --bin client_list_tables

client-migration-refresh:
    cargo run --bin client_migration -- refresh --database-url postgres://postgres:@127.0.0.1:5432/mangrobedb-development

fmt:
    cargo fmt
    cargo clippy --fix --allow-dirty
