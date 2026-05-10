set dotenv-load := true

run:
    cargo run -- --config mangrobe_db.dev.yml

client-import:
    cargo run --bin client_import

default_sql := "select * from hello_table"

client-query sql=default_sql:
    cargo run --bin client_query -- --sql "{{sql}}"

client-create-table:
    cargo run --bin client_query -- --sql "CREATE EXTERNAL TABLE hello_table STORED AS VORTEX LOCATION 's3://mangrobe-db-development/bar'"

client-migration-refresh:
    cargo run --bin client_migration -- refresh --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-db-development

fmt:
    cargo fmt
    cargo clippy --fix --allow-dirty
