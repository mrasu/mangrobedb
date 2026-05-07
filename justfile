set dotenv-load := true

run:
    cargo run -- --config mangrobe_db.dev.yml

client-import:
    cargo run --bin client_import

default_sql := "select * from dummy_table"

client-query sql=default_sql:
    cargo run --bin client_query -- --sql "{{sql}}"

client-migration-refresh:
    cargo run --bin client_migration -- refresh --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-db-development

fmt:
    cargo fmt
    cargo clippy --fix --allow-dirty
