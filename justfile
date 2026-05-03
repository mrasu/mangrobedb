set dotenv-load := true

run:
    cargo run -- --config mangrobe_db.dev.yml

client-import:
    cargo run --bin client_import

client-query:
    cargo run --bin client_query

client-query-with-sql sql:
    cargo run --bin client_query -- --sql '{{sql}}'
