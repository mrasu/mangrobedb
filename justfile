set dotenv-load := true

run:
    cargo run -- --config mangrobe_db.dev.yml

client:
    cargo run --bin client
