# Mangrobe DB


## Run

1. Start server:
   ```bash
   cargo run -- --addr 127.0.0.1:50051
   ```

2. In another terminal, send sample import:
   ```bash
   cargo run --bin flight_import_client -- --addr 127.0.0.1:50051
   ```
