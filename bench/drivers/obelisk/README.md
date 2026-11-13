# Obelisk

Use Obelisk 0.42.0, Rust 1.95 with the `wasm32-wasip2` and
`wasm32-unknown-unknown` targets, and wasm-tools 1.263.0. From this directory:

```sh
./build.sh
OBELISK_APP_NAME=fusee-bench obelisk server run --no-auth --server-config server.toml --deployment obelisk.toml
```

The server listens on loopback. Set XDG_DATA_HOME to a fresh directory on the
benchmark disk. In another terminal, run `python3 chain.py 100 2`. The driver
submits workflows over one HTTP connection and waits for the execution result
stream. Each WASI activity receives the preceding result and returns its value
and timestamps. The driver checks the entire value chain and computes each gap
from the previous function's end to the next one's start using integer host
wall-clock nanoseconds. SQLite writer connections use synchronous=FULL.
