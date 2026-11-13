# Installation

Fusee runs on Linux and macOS. Choose the SDK for your application. The TypeScript and Python packages include the CLI used in the quickstarts.

## TypeScript

Use Node.js 22.18 or later to run the TypeScript quickstart. In an existing project, install the SDK from npm:

```sh
npm install @bitsinfra/fusee
```

Import it with `import { Engine, workflow } from "@bitsinfra/fusee"`. The package also provides the `fusee` CLI.

To try the starter in a new directory, run:

```sh
mkdir hello && cd hello
npm exec --yes --package=@bitsinfra/fusee@1.0.1 -- fusee init --lang ts
npm install
npx --no-install fusee dev
```

Continue at [Run it](tutorial-typescript.md#2-run-it).

## Python

Use Python 3.9 or later. Install the SDK and CLI from PyPI in a virtual environment:

```sh
mkdir hello && cd hello
python3 -m venv .venv
. .venv/bin/activate
python -m pip install fusee==1.0.1
fusee init --lang python
fusee dev
```

Keep the environment active when running CLI commands. Continue at [Run it](tutorial-python.md#2-run-it).

## Rust

Use Rust 1.88 or later. Add the SDK to an existing Cargo project with `cargo add fusee`. To start the tutorial in a new directory, install the CLI from crates.io:

```sh
cargo install --locked fusee-cli --version 1.0.1
mkdir hello && cd hello
fusee init --lang rust
fusee dev -- cargo run
```

Continue at [Run it](tutorial-rust.md#2-run-it).

## Go

Use Go 1.23 or later with cgo enabled, a C compiler, and the Rust toolchain. From the Fusee checkout:

```sh
cargo build --release -p fusee-c
(cd examples/go && go run ./payment_saga)
```

The [Go SDK guide](../bindings/go/README.md) covers linking the engine into your application.

## Java

Use JDK 17 or later, Maven, a C compiler, and the Rust toolchain. From the Fusee checkout:

```sh
cargo build --release -p fusee-c
(cd bindings/java && mvn install)
java -cp bindings/java/target/classes examples/java/PaymentSaga.java
```

The [Java SDK guide](../bindings/java/README.md) describes the Maven artifact and how it loads the native library.
