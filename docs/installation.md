# Installation

Fusee runs on Linux and macOS. Choose the SDK for your application. The TypeScript and Python packages include the CLI used in the quickstarts.

## TypeScript

Use Node.js 22.18 or later to run the tutorial's TypeScript files directly. From the [Fusee releases](https://github.com/bitsinfra/fusee/releases), download the SDK tarball (`fusee-1.0.0.tgz`) and the native tarball for your OS, CPU, and, on Linux, libc.

Put just those two tarballs in one directory. From that directory:

```sh
mkdir hello && cd hello
npm install --no-save ../fusee-*.tgz
npx --no-install fusee init --lang ts
npx --no-install fusee dev
```

The CLI prints the local UI address and starts the greeting workflow. Continue at [Run it](tutorial-typescript.md#2-run-it). Use `npx --no-install fusee` for CLI commands when using a project-local install.

## Python

Use Python 3.9 or later. Download the wheel matching your platform from the [Fusee releases](https://github.com/bitsinfra/fusee/releases) into an empty directory, then run:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./fusee-*.whl
mkdir hello && cd hello
fusee init --lang python
fusee dev
```

Keep the environment active when running CLI commands. Continue at [Run it](tutorial-python.md#2-run-it).

## Rust

Use Rust 1.88 or later. From a [Fusee checkout](https://github.com/bitsinfra/fusee), install the CLI and create the sample next to the checkout:

```sh
cargo install --locked --path crates/fusee-cli
mkdir ../hello && cd ../hello
fusee init --lang rust
```

In the generated `Cargo.toml`, replace the `fusee` dependency under `[dependencies]` with the path to the checkout. This example assumes the checkout is named `fusee`:

```toml
fusee = { path = "../fusee/crates/fusee" }
```

Run `fusee dev -- cargo run`, then continue at [Run it](tutorial-rust.md#2-run-it).

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
