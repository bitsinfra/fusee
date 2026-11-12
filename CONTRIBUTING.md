# Contributing

Thank you for helping. A few things make reviews fast.

## Sign-off (DCO)

Every commit must carry a `Signed-off-by` line certifying the
[Developer Certificate of Origin](https://developercertificate.org/):

```bash
git commit -s
```

## Before you open a pull request

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
cargo test --release -p fusee-sim --test seeds          # 500 simulation seeds
cargo run --release -p fusee-sim --bin fusee-crashtest -- driver /tmp/fusee-crash 10
```

A change to the journal machine, recovery, the log format or the replication
protocol also runs a larger seed range (`fusee-sim --seeds 0..20000`,
`FUSEE_VSR_SEEDS=2000 cargo test --release -p fusee-core vsr`) and the
cross-SDK conformance suite (see `conformance/README.md`).

## Ground rules

- The core (`fusee-core`) stays free of I/O, threads and clocks, so the
  simulator can run it.
- A durability guarantee is never weakened silently. If a change alters what
  waits for what, it changes `docs/guarantees.md` in the same pull request.
- Every benchmark number in documentation names its hardware, its
  durability mode and the `fusee-bench` case that reproduces it.
- A new on-disk format bumps the format version and either converts old data
  or refuses it with a clear message.

## Documentation

The docs use Fumadocs UI with Next.js. Edit guides in `docs/` and the navigation in
`docs/tooling/navigation.mjs`. Pages with synchronized language tabs use MDX.
The site reads the SDK, deployment, and examples READMEs directly from the repository.

With Node 24, run `npm ci --prefix docs` and `npm run dev --prefix docs` to
preview the guides and search. To serve a static export, run `npm run build --prefix docs` followed by
`npm run preview --prefix docs`.

For the full site with rustdoc, TypeDoc, and pdoc, install the Node SDK dependencies
(`npm ci --prefix bindings/node`) and Python docs tools
(`python -m pip install -r docs/tooling/requirements.txt`). Run
`python scripts/build-docs.py`, then `python -m http.server --directory target/site`.
