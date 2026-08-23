# Native vector benchmark harness

`scripts/native-vector-search-benchmark.mjs` reads source inputs and build
manifests, copies them through held file descriptors into a private `0700`
workspace, and emits one shareable JSON artifact on stdout. Capture stdout via
the runner/API; never redirect it into a live data path.

The build manifest beside each binary (`<binary>.manifest.json`) must contain
`schema`, `sourceSha`, `binarySha256`, `cargoProfile`, `rustcVersion`, and
`buildCommand`. The harness rejects stale or modified binaries, missing
generation descriptors, WAL/recovery state, and ambiguous blob identities.

After a clean exact checkout and supported build, generate the attestation
before running the benchmark:

```sh
node scripts/native-build-manifest.mjs \
  --repo /path/to/aira-graphdb \
  --binary /path/to/aira-graphdb/target/release/aira-graphdb-native \
  --source-sha "$(git -C /path/to/aira-graphdb rev-parse HEAD)" \
  --cargo-profile release \
  --rustc-version "$(rustc --version)" \
  --build-command 'cargo build --release --bin aira-graphdb-native'
```

SIGINT/SIGTERM kills active native process groups, removes the owned temporary
workspace in `finally`, and exits `128+signal`. SIGKILL cannot run cleanup. A
stale workspace is safe to remove only after confirming it is a directory named
`aira-vector-benchmark-*`, owned by the current user, mode `0700`, and not
currently referenced by a running benchmark; do not use broad recursive
deletion or remove live Literature Hub data.

The unsampled counterbalanced phase is the latency authority. The sampled
counterbalanced phase is memory telemetry only; timing across phases and
page-cache-cold claims are invalid. `--sample-interval-ms 0` aliases the
unsampled phase and performs no `/proc` reads.
