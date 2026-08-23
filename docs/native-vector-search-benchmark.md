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
  --destination-dir /path/to/private/old-build \
  --source-sha "$(git -C /path/to/aira-graphdb rev-parse HEAD)"
```

The generator builds in a fresh private `CARGO_TARGET_DIR`, rechecks the
checkout after the build, and creates the binary-plus-manifest pair in a new
unguessable private directory.  That directory is a capability: it becomes an
authoritative build only when the generator emits its path in a successful
stdout token.  No shared output pathname is replaced, and failure cleanup is
limited to that invocation's target and result directories.

The generator runs Cargo in its own process group with a fixed 30-minute
deadline.  SIGINT/SIGTERM terminates that group, waits for it to be reaped,
and removes only the interrupted invocation's private directories. SIGKILL
cannot run cleanup; an orphan beginning `.cargo-target-` or `.build-result-`
must be checked for a live owning process before manual removal.

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

The canonical JSON descriptor is read from a fixed 64 KiB suffix matching the
native `State` serialization contract. The multi-gigabyte state is never
materialized in JavaScript; full-file integrity is streamed under the same
whole-run deadline and signal authority used for snapshot copying and RPCs.
