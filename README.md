# Ingest Benchmark Suite

This standalone benchmark suite compares:

- optimized multi-row `INSERT`
- `COPY FROM STDIN`

through one Picodata-native harness. It lives outside the Picodata source tree, but still needs a Picodata checkout to build the server and start benchmark clusters.

The suite is intended first for ongoing development and branch comparison.

## What It Reuses

The runner deliberately builds on existing Picodata scaffolding:

- cluster and instance lifecycle from the Picodata checkout's `test/conftest.py`
- COPY usage patterns from the Picodata checkout's `test/pgproto/copy_from_stdin_test.py`
- existing insert benchmark context from the Picodata checkout's `benchmark/batched-inserts/`

## Requirements

- run from this benchmark repository root
- keep a Picodata checkout available via `--picodata-source /path/to/picodata`, `PICODATA_SOURCE`, the current directory, or a sibling `../picodata`
- use a built Picodata binary available via the current Cargo build profile
- prefer a release-style profile for meaningful measurements
- use a stable machine when comparing branches

## Recommended Environment

- Treat Linux as the benchmark host. For numbers you want to compare across branches, run the suite either on Linux bare metal or inside a dedicated Linux VM.
- On macOS, prefer Linux virtualization over patching local dependency builds. `colima`, `lima`, or another Linux VM is a better fit than carrying macOS-specific CMake fixes in the repository.
- Keep using the same Picodata test harness inside that Linux environment. The goal is to move the execution environment, not to replace `test/conftest.py`.
- The benchmark has its own Linux image so it does not depend on the x86-oriented `build_base` path.
- `python -m picodata_ingest_bench run --runtime auto ...` uses that same image and will build it on first use if needed.
- For publishable or tuning-grade throughput numbers, prefer a VM with local Linux storage over a bind-mounted container filesystem. Containers are still useful for development and relative comparisons.

## Quick Start

Describe available presets:

```bash
python -m picodata_ingest_bench describe
```

Recommended workflow:

1. `describe`
   See the available profiles, workloads, and mode defaults in human-readable form.
2. `plan`
   Choose the profile, workload, mode, method, and fairness mix you want to compare.
   This prints the exact candidate matrix, cluster count, row count, and profiling cost before execution.
3. `run`
   Execute the chosen plan.
   Use `--mode smoke` for a fast local check and `--mode reference` for a fresh-cluster comparison.

## Workload Scenarios

- `narrow`
  Two columns: integer primary key plus short text.
  This is the small-row baseline. Use it to see protocol batching, prepared statement, COPY framing, and per-row overhead with little payload work.
- `kafka-like`
  Four columns: integer primary key, timestamp, message key, and deterministic 232-byte payload.
  This is the event-ingest shape. Use it when payload construction, timestamp/text conversion, and larger row bytes should be part of the result.
- `narrow-indexed`
  Narrow rows plus tenant/stream dimensions and 3 secondary indexes.
  This keeps rows small but adds index maintenance, so it isolates write amplification from secondary indexes.
- `kafka-like-indexed`
  Event-shaped rows plus tenant dimension and 4 secondary indexes.
  This combines event payload cost with index write amplification. Use it as the heaviest checked-in ingest scenario.

Kafka-like scenarios are not Kafka broker/protocol benchmarks.
They do not include producer ACKs, broker partitioning, consumer flow, Kafka network hops, or Kafka serialization.
They only model a table row shape common in event ingestion: timestamped keyed records with payload bytes.
The actual measured path is still Picodata ingestion through prepared multi-row `INSERT` or `COPY FROM STDIN`.

Narrow scenarios are the non-Kafka-like baseline.
Rows are intentionally small, so payload generation and text/timestamp conversion do not dominate the run.
Use them to expose client protocol, batching, and per-row database overhead.

Run a smoke benchmark:

```bash
python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --profile multi-node-sharded \
  --workload narrow \
  --method all \
  --fairness both \
  --mode smoke \
  --output /tmp/ingest-bench.json
```

That prints a compact text summary to stdout and writes the full JSON report to `--output`.
Use `--format json` only when you want the full report on stdout too.

Inspect one candidate before collecting server profiles:

```bash
python -m picodata_ingest_bench plan \
  --picodata-source ../picodata \
  --profile multi-node-sharded \
  --workload narrow \
  --method copy \
  --fairness fixed \
  --mode smoke
```

Then run a selected candidate with profiling:

```bash
python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --profile multi-node-sharded \
  --workload narrow \
  --method copy \
  --fairness fixed \
  --mode smoke \
  --candidate-index 1 \
  --server-profile \
  --server-profile-scope selected \
  --output /tmp/ingest-bench.json
```

On non-Linux hosts, `--runtime auto` delegates `run` to the benchmark's Linux image:

```bash
python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --runtime auto \
  --profile multi-node-unsharded \
  --workload kafka-like \
  --method all \
  --fairness both \
  --mode reference \
  --output /tmp/ingest-bench.json
```

After one successful container build, reuse the existing Linux target artifact when you only want to rerun the suite:

```bash
python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --runtime container \
  --reuse-container-build \
  --profile multi-node-sharded \
  --workload kafka-like-indexed \
  --method all \
  --fairness fixed \
  --mode reference \
  --server-profile \
  --server-profile-scope all \
  --output /tmp/ingest-bench.json
```

`--reuse-container-build` skips the inner `make build-release` step.
Use it only after the container has already built `target/ingest-linux` for the current code.
`--picodata-source` makes the Picodata checkout explicit; it defaults to `PICODATA_SOURCE`, the current directory when it is a Picodata checkout, or a sibling `../picodata`.
Container runs mount that source at `/work/picodata` and rewrite reports back to host paths.

Run exact candidates instead of a preset grid:

```bash
python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --runtime container \
  --reuse-container-build \
  --profile multi-node-sharded \
  --workload kafka-like-indexed \
  --method all \
  --mode reference \
  --scale 5000000 \
  --custom-insert 256:7813 \
  --custom-copy 4096:16384 \
  --server-profile \
  --server-profile-scope all \
  --output /tmp/ingest-bench.json
```

`--custom-insert BATCH:SYNC` and `--custom-copy BATCH:CHUNK` imply custom fairness and run only the listed candidates.

Run the same benchmark explicitly through the Python-owned container wrapper:

```bash
python3 -m picodata_ingest_bench.container --container-image picodata-ingest-bench-base run -- \
  --profile multi-node-unsharded \
  --workload kafka-like \
  --method all \
  --fairness both \
  --mode reference \
  --output /tmp/ingest-bench.json
```

The wrapper accepts `--container-image` before the subcommand.
`picodata_ingest_bench.container describe` and `picodata_ingest_bench.container plan` are metadata-only and do not inspect or build a container image.

## Contract

- `multi-node-unsharded` is the historical CLI key for the global-table profile.
  The report and summaries describe it as `3-node global table` to make that meaning explicit.
- `describe` is the contract discovery command.
  It reports profiles, workloads, modes, methods, fairness modes, and server profiling scopes.
- `plan` is the suite planning contract.
  It records the chosen profile/workload/mode/method/fairness combination before execution.
- `run` executes the selected benchmark plan.
  Use small `--scale` values for local smoke checks and the checked-in mode defaults for branch comparison.
- The JSON output is the source-of-truth result format.
- The report's `summary` block is the primary human-facing view.
- `same_batch_comparisons` is the same-logical-batch-size view for `reference` runs.
- `trials` is the detailed source-of-truth record for each measured candidate.
- `same_batch_comparisons` compares the best insert trial against the best COPY trial at the same logical batch size.
  This summary is emitted only for `reference` runs, where each candidate gets a fresh cluster.
- Additional indexed workloads are available when you want to measure write amplification from secondary indexes:
  `narrow-indexed` and `kafka-like-indexed`.
- Fixed fairness compares equal logical batch sizes.
- Fixed fairness keeps method-specific transport defaults.
- The report records those fixed-mode defaults explicitly:
  `fixed_mode_defaults.insert_pipeline_sync_batches` and `fixed_mode_defaults.copy_chunk_bytes`.
- Tuned fairness searches checked-in grids and records the winning settings.
  INSERT tunes both logical batch size and pipeline sync window; COPY tunes logical batch size and client chunk size.
  The checked-in grid is exhaustive for the selected mode; candidates are not skipped adaptively.
- Custom fairness is selected by `--custom-insert` or `--custom-copy` and runs only the exact listed candidates.
- Numeric inputs are validated during planning.
  `--scale`, `--concurrency`, custom candidate values, and `--server-profile-frequency` must be positive.
- `--candidate-index` runs one candidate by its 1-based number from `plan`.
  Use it for targeted checks and with `--server-profile-scope selected`.
- `smoke` mode reuses one benchmark cluster across candidates to stay cheap.
- `reference` mode uses a fresh cluster per candidate.
  The report calls this `candidate_isolation`, with values `reused_cluster` and `fresh_cluster_per_candidate`.
- `auto` runtime stays native on Linux and re-executes `run` inside a Linux container elsewhere.
- `--reuse-container-build` applies only to container runtime and reuses the existing `target/ingest-linux` build artifact.
  Native plans reject it instead of printing a misleading build mode.
- Trial metrics distinguish wall-clock time from accumulated worker time.
  The human summary prints one line per measured trial with worker wall and CPU time split into start, build, submit, finish, protocol teardown, and unattributed time.
- Latency summaries are method-specific.
  Insert reports `sync_window_latency_ms`; COPY reports `client_write_latency_ms`.
  Throughput is the cross-method comparison metric; latency is descriptive per method.
- The top-level report records `runtime_host_metadata` for the environment that executed the benchmark process.
- The report records client-to-server bytes as `client_protocol_bytes`.
  It is benchmark-side SQL text plus submitted parameter/COPY payload bytes, excluding pgproto frame overhead.
  It does not depend on verbose Picodata logs.
- The report also records `resource_snapshots` from server metrics before and after each measured trial.
  On Linux it also records per-instance Picodata child-process CPU/RSS deltas from `/proc`.
- `--server-profile` enables diagnostic server-side sampling around the measured trial.
  It collects folded stacks and flamegraph artifacts, so it is much more expensive than a normal run and should only be used when you are debugging a result.
- `--server-profile-scope all` profiles every measured candidate.
- `--server-profile-scope selected` profiles one explicitly selected candidate and rejects multi-candidate plans.
- Profiling never covers warmup; artifacts are grouped under the selected output root per trial/candidate.
- When profiling is enabled, the CLI summary prints per-instance artifact paths you can open in external tools:
  `data` (`perf.data`), `script` (`perf script` output), `folded`, `report`, and `flamegraph`.
- Artifact-generation failures are recorded stage by stage and printed next to the affected instance.
- The same paths are recorded in JSON under `summary.server_profile.trials[].instances[].artifacts`.
- On Linux, Picodata profiling follows this chain per instance:
  `perf record` writes raw `perf.data`, `perf report` writes the annotated text report, `perf script` writes expanded samples, `stackcollapse-perf.pl` writes folded stacks, and `flamegraph.pl` writes the SVG flamegraph.
  The `samples` line prints raw perf samples, folded-stack sample-period weight, and lost samples.
  The `sample weight by category` line uses the category names stored in `category_definitions`; COPY categories separate protocol handling, row decode, target encode, batch flush, remote dispatch, routing, and downstream Tarantool/memtx storage work.
  INSERT categories separate Bind decode, Execute/portal control, SQL dispatch/routing/materialization, tuple/request encoding, and downstream Tarantool/memtx storage work.
  Treat category percentages as a fast orientation view before opening the raw artifacts, not as exact attribution.
- `workload_spec.shape` records generator details such as payload size, timestamp cadence, and cardinalities.
- Summary setup/balance numbers share one `cluster_timing_scope`:
  `shared_cluster` for smoke runs and `candidate_average` for reference runs.
- Successful trials are required to pass row validation, clean-success metric checks, and log validation.
  A run that writes wrong rows, produces suspicious server log lines, or reports inconsistent method-specific metrics must fail instead of producing a successful report.
- Sharded profiles wait for cluster balancing before the timed portion starts, so startup is slower but does not affect measured ingest throughput.
- Benchmark presets apply an explicit `instance.memtx.memory` budget per mode so reference runs measure ingest throughput instead of the raw `64M` default memtx ceiling.
- Benchmark-spawned Picodata instances use `PICODATA_LOG_LEVEL=info` by default.
  Override with `INGEST_BENCH_PICODATA_LOG_LEVEL` only when debugging benchmark startup or protocol failures.
- Set `INGEST_BENCH_VERBOSE=1` only when you want to see harness startup/rebalance logs.
