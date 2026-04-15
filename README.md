# Ingest Benchmark Suite

Standalone benchmark suite for comparing Picodata ingest through:

- prepared multi-row `INSERT`
- `COPY FROM STDIN`

The suite lives outside the Picodata source tree, but it needs a Picodata
checkout to build the server and start benchmark clusters. It is intended first
for development work: smoke checks, branch comparisons, candidate tuning, and
profiling suspicious results.

The runner reuses Picodata's existing test scaffolding for cluster lifecycle,
instance startup, and COPY behavior. The benchmark code owns the workload
generation, candidate planning, result reporting, and optional server profiling.

## Before You Run

Run commands from this repository root.

Picodata source is resolved in this order:

1. `--picodata-source /path/to/picodata`
2. `PICODATA_SOURCE`
3. the current directory, if it is a Picodata checkout
4. sibling `../picodata`

For meaningful numbers, use a release-style Picodata build and a stable Linux
host. On macOS, prefer `--runtime auto` or `--runtime container` over carrying
macOS-specific dependency fixes in this repository. Container runs are useful
for local and relative comparisons, but publishable throughput numbers should
come from Linux bare metal or a dedicated Linux VM with local storage.

Examples below use:

```bash
uv run python -m picodata_ingest_bench ...
```

If the package is already installed in a virtualenv, use:

```bash
python -m picodata_ingest_bench ...
```

or:

```bash
picodata-ingest-bench ...
```

## Choose the Benchmark

Start by deciding what question you are trying to answer. Then choose the flags
that match that question.

| Question | Use |
| --- | --- |
| Does the benchmark still run? | `--mode smoke`, small/default scale, usually `--method all --fairness fixed` |
| Which branch is faster? | `--mode reference`, same Picodata build profile, same host, same workload, same candidate set |
| Is COPY faster than INSERT at the same logical batch size? | `--method all --fairness fixed --mode reference`, then inspect `summary.same_batch_comparisons` |
| Which checked-in tuning knob wins? | `--fairness tuned`, then inspect `summary.best_by_method` and `trials` |
| Do these exact settings behave as expected? | `--custom-insert BATCH:SYNC` or `--custom-copy BATCH:CHUNK` |
| Where is server CPU time going? | Select one candidate with `--candidate-index`, then add `--server-profile --server-profile-scope selected` |

### Profiles

| `--profile` | Meaning |
| --- | --- |
| `multi-node-sharded` | 3-node cluster, table distributed by `id`. Use this for sharded ingest behavior. |
| `multi-node-unsharded` | 3-node cluster, global table. This is the historical CLI key; reports label it as `3-node global table`. |

### Workloads

| `--workload` | Shape | Use when |
| --- | --- | --- |
| `narrow` | `id INTEGER PRIMARY KEY`, short `TEXT` value | You want a small-row baseline where protocol and batching overhead are visible. |
| `kafka-like` | `id`, timestamp, message key, deterministic 232-byte payload | You want event-shaped rows with timestamp/text conversion and larger row bytes. This is not a Kafka broker/protocol benchmark. |
| `narrow-indexed` | Narrow row plus tenant/stream dimensions and 3 secondary indexes | You want secondary-index write amplification without large payload work. |
| `kafka-like-indexed` | Event-shaped row plus tenant dimension and 4 secondary indexes | You want the heaviest checked-in scenario: payload work plus index maintenance. |

### Methods and Fairness

| Flag | Values | Meaning |
| --- | --- | --- |
| `--method` | `insert`, `copy`, `all` | Which ingest path to measure. |
| `--fairness fixed` | same logical batch sizes | INSERT keeps the fixed pipeline sync window; COPY keeps the fixed client chunk size. Defaults are recorded in `summary.fixed_mode_defaults`. |
| `--fairness tuned` | checked-in search grid | INSERT varies batch size and sync window. COPY varies batch size and client chunk size. Every candidate printed by `plan` is measured. |
| `--fairness both` | fixed plus tuned | Useful for exploration, but it can produce many candidates. Check `plan` first. |
| custom candidates | `--custom-insert BATCH:SYNC`, `--custom-copy BATCH:CHUNK` | Runs only the exact listed candidates and sets fairness to `custom`. |

### Modes and Runtime

| Flag | Values | Meaning |
| --- | --- | --- |
| `--mode smoke` | one shared cluster | Cheap local check. Candidates run against one benchmark cluster. Reports use `candidate_isolation=reused_cluster`. |
| `--mode reference` | fresh cluster per candidate | Branch-comparison mode. Reports use `candidate_isolation=fresh_cluster_per_candidate`. |
| `--runtime auto` | default | Native on Linux, container elsewhere. |
| `--runtime native` | force host runtime | Use when the host can build and run Picodata directly. |
| `--runtime container` | force benchmark Linux image | Use on non-Linux hosts or when you want the benchmark-owned Linux image. |
| `--reuse-container-build` | container only | Reuse existing `target/ingest-linux`; use only after a successful container build for the current code. |

## Plan Before Running

`plan` is the evidence for what `run` will do. Use it before expensive runs,
profiling, or branch comparisons.

```bash
uv run python -m picodata_ingest_bench plan \
  --picodata-source ../picodata \
  --runtime native \
  --profile multi-node-sharded \
  --workload narrow \
  --method all \
  --fairness fixed \
  --mode smoke
```

Check these lines in the human plan:

- `rows`: measured rows and warmup rows
- `candidates`: how many measured trials will run
- `clusters`: `1` for smoke; usually candidate count for reference mode
- `Candidates`: candidate numbers and method-specific knobs
- `Errors`: anything listed here means the run will fail validation

For scripts, use JSON and inspect the fields directly:

```bash
uv run python -m picodata_ingest_bench plan \
  --format json \
  --picodata-source ../picodata \
  --runtime native \
  --profile multi-node-sharded \
  --workload narrow \
  --method copy \
  --fairness fixed \
  --mode smoke \
| jq '{
    row_count,
    warmup_row_count,
    candidate_count,
    cluster_count,
    candidates,
    errors
  }'
```

Use `candidate_index` from the plan when you want to rerun one candidate:

```bash
uv run python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --runtime native \
  --profile multi-node-sharded \
  --workload kafka-like-indexed \
  --method copy \
  --fairness fixed \
  --mode reference \
  --candidate-index 1 \
  --output /tmp/ingest-bench.json
```

For exact settings, skip the preset grid:

```bash
uv run python -m picodata_ingest_bench plan \
  --picodata-source ../picodata \
  --runtime native \
  --profile multi-node-sharded \
  --workload kafka-like-indexed \
  --method all \
  --mode reference \
  --scale 5000000 \
  --custom-insert 256:7813 \
  --custom-copy 4096:16384
```

That plan should contain only the custom INSERT and COPY candidates you listed.

## Run and Save a Report

Prefer writing JSON to a file. Stdout is useful for a quick sanity check, but
the JSON report is the source of truth for comparisons.

```bash
uv run python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --runtime container \
  --profile multi-node-sharded \
  --workload narrow \
  --method all \
  --fairness fixed \
  --mode smoke \
  --output /tmp/ingest-bench.json
```

Use `--format json` only when you want the full report on stdout too.

## Read the Report

Start with `summary`. It is the compact view used by the text summary.

```bash
jq '.summary' /tmp/ingest-bench.json
```

### Find the Winner

```bash
jq '.summary.best_by_method' /tmp/ingest-bench.json
```

This groups winners by `method:fairness`, for example `insert:fixed` or
`copy:tuned`. Use `rows_per_second` for cross-method throughput comparisons.
Also check the selected knobs:

- `configured_batch_rows`
- `configured_insert_pipeline_sync_batches`
- `configured_copy_chunk_bytes`
- `trial_id`

### Compare INSERT and COPY at the Same Batch Size

Use this only for `reference` runs with fixed fairness:

```bash
jq '.summary.same_batch_comparisons.fixed' /tmp/ingest-bench.json
```

This view compares the best fixed INSERT trial and best fixed COPY trial for the
same logical `batch_rows`. For `smoke` runs it is empty because candidates share
one cluster and are not isolated enough for this comparison.

### Inspect Every Candidate

```bash
jq '.trials[] | {
  trial_id,
  method: .metrics.method,
  fairness: .metrics.fairness,
  batch_rows: .metrics.configured_batch_rows,
  copy_chunk_bytes: .metrics.configured_copy_chunk_bytes,
  insert_sync_batches: .metrics.configured_insert_pipeline_sync_batches,
  rows_per_second: .metrics.rows_per_second,
  wall_seconds: .metrics.wall_seconds,
  client_protocol_bytes: .metrics.client_protocol_bytes
}' /tmp/ingest-bench.json
```

Use `trials` when you need to explain why a candidate won or lost. It contains
per-candidate timing, payload, latency, resource, validation, and profiling
data.

### Check Run Context

```bash
jq '{
  generated_at,
  build_profile,
  execution_runtime,
  profile,
  workload,
  mode,
  candidate_isolation,
  requested_method,
  requested_fairness,
  requested_rows,
  warmup_rows,
  git_metadata,
  runtime_host_metadata
}' /tmp/ingest-bench.json
```

These fields are the evidence that two reports are comparable. Branch
comparisons should match on workload, mode, candidate set, runtime class, host
class, build profile, and scale.

### Interpret Metrics

| Field | How to use it |
| --- | --- |
| `rows_per_second` | Main cross-method throughput metric. |
| `logical_bytes_per_second` | Throughput over generated row payload bytes. |
| `client_protocol_bytes` | Benchmark-side SQL text plus parameter/COPY payload bytes. It excludes pgproto frame overhead. |
| `sync_window_latency_ms` | INSERT-specific latency summary. Descriptive, not directly comparable to COPY latency. |
| `client_write_latency_ms` | COPY-specific latency summary. Descriptive, not directly comparable to INSERT latency. |
| `worker_phase_breakdown` | Client-side worker time split into start, build, submit, finish, protocol teardown, and unattributed time. |
| `resource_snapshots` | Server metrics before/after the measured trial. On Linux, includes per-instance child-process CPU/RSS deltas. |

Successful text output includes a validation line. For JSON-level inspection,
look at per-trial row counts and validation details:

```bash
jq '.trials[] | {
  trial_id,
  row_count,
  total_rows: .metrics.total_rows,
  log_validation: .metrics.log_validation,
  sharded_distribution: .metrics.sharded_distribution
}' /tmp/ingest-bench.json
```

## Server Profiling

Use profiling only after a normal run shows something worth investigating.
Profiling samples Picodata server processes around the measured trial, skips
warmup, and can materially change run cost.

Profile one selected candidate when possible:

```bash
uv run python -m picodata_ingest_bench run \
  --picodata-source ../picodata \
  --runtime native \
  --profile multi-node-sharded \
  --workload kafka-like-indexed \
  --method copy \
  --fairness fixed \
  --mode reference \
  --candidate-index 1 \
  --server-profile \
  --server-profile-scope selected \
  --output /tmp/ingest-bench.json
```

`--server-profile-scope selected` requires exactly one measured candidate. Use
`--candidate-index` or a single custom candidate to satisfy that requirement.

If you pass `--output /tmp/ingest-bench.json`, the default profile root is:

```text
/tmp/ingest-bench-profiles
```

If you do not pass `--output`, the default is `tmp/ingest-profiles`. An explicit
`--server-profile-dir` overrides both defaults.

Each profiled trial gets its own subdirectory under the profile root. The
subdirectory name is the trial table name, also recorded as `trial_id` in the
report.

The report records the exact artifact paths:

```bash
jq '.summary.server_profile.trials[] | {
  trial_id,
  method,
  fairness,
  output_dir,
  instances: [.instances[] | {
    name,
    pid,
    artifacts,
    artifact_errors
  }]
}' /tmp/ingest-bench.json
```

Open artifacts in this order:

1. `perf_svg` / `flamegraph`: browser view of hot stack width.
2. `perf_report` / `report`: text view of sampled functions.
3. `perf_data` / `data`: raw input for external `perf report`.
4. `perf_out` / `script`: expanded perf script output.
5. `perf_folded` / `folded`: folded stacks used to build the flamegraph.

Artifact names include the Picodata instance name, usually `ingest_1`,
`ingest_2`, and `ingest_3` for the 3-node profiles. Compare instances when the
workload should be spread across the cluster.

Category percentages in the text summary are an orientation aid. Use the
flamegraph or perf report before making a tuning decision.

## Command Reference

Use `describe` when you need the current built-in values:

```bash
uv run python -m picodata_ingest_bench describe
uv run python -m picodata_ingest_bench describe --format json
```

Use `plan` whenever run cost matters:

```bash
uv run python -m picodata_ingest_bench plan [run options]
uv run python -m picodata_ingest_bench plan --format json [run options]
```

Use `run` for measurement:

```bash
uv run python -m picodata_ingest_bench run [run options] --output /tmp/ingest-bench.json
```
