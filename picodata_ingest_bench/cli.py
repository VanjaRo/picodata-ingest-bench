from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, cast

from picodata_ingest_bench.config import (
    BenchmarkMethod,
    CLUSTER_PROFILES,
    ExecutionRuntime,
    FairnessMode,
    RUN_PRESETS,
    RunMode,
    ServerProfileScope,
    distribution_mode_label,
)
from picodata_ingest_bench.harness import repo_root_path
from picodata_ingest_bench.planner import build_benchmark_plan, render_plan_text
from picodata_ingest_bench.runtime import (
    DEFAULT_CONTAINER_IMAGE,
    maybe_delegate_run_to_container,
    report_runtime_label,
    resolve_runtime,
)
from picodata_ingest_bench.runner import RunArgs, run_benchmark
from picodata_ingest_bench.results import display_path, render_text_summary
from picodata_ingest_bench.workloads import WORKLOADS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark prepared multi-row INSERT against COPY FROM STDIN using Picodata scaffolding.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run the ingest benchmark suite")
    _add_run_arguments(run_parser)

    plan_parser = subparsers.add_parser("plan", help="show what a benchmark run would execute")
    _add_run_arguments(plan_parser)

    describe_parser = subparsers.add_parser("describe", help="print available benchmark presets")
    describe_parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
    )

    return parser


def _add_run_arguments(run_parser: argparse.ArgumentParser) -> None:
    run_parser.add_argument(
        "--profile",
        choices=sorted(CLUSTER_PROFILES),
        required=True,
        help="cluster preset; use 'describe' to see the human-facing profile labels",
    )
    run_parser.add_argument(
        "--workload",
        choices=sorted(WORKLOADS),
        required=True,
        help="workload preset; indexed variants add secondary-index write cost",
    )
    run_parser.add_argument(
        "--method",
        choices=[member.value for member in BenchmarkMethod],
        default=BenchmarkMethod.ALL.value,
        help="which ingest path to measure: insert, copy, or both",
    )
    run_parser.add_argument(
        "--fairness",
        choices=[member.value for member in FairnessMode],
        default=FairnessMode.BOTH.value,
        help="fixed compares same logical batch sizes; tuned searches the knob grid; custom uses exact custom candidates",
    )
    run_parser.add_argument(
        "--custom-insert",
        action="append",
        default=[],
        metavar="BATCH:SYNC",
        help="exact INSERT candidate; example: --custom-insert 256:7813",
    )
    run_parser.add_argument(
        "--custom-copy",
        action="append",
        default=[],
        metavar="BATCH:CHUNK",
        help="exact COPY candidate; example: --custom-copy 4096:16384",
    )
    run_parser.add_argument(
        "--mode",
        choices=[member.value for member in RunMode],
        default=RunMode.SMOKE.value,
        help="smoke reuses one cluster; reference gives each candidate a fresh cluster",
    )
    run_parser.add_argument("--scale", type=int, help="measured row count override")
    run_parser.add_argument("--output", type=Path, help="optional JSON output path")
    run_parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="stdout format; run writes full JSON to --output when provided",
    )
    run_parser.add_argument("--seed", type=int, default=1)
    run_parser.add_argument("--concurrency", type=int, default=1)
    run_parser.add_argument(
        "--candidate-index",
        type=int,
        help="1-based candidate number from `plan`; narrows run to one measured candidate",
    )
    run_parser.add_argument(
        "--runtime",
        choices=[member.value for member in ExecutionRuntime],
        default=ExecutionRuntime.AUTO.value,
        help="Benchmark host runtime. 'auto' uses native on Linux and a Linux container elsewhere.",
    )
    run_parser.add_argument(
        "--picodata-source",
        type=Path,
        help="Picodata source checkout to build and benchmark; defaults to PICODATA_SOURCE, cwd, or sibling ../picodata",
    )
    run_parser.add_argument(
        "--container-image",
        default=DEFAULT_CONTAINER_IMAGE,
        help="Container image to use when --runtime resolves to 'container'.",
    )
    run_parser.add_argument(
        "--reuse-container-build",
        action="store_true",
        help="container runtime only: skip Picodata rebuild and reuse target/ingest-linux from a previous container run",
    )
    run_parser.add_argument(
        "--base-port",
        type=int,
        help="optional base port for benchmark-spawned Picodata instances",
    )
    run_parser.add_argument(
        "--server-profile",
        action="store_true",
        help="collect per-instance perf folded stacks and flamegraph artifacts around each measured trial",
    )
    run_parser.add_argument(
        "--server-profile-scope",
        choices=[member.value for member in ServerProfileScope],
        default=ServerProfileScope.ALL.value,
        help="all profiles every measured candidate; selected requires exactly one candidate, usually via --candidate-index",
    )
    run_parser.add_argument(
        "--server-profile-dir",
        type=Path,
        help="directory for server profiling artifacts; defaults next to --output when profiling is enabled",
    )
    run_parser.add_argument(
        "--server-profile-frequency",
        type=int,
        default=99,
        help="perf sampling frequency in Hz when --server-profile is enabled",
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    args = parser.parse_args(argv)

    if args.command == "describe":
        payload = {
            "profiles": {
                key: {
                    "instance_count": profile.instance_count,
                    "distribution_sql": profile.distribution_sql,
                    "distribution_mode": profile.distribution_mode,
                    "display_name": profile.display_name,
                    "description": profile.description,
                }
                for key, profile in sorted(CLUSTER_PROFILES.items())
            },
            "workloads": {
                key: {
                    "description": workload.description,
                    "columns": [
                        {
                            "name": column.name,
                            "sql_type": column.sql_type,
                            "nullable": column.nullable,
                            "primary_key": column.primary_key,
                        }
                        for column in workload.columns
                    ],
                    "indexes": [
                        {
                            "name": index.name,
                            "columns": list(index.columns),
                        }
                        for index in workload.secondary_indexes
                    ],
                    "shape": dict(workload.shape),
                }
                for key, workload in sorted(WORKLOADS.items())
            },
            "modes": {
                mode.value: {
                    "default_row_count": preset.default_row_count,
                    "warmup_row_count": preset.warmup_row_count,
                    "fixed_batch_rows": list(preset.fixed_batch_rows),
                    "tuned_batch_rows": list(preset.tuned_batch_rows),
                    "fixed_copy_chunk_bytes": preset.fixed_copy_chunk_bytes,
                    "copy_chunk_bytes": list(preset.copy_chunk_bytes),
                    "fixed_insert_pipeline_sync_batches": preset.fixed_insert_pipeline_sync_batches,
                    "tuned_insert_pipeline_sync_batches": list(preset.tuned_insert_pipeline_sync_batches),
                    "instance_memtx_memory": preset.instance_memtx_memory,
                }
                for mode, preset in RUN_PRESETS.items()
            },
            "methods": [member.value for member in BenchmarkMethod],
            "fairness": [member.value for member in FairnessMode],
            "server_profile_scopes": [member.value for member in ServerProfileScope],
        }
        if args.format == "json":
            sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        else:
            sys.stdout.write(_render_describe_text(payload) + "\n")
        return 0

    try:
        custom_insert_configs = tuple(_parse_custom_pair(value, "--custom-insert") for value in args.custom_insert)
        custom_copy_configs = tuple(_parse_custom_pair(value, "--custom-copy") for value in args.custom_copy)
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")
    fairness = FairnessMode.CUSTOM if custom_insert_configs or custom_copy_configs else FairnessMode(args.fairness)
    try:
        picodata_source = repo_root_path(args.picodata_source)
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")

    plan = build_benchmark_plan(
        profile=args.profile,
        workload=args.workload,
        method=BenchmarkMethod(args.method),
        fairness=fairness,
        mode=RunMode(args.mode),
        scale=args.scale,
        concurrency=args.concurrency,
        seed=args.seed,
        execution_runtime=resolve_runtime(ExecutionRuntime(args.runtime)).value,
        picodata_source=picodata_source,
        reuse_container_build=args.reuse_container_build,
        base_port=args.base_port,
        candidate_index=args.candidate_index,
        custom_insert_configs=custom_insert_configs,
        custom_copy_configs=custom_copy_configs,
        output=args.output,
        server_profile=args.server_profile,
        server_profile_scope=ServerProfileScope(args.server_profile_scope),
        server_profile_dir=args.server_profile_dir,
        server_profile_frequency=args.server_profile_frequency,
    )

    if args.command == "plan":
        if args.format == "json":
            sys.stdout.write(json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n")
        else:
            sys.stdout.write(render_plan_text(plan) + "\n")
        return 0 if not plan.errors else 2

    if plan.errors:
        parser.exit(2, "error: " + "; ".join(plan.errors) + "\n")

    try:
        delegated_exit_code = maybe_delegate_run_to_container(
            argv=argv,
            requested_runtime=ExecutionRuntime(args.runtime),
            container_image=args.container_image,
            picodata_source=picodata_source,
        )
    except RuntimeError as exc:
        parser.exit(2, f"error: {exc}\n")
    if delegated_exit_code is not None:
        return delegated_exit_code

    try:
        report = run_benchmark(
            RunArgs(
                profile=args.profile,
                workload=args.workload,
                method=BenchmarkMethod(args.method),
                fairness=fairness,
                mode=RunMode(args.mode),
                scale=args.scale,
                output=args.output,
                seed=args.seed,
                concurrency=args.concurrency,
                execution_runtime=report_runtime_label(ExecutionRuntime(args.runtime)),
                picodata_source=picodata_source,
                reuse_container_build=args.reuse_container_build,
                base_port=args.base_port,
                candidate_index=args.candidate_index,
                custom_insert_configs=custom_insert_configs,
                custom_copy_configs=custom_copy_configs,
                server_profile=args.server_profile,
                server_profile_scope=ServerProfileScope(args.server_profile_scope),
                server_profile_dir=args.server_profile_dir,
                server_profile_frequency=args.server_profile_frequency,
            )
        )
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")
    if args.format == "json":
        sys.stdout.write(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n")
    else:
        summary = render_text_summary(report)
        if args.output is not None:
            summary += f"\nJSON: {display_path(str(args.output))}"
        sys.stdout.write(summary + "\n")
    return 0


def _render_describe_text(payload: dict[str, object]) -> str:
    profiles = cast(dict[str, dict[str, Any]], payload["profiles"])
    workloads = cast(dict[str, dict[str, Any]], payload["workloads"])
    modes = cast(dict[str, dict[str, Any]], payload["modes"])
    methods = cast(list[str], payload["methods"])
    fairness_modes = cast(list[str], payload["fairness"])
    server_profile_scopes = cast(list[str], payload["server_profile_scopes"])

    lines = ["Profiles:"]
    for key, profile in sorted(profiles.items()):
        lines.append(
            "  "
            + f"{profile['display_name']} [{key}]; "
            + f"distribution={distribution_mode_label(profile['distribution_mode'])}; "
            + f"instances={profile['instance_count']}"
        )

    lines.append("Workloads:")
    for key, workload in sorted(workloads.items()):
        column_count = len(workload["columns"])
        index_count = len(workload["indexes"])
        shape = cast(dict[str, Any], workload.get("shape") or {})
        index_label = (
            "no secondary indexes"
            if index_count == 0
            else "1 secondary index"
            if index_count == 1
            else f"{index_count} secondary indexes"
        )
        shape_text = _workload_shape_text(shape)
        suffix = f"; {shape_text}" if shape_text else ""
        description = str(workload["description"]).rstrip(".")
        lines.append("  " + f"{key}: {column_count} columns, {index_label}; {description}{suffix}")
    lines.append(
        "  note: kafka-like means event-shaped table rows loaded through INSERT/COPY, "
        + "not a Kafka broker/protocol benchmark; narrow means small rows for protocol and batching baseline"
    )

    lines.append("Modes:")
    for key, mode in sorted(modes.items()):
        mode_label = (
            "quick run"
            if key == RunMode.SMOKE.value
            else "isolated fresh-cluster run"
            if key == RunMode.REFERENCE.value
            else key
        )
        lines.append(
            "  "
            + f"{key}: {mode_label}; "
            + f"default_rows={mode['default_row_count']}, "
            + f"warmup_rows={mode['warmup_row_count']}, "
            + f"memtx={mode['instance_memtx_memory']}, "
            + f"fixed_copy_chunk={mode['fixed_copy_chunk_bytes']}, "
            + f"fixed_insert_sync={mode['fixed_insert_pipeline_sync_batches']}"
        )

    lines.append("Methods: " + ", ".join(methods))
    lines.append("Fairness: " + ", ".join(fairness_modes))
    lines.append("Server profile scopes: " + ", ".join(server_profile_scopes))
    return "\n".join(lines)


def _parse_custom_pair(value: str, option_name: str) -> tuple[int, int]:
    normalized = value.replace(",", ":")
    parts = normalized.split(":")
    if len(parts) != 2:
        raise ValueError(f"{option_name} expects BATCH:VALUE, got {value!r}")
    try:
        first = int(parts[0])
        second = int(parts[1])
    except ValueError as exc:
        raise ValueError(f"{option_name} expects positive integers, got {value!r}") from exc
    if first <= 0 or second <= 0:
        raise ValueError(f"{option_name} expects positive integers, got {value!r}")
    return first, second


def _workload_shape_text(shape: dict[str, Any]) -> str:
    parts: list[str] = []
    if payload_bytes := shape.get("payload_total_bytes"):
        parts.append(f"payload={payload_bytes}B")
    if partition_count := shape.get("partition_count"):
        parts.append(f"partitions={partition_count}")
    if tenant_cardinality := shape.get("tenant_cardinality"):
        parts.append(f"tenants={tenant_cardinality}")
    if stream_cardinality := shape.get("stream_cardinality"):
        parts.append(f"streams={stream_cardinality}")
    if timestamp_step := shape.get("timestamp_step_seconds"):
        parts.append(f"timestamp_step={timestamp_step}s")
    if value_pattern := shape.get("value_pattern"):
        parts.append(f"value={value_pattern}")
    return ", ".join(parts)
