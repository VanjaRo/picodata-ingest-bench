from __future__ import annotations

from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import uuid
from typing import Any

import psycopg

from picodata_ingest_bench import picodata
from picodata_ingest_bench.config import (
    BenchmarkMethod,
    ClusterProfileSpec,
    FairnessMode,
    RunMode,
    ServerProfileScope,
    candidate_isolation_for_mode,
    get_cluster_profile,
    get_run_preset,
)
from picodata_ingest_bench.harness import PicodataBenchmarkCluster, load_runtime_modules, repo_root_path
from picodata_ingest_bench.methods.copy import CopyTrialConfig, run_copy_trial
from picodata_ingest_bench.methods.insert import InsertTrialConfig, run_insert_trial
from picodata_ingest_bench.planner import build_benchmark_plan
from picodata_ingest_bench.profiling import ServerProfiler
from picodata_ingest_bench.results import BenchmarkReport, TrialResult
from picodata_ingest_bench.workloads import (
    generate_rows,
    get_workload,
)


INTERESTING_SERVER_METRICS = {
    "pico_pgproto_copy_sessions_started_total",
    "pico_pgproto_copy_bytes_received_total",
    "pico_pgproto_copy_rows_inserted_total",
    "pico_pgproto_copy_batches_flushed_total",
    "pico_pgproto_copy_batch_bytes_total",
    "pico_pgproto_copy_batch_flush_duration_count",
    "pico_pgproto_copy_batch_flush_duration_sum",
    "pico_pgproto_copy_batch_rows_total",
    "pico_pgproto_copy_record_limit_errors_total",
    "pico_pgproto_copy_data_frames_total",
    "pico_pgproto_copy_flush_duration_count",
    "pico_pgproto_copy_flush_duration_sum",
    "pico_pgproto_copy_phase_duration_count",
    "pico_pgproto_copy_phase_duration_sum",
    "pico_pgproto_copy_rows_decoded_total",
    "pico_pgproto_copy_session_duration_count",
    "pico_pgproto_copy_session_duration_sum",
    "pico_pgproto_copy_sessions_completed_total",
    "pico_pgproto_insert_execute_duration_count",
    "pico_pgproto_insert_execute_duration_sum",
    "pico_pgproto_insert_requests_total",
    "pico_pgproto_insert_rows_inserted_total",
    "pico_sql_query_total",
    "pico_sql_query_duration_count",
    "pico_sql_query_duration_sum",
    "pico_sql_query_errors_total",
    "pico_sql_global_dml_query_total",
    "pico_sql_global_dml_query_retries_total",
    "pico_sql_temp_table_leases_total",
    "pico_sql_temp_table_lock_waits_total",
    "tnt_net_received",
    "tnt_net_sent",
    "tnt_cpu_system_time",
    "tnt_cpu_user_time",
    "tnt_info_memory_cache",
    "tnt_info_memory_data",
    "tnt_info_memory_index",
    "tnt_info_memory_lua",
    "tnt_info_memory_net",
    "tnt_info_memory_tx",
    "tnt_slab_arena_used",
    "tnt_slab_quota_used",
}

CLUSTER_PORT_RANGE = 500
VALIDATION_FETCH_ROWS = 1_024
SUSPICIOUS_LOG_RE = re.compile(r"(?i)(?:\spanicked at\s|\bpanic\b|\bsegmentation fault\b|\bassertion failed\b|\sE>\s)")
RESOURCE_SNAPSHOT_METRICS = {
    "cpu_user_seconds": "tnt_cpu_user_time",
    "cpu_system_seconds": "tnt_cpu_system_time",
    "memory_data_bytes": "tnt_info_memory_data",
    "memory_index_bytes": "tnt_info_memory_index",
    "memory_cache_bytes": "tnt_info_memory_cache",
    "memory_lua_bytes": "tnt_info_memory_lua",
    "memory_net_bytes": "tnt_info_memory_net",
    "memory_tx_bytes": "tnt_info_memory_tx",
    "slab_arena_used_bytes": "tnt_slab_arena_used",
    "slab_quota_used_bytes": "tnt_slab_quota_used",
}


@dataclass(frozen=True)
class RunArgs:
    profile: str
    workload: str
    method: BenchmarkMethod
    fairness: FairnessMode
    mode: RunMode
    scale: int | None
    output: Path | None
    seed: int
    concurrency: int
    execution_runtime: str
    picodata_source: Path
    reuse_container_build: bool = False
    base_port: int | None = None
    candidate_index: int | None = None
    custom_insert_configs: tuple[tuple[int, int], ...] = ()
    custom_copy_configs: tuple[tuple[int, int], ...] = ()
    server_profile: bool = False
    server_profile_scope: ServerProfileScope = ServerProfileScope.ALL
    server_profile_dir: Path | None = None
    server_profile_frequency: int = 99


def run_benchmark(args: RunArgs) -> BenchmarkReport:
    profile = get_cluster_profile(args.profile)
    workload = get_workload(args.workload)
    plan = build_benchmark_plan(
        profile=args.profile,
        workload=args.workload,
        method=args.method,
        fairness=args.fairness,
        mode=args.mode,
        scale=args.scale,
        concurrency=args.concurrency,
        seed=args.seed,
        execution_runtime=args.execution_runtime,
        picodata_source=args.picodata_source,
        reuse_container_build=args.reuse_container_build,
        base_port=args.base_port,
        candidate_index=args.candidate_index,
        custom_insert_configs=args.custom_insert_configs,
        custom_copy_configs=args.custom_copy_configs,
        output=args.output,
        server_profile=args.server_profile,
        server_profile_scope=args.server_profile_scope,
        server_profile_dir=args.server_profile_dir,
        server_profile_frequency=args.server_profile_frequency,
    )
    if plan.errors:
        raise ValueError("; ".join(plan.errors))

    preset = get_run_preset(args.mode)
    row_count = plan.row_count
    measured_rows = generate_rows(workload, row_count, args.seed)
    warmup_rows = generate_rows(workload, plan.warmup_row_count, args.seed + 1)
    run_candidates = [(candidate.method, candidate.config) for candidate in plan.candidates]
    trials: list[TrialResult] = []
    server_profile_root = plan.server_profile_dir

    if args.mode is RunMode.REFERENCE:
        for index, (method, candidate) in enumerate(run_candidates):
            with PicodataBenchmarkCluster(
                instance_count=profile.instance_count,
                wait_balanced=profile.is_sharded,
                port_start=_candidate_port_start(args.base_port, index),
                instance_memtx_memory=preset.instance_memtx_memory,
                picodata_source=args.picodata_source,
            ) as cluster:
                _log_candidate_start(index + 1, len(run_candidates), method, candidate, row_count)
                trial = _run_candidate(
                    cluster=cluster,
                    profile=profile,
                    workload=workload,
                    mode=args.mode,
                    seed=args.seed,
                    row_count=row_count,
                    method=method,
                    candidate=candidate,
                    warmup_rows=warmup_rows,
                    measured_rows=measured_rows,
                    server_profile_root=_candidate_profile_root(
                        server_profile_root=server_profile_root,
                        scope=args.server_profile_scope,
                        candidate_count=len(run_candidates),
                    ),
                    server_profile_frequency=args.server_profile_frequency,
                )
                trials.append(trial)
                _log_candidate_result(index + 1, len(run_candidates), method, candidate, trial)
    else:
        with PicodataBenchmarkCluster(
            instance_count=profile.instance_count,
            wait_balanced=profile.is_sharded,
            port_start=args.base_port,
            instance_memtx_memory=preset.instance_memtx_memory,
            picodata_source=args.picodata_source,
        ) as cluster:
            for index, (method, candidate) in enumerate(run_candidates):
                _log_candidate_start(index + 1, len(run_candidates), method, candidate, row_count)
                trial = _run_candidate(
                    cluster=cluster,
                    profile=profile,
                    workload=workload,
                    mode=args.mode,
                    seed=args.seed,
                    row_count=row_count,
                    method=method,
                    candidate=candidate,
                    warmup_rows=warmup_rows,
                    measured_rows=measured_rows,
                    server_profile_root=_candidate_profile_root(
                        server_profile_root=server_profile_root,
                        scope=args.server_profile_scope,
                        candidate_count=len(run_candidates),
                    ),
                    server_profile_frequency=args.server_profile_frequency,
                )
                trials.append(trial)
                _log_candidate_result(index + 1, len(run_candidates), method, candidate, trial)

    server_profile_config = {
        "enabled": args.server_profile,
        "scope": args.server_profile_scope.value,
        "frequency_hz": args.server_profile_frequency,
        "output_dir": str(server_profile_root) if server_profile_root is not None else None,
    }
    report = BenchmarkReport.new(
        build_profile=load_runtime_modules(args.picodata_source).cargo_build_profile(),
        execution_runtime=args.execution_runtime,
        git_metadata=_git_metadata(args.picodata_source),
        runtime_host_metadata=_host_metadata(),
        profile=profile.key,
        database=picodata.DATABASE_NAME,
        profile_spec={
            "display_name": profile.display_name,
            "instance_count": profile.instance_count,
            "distribution_sql": profile.distribution_sql,
            "distribution_mode": profile.distribution_mode,
            "description": profile.description,
        },
        workload=workload.key,
        workload_spec={
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
        },
        mode=args.mode.value,
        candidate_isolation=candidate_isolation_for_mode(args.mode),
        topology_ready_before_timing=True,
        requested_method=args.method.value,
        requested_fairness=args.fairness.value,
        concurrency=args.concurrency,
        seed=args.seed,
        requested_rows=row_count,
        warmup_rows=len(warmup_rows),
        candidate_space=plan.candidate_space,
        server_profile=server_profile_config,
        environment={
            "instance_memtx_memory": preset.instance_memtx_memory,
            "picodata_source": str(args.picodata_source),
            "reuse_container_build": args.reuse_container_build,
            "candidate_index": args.candidate_index,
            "available_candidate_count": plan.available_candidate_count,
            "custom_insert_configs": [
                {"batch_rows": batch_rows, "pipeline_sync_batches": sync_batches}
                for batch_rows, sync_batches in args.custom_insert_configs
            ],
            "custom_copy_configs": [
                {"batch_rows": batch_rows, "copy_chunk_bytes": chunk_bytes}
                for batch_rows, chunk_bytes in args.custom_copy_configs
            ],
        },
        trials=trials,
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n")
    return report


def _run_candidate(
    *,
    cluster,
    profile,
    workload,
    mode: RunMode,
    seed: int,
    row_count: int,
    method,
    candidate,
    warmup_rows,
    measured_rows,
    server_profile_root,
    server_profile_frequency: int,
) -> TrialResult:
    with cluster.connect_admin() as conn:
        dsn = cluster.admin_dsn()
        table_name = f"ingest_{method.value}_{uuid.uuid4().hex[:10]}"
        warmup_table = f"{table_name}_warmup" if warmup_rows else None
        try:
            _create_table_schema(conn, table_name, workload, profile)

            if warmup_table is not None:
                _create_table_schema(conn, warmup_table, workload, profile)
                _run_table_trial(
                    dsn=dsn,
                    cluster=cluster,
                    profile=profile,
                    method=method,
                    candidate=candidate,
                    workload=workload,
                    table_name=warmup_table,
                    rows=warmup_rows,
                    conn=conn,
                    server_profile_dir=None,
                    server_profile_frequency=server_profile_frequency,
                )
                _drop_table(dsn, warmup_table)
                warmup_table = None

            metrics = _run_table_trial(
                dsn=dsn,
                cluster=cluster,
                profile=profile,
                method=method,
                candidate=candidate,
                workload=workload,
                table_name=table_name,
                rows=measured_rows,
                conn=conn,
                server_profile_dir=(server_profile_root / table_name if server_profile_root is not None else None),
                server_profile_frequency=server_profile_frequency,
            )
        finally:
            if warmup_table is not None:
                _drop_table(dsn, warmup_table, tolerate_errors=mode is RunMode.REFERENCE)
            _drop_table(dsn, table_name, tolerate_errors=mode is RunMode.REFERENCE)

    return TrialResult(
        profile=profile.key,
        workload=workload.key,
        mode=mode.value,
        seed=seed,
        row_count=row_count,
        trial_id=table_name,
        cluster_setup_seconds=cluster.setup_seconds,
        cluster_balance_seconds=cluster.balance_seconds,
        metrics=metrics,
    )


def _log_candidate_start(
    index: int,
    total: int,
    method: BenchmarkMethod,
    candidate,
    row_count: int,
) -> None:
    parts = [
        f"[ingest] candidate {index}/{total}",
        f"rows={row_count}",
        f"method={method.value}",
        f"fairness={candidate.fairness}",
        f"batch={candidate.batch_rows}",
    ]
    if isinstance(candidate, InsertTrialConfig):
        parts.append(f"sync={candidate.pipeline_sync_batches}")
    elif isinstance(candidate, CopyTrialConfig):
        parts.append(f"chunk={candidate.copy_chunk_bytes}")
    print(" ".join(parts), file=sys.stderr, flush=True)


def _log_candidate_result(
    index: int,
    total: int,
    method: BenchmarkMethod,
    candidate,
    trial: TrialResult,
) -> None:
    parts = [
        f"[ingest] result {index}/{total}",
        f"method={method.value}",
        f"fairness={candidate.fairness}",
        f"batch={candidate.batch_rows}",
        f"rows_per_second={trial.metrics.rows_per_second:.1f}",
        f"wall_seconds={trial.metrics.wall_seconds:.3f}",
    ]
    if isinstance(candidate, InsertTrialConfig):
        parts.append(f"sync={candidate.pipeline_sync_batches}")
    elif isinstance(candidate, CopyTrialConfig):
        parts.append(f"chunk={candidate.copy_chunk_bytes}")
    print(" ".join(parts), file=sys.stderr, flush=True)


def _run_table_trial(
    *,
    dsn,
    cluster,
    profile,
    method,
    candidate,
    workload,
    table_name,
    rows,
    conn,
    server_profile_dir,
    server_profile_frequency: int,
):
    metrics = _run_trial(
        dsn=dsn,
        cluster=cluster,
        method=method,
        candidate=candidate,
        table_name=table_name,
        workload=workload,
        rows=rows,
        server_profile_dir=server_profile_dir,
        server_profile_frequency=server_profile_frequency,
    )
    sharded_distribution = _validate_trial_rows(
        cluster=cluster,
        profile=profile,
        conn=conn,
        table_name=table_name,
        workload=workload,
        expected_rows=rows,
    )
    metrics = replace(metrics, sharded_distribution=sharded_distribution)
    _validate_trial_metrics(
        profile=profile,
        method=method,
        metrics=metrics,
        expected_row_count=len(rows),
    )
    return metrics


def _run_trial(
    *,
    dsn,
    cluster,
    method,
    candidate,
    table_name,
    workload,
    rows,
    server_profile_dir,
    server_profile_frequency: int,
):
    log_offsets: dict[str, int] = {}
    before_metrics: dict[str, float] | None = None
    before_process_stats: dict[str, Any] | None = None
    profiler: ServerProfiler | None = None
    if server_profile_dir is not None:
        ServerProfiler.ensure_available()

    def on_ready() -> None:
        nonlocal profiler
        nonlocal before_metrics
        nonlocal before_process_stats
        log_offsets.update(_snapshot_log_offsets(cluster))
        before_metrics = _collect_server_metrics(cluster)
        before_process_stats = _collect_process_stats(cluster)
        if server_profile_dir is not None:
            profiler = ServerProfiler.start(
                cluster=cluster,
                output_dir=server_profile_dir,
                frequency=server_profile_frequency,
            )

    try:
        if method is BenchmarkMethod.INSERT:
            metrics = run_insert_trial(
                dsn=dsn,
                table_name=table_name,
                workload=workload,
                rows=rows,
                config=candidate,
                on_ready=on_ready,
            )
        else:
            metrics = run_copy_trial(
                dsn=dsn,
                table_name=table_name,
                workload=workload,
                rows=rows,
                config=candidate,
                on_ready=on_ready,
            )
    finally:
        profile_summary = profiler.stop() if profiler is not None else {}

    if before_metrics is None:
        raise AssertionError("benchmark metric collection did not start at the ready barrier")
    after = _collect_server_metrics(cluster)
    after_process_stats = _collect_process_stats(cluster)
    return _finalize_trial_metrics(
        metrics=replace(
            metrics,
            server_metric_deltas=_diff_server_metrics(before_metrics, after),
            server_profile=profile_summary,
        ),
        before_metrics=before_metrics,
        after_metrics=after,
        before_process_stats=before_process_stats or {},
        after_process_stats=after_process_stats,
        cluster=cluster,
        log_offsets=log_offsets,
    )


def _candidate_profile_root(
    *,
    server_profile_root: Path | None,
    scope: ServerProfileScope,
    candidate_count: int,
) -> Path | None:
    if server_profile_root is None:
        return None
    if scope is ServerProfileScope.ALL:
        return server_profile_root
    if candidate_count == 1:
        return server_profile_root
    return None


def _validate_trial_rows(
    *, cluster, profile: ClusterProfileSpec, conn, table_name, workload, expected_rows
) -> dict[str, object]:
    expected = expected_rows
    pk_columns = [column for column in workload.columns if column.primary_key]
    if len(pk_columns) != 1:
        raise AssertionError("benchmark validation requires exactly one primary key column")

    pk_name = pk_columns[0].name
    column_list = ", ".join(f'"{column.name}"' for column in workload.columns)
    last_pk = None
    actual_count = 0

    while True:
        if last_pk is None:
            query = f'SELECT {column_list} FROM "{table_name}" ORDER BY "{pk_name}" LIMIT {VALIDATION_FETCH_ROWS}'
            rows = conn.execute(query).fetchall()
        else:
            query = (
                f'SELECT {column_list} FROM "{table_name}" '
                f'WHERE "{pk_name}" > %s '
                f'ORDER BY "{pk_name}" LIMIT {VALIDATION_FETCH_ROWS}'
            )
            rows = conn.execute(query, (last_pk,)).fetchall()

        if not rows:
            break

        normalized_rows = [picodata.normalize_result_row(workload, row) for row in rows]
        expected_chunk = expected[actual_count : actual_count + len(normalized_rows)]
        if normalized_rows != expected_chunk:
            raise AssertionError(f"benchmark validation failed for {table_name} near row offset {actual_count}")

        actual_count += len(normalized_rows)
        last_pk = rows[-1][0]

    if actual_count != len(expected):
        raise AssertionError(
            f"benchmark validation failed for {table_name}: expected {len(expected)} rows, got {actual_count}"
        )

    if profile.is_global_table:
        return {}

    local_counts: dict[str, int] = {}
    for instance in cluster.instances:
        local_counts[instance.name] = int(instance.eval(f'return box.space["{table_name}"]:count()'))

    if sum(local_counts.values()) != len(expected):
        raise AssertionError(
            f"sharded distribution validation failed for {table_name}: "
            f"expected {len(expected)} local rows, got {sum(local_counts.values())}"
        )

    nonzero_instances = [name for name, count in local_counts.items() if count > 0]
    minimum_active_instances = 1 if len(expected) <= 1 else min(profile.instance_count, 2)
    if len(nonzero_instances) < minimum_active_instances:
        raise AssertionError(
            f"sharded distribution validation failed for {table_name}: "
            f"expected rows on at least {minimum_active_instances} instances, "
            f"got {len(nonzero_instances)} active instances, local counts={local_counts}"
        )

    return {
        "local_instance_row_counts": local_counts,
        "active_instance_count": len(nonzero_instances),
    }


def _validate_trial_metrics(
    *, profile: ClusterProfileSpec, method: BenchmarkMethod, metrics, expected_row_count: int
) -> None:
    if metrics.total_rows != expected_row_count:
        raise AssertionError(
            f"benchmark metrics mismatch: expected {expected_row_count} rows, got {metrics.total_rows}"
        )
    if expected_row_count > 0 and metrics.client_protocol_bytes <= 0:
        source = metrics.log_validation.get("client_protocol_byte_source", "unknown source")
        raise AssertionError(f"benchmark protocol byte metric is empty; source={source}")
    if method is not BenchmarkMethod.COPY:
        _validate_clean_success_metrics(metrics, expected_sessions=0)
        return

    expected_sessions = min(metrics.concurrency, expected_row_count) if expected_row_count else 0
    _validate_clean_success_metrics(metrics, expected_sessions=expected_sessions)
    inserted_rows = sum(
        value
        for key, value in metrics.server_metric_deltas.items()
        if key.startswith("pico_pgproto_copy_rows_inserted_total")
    )
    if inserted_rows != expected_row_count:
        raise AssertionError(f"COPY metrics mismatch: expected {expected_row_count} inserted rows, got {inserted_rows}")
    copy_bytes_received = _metric_total(metrics.server_metric_deltas, "pico_pgproto_copy_bytes_received_total")
    if copy_bytes_received != metrics.parameter_payload_bytes:
        raise AssertionError(
            f"COPY bytes mismatch: expected {metrics.parameter_payload_bytes} bytes, got {copy_bytes_received}"
        )
    if profile.is_sharded and not metrics.sharded_distribution:
        raise AssertionError("sharded benchmark run is missing local distribution diagnostics")


def _collect_server_metrics(cluster) -> dict[str, float]:
    totals: dict[str, float] = {}

    for instance in cluster.instances:
        metrics = instance.get_metrics()
        for family in metrics.values():
            for sample in family.samples:
                if sample.name not in INTERESTING_SERVER_METRICS:
                    continue
                key = sample.name
                if sample.labels:
                    labels = ",".join(f"{name}={value}" for name, value in sorted(sample.labels.items()))
                    key = f"{key}{{{labels}}}"
                totals[key] = totals.get(key, 0.0) + float(sample.value)

    return totals


def _diff_server_metrics(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    deltas: dict[str, float] = {}
    for key in sorted(set(before) | set(after)):
        delta = after.get(key, 0.0) - before.get(key, 0.0)
        if delta:
            deltas[key] = delta
    return deltas


def _create_table_schema(conn, table_name: str, workload, profile: ClusterProfileSpec) -> None:
    conn.execute(picodata.create_table_sql(table_name, workload, profile.key))
    for statement in picodata.create_index_sql(table_name, workload):
        conn.execute(statement)


def _snapshot_log_offsets(cluster) -> dict[str, int]:
    offsets: dict[str, int] = {}
    for instance in cluster.instances:
        log_file = instance.log_file()
        if log_file is None:
            continue
        offsets[instance.name] = Path(log_file).stat().st_size
    return offsets


def _finalize_trial_metrics(
    *, metrics, before_metrics, after_metrics, before_process_stats, after_process_stats, cluster, log_offsets
):
    log_validation = _validate_trial_logs(cluster, log_offsets)
    accounted_bytes = metrics.statement_template_bytes + metrics.parameter_payload_bytes
    log_validation.update(
        {
            "client_protocol_bytes": accounted_bytes,
            "client_protocol_byte_source": "client-side benchmark byte accounting",
            "client_protocol_byte_note": (
                "SQL text plus submitted parameter/COPY payload bytes; excludes pgproto frame overhead."
            ),
        }
    )
    return replace(
        metrics,
        client_protocol_bytes=int(log_validation["client_protocol_bytes"]),
        resource_snapshots=_resource_snapshots(
            before_metrics, after_metrics, before_process_stats, after_process_stats
        ),
        log_validation=log_validation,
    )


def _resource_snapshots(
    before: dict[str, float],
    after: dict[str, float],
    before_process_stats: dict[str, Any],
    after_process_stats: dict[str, Any],
) -> dict[str, Any]:
    before_snapshot = {
        alias: _metric_total(before, metric_name) for alias, metric_name in RESOURCE_SNAPSHOT_METRICS.items()
    }
    after_snapshot = {
        alias: _metric_total(after, metric_name) for alias, metric_name in RESOURCE_SNAPSHOT_METRICS.items()
    }
    return {
        "before": before_snapshot,
        "after": after_snapshot,
        "delta": {alias: after_snapshot[alias] - before_snapshot[alias] for alias in RESOURCE_SNAPSHOT_METRICS},
        "per_instance_delta": _per_instance_resource_delta(before, after),
        "process": _process_stats_snapshot(before_process_stats, after_process_stats),
    }


def _validate_trial_logs(cluster, offsets: dict[str, int]) -> dict[str, object]:
    suspicious: list[dict[str, str]] = []
    scanned_instances = 0
    scanned_lines = 0

    for instance in cluster.instances:
        log_file = instance.log_file()
        if log_file is None:
            continue
        scanned_instances += 1
        path = Path(log_file)
        if not path.exists():
            continue
        data = path.read_bytes()
        start = min(offsets.get(instance.name, 0), len(data))
        new_text = data[start:].decode("utf-8", errors="replace")
        for line in new_text.splitlines():
            scanned_lines += 1
            if SUSPICIOUS_LOG_RE.search(line):
                suspicious.append({"instance": instance.name, "line": line[-400:]})

    if suspicious:
        raise AssertionError(
            "benchmark log validation failed: "
            + "; ".join(f"{item['instance']}: {item['line']}" for item in suspicious[:5])
        )

    return {
        "instances_scanned": scanned_instances,
        "new_line_count": scanned_lines,
        "suspicious_line_count": 0,
    }


def _metric_total(metrics: dict[str, float], metric_name: str) -> float:
    return sum(value for key, value in metrics.items() if key == metric_name or key.startswith(f"{metric_name}{{"))


def _per_instance_resource_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, dict[str, float]]:
    instance_names: set[str] = set()
    for metric_name in RESOURCE_SNAPSHOT_METRICS.values():
        instance_names.update(_instance_metric_values(before, metric_name))
        instance_names.update(_instance_metric_values(after, metric_name))

    per_instance: dict[str, dict[str, float]] = {}
    for instance_name in sorted(instance_names):
        per_instance[instance_name] = {
            alias: _instance_metric_values(after, metric_name).get(instance_name, 0.0)
            - _instance_metric_values(before, metric_name).get(instance_name, 0.0)
            for alias, metric_name in RESOURCE_SNAPSHOT_METRICS.items()
        }
    return per_instance


def _collect_process_stats(cluster) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for instance in cluster.instances:
        pid = _picodata_process_pid(instance)
        if pid is None:
            continue
        stat = _linux_process_stat(pid)
        if stat:
            stats[instance.name] = {"pid": pid, **stat}
        else:
            stats[instance.name] = {"pid": pid}
    return stats


def _process_stats_snapshot(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    instances: dict[str, Any] = {}
    for instance_name in sorted(set(before) | set(after)):
        before_instance = before.get(instance_name, {})
        after_instance = after.get(instance_name, {})
        delta = {
            key: after_instance[key] - before_instance[key]
            for key in ("cpu_user_seconds", "cpu_system_seconds", "rss_bytes", "vms_bytes")
            if key in before_instance and key in after_instance
        }
        instances[instance_name] = {
            "before": before_instance,
            "after": after_instance,
            "delta": delta,
        }
    return {
        "source": "linux /proc for Picodata child processes",
        "instances": instances,
    }


def _linux_process_stat(pid: int) -> dict[str, float] | None:
    if platform.system() != "Linux":
        return None
    proc = Path("/proc") / str(pid)
    try:
        stat_fields = (proc / "stat").read_text().split()
        statm_fields = (proc / "statm").read_text().split()
    except OSError:
        return None
    if len(stat_fields) < 15 or len(statm_fields) < 2:
        return None

    clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    page_size = os.sysconf("SC_PAGE_SIZE")
    return {
        "cpu_user_seconds": int(stat_fields[13]) / clock_ticks,
        "cpu_system_seconds": int(stat_fields[14]) / clock_ticks,
        "vms_bytes": int(statm_fields[0]) * page_size,
        "rss_bytes": int(statm_fields[1]) * page_size,
    }


def _picodata_process_pid(instance) -> int | None:
    if instance.process is None or instance.process.pid is None:
        return None
    try:
        from conftest import pgrep_tree, pid_alive  # type: ignore
    except ImportError:
        return instance.process.pid
    if not pid_alive(instance.process.pid):
        return None
    pids = pgrep_tree(instance.process.pid)
    if len(pids) >= 2 and pid_alive(pids[1]):
        return pids[1]
    return instance.process.pid


def _instance_metric_values(metrics: dict[str, float], metric_name: str) -> dict[str, float]:
    prefix = f"{metric_name}{{"
    values: dict[str, float] = {}
    for key, value in metrics.items():
        if not key.startswith(prefix):
            continue
        labels = key[len(prefix) : -1]
        for label in labels.split(","):
            if label.startswith("instance_name="):
                values[label.split("=", 1)[1]] = value
                break
    return values


def _validate_clean_success_metrics(metrics, *, expected_sessions: int) -> None:
    if _metric_total(metrics.server_metric_deltas, "pico_sql_query_errors_total") != 0:
        raise AssertionError("benchmark run reported SQL query errors")
    if _metric_total(metrics.server_metric_deltas, "pico_pgproto_copy_record_limit_errors_total") != 0:
        raise AssertionError("benchmark run reported COPY record limit errors")
    if metrics.method == "copy":
        sessions_started = _metric_total(metrics.server_metric_deltas, "pico_pgproto_copy_sessions_started_total")
        if sessions_started != expected_sessions:
            raise AssertionError(f"COPY session count mismatch: expected {expected_sessions}, got {sessions_started}")


def _candidate_port_start(base_port: int | None, index: int) -> int | None:
    if base_port is None:
        return None
    return base_port + (index * CLUSTER_PORT_RANGE)


def _git_metadata(picodata_source: Path) -> dict[str, object]:
    source_root = repo_root_path(picodata_source)
    revision = _git_output(source_root, "rev-parse", "--short=12", "HEAD")
    branch = _git_output(source_root, "rev-parse", "--abbrev-ref", "HEAD")
    if branch == "HEAD":
        branch = None

    status = _git_output(source_root, "status", "--short", "--untracked-files=no")
    dirty = bool(status) if status is not None else None

    return {
        "revision": revision,
        "branch": branch,
        "dirty": dirty,
    }


def _git_output(repo_root: Path, *args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None

    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def _host_metadata() -> dict[str, object]:
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "processor": _processor_name(),
        "cgroup": _linux_cgroup_limits(),
    }


def _processor_name() -> str | None:
    processor = platform.processor()
    if processor:
        return processor

    if platform.system() == "Darwin":
        return _command_output("sysctl", "-n", "machdep.cpu.brand_string")

    if platform.system() == "Linux":
        try:
            cpuinfo = Path("/proc/cpuinfo").read_text()
        except OSError:
            return None
        for line in cpuinfo.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            if key.strip().lower() == "model name":
                return value.strip()
    return None


def _linux_cgroup_limits() -> dict[str, str]:
    if platform.system() != "Linux":
        return {}
    cgroup_root = Path("/sys/fs/cgroup")
    result: dict[str, str] = {}
    for name in ("cpu.max", "memory.max", "memory.high", "io.stat"):
        path = cgroup_root / name
        try:
            result[name.replace(".", "_")] = path.read_text().strip()
        except OSError:
            continue
    return result


def _command_output(*command: str) -> str | None:
    try:
        completed = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def _drop_table(dsn: str, table_name: str, *, tolerate_errors: bool = False) -> None:
    last_error: psycopg.Error | None = None
    timeout_candidates = (15,) if tolerate_errors else (60, 120, 240)
    for timeout_seconds in timeout_candidates:
        try:
            with psycopg.connect(dsn, autocommit=True) as cleanup_conn:
                cleanup_conn.execute(f'DROP TABLE IF EXISTS "{table_name}" OPTION (TIMEOUT = {timeout_seconds})')
            return
        except psycopg.Error as exc:
            last_error = exc
    if not tolerate_errors and last_error is not None:
        raise last_error
