from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from statistics import median
from typing import Any

from picodata_ingest_bench.config import distribution_mode_label


@dataclass(frozen=True)
class TrialMetrics:
    method: str
    fairness: str
    configured_batch_rows: int
    configured_copy_chunk_bytes: int | None
    configured_insert_pipeline_sync_batches: int | None
    batch_latency_label: str
    concurrency: int
    batch_count: int
    pipeline_sync_count: int
    total_rows: int
    logical_payload_bytes: int
    statement_template_bytes: int
    parameter_payload_bytes: int
    prepared_statement_count: int
    wall_seconds: float
    worker_build_seconds: float
    worker_submit_seconds: float
    worker_finish_seconds: float
    worker_elapsed_wall_seconds_sum: float
    worker_elapsed_wall_seconds_max: float
    worker_elapsed_cpu_seconds_sum: float
    worker_build_cpu_seconds: float
    worker_submit_cpu_seconds: float
    worker_finish_cpu_seconds: float
    worker_build_wait_seconds: float
    worker_submit_wait_seconds: float
    worker_finish_wait_seconds: float
    worker_start_command_wall_seconds: float
    worker_start_command_cpu_seconds: float
    worker_start_command_wait_seconds: float
    worker_protocol_teardown_wall_seconds: float
    worker_protocol_teardown_cpu_seconds: float
    worker_protocol_teardown_wait_seconds: float
    insert_execute_count: int
    copy_write_count: int
    copy_write_bytes: int
    batch_latencies_ms: list[float]
    client_protocol_bytes: int = 0
    resource_snapshots: dict[str, Any] = field(default_factory=dict)
    sharded_distribution: dict[str, Any] = field(default_factory=dict)
    log_validation: dict[str, Any] = field(default_factory=dict)
    server_metric_deltas: dict[str, float] = field(default_factory=dict)
    server_profile: dict[str, Any] = field(default_factory=dict)

    @property
    def rows_per_second(self) -> float:
        if self.wall_seconds == 0:
            return 0.0
        return self.total_rows / self.wall_seconds

    @property
    def logical_bytes_per_second(self) -> float:
        if self.wall_seconds == 0:
            return 0.0
        return self.logical_payload_bytes / self.wall_seconds

    @property
    def client_protocol_bytes_per_second(self) -> float:
        if self.wall_seconds == 0:
            return 0.0
        return self.client_protocol_bytes / self.wall_seconds

    @property
    def avg_rows_per_batch(self) -> float:
        if self.batch_count == 0:
            return 0.0
        return self.total_rows / self.batch_count

    @property
    def avg_logical_batch_bytes(self) -> float:
        if self.batch_count == 0:
            return 0.0
        return self.logical_payload_bytes / self.batch_count

    @property
    def avg_rows_per_pipeline_sync(self) -> float:
        if self.pipeline_sync_count == 0:
            return 0.0
        return self.total_rows / self.pipeline_sync_count

    @property
    def avg_client_protocol_bytes(self) -> float:
        if self.batch_count == 0:
            return 0.0
        return self.client_protocol_bytes / self.batch_count

    def to_dict(self) -> dict[str, Any]:
        latency_metric_name = self.batch_latency_label
        return {
            "method": self.method,
            "fairness": self.fairness,
            "configured_batch_rows": self.configured_batch_rows,
            "configured_copy_chunk_bytes": self.configured_copy_chunk_bytes,
            "configured_insert_pipeline_sync_batches": self.configured_insert_pipeline_sync_batches,
            "latency_metric_name": latency_metric_name,
            "concurrency": self.concurrency,
            "batch_count": self.batch_count,
            "pipeline_sync_count": self.pipeline_sync_count,
            "total_rows": self.total_rows,
            "logical_payload_bytes": self.logical_payload_bytes,
            "statement_template_bytes": self.statement_template_bytes,
            "parameter_payload_bytes": self.parameter_payload_bytes,
            "prepared_statement_count": self.prepared_statement_count,
            "wall_seconds": self.wall_seconds,
            "worker_build_seconds": self.worker_build_seconds,
            "worker_submit_seconds": self.worker_submit_seconds,
            "worker_finish_seconds": self.worker_finish_seconds,
            "worker_elapsed_wall_seconds_sum": self.worker_elapsed_wall_seconds_sum,
            "worker_elapsed_wall_seconds_max": self.worker_elapsed_wall_seconds_max,
            "worker_elapsed_cpu_seconds_sum": self.worker_elapsed_cpu_seconds_sum,
            "worker_build_cpu_seconds": self.worker_build_cpu_seconds,
            "worker_submit_cpu_seconds": self.worker_submit_cpu_seconds,
            "worker_finish_cpu_seconds": self.worker_finish_cpu_seconds,
            "worker_build_wait_seconds": self.worker_build_wait_seconds,
            "worker_submit_wait_seconds": self.worker_submit_wait_seconds,
            "worker_finish_wait_seconds": self.worker_finish_wait_seconds,
            "worker_start_command_wall_seconds": self.worker_start_command_wall_seconds,
            "worker_start_command_cpu_seconds": self.worker_start_command_cpu_seconds,
            "worker_start_command_wait_seconds": self.worker_start_command_wait_seconds,
            "worker_protocol_teardown_wall_seconds": self.worker_protocol_teardown_wall_seconds,
            "worker_protocol_teardown_cpu_seconds": self.worker_protocol_teardown_cpu_seconds,
            "worker_protocol_teardown_wait_seconds": self.worker_protocol_teardown_wait_seconds,
            "worker_phase_breakdown": self.worker_phase_breakdown(),
            "insert_execute_count": self.insert_execute_count,
            "copy_write_count": self.copy_write_count,
            "copy_write_bytes": self.copy_write_bytes,
            "rows_per_second": self.rows_per_second,
            "logical_bytes_per_second": self.logical_bytes_per_second,
            "client_protocol_bytes": self.client_protocol_bytes,
            "client_protocol_bytes_per_second": self.client_protocol_bytes_per_second,
            "avg_rows_per_batch": self.avg_rows_per_batch,
            "avg_rows_per_pipeline_sync": self.avg_rows_per_pipeline_sync,
            "avg_logical_batch_bytes": self.avg_logical_batch_bytes,
            "avg_client_protocol_bytes": self.avg_client_protocol_bytes,
            latency_metric_name: summarize_latencies(self.batch_latencies_ms),
            "resource_snapshots": self.resource_snapshots,
            "sharded_distribution": self.sharded_distribution,
            "log_validation": self.log_validation,
            "server_metric_deltas": self.server_metric_deltas,
            "server_profile": self.server_profile,
        }

    def worker_phase_breakdown(self) -> dict[str, Any]:
        wall_phases = {
            "start_command": self.worker_start_command_wall_seconds,
            "build": self.worker_build_seconds,
            "submit": self.worker_submit_seconds,
            "finish": self.worker_finish_seconds,
            "protocol_teardown": self.worker_protocol_teardown_wall_seconds,
        }
        cpu_phases = {
            "start_command": self.worker_start_command_cpu_seconds,
            "build": self.worker_build_cpu_seconds,
            "submit": self.worker_submit_cpu_seconds,
            "finish": self.worker_finish_cpu_seconds,
            "protocol_teardown": self.worker_protocol_teardown_cpu_seconds,
        }
        wait_phases = {
            "start_command": self.worker_start_command_wait_seconds,
            "build": self.worker_build_wait_seconds,
            "submit": self.worker_submit_wait_seconds,
            "finish": self.worker_finish_wait_seconds,
            "protocol_teardown": self.worker_protocol_teardown_wait_seconds,
        }
        known_wall = sum(wall_phases.values())
        known_cpu = sum(cpu_phases.values())
        return {
            "basis": "summed worker time; compare with wall_seconds only when concurrency=1",
            "wall_seconds": {
                **wall_phases,
                "unattributed": max(self.worker_elapsed_wall_seconds_sum - known_wall, 0.0),
                "total": self.worker_elapsed_wall_seconds_sum,
            },
            "cpu_seconds": {
                **cpu_phases,
                "unattributed": max(self.worker_elapsed_cpu_seconds_sum - known_cpu, 0.0),
                "total": self.worker_elapsed_cpu_seconds_sum,
            },
            "wait_seconds": wait_phases,
        }


@dataclass(frozen=True)
class TrialResult:
    profile: str
    workload: str
    mode: str
    seed: int
    row_count: int
    trial_id: str
    cluster_setup_seconds: float
    cluster_balance_seconds: float
    metrics: TrialMetrics

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "workload": self.workload,
            "mode": self.mode,
            "seed": self.seed,
            "row_count": self.row_count,
            "trial_id": self.trial_id,
            "cluster_setup_seconds": self.cluster_setup_seconds,
            "cluster_balance_seconds": self.cluster_balance_seconds,
            "metrics": self.metrics.to_dict(),
        }


@dataclass(frozen=True)
class BenchmarkReport:
    generated_at: str
    build_profile: str
    execution_runtime: str
    git_metadata: dict[str, Any]
    runtime_host_metadata: dict[str, Any]
    database: str
    profile: str
    profile_spec: dict[str, Any]
    workload: str
    workload_spec: dict[str, Any]
    mode: str
    candidate_isolation: str
    topology_ready_before_timing: bool
    requested_method: str
    requested_fairness: str
    concurrency: int
    seed: int
    requested_rows: int
    warmup_rows: int
    candidate_space: dict[str, Any]
    server_profile: dict[str, Any]
    environment: dict[str, Any]
    trials: list[TrialResult]

    @classmethod
    def new(
        cls,
        *,
        build_profile: str,
        execution_runtime: str,
        git_metadata: dict[str, Any],
        runtime_host_metadata: dict[str, Any],
        database: str,
        profile: str,
        profile_spec: dict[str, Any],
        workload: str,
        workload_spec: dict[str, Any],
        mode: str,
        candidate_isolation: str,
        topology_ready_before_timing: bool,
        requested_method: str,
        requested_fairness: str,
        concurrency: int,
        seed: int,
        requested_rows: int,
        warmup_rows: int,
        candidate_space: dict[str, Any],
        server_profile: dict[str, Any],
        environment: dict[str, Any],
        trials: list[TrialResult],
    ) -> "BenchmarkReport":
        return cls(
            generated_at=datetime.now(timezone.utc).isoformat(),
            build_profile=build_profile,
            execution_runtime=execution_runtime,
            git_metadata=git_metadata,
            runtime_host_metadata=runtime_host_metadata,
            database=database,
            profile=profile,
            profile_spec=profile_spec,
            workload=workload,
            workload_spec=workload_spec,
            mode=mode,
            candidate_isolation=candidate_isolation,
            topology_ready_before_timing=topology_ready_before_timing,
            requested_method=requested_method,
            requested_fairness=requested_fairness,
            concurrency=concurrency,
            seed=seed,
            requested_rows=requested_rows,
            warmup_rows=warmup_rows,
            candidate_space=candidate_space,
            server_profile=server_profile,
            environment=environment,
            trials=trials,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "build_profile": self.build_profile,
            "execution_runtime": self.execution_runtime,
            "git_metadata": self.git_metadata,
            "runtime_host_metadata": self.runtime_host_metadata,
            "database": self.database,
            "profile": self.profile,
            "profile_spec": self.profile_spec,
            "workload": self.workload,
            "workload_spec": self.workload_spec,
            "mode": self.mode,
            "candidate_isolation": self.candidate_isolation,
            "topology_ready_before_timing": self.topology_ready_before_timing,
            "requested_method": self.requested_method,
            "requested_fairness": self.requested_fairness,
            "concurrency": self.concurrency,
            "seed": self.seed,
            "requested_rows": self.requested_rows,
            "warmup_rows": self.warmup_rows,
            "cluster_timing_scope": cluster_timing_scope(self.candidate_isolation),
            "cluster_setup_seconds": summary_cluster_seconds(self, "cluster_setup_seconds"),
            "cluster_balance_seconds": summary_cluster_seconds(self, "cluster_balance_seconds"),
            "candidate_space": self.candidate_space,
            "server_profile": self.server_profile,
            "environment": self.environment,
            "trials": [trial.to_dict() for trial in self.trials],
            "best_by_method": best_trials_by_method(self.trials),
            "same_batch_comparisons": (
                same_batch_comparisons(self.trials) if self.candidate_isolation == "fresh_cluster_per_candidate" else {}
            ),
            "summary": build_summary(self),
        }


def summarize_latencies(latencies_ms: list[float]) -> dict[str, float | int]:
    if not latencies_ms:
        return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}

    ordered = sorted(latencies_ms)
    return {
        "count": len(ordered),
        "p50": median(ordered),
        "p95": percentile(ordered, 95.0),
        "p99": percentile(ordered, 99.0),
        "max": ordered[-1],
    }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    rank = (pct / 100.0) * (len(values) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(values) - 1)
    fraction = rank - lower
    return values[lower] + (values[upper] - values[lower]) * fraction


def best_trials_by_method(trials: list[TrialResult]) -> dict[str, dict[str, Any]]:
    best: dict[str, TrialResult] = {}
    for trial in trials:
        method = f"{trial.metrics.method}:{trial.metrics.fairness}"
        current = best.get(method)
        if current is None or trial.metrics.rows_per_second > current.metrics.rows_per_second:
            best[method] = trial

    return {method: trial.to_dict() for method, trial in sorted(best.items())}


def same_batch_comparisons(trials: list[TrialResult]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[tuple[str, int], dict[str, TrialResult]] = {}
    for trial in trials:
        if trial.metrics.fairness != "fixed":
            continue
        key = (trial.metrics.fairness, trial.metrics.configured_batch_rows)
        bucket = grouped.setdefault(key, {})
        current = bucket.get(trial.metrics.method)
        if current is None or trial.metrics.rows_per_second > current.metrics.rows_per_second:
            bucket[trial.metrics.method] = trial

    result: dict[str, list[dict[str, Any]]] = {}
    for (fairness, batch_rows), bucket in sorted(grouped.items()):
        insert_trial = bucket.get("insert")
        copy_trial = bucket.get("copy")
        if insert_trial is None or copy_trial is None:
            continue

        insert_rows_per_second = insert_trial.metrics.rows_per_second
        copy_rows_per_second = copy_trial.metrics.rows_per_second
        ratio = 0.0 if insert_rows_per_second == 0 else copy_rows_per_second / insert_rows_per_second

        result.setdefault(fairness, []).append(
            {
                "batch_rows": batch_rows,
                "insert_rows_per_second": insert_rows_per_second,
                "copy_rows_per_second": copy_rows_per_second,
                "copy_vs_insert_ratio": ratio,
                "insert": {
                    "trial_id": insert_trial.trial_id,
                    "configured_batch_rows": insert_trial.metrics.configured_batch_rows,
                    "configured_pipeline_sync_batches": insert_trial.metrics.configured_insert_pipeline_sync_batches,
                    "rows_per_second": insert_rows_per_second,
                },
                "copy": {
                    "trial_id": copy_trial.trial_id,
                    "configured_batch_rows": copy_trial.metrics.configured_batch_rows,
                    "configured_copy_chunk_bytes": copy_trial.metrics.configured_copy_chunk_bytes,
                    "rows_per_second": copy_rows_per_second,
                },
            }
        )
    return result


def build_summary(report: BenchmarkReport) -> dict[str, Any]:
    best_trials = best_trials_by_method(report.trials)
    best_summary: dict[str, Any] = {}
    for method_key, trial in best_trials.items():
        metrics = trial["metrics"]
        best_summary[method_key] = {
            "rows_per_second": metrics["rows_per_second"],
            "logical_bytes_per_second": metrics["logical_bytes_per_second"],
            "client_protocol_bytes_per_second": metrics["client_protocol_bytes_per_second"],
            "configured_batch_rows": metrics["configured_batch_rows"],
            "configured_copy_chunk_bytes": metrics["configured_copy_chunk_bytes"],
            "configured_insert_pipeline_sync_batches": metrics["configured_insert_pipeline_sync_batches"],
            "batch_count": metrics["batch_count"],
            "pipeline_sync_count": metrics["pipeline_sync_count"],
            "latency_metric_name": metrics["latency_metric_name"],
            "latency_p50_ms": metrics[metrics["latency_metric_name"]]["p50"],
            "latency_p95_ms": metrics[metrics["latency_metric_name"]]["p95"],
            "active_instance_count": metrics["sharded_distribution"].get("active_instance_count"),
            "trial_id": trial["trial_id"],
        }

    return {
        "profile_key": report.profile,
        "database": report.database,
        "profile_label": report.profile_spec.get("display_name", report.profile),
        "distribution_mode": report.profile_spec.get("distribution_mode"),
        "distribution_label": distribution_mode_label(report.profile_spec.get("distribution_mode")),
        "workload_key": report.workload,
        "workload_label": workload_label(report.workload_spec, report.workload),
        "warmup_rows": report.warmup_rows,
        "instance_memtx_memory": report.environment.get("instance_memtx_memory"),
        "cluster_setup_seconds": summary_cluster_seconds(report, "cluster_setup_seconds"),
        "cluster_balance_seconds": summary_cluster_seconds(report, "cluster_balance_seconds"),
        "git_revision": report.git_metadata.get("revision"),
        "git_branch": report.git_metadata.get("branch"),
        "git_dirty": report.git_metadata.get("dirty"),
        "cluster_timing_scope": cluster_timing_scope(report.candidate_isolation),
        "candidate_isolation": report.candidate_isolation,
        "candidate_isolation_label": candidate_isolation_label(report.candidate_isolation),
        "comparison_mode_label": comparison_mode_label(report.requested_fairness),
        "fixed_mode_defaults": {
            "insert_pipeline_sync_batches": report.candidate_space.get("fixed_insert_pipeline_sync_batches"),
            "copy_chunk_bytes": report.candidate_space.get("fixed_copy_chunk_bytes"),
        },
        "mode_label": mode_label(report.mode, report.candidate_isolation),
        "best_by_method": best_summary,
        "server_profile": server_profile_summary(report),
        "same_batch_comparisons": (
            same_batch_comparisons(report.trials) if report.candidate_isolation == "fresh_cluster_per_candidate" else {}
        ),
    }


def server_profile_summary(report: BenchmarkReport) -> dict[str, Any]:
    if not report.server_profile.get("enabled"):
        return {}

    trials: list[dict[str, Any]] = []
    for trial in report.trials:
        profile = trial.metrics.server_profile
        if not profile:
            continue
        aggregate = profile.get("aggregate") or {}
        trials.append(
            {
                "trial_id": trial.trial_id,
                "method": trial.metrics.method,
                "fairness": trial.metrics.fairness,
                "configured_batch_rows": trial.metrics.configured_batch_rows,
                "configured_insert_pipeline_sync_batches": trial.metrics.configured_insert_pipeline_sync_batches,
                "configured_copy_chunk_bytes": trial.metrics.configured_copy_chunk_bytes,
                "rows_per_second": trial.metrics.rows_per_second,
                "wall_seconds": trial.metrics.wall_seconds,
                "tool": profile.get("tool"),
                "output_dir": display_path(profile.get("output_dir")),
                "sample_unit": aggregate.get("sample_unit"),
                "sample_note": aggregate.get("sample_note"),
                "total_samples": aggregate.get("total_samples"),
                "raw_sample_count": aggregate.get("raw_sample_count"),
                "lost_sample_count": aggregate.get("lost_sample_count"),
                "top_categories": list((aggregate.get("top_categories") or [])[:10]),
                "top_leaf_functions": list((aggregate.get("top_leaf_functions_from_instance_tops") or [])[:10]),
                "instances": server_profile_instances(profile),
            }
        )

    return {
        "enabled": True,
        "scope": report.server_profile.get("scope"),
        "frequency_hz": report.server_profile.get("frequency_hz"),
        "output_dir": display_path(report.server_profile.get("output_dir")),
        "trials": trials,
    }


def server_profile_instances(profile: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for name, instance in sorted((profile.get("instances") or {}).items()):
        artifacts: dict[str, str] = {}
        for key in (
            "perf_data",
            "perf_report",
            "perf_out",
            "perf_folded",
            "perf_svg",
            "sample_report",
        ):
            if key not in instance:
                continue
            path = display_path(instance[key])
            if path is not None:
                artifacts[key] = path
        artifact_errors = {
            key: instance[key]
            for key in ("perf_report_error", "perf_script_error", "folded_error", "svg_error")
            if key in instance
        }
        result.append(
            {
                "name": name,
                "pid": instance.get("pid"),
                "samples": (instance.get("samples") or {}).get("total_samples"),
                "raw_samples": (instance.get("samples") or {}).get("raw_sample_count"),
                "lost_samples": (instance.get("samples") or {}).get("lost_sample_count"),
                "sample_parse_error": (instance.get("samples") or {}).get("parse_error"),
                "error": instance.get("error"),
                "artifacts": artifacts,
                "artifact_errors": artifact_errors,
                "artifact_status": instance.get("artifact_status", {}),
            }
        )
    return result


def render_text_summary(report: BenchmarkReport) -> str:
    summary = build_summary(report)
    lines = [
        f"Profile: {summary['profile_label']}",
        f"Workload: {summary['workload_label']}",
        f"Mode: {summary['mode_label']}",
        context_line(summary),
        (
            "Selection: "
            f"method={report.requested_method}, comparison={summary['comparison_mode_label']}, "
            f"rows={report.requested_rows}, concurrency={report.concurrency}, runtime={report.execution_runtime}"
        ),
    ]
    if report.environment.get("candidate_index") is not None:
        lines.append(
            "Candidate: "
            + f"#{report.environment['candidate_index']} of {report.environment.get('available_candidate_count')}"
        )
    if report.server_profile.get("enabled"):
        lines.append(
            "Server profile: "
            + f"scope={report.server_profile.get('scope')}, "
            + f"frequency={report.server_profile.get('frequency_hz')} Hz, "
            + f"output={display_path(report.server_profile.get('output_dir'))}"
        )
    if summary["git_revision"] is not None:
        branch = summary["git_branch"]
        dirty_suffix = " dirty" if summary["git_dirty"] else ""
        if branch:
            lines.append(f"Revision: {summary['git_revision']} ({branch}{dirty_suffix})")
        else:
            lines.append(f"Revision: {summary['git_revision']}{dirty_suffix}")
    host_metadata = report.runtime_host_metadata
    if host_metadata:
        lines.append(
            "Runtime host: "
            + f"{host_metadata.get('system')} {host_metadata.get('release')}, "
            + f"{host_metadata.get('machine')}, "
            + f"cpu_count={host_metadata.get('cpu_count')}, "
            + f"processor={host_metadata.get('processor') or 'unknown'}"
        )
    if report.requested_fairness in {"fixed", "both"}:
        fixed_defaults = summary["fixed_mode_defaults"]
        lines.append(
            "Fixed defaults: "
            + f"insert sync={fixed_defaults['insert_pipeline_sync_batches']}, "
            + f"copy chunk={fixed_defaults['copy_chunk_bytes']}"
        )

    best_by_method = summary["best_by_method"]
    if best_by_method:
        lines.append("Best results:")
        for method_key in sorted(best_by_method):
            trial = best_by_method[method_key]
            tuning: list[str] = [f"batch={trial['configured_batch_rows']}"]
            if trial["configured_copy_chunk_bytes"] is not None:
                tuning.append(f"chunk={trial['configured_copy_chunk_bytes']}")
            if trial["configured_insert_pipeline_sync_batches"] is not None:
                tuning.append(f"sync={trial['configured_insert_pipeline_sync_batches']}")
            if trial["active_instance_count"] is not None:
                tuning.append(f"active_instances={trial['active_instance_count']}")
            lines.append(
                "  "
                + f"{method_key}: "
                + f"{trial['rows_per_second']:.1f} rows/s, "
                + f"{trial['logical_bytes_per_second']:.1f} logical B/s, "
                + f"{trial['client_protocol_bytes_per_second']:.1f} client B/s, "
                + ", ".join(tuning)
            )

    same_batch = summary["same_batch_comparisons"]
    fixed_rows = same_batch.get("fixed", [])
    if fixed_rows:
        lines.append("Same-batch fixed comparisons:")
        for row in fixed_rows:
            lines.append(
                "  "
                + f"batch={row['batch_rows']}: "
                + f"insert={row['insert_rows_per_second']:.1f} rows/s, "
                + f"copy={row['copy_rows_per_second']:.1f} rows/s, "
                + f"copy_vs_insert={row['copy_vs_insert_ratio']:.2f}x"
            )

    trial_detail_lines = trial_time_resource_lines(report.trials)
    if trial_detail_lines:
        lines.append("Time/resource detail:")
        lines.extend(trial_detail_lines)

    profile_summary = summary["server_profile"]
    if profile_summary:
        lines.append("Server profile insights:")
        lines.append(
            "  "
            + f"scope={profile_summary['scope']}, "
            + f"frequency={profile_summary['frequency_hz']} Hz, "
            + f"output={profile_summary['output_dir']}"
        )
        for trial in profile_summary["trials"]:
            lines.append(
                "  "
                + f"{trial['method']}:{trial['fairness']} "
                + profile_trial_tuning(trial)
                + f": {trial['rows_per_second']:.1f} rows/s, "
                + f"wall={trial['wall_seconds']:.3f}s, "
                + f"tool={trial['tool']}, "
                + f"profile={trial['output_dir']}"
            )
            sample_text = profile_trial_sample_text(trial)
            if sample_text:
                lines.append(f"    samples: {sample_text}")
            if trial["top_categories"]:
                lines.append(
                    "    sample weight by category: " + percent_rows(visible_profile_rows(trial["top_categories"]))
                )
            if trial["top_leaf_functions"]:
                lines.append("    top leaf functions: " + percent_rows(trial["top_leaf_functions"][:5]))
            if trial["sample_unit"]:
                lines.append(f"    sample unit: {trial['sample_unit']}")
            if trial["sample_note"]:
                lines.append(f"    sample note: {trial['sample_note']}")
            lines.append("    perf artifacts:")
            for instance in trial["instances"]:
                artifact_text = profile_artifact_text(instance["artifacts"])
                instance_sample_text = profile_instance_sample_text(instance)
                prefix = f"{instance['name']}"
                if instance_sample_text:
                    prefix += f" ({instance_sample_text})"
                if artifact_text:
                    lines.append(f"      {prefix}: {artifact_text}")
                elif instance["error"]:
                    lines.append(f"      {prefix}: error={instance['error']}")
                else:
                    lines.append(f"      {prefix}: no artifacts")
                artifact_error_text = profile_artifact_error_text(instance["artifact_errors"])
                if artifact_error_text:
                    lines.append(f"        artifact errors: {artifact_error_text}")
                if instance["sample_parse_error"]:
                    lines.append(f"        sample parse error: {instance['sample_parse_error']}")

    lines.append("Validation: successful trials passed exact row validation, server metric checks, and log validation.")
    lines.append("Latency: throughput is comparable across methods; latency metrics are method-specific.")
    return "\n".join(lines)


def profile_trial_tuning(trial: dict[str, Any]) -> str:
    parts = [f"batch={trial['configured_batch_rows']}"]
    if trial["configured_insert_pipeline_sync_batches"] is not None:
        parts.append(f"sync={trial['configured_insert_pipeline_sync_batches']}")
    if trial["configured_copy_chunk_bytes"] is not None:
        parts.append(f"chunk={trial['configured_copy_chunk_bytes']}")
    return ", ".join(parts)


def percent_rows(rows: list[dict[str, Any]]) -> str:
    return ", ".join(f"{row['name']}={row['percent']:.1f}%" for row in rows)


def visible_profile_rows(
    rows: list[dict[str, Any]], *, limit: int = 5, unknown_threshold_percent: float = 5.0
) -> list[dict[str, Any]]:
    visible: list[dict[str, Any]] = []
    for row in rows:
        if row["name"] == "unknown" and row["percent"] < unknown_threshold_percent:
            continue
        visible.append(row)
        if len(visible) >= limit:
            break
    return visible


def profile_trial_sample_text(trial: dict[str, Any]) -> str:
    parts: list[str] = []
    if trial.get("raw_sample_count") is not None:
        parts.append(f"raw={trial['raw_sample_count']}")
    if trial.get("total_samples") is not None:
        parts.append(f"weighted={trial['total_samples']}")
    if trial.get("lost_sample_count") is not None:
        parts.append(f"lost={trial['lost_sample_count']}")
    return ", ".join(parts)


def profile_instance_sample_text(instance: dict[str, Any]) -> str:
    parts: list[str] = []
    if instance.get("raw_samples") is not None:
        parts.append(f"raw={instance['raw_samples']}")
    if instance.get("samples") is not None:
        parts.append(f"weighted={instance['samples']}")
    if instance.get("lost_samples") is not None:
        parts.append(f"lost={instance['lost_samples']}")
    return ", ".join(parts)


def trial_time_resource_lines(trials: list[TrialResult]) -> list[str]:
    lines: list[str] = []
    for trial in trials:
        metrics = trial.metrics
        phases = metrics.worker_phase_breakdown()
        lines.append(
            "  "
            + f"{trial.trial_id} {metrics.method}:{metrics.fairness} "
            + trial_tuning_text(metrics)
            + f": wall={metrics.wall_seconds:.3f}s, "
            + f"worker_wall[{phase_text(phases['wall_seconds'])}], "
            + f"worker_cpu[{phase_text(phases['cpu_seconds'])}]"
        )
        resource_text = resource_delta_text(metrics.resource_snapshots)
        if resource_text:
            lines.append(f"    resource delta: {resource_text}")
        process_text = process_delta_text(metrics.resource_snapshots)
        if process_text:
            lines.append(f"    process delta: {process_text}")
    return lines


def trial_tuning_text(metrics: TrialMetrics) -> str:
    parts = [f"batch={metrics.configured_batch_rows}"]
    if metrics.configured_insert_pipeline_sync_batches is not None:
        parts.append(f"sync={metrics.configured_insert_pipeline_sync_batches}")
    if metrics.configured_copy_chunk_bytes is not None:
        parts.append(f"chunk={metrics.configured_copy_chunk_bytes}")
    return ", ".join(parts)


def phase_text(phases: dict[str, float]) -> str:
    labels = (
        ("start_command", "start"),
        ("build", "build"),
        ("submit", "submit"),
        ("finish", "finish"),
        ("protocol_teardown", "teardown"),
        ("unattributed", "unattr"),
        ("total", "total"),
    )
    return ", ".join(f"{label}={phases.get(key, 0.0):.3f}s" for key, label in labels)


def resource_delta_text(resource_snapshots: dict[str, Any]) -> str:
    delta = resource_snapshots.get("delta") or {}
    if not delta:
        return ""
    parts: list[str] = []
    for key, label in (
        ("cpu_user_seconds", "cpu_user"),
        ("cpu_system_seconds", "cpu_system"),
    ):
        if key in delta and delta[key] != 0:
            parts.append(f"{label}={delta[key]:.3f}s")
    for key, label in (
        ("memory_data_bytes", "data"),
        ("memory_index_bytes", "index"),
        ("memory_cache_bytes", "cache"),
        ("memory_lua_bytes", "lua"),
        ("memory_net_bytes", "net"),
        ("memory_tx_bytes", "tx"),
        ("slab_arena_used_bytes", "slab_arena"),
        ("slab_quota_used_bytes", "slab_quota"),
    ):
        if key in delta and delta[key] != 0:
            parts.append(f"{label}={format_bytes_delta(delta[key])}")
    return ", ".join(parts)


def process_delta_text(resource_snapshots: dict[str, Any]) -> str:
    instances = (resource_snapshots.get("process") or {}).get("instances") or {}
    parts: list[str] = []
    for name, instance in sorted(instances.items()):
        delta = instance.get("delta") or {}
        if not delta:
            continue
        after = instance.get("after") or {}
        before = instance.get("before") or {}
        pid = after.get("pid") or before.get("pid")
        cpu = float(delta.get("cpu_user_seconds", 0.0)) + float(delta.get("cpu_system_seconds", 0.0))
        item = f"{name}"
        if pid is not None:
            item += f"(pid={pid})"
        fields: list[str] = []
        if cpu != 0:
            fields.append(f"cpu={cpu:.3f}s")
        if "rss_bytes" in delta:
            if delta["rss_bytes"] != 0:
                fields.append(f"rss={format_bytes_delta(delta['rss_bytes'])}")
        if "vms_bytes" in delta:
            if delta["vms_bytes"] != 0:
                fields.append(f"vms={format_bytes_delta(delta['vms_bytes'])}")
        if fields:
            parts.append(f"{item} " + " ".join(fields))
    return "; ".join(parts)


def format_bytes_delta(value: float | int) -> str:
    sign = "+" if value >= 0 else "-"
    absolute = abs(float(value))
    units = ("B", "KiB", "MiB", "GiB")
    unit = units[0]
    for unit in units:
        if absolute < 1024.0 or unit == units[-1]:
            break
        absolute /= 1024.0
    if unit == "B":
        return f"{sign}{absolute:.0f}{unit}"
    return f"{sign}{absolute:.1f}{unit}"


def profile_artifact_text(artifacts: dict[str, str]) -> str:
    labels = (
        ("perf_data", "data"),
        ("perf_out", "script"),
        ("perf_folded", "folded"),
        ("perf_report", "report"),
        ("perf_svg", "flamegraph"),
        ("sample_report", "sample"),
    )
    return ", ".join(f"{label}={artifacts[key]}" for key, label in labels if key in artifacts)


def profile_artifact_error_text(errors: dict[str, str]) -> str:
    labels = (
        ("perf_report_error", "report"),
        ("perf_script_error", "script"),
        ("folded_error", "folded"),
        ("svg_error", "flamegraph"),
    )
    return ", ".join(f"{label}={_shorten_error(errors[key])}" for key, label in labels if key in errors)


def _shorten_error(value: str, *, limit: int = 180) -> str:
    collapsed = " ".join(str(value).split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3] + "..."


def display_path(value: str | None) -> str | None:
    if value is None:
        return None
    for source, target in display_path_rewrites():
        if value == source:
            return target
        if value.startswith(source + "/"):
            return target + value[len(source) :]
    return value


def display_path_rewrites() -> list[tuple[str, str]]:
    raw = os.environ.get("INGEST_BENCH_DISPLAY_PATH_REWRITES")
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []

    rewrites: list[tuple[str, str]] = []
    if not isinstance(payload, list):
        return rewrites

    for item in payload:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        source, target = item
        if isinstance(source, str) and isinstance(target, str):
            rewrites.append((source, target))

    return sorted(rewrites, key=lambda item: len(item[0]), reverse=True)


def candidate_isolation_label(candidate_isolation: str) -> str:
    if candidate_isolation == "fresh_cluster_per_candidate":
        return "fresh cluster per candidate"
    return "shared cluster across candidates"


def comparison_mode_label(fairness: str) -> str:
    if fairness == "fixed":
        return "same logical batch sizes (method defaults retained)"
    if fairness == "tuned":
        return "checked-in tuning grid"
    if fairness == "custom":
        return "exact custom candidates"
    return "same logical batch sizes + checked-in tuning grid"


def mode_label(mode: str, candidate_isolation: str) -> str:
    if mode == "reference":
        return "isolated fresh-cluster run"
    if mode == "smoke":
        return f"quick run ({candidate_isolation_label(candidate_isolation)})"
    return mode


def workload_label(workload_spec: dict[str, Any], workload_key: str) -> str:
    column_count = len(workload_spec.get("columns", []))
    index_count = len(workload_spec.get("indexes", []))
    if index_count == 0:
        index_label = "no secondary indexes"
    elif index_count == 1:
        index_label = "1 secondary index"
    else:
        index_label = f"{index_count} secondary indexes"
    return f"{workload_key} ({column_count} columns, {index_label})"


def format_seconds(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1f}s"


def report_cluster_seconds(report: BenchmarkReport, field_name: str) -> float | None:
    if report.candidate_isolation != "reused_cluster" or not report.trials:
        return None
    return getattr(report.trials[0], field_name)


def summary_cluster_seconds(report: BenchmarkReport, field_name: str) -> float | None:
    shared_seconds = report_cluster_seconds(report, field_name)
    if shared_seconds is not None:
        return shared_seconds
    trial_seconds = [getattr(trial, field_name) for trial in report.trials]
    if not trial_seconds:
        return None
    return sum(trial_seconds) / len(trial_seconds)


def cluster_timing_scope(candidate_isolation: str) -> str:
    if candidate_isolation == "reused_cluster":
        return "shared_cluster"
    return "candidate_average"


def context_line(summary: dict[str, Any]) -> str:
    setup_label = "setup"
    balance_label = "balance"
    if summary["candidate_isolation"] == "fresh_cluster_per_candidate":
        setup_label = "candidate_setup_avg"
        balance_label = "candidate_balance_avg"

    return (
        "Context: "
        f"distribution={summary['distribution_label']}, "
        f"warmup={summary['warmup_rows']}, "
        f"memtx={summary['instance_memtx_memory']}, "
        f"{setup_label}={format_seconds(summary['cluster_setup_seconds'])}, "
        f"{balance_label}={format_seconds(summary['cluster_balance_seconds'])}"
    )
