from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from math import ceil
from pathlib import Path
from typing import Any

from picodata_ingest_bench.config import (
    BenchmarkMethod,
    FairnessMode,
    RunMode,
    ServerProfileScope,
    get_cluster_profile,
    get_run_preset,
)
from picodata_ingest_bench.methods.copy import CopyTrialConfig
from picodata_ingest_bench.methods.insert import InsertTrialConfig
from picodata_ingest_bench.workloads import get_workload


@dataclass(frozen=True)
class PlannedCandidate:
    ordinal: int
    method: BenchmarkMethod
    config: InsertTrialConfig | CopyTrialConfig

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "candidate_index": self.ordinal,
            "method": self.method.value,
            "fairness": self.config.fairness,
            "batch_rows": self.config.batch_rows,
            "concurrency": self.config.concurrency,
        }
        if isinstance(self.config, InsertTrialConfig):
            payload["insert_pipeline_sync_batches"] = self.config.pipeline_sync_batches
        else:
            payload["copy_chunk_bytes"] = self.config.copy_chunk_bytes
        return payload


@dataclass(frozen=True)
class BenchmarkPlan:
    profile_key: str
    workload_key: str
    method: BenchmarkMethod
    fairness: FairnessMode
    mode: RunMode
    row_count: int
    warmup_row_count: int
    concurrency: int
    seed: int
    execution_runtime: str
    picodata_source: Path
    reuse_container_build: bool
    base_port: int | None
    candidate_index: int | None
    available_candidate_count: int
    custom_insert_configs: tuple[tuple[int, int], ...]
    custom_copy_configs: tuple[tuple[int, int], ...]
    candidate_space: dict[str, object]
    candidates: tuple[PlannedCandidate, ...]
    server_profile: bool
    server_profile_scope: ServerProfileScope
    server_profile_dir: Path | None
    server_profile_frequency: int
    validation_errors: tuple[str, ...]

    @property
    def cluster_count(self) -> int:
        if self.mode is RunMode.REFERENCE:
            return len(self.candidates)
        return 1 if self.candidates else 0

    @property
    def profiled_trial_count(self) -> int:
        if not self.server_profile:
            return 0
        if self.server_profile_scope is ServerProfileScope.SELECTED:
            return 1 if len(self.candidates) == 1 else 0
        return len(self.candidates)

    @property
    def errors(self) -> tuple[str, ...]:
        errors = list(self.validation_errors)
        if (
            self.server_profile
            and self.server_profile_scope is ServerProfileScope.SELECTED
            and len(self.candidates) != 1
        ):
            errors.append(
                "--server-profile-scope=selected requires exactly one measured "
                f"candidate, but this invocation has {len(self.candidates)}; use --candidate-index from `plan`"
            )
        return tuple(errors)

    def to_dict(self) -> dict[str, Any]:
        profile = get_cluster_profile(self.profile_key)
        workload = get_workload(self.workload_key)
        return {
            "profile": {
                "key": profile.key,
                "display_name": profile.display_name,
                "instance_count": profile.instance_count,
                "distribution_mode": profile.distribution_mode,
            },
            "workload": {
                "key": workload.key,
                "description": workload.description,
                "column_count": len(workload.columns),
                "index_count": len(workload.secondary_indexes),
            },
            "mode": self.mode.value,
            "method": self.method.value,
            "fairness": self.fairness.value,
            "row_count": self.row_count,
            "warmup_row_count": self.warmup_row_count,
            "concurrency": self.concurrency,
            "seed": self.seed,
            "execution_runtime": self.execution_runtime,
            "picodata_source": str(self.picodata_source),
            "reuse_container_build": self.reuse_container_build,
            "base_port": self.base_port,
            "candidate_index": self.candidate_index,
            "available_candidate_count": self.available_candidate_count,
            "custom_insert_configs": [
                {"batch_rows": batch_rows, "pipeline_sync_batches": sync_batches}
                for batch_rows, sync_batches in self.custom_insert_configs
            ],
            "custom_copy_configs": [
                {"batch_rows": batch_rows, "copy_chunk_bytes": chunk_bytes}
                for batch_rows, chunk_bytes in self.custom_copy_configs
            ],
            "cluster_count": self.cluster_count,
            "candidate_count": len(self.candidates),
            "candidate_space": self.candidate_space,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "server_profile": {
                "enabled": self.server_profile,
                "scope": self.server_profile_scope.value,
                "frequency_hz": self.server_profile_frequency,
                "profiled_trial_count": self.profiled_trial_count,
                "output_dir": str(self.server_profile_dir) if self.server_profile_dir is not None else None,
            },
            "errors": list(self.errors),
        }


def build_benchmark_plan(
    *,
    profile: str,
    workload: str,
    method: BenchmarkMethod,
    fairness: FairnessMode,
    mode: RunMode,
    scale: int | None,
    concurrency: int,
    seed: int,
    execution_runtime: str,
    picodata_source: Path,
    reuse_container_build: bool,
    base_port: int | None,
    candidate_index: int | None,
    custom_insert_configs: tuple[tuple[int, int], ...],
    custom_copy_configs: tuple[tuple[int, int], ...],
    output: Path | None,
    server_profile: bool,
    server_profile_scope: ServerProfileScope,
    server_profile_dir: Path | None,
    server_profile_frequency: int,
) -> BenchmarkPlan:
    preset = get_run_preset(mode)
    row_count = preset.default_row_count if scale is None else scale
    warmup_row_count = min(preset.warmup_row_count, max(1, row_count // 4)) if row_count > 0 else 0
    validation_errors = _validate_plan_inputs(
        scale=scale,
        row_count=row_count,
        concurrency=concurrency,
        execution_runtime=execution_runtime,
        reuse_container_build=reuse_container_build,
        base_port=base_port,
        server_profile_frequency=server_profile_frequency,
        custom_insert_configs=custom_insert_configs,
        custom_copy_configs=custom_copy_configs,
    )
    custom_requested = bool(custom_insert_configs or custom_copy_configs or fairness is FairnessMode.CUSTOM)
    if custom_requested:
        all_candidates = _custom_candidates(
            method=method,
            insert_configs=custom_insert_configs,
            copy_configs=custom_copy_configs,
            concurrency=concurrency,
        )
    else:
        candidate_methods = (
            [BenchmarkMethod.INSERT, BenchmarkMethod.COPY] if method is BenchmarkMethod.ALL else [method]
        )
        fairness_modes = [FairnessMode.FIXED, FairnessMode.TUNED] if fairness is FairnessMode.BOTH else [fairness]
        all_candidates = tuple(
            PlannedCandidate(ordinal, candidate_method, candidate)
            for ordinal, (candidate_method, candidate) in enumerate(
                (
                    (candidate_method, candidate)
                    for candidate_method in candidate_methods
                    for candidate_fairness in fairness_modes
                    for candidate in candidate_configs(
                        method=candidate_method,
                        fairness=candidate_fairness,
                        preset=preset,
                        concurrency=concurrency,
                        row_count=row_count,
                    )
                ),
                start=1,
            )
        )

    if custom_requested and not all_candidates:
        validation_errors.append("custom fairness requires at least one custom candidate matching --method")

    if candidate_index is None:
        candidates = all_candidates
    elif 1 <= candidate_index <= len(all_candidates):
        candidates = (all_candidates[candidate_index - 1],)
    else:
        candidates = all_candidates
        validation_errors.append(
            f"--candidate-index must be between 1 and {len(all_candidates)}, got {candidate_index}"
        )

    return BenchmarkPlan(
        profile_key=profile,
        workload_key=workload,
        method=method,
        fairness=fairness,
        mode=mode,
        row_count=row_count,
        warmup_row_count=warmup_row_count,
        concurrency=concurrency,
        seed=seed,
        execution_runtime=execution_runtime,
        picodata_source=picodata_source,
        reuse_container_build=reuse_container_build,
        base_port=base_port,
        candidate_index=candidate_index,
        available_candidate_count=len(all_candidates),
        custom_insert_configs=custom_insert_configs,
        custom_copy_configs=custom_copy_configs,
        candidate_space=candidate_space(
            preset,
            row_count=row_count,
            custom_insert_configs=custom_insert_configs,
            custom_copy_configs=custom_copy_configs,
        ),
        candidates=candidates,
        server_profile=server_profile,
        server_profile_scope=server_profile_scope,
        server_profile_dir=server_profile_root(
            enabled=server_profile,
            explicit_dir=server_profile_dir,
            output=output,
        ),
        server_profile_frequency=server_profile_frequency,
        validation_errors=tuple(validation_errors),
    )


def candidate_configs(
    *,
    method: BenchmarkMethod,
    fairness: FairnessMode,
    preset,
    concurrency: int,
    row_count: int | None = None,
):
    if fairness is FairnessMode.FIXED:
        batch_rows = _effective_batch_rows(preset.fixed_batch_rows, row_count)
    else:
        batch_rows = _effective_batch_rows(preset.tuned_batch_rows, row_count)

    if method is BenchmarkMethod.INSERT:
        if fairness is FairnessMode.FIXED:
            return [
                InsertTrialConfig(
                    batch_rows=batch_size,
                    pipeline_sync_batches=preset.fixed_insert_pipeline_sync_batches,
                    fairness=fairness.value,
                    concurrency=concurrency,
                )
                for batch_size in batch_rows
            ]

        return [
            InsertTrialConfig(
                batch_rows=batch_size,
                pipeline_sync_batches=pipeline_sync_batches,
                fairness=fairness.value,
                concurrency=concurrency,
            )
            for batch_size in batch_rows
            for pipeline_sync_batches in _insert_pipeline_sync_candidates(
                preset=preset,
                batch_rows=batch_size,
                row_count=row_count,
            )
        ]

    if fairness is FairnessMode.FIXED:
        return [
            CopyTrialConfig(
                batch_rows=batch_size,
                copy_chunk_bytes=preset.fixed_copy_chunk_bytes,
                fairness=fairness.value,
                concurrency=concurrency,
            )
            for batch_size in batch_rows
        ]

    return [
        CopyTrialConfig(
            batch_rows=batch_size,
            copy_chunk_bytes=chunk_bytes,
            fairness=fairness.value,
            concurrency=concurrency,
        )
        for batch_size, chunk_bytes in product(batch_rows, reversed(preset.copy_chunk_bytes))
    ]


def _custom_candidates(
    *,
    method: BenchmarkMethod,
    insert_configs: tuple[tuple[int, int], ...],
    copy_configs: tuple[tuple[int, int], ...],
    concurrency: int,
) -> tuple[PlannedCandidate, ...]:
    candidates: list[tuple[BenchmarkMethod, InsertTrialConfig | CopyTrialConfig]] = []
    if method is not BenchmarkMethod.COPY:
        candidates.extend(
            (
                BenchmarkMethod.INSERT,
                InsertTrialConfig(
                    batch_rows=batch_rows,
                    pipeline_sync_batches=sync_batches,
                    fairness=FairnessMode.CUSTOM.value,
                    concurrency=concurrency,
                ),
            )
            for batch_rows, sync_batches in insert_configs
        )
    if method is not BenchmarkMethod.INSERT:
        candidates.extend(
            (
                BenchmarkMethod.COPY,
                CopyTrialConfig(
                    batch_rows=batch_rows,
                    copy_chunk_bytes=chunk_bytes,
                    fairness=FairnessMode.CUSTOM.value,
                    concurrency=concurrency,
                ),
            )
            for batch_rows, chunk_bytes in copy_configs
        )

    return tuple(
        PlannedCandidate(ordinal, candidate_method, config)
        for ordinal, (candidate_method, config) in enumerate(candidates, start=1)
    )


def candidate_space(
    preset,
    *,
    row_count: int | None = None,
    custom_insert_configs: tuple[tuple[int, int], ...] = (),
    custom_copy_configs: tuple[tuple[int, int], ...] = (),
) -> dict[str, object]:
    return {
        "fixed_batch_rows": list(preset.fixed_batch_rows),
        "tuned_batch_rows": list(preset.tuned_batch_rows),
        "effective_fixed_batch_rows": list(reversed(_effective_batch_rows(preset.fixed_batch_rows, row_count))),
        "effective_tuned_batch_rows": list(reversed(_effective_batch_rows(preset.tuned_batch_rows, row_count))),
        "fixed_copy_chunk_bytes": preset.fixed_copy_chunk_bytes,
        "copy_chunk_bytes": list(preset.copy_chunk_bytes),
        "fixed_insert_pipeline_sync_batches": preset.fixed_insert_pipeline_sync_batches,
        "tuned_insert_pipeline_sync_batches": list(preset.tuned_insert_pipeline_sync_batches),
        "tuned_insert_pipeline_sync_batches_by_batch_rows": {
            str(batch_rows): list(
                _insert_pipeline_sync_candidates(
                    preset=preset,
                    batch_rows=batch_rows,
                    row_count=row_count,
                )
            )
            for batch_rows in preset.tuned_batch_rows
        },
        "custom_insert_configs": [
            {"batch_rows": batch_rows, "pipeline_sync_batches": sync_batches}
            for batch_rows, sync_batches in custom_insert_configs
        ],
        "custom_copy_configs": [
            {"batch_rows": batch_rows, "copy_chunk_bytes": chunk_bytes}
            for batch_rows, chunk_bytes in custom_copy_configs
        ],
    }


def server_profile_root(
    *,
    enabled: bool,
    explicit_dir: Path | None,
    output: Path | None,
) -> Path | None:
    if not enabled:
        return None
    if explicit_dir is not None:
        return explicit_dir
    if output is not None:
        return output.parent / f"{output.stem}-profiles"
    return Path("tmp") / "ingest-profiles"


def render_plan_text(plan: BenchmarkPlan) -> str:
    profile = get_cluster_profile(plan.profile_key)
    workload = get_workload(plan.workload_key)
    execution_lines = [
        "Execution:",
        f"  runtime: {plan.execution_runtime}",
        f"  Picodata source: {plan.picodata_source}",
    ]
    if plan.execution_runtime == "container":
        container_build = "reuse existing target artifact" if plan.reuse_container_build else "build before run"
        execution_lines.append(f"  container build: {container_build}")
    execution_lines.extend(
        [
            f"  base port: {plan.base_port if plan.base_port is not None else 'auto'}",
            f"  selected candidate: {plan.candidate_index if plan.candidate_index is not None else 'all'}",
            f"  available candidates: {plan.available_candidate_count}",
            f"  candidates: {len(plan.candidates)} measured trials",
            f"  clusters: {plan.cluster_count}",
            f"  instances per cluster: {profile.instance_count}",
            f"  balancing before timing: {'yes' if profile.is_sharded else 'no'}",
        ]
    )
    lines = [
        "Benchmark plan:",
        f"  profile: {profile.display_name} [{profile.key}]",
        f"  workload: {workload.key}, {len(workload.columns)} columns, {len(workload.secondary_indexes)} indexes",
        f"  mode: {plan.mode.value}",
        f"  rows: {plan.row_count} measured, {plan.warmup_row_count} warmup",
        f"  method: {plan.method.value}",
        f"  fairness: {plan.fairness.value}",
        f"  concurrency: {plan.concurrency}",
        f"  seed: {plan.seed}",
        "",
        *execution_lines,
        "",
        "Candidates:",
    ]
    for candidate in plan.candidates:
        lines.append(f"  {_candidate_label(candidate)}")

    lines.extend(
        [
            "",
            "Profiling:",
            f"  enabled: {'yes' if plan.server_profile else 'no'}",
        ]
    )
    if plan.server_profile:
        lines.extend(
            [
                f"  scope: {plan.server_profile_scope.value}",
                f"  frequency: {plan.server_profile_frequency} Hz",
                f"  profiled trials: {plan.profiled_trial_count}",
                f"  expected profile sets: {plan.profiled_trial_count * profile.instance_count}",
                f"  output dir: {plan.server_profile_dir}",
                "  note: profiling is diagnostic and can require perf/sample privileges",
            ]
        )

    if plan.errors:
        lines.append("")
        lines.append("Errors:")
        lines.extend(f"  {error}" for error in plan.errors)

    return "\n".join(lines)


def _insert_pipeline_sync_candidates(*, preset, batch_rows: int, row_count: int | None) -> tuple[int, ...]:
    if row_count is None:
        return tuple(reversed(preset.tuned_insert_pipeline_sync_batches))

    upper = max(1, ceil(row_count / batch_rows))
    candidates = {value for value in preset.tuned_insert_pipeline_sync_batches if value <= upper}

    sync = 1
    while sync < upper:
        candidates.add(sync)
        sync *= 2
    candidates.add(upper)
    return tuple(sorted(candidates, reverse=True))


def _effective_batch_rows(configured_batch_rows: tuple[int, ...], row_count: int | None) -> tuple[int, ...]:
    ordered_batch_rows = tuple(reversed(configured_batch_rows))
    if row_count is None:
        return ordered_batch_rows
    candidates = tuple(batch_rows for batch_rows in ordered_batch_rows if batch_rows <= row_count)
    if candidates:
        return candidates
    return (max(row_count, 1),)


def _validate_plan_inputs(
    *,
    scale: int | None,
    row_count: int,
    concurrency: int,
    execution_runtime: str,
    reuse_container_build: bool,
    base_port: int | None,
    server_profile_frequency: int,
    custom_insert_configs: tuple[tuple[int, int], ...],
    custom_copy_configs: tuple[tuple[int, int], ...],
) -> list[str]:
    errors: list[str] = []
    if scale is not None and row_count < 1:
        errors.append(f"--scale must be >= 1, got {scale}")
    if concurrency < 1:
        errors.append(f"--concurrency must be >= 1, got {concurrency}")
    elif row_count > 0 and concurrency > row_count:
        errors.append(f"--concurrency must be <= measured rows ({row_count}), got {concurrency}")
    if reuse_container_build and execution_runtime != "container":
        errors.append(
            f"--reuse-container-build requires container runtime, but runtime resolved to {execution_runtime}"
        )
    if server_profile_frequency < 1:
        errors.append(f"--server-profile-frequency must be >= 1, got {server_profile_frequency}")
    if base_port is not None and not (1 <= base_port <= 65_535):
        errors.append(f"--base-port must be between 1 and 65535, got {base_port}")

    for batch_rows, sync_batches in custom_insert_configs:
        if batch_rows < 1 or sync_batches < 1:
            errors.append(f"--custom-insert values must be >= 1, got {batch_rows}:{sync_batches}")
    for batch_rows, chunk_bytes in custom_copy_configs:
        if batch_rows < 1 or chunk_bytes < 1:
            errors.append(f"--custom-copy values must be >= 1, got {batch_rows}:{chunk_bytes}")
    return errors


def _candidate_label(candidate: PlannedCandidate) -> str:
    config = candidate.config
    parts = [
        f"#{candidate.ordinal}",
        candidate.method.value,
        config.fairness,
        f"batch={config.batch_rows}",
    ]
    if isinstance(config, InsertTrialConfig):
        parts.append(f"sync={config.pipeline_sync_batches}")
    else:
        parts.append(f"chunk={config.copy_chunk_bytes}")
    return " ".join(parts)
