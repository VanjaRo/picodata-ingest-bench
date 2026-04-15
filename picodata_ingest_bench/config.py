from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class BenchmarkMethod(str, Enum):
    INSERT = "insert"
    COPY = "copy"
    ALL = "all"


class FairnessMode(str, Enum):
    FIXED = "fixed"
    TUNED = "tuned"
    CUSTOM = "custom"
    BOTH = "both"


class RunMode(str, Enum):
    SMOKE = "smoke"
    REFERENCE = "reference"


class ExecutionRuntime(str, Enum):
    AUTO = "auto"
    NATIVE = "native"
    CONTAINER = "container"


class ServerProfileScope(str, Enum):
    ALL = "all"
    SELECTED = "selected"


@dataclass(frozen=True)
class ClusterProfileSpec:
    key: str
    instance_count: int
    distribution_sql: str
    distribution_mode: str
    display_name: str
    description: str

    @property
    def is_global_table(self) -> bool:
        return self.distribution_mode == "global_table"

    @property
    def is_sharded(self) -> bool:
        return self.distribution_mode == "sharded"


@dataclass(frozen=True)
class RunPreset:
    mode: RunMode
    default_row_count: int
    warmup_row_count: int
    fixed_batch_rows: tuple[int, ...]
    tuned_batch_rows: tuple[int, ...]
    fixed_copy_chunk_bytes: int
    copy_chunk_bytes: tuple[int, ...]
    fixed_insert_pipeline_sync_batches: int
    tuned_insert_pipeline_sync_batches: tuple[int, ...]
    instance_memtx_memory: str


CLUSTER_PROFILES: dict[str, ClusterProfileSpec] = {
    "multi-node-sharded": ClusterProfileSpec(
        key="multi-node-sharded",
        instance_count=3,
        distribution_sql='DISTRIBUTED BY ("id")',
        distribution_mode="sharded",
        display_name="3-node sharded table",
        description="Multi-node cluster with a sharded table profile.",
    ),
    "multi-node-unsharded": ClusterProfileSpec(
        key="multi-node-unsharded",
        instance_count=3,
        distribution_sql="DISTRIBUTED GLOBALLY",
        distribution_mode="global_table",
        display_name="3-node global table",
        description="Multi-node cluster with a global-table profile.",
    ),
}


RUN_PRESETS: dict[RunMode, RunPreset] = {
    RunMode.SMOKE: RunPreset(
        mode=RunMode.SMOKE,
        default_row_count=2_048,
        warmup_row_count=256,
        fixed_batch_rows=(64, 256),
        tuned_batch_rows=(64, 128, 256, 512, 1024, 2048, 4096),
        fixed_copy_chunk_bytes=64 << 10,
        copy_chunk_bytes=(4 << 10, 16 << 10, 64 << 10),
        fixed_insert_pipeline_sync_batches=16,
        tuned_insert_pipeline_sync_batches=(1, 2, 4, 8, 16),
        instance_memtx_memory="256M",
    ),
    RunMode.REFERENCE: RunPreset(
        mode=RunMode.REFERENCE,
        default_row_count=100_000,
        warmup_row_count=8_192,
        fixed_batch_rows=(64, 256, 1_024),
        tuned_batch_rows=(64, 128, 256, 512, 1_024, 2_048),
        fixed_copy_chunk_bytes=256 << 10,
        copy_chunk_bytes=(4 << 10, 16 << 10, 64 << 10, 256 << 10),
        fixed_insert_pipeline_sync_batches=16,
        tuned_insert_pipeline_sync_batches=(1, 2, 4, 8, 16),
        instance_memtx_memory="1G",
    ),
}


def get_cluster_profile(key: str) -> ClusterProfileSpec:
    return CLUSTER_PROFILES[key]


def get_run_preset(mode: RunMode) -> RunPreset:
    return RUN_PRESETS[mode]


def distribution_mode_label(distribution_mode: str | None) -> str:
    if distribution_mode == "global_table":
        return "global table"
    if distribution_mode == "sharded":
        return "sharded"
    return distribution_mode or "unknown"


def candidate_isolation_for_mode(mode: RunMode) -> str:
    if mode is RunMode.REFERENCE:
        return "fresh_cluster_per_candidate"
    return "reused_cluster"
