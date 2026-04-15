from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from threading import Barrier, BrokenBarrierError, Event
from time import perf_counter, thread_time
from typing import Any, Callable, Sequence, TypeVar


@dataclass
class WorkerMetrics:
    logical_batch_count: int = 0
    pipeline_sync_count: int = 0
    row_count: int = 0
    logical_payload_bytes: int = 0
    statement_template_bytes: int = 0
    parameter_payload_bytes: int = 0
    prepared_statement_count: int = 0
    build_seconds: float = 0.0
    submit_seconds: float = 0.0
    finish_seconds: float = 0.0
    elapsed_wall_seconds: float = 0.0
    elapsed_cpu_seconds: float = 0.0
    build_cpu_seconds: float = 0.0
    submit_cpu_seconds: float = 0.0
    finish_cpu_seconds: float = 0.0
    build_wait_seconds: float = 0.0
    submit_wait_seconds: float = 0.0
    finish_wait_seconds: float = 0.0
    start_command_wall_seconds: float = 0.0
    start_command_cpu_seconds: float = 0.0
    start_command_wait_seconds: float = 0.0
    protocol_teardown_wall_seconds: float = 0.0
    protocol_teardown_cpu_seconds: float = 0.0
    protocol_teardown_wait_seconds: float = 0.0
    insert_execute_count: int = 0
    copy_write_count: int = 0
    copy_write_bytes: int = 0
    batch_latencies_ms: list[float] = field(default_factory=list)


def timed_delta(started_wall: float, started_cpu: float) -> tuple[float, float, float]:
    wall = perf_counter() - started_wall
    cpu = thread_time() - started_cpu
    return wall, cpu, max(wall - cpu, 0.0)


PartitionT = TypeVar("PartitionT")
ResultT = TypeVar("ResultT")


def run_partitioned_workers(
    *,
    partitions: Sequence[PartitionT],
    max_workers: int,
    setup_error_message: str,
    submit_worker: Callable[[PartitionT, Barrier, Event], ResultT],
    on_ready: Callable[[], None] | None = None,
) -> tuple[list[ResultT], float]:
    active_partitions = [partition for partition in partitions if partition]
    ready_barrier = Barrier(len(active_partitions) + 1)
    start_event = Event()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(submit_worker, partition, ready_barrier, start_event) for partition in active_partitions
        ]
        try:
            ready_barrier.wait(timeout=60)
        except BrokenBarrierError as exc:
            for future in futures:
                future.result()
            raise RuntimeError(setup_error_message) from exc

        if on_ready is not None:
            on_ready()
        started = perf_counter()
        start_event.set()
        worker_results = [future.result() for future in futures]

    return worker_results, perf_counter() - started


def aggregate_worker_metrics(worker_metrics: Sequence[WorkerMetrics]) -> dict[str, Any]:
    return {
        "batch_count": sum(worker.logical_batch_count for worker in worker_metrics),
        "pipeline_sync_count": sum(worker.pipeline_sync_count for worker in worker_metrics),
        "total_rows": sum(worker.row_count for worker in worker_metrics),
        "logical_payload_bytes": sum(worker.logical_payload_bytes for worker in worker_metrics),
        "statement_template_bytes": sum(worker.statement_template_bytes for worker in worker_metrics),
        "parameter_payload_bytes": sum(worker.parameter_payload_bytes for worker in worker_metrics),
        "prepared_statement_count": sum(worker.prepared_statement_count for worker in worker_metrics),
        "worker_build_seconds": sum(worker.build_seconds for worker in worker_metrics),
        "worker_submit_seconds": sum(worker.submit_seconds for worker in worker_metrics),
        "worker_finish_seconds": sum(worker.finish_seconds for worker in worker_metrics),
        "worker_elapsed_wall_seconds_sum": sum(worker.elapsed_wall_seconds for worker in worker_metrics),
        "worker_elapsed_wall_seconds_max": max((worker.elapsed_wall_seconds for worker in worker_metrics), default=0.0),
        "worker_elapsed_cpu_seconds_sum": sum(worker.elapsed_cpu_seconds for worker in worker_metrics),
        "worker_build_cpu_seconds": sum(worker.build_cpu_seconds for worker in worker_metrics),
        "worker_submit_cpu_seconds": sum(worker.submit_cpu_seconds for worker in worker_metrics),
        "worker_finish_cpu_seconds": sum(worker.finish_cpu_seconds for worker in worker_metrics),
        "worker_build_wait_seconds": sum(worker.build_wait_seconds for worker in worker_metrics),
        "worker_submit_wait_seconds": sum(worker.submit_wait_seconds for worker in worker_metrics),
        "worker_finish_wait_seconds": sum(worker.finish_wait_seconds for worker in worker_metrics),
        "worker_start_command_wall_seconds": sum(worker.start_command_wall_seconds for worker in worker_metrics),
        "worker_start_command_cpu_seconds": sum(worker.start_command_cpu_seconds for worker in worker_metrics),
        "worker_start_command_wait_seconds": sum(worker.start_command_wait_seconds for worker in worker_metrics),
        "worker_protocol_teardown_wall_seconds": sum(
            worker.protocol_teardown_wall_seconds for worker in worker_metrics
        ),
        "worker_protocol_teardown_cpu_seconds": sum(worker.protocol_teardown_cpu_seconds for worker in worker_metrics),
        "worker_protocol_teardown_wait_seconds": sum(
            worker.protocol_teardown_wait_seconds for worker in worker_metrics
        ),
        "insert_execute_count": sum(worker.insert_execute_count for worker in worker_metrics),
        "copy_write_count": sum(worker.copy_write_count for worker in worker_metrics),
        "copy_write_bytes": sum(worker.copy_write_bytes for worker in worker_metrics),
        "batch_latencies_ms": [latency for worker in worker_metrics for latency in worker.batch_latencies_ms],
    }
