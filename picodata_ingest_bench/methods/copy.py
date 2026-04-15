from __future__ import annotations

from dataclasses import dataclass
from threading import Barrier, BrokenBarrierError, Event
from time import perf_counter, thread_time
from typing import Callable
from typing import Sequence

import psycopg

from picodata_ingest_bench.methods.common import (
    WorkerMetrics,
    aggregate_worker_metrics,
    run_partitioned_workers,
    timed_delta,
)
from picodata_ingest_bench.results import TrialMetrics
from picodata_ingest_bench import picodata
from picodata_ingest_bench.workloads import (
    Row,
    WorkloadSpec,
    build_copy_payload,
    chunk_rows,
    split_rows,
)


@dataclass(frozen=True)
class CopyTrialConfig:
    batch_rows: int
    copy_chunk_bytes: int
    fairness: str
    concurrency: int


def run_copy_trial(
    *,
    dsn: str,
    table_name: str,
    workload: WorkloadSpec,
    rows: Sequence[Row],
    config: CopyTrialConfig,
    on_ready: Callable[[], None] | None = None,
) -> TrialMetrics:
    worker_metrics, wall_seconds = run_partitioned_workers(
        partitions=split_rows(rows, config.concurrency),
        max_workers=config.concurrency,
        setup_error_message="COPY worker failed during setup",
        on_ready=on_ready,
        submit_worker=lambda partition, ready_barrier, start_event: _run_copy_worker(
            dsn=dsn,
            table_name=table_name,
            workload=workload,
            rows=partition,
            batch_rows=config.batch_rows,
            copy_chunk_bytes=config.copy_chunk_bytes,
            ready_barrier=ready_barrier,
            start_event=start_event,
        ),
    )

    aggregated = aggregate_worker_metrics(worker_metrics)

    return TrialMetrics(
        method="copy",
        fairness=config.fairness,
        configured_batch_rows=config.batch_rows,
        configured_copy_chunk_bytes=config.copy_chunk_bytes,
        configured_insert_pipeline_sync_batches=None,
        batch_latency_label="client_write_latency_ms",
        concurrency=config.concurrency,
        batch_count=aggregated["batch_count"],
        pipeline_sync_count=0,
        total_rows=aggregated["total_rows"],
        logical_payload_bytes=aggregated["logical_payload_bytes"],
        statement_template_bytes=aggregated["statement_template_bytes"],
        parameter_payload_bytes=aggregated["parameter_payload_bytes"],
        prepared_statement_count=aggregated["prepared_statement_count"],
        wall_seconds=wall_seconds,
        worker_build_seconds=aggregated["worker_build_seconds"],
        worker_submit_seconds=aggregated["worker_submit_seconds"],
        worker_finish_seconds=aggregated["worker_finish_seconds"],
        worker_elapsed_wall_seconds_sum=aggregated["worker_elapsed_wall_seconds_sum"],
        worker_elapsed_wall_seconds_max=aggregated["worker_elapsed_wall_seconds_max"],
        worker_elapsed_cpu_seconds_sum=aggregated["worker_elapsed_cpu_seconds_sum"],
        worker_build_cpu_seconds=aggregated["worker_build_cpu_seconds"],
        worker_submit_cpu_seconds=aggregated["worker_submit_cpu_seconds"],
        worker_finish_cpu_seconds=aggregated["worker_finish_cpu_seconds"],
        worker_build_wait_seconds=aggregated["worker_build_wait_seconds"],
        worker_submit_wait_seconds=aggregated["worker_submit_wait_seconds"],
        worker_finish_wait_seconds=aggregated["worker_finish_wait_seconds"],
        worker_start_command_wall_seconds=aggregated["worker_start_command_wall_seconds"],
        worker_start_command_cpu_seconds=aggregated["worker_start_command_cpu_seconds"],
        worker_start_command_wait_seconds=aggregated["worker_start_command_wait_seconds"],
        worker_protocol_teardown_wall_seconds=aggregated["worker_protocol_teardown_wall_seconds"],
        worker_protocol_teardown_cpu_seconds=aggregated["worker_protocol_teardown_cpu_seconds"],
        worker_protocol_teardown_wait_seconds=aggregated["worker_protocol_teardown_wait_seconds"],
        insert_execute_count=aggregated["insert_execute_count"],
        copy_write_count=aggregated["copy_write_count"],
        copy_write_bytes=aggregated["copy_write_bytes"],
        batch_latencies_ms=aggregated["batch_latencies_ms"],
    )


def _run_copy_worker(
    *,
    dsn: str,
    table_name: str,
    workload: WorkloadSpec,
    rows: Sequence[Row],
    batch_rows: int,
    copy_chunk_bytes: int,
    ready_barrier: Barrier,
    start_event: Event,
) -> WorkerMetrics:
    worker = WorkerMetrics()
    copy_sql = picodata.copy_from_stdin_sql(table_name, workload, batch_rows)
    worker.statement_template_bytes = len(copy_sql.encode("utf-8"))

    with psycopg.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                ready_barrier.wait(timeout=60)
            except BrokenBarrierError as exc:
                raise RuntimeError("COPY setup barrier failed") from exc
            if not start_event.wait(timeout=60):
                raise RuntimeError("COPY start signal timed out")

            worker_started_wall = perf_counter()
            worker_started_cpu = thread_time()
            copy_started_wall = perf_counter()
            copy_started_cpu = thread_time()
            with cur.copy(copy_sql) as copy:
                wall, cpu, wait = timed_delta(copy_started_wall, copy_started_cpu)
                worker.start_command_wall_seconds += wall
                worker.start_command_cpu_seconds += cpu
                worker.start_command_wait_seconds += wait
                finish_started: float | None = None
                finish_started_cpu: float | None = None
                for batch in chunk_rows(rows, batch_rows):
                    batch_started = perf_counter()

                    build_started = perf_counter()
                    build_started_cpu = thread_time()
                    payload = build_copy_payload(batch).encode("utf-8")
                    wall, cpu, wait = timed_delta(build_started, build_started_cpu)
                    worker.build_seconds += wall
                    worker.build_cpu_seconds += cpu
                    worker.build_wait_seconds += wait

                    submit_started = perf_counter()
                    submit_started_cpu = thread_time()
                    for start in range(0, len(payload), copy_chunk_bytes):
                        chunk = payload[start : start + copy_chunk_bytes]
                        copy.write(chunk)
                        worker.copy_write_count += 1
                        worker.copy_write_bytes += len(chunk)
                    wall, cpu, wait = timed_delta(submit_started, submit_started_cpu)
                    worker.submit_seconds += wall
                    worker.submit_cpu_seconds += cpu
                    worker.submit_wait_seconds += wait

                    worker.row_count += len(batch)
                    worker.logical_batch_count += 1
                    worker.parameter_payload_bytes += len(payload)
                    worker.logical_payload_bytes += len(payload)
                    worker.batch_latencies_ms.append((perf_counter() - batch_started) * 1000.0)

                finish_started = perf_counter()
                finish_started_cpu = thread_time()
            if finish_started is not None and finish_started_cpu is not None:
                wall, cpu, wait = timed_delta(finish_started, finish_started_cpu)
                worker.finish_seconds += wall
                worker.finish_cpu_seconds += cpu
                worker.finish_wait_seconds += wait
            worker.elapsed_wall_seconds += perf_counter() - worker_started_wall
            worker.elapsed_cpu_seconds += thread_time() - worker_started_cpu
            status_message = cur.statusmessage

    expected_status = f"COPY {len(rows)}"
    if status_message != expected_status:
        raise AssertionError(
            f"unexpected COPY completion status for {table_name}: expected {expected_status!r}, got {status_message!r}"
        )

    return worker
