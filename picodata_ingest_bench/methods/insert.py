from __future__ import annotations

from dataclasses import dataclass
from threading import Barrier, BrokenBarrierError, Event
from time import perf_counter, thread_time
from typing import Callable, Sequence

import psycopg

from picodata_ingest_bench.methods.common import (
    WorkerMetrics,
    aggregate_worker_metrics,
    run_partitioned_workers,
    timed_delta,
)
from picodata_ingest_bench.results import TrialMetrics
from picodata_ingest_bench.workloads import (
    Row,
    WorkloadSpec,
    build_insert_parameters,
    build_prepared_insert_statement,
    chunk_rows,
    logical_payload_bytes,
    split_rows,
)


@dataclass(frozen=True)
class InsertTrialConfig:
    batch_rows: int
    pipeline_sync_batches: int
    fairness: str
    concurrency: int


def run_insert_trial(
    *,
    dsn: str,
    table_name: str,
    workload: WorkloadSpec,
    rows: Sequence[Row],
    config: InsertTrialConfig,
    on_ready: Callable[[], None] | None = None,
) -> TrialMetrics:
    worker_metrics, wall_seconds = run_partitioned_workers(
        partitions=split_rows(rows, config.concurrency),
        max_workers=config.concurrency,
        setup_error_message="insert worker failed during prewarm",
        on_ready=on_ready,
        submit_worker=lambda partition, ready_barrier, start_event: _run_insert_worker(
            dsn=dsn,
            table_name=table_name,
            workload=workload,
            rows=partition,
            batch_rows=config.batch_rows,
            pipeline_sync_batches=config.pipeline_sync_batches,
            ready_barrier=ready_barrier,
            start_event=start_event,
        ),
    )

    aggregated = aggregate_worker_metrics(worker_metrics)

    return TrialMetrics(
        method="insert",
        fairness=config.fairness,
        configured_batch_rows=config.batch_rows,
        configured_copy_chunk_bytes=None,
        configured_insert_pipeline_sync_batches=config.pipeline_sync_batches,
        batch_latency_label="sync_window_latency_ms",
        concurrency=config.concurrency,
        batch_count=aggregated["batch_count"],
        pipeline_sync_count=aggregated["pipeline_sync_count"],
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


def insert_prewarm_batches(rows: Sequence[Row], batch_rows: int) -> list[Sequence[Row]]:
    row_count = len(rows)
    if row_count == 0:
        return []
    full_batch_rows = min(batch_rows, row_count)
    batches = [rows[:full_batch_rows]]
    remainder = row_count % batch_rows
    if remainder and remainder != full_batch_rows:
        batches.append(rows[row_count - remainder :])
    return batches


def _run_insert_worker(
    *,
    dsn: str,
    table_name: str,
    workload: WorkloadSpec,
    rows: Sequence[Row],
    batch_rows: int,
    pipeline_sync_batches: int,
    ready_barrier: Barrier,
    start_event: Event,
) -> WorkerMetrics:
    worker = WorkerMetrics()
    worker.logical_payload_bytes = logical_payload_bytes(rows)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.prepare_threshold = 0
        with conn.cursor() as cur:
            prepared_queries: dict[int, str] = {}
            for batch in insert_prewarm_batches(rows, batch_rows):
                current_batch_rows = len(batch)
                prepared_query = build_prepared_insert_statement(table_name, workload, current_batch_rows)
                prepared_queries[current_batch_rows] = prepared_query
                worker.statement_template_bytes += len(prepared_query.encode("utf-8"))
                worker.prepared_statement_count += 1

                params, _ = build_insert_parameters(batch)
                cur.execute(prepared_query, params, prepare=True)

            cur.execute(f'TRUNCATE TABLE "{table_name}"')
            try:
                ready_barrier.wait(timeout=60)
            except BrokenBarrierError as exc:
                raise RuntimeError("insert prewarm barrier failed") from exc
            if not start_event.wait(timeout=60):
                raise RuntimeError("insert start signal timed out")

            worker_started_wall = perf_counter()
            worker_started_cpu = thread_time()
            protocol_teardown_started_wall: float | None = None
            protocol_teardown_started_cpu: float | None = None

            pipeline_started_wall = perf_counter()
            pipeline_started_cpu = thread_time()
            with conn.pipeline() as pipeline:
                wall, cpu, wait = timed_delta(pipeline_started_wall, pipeline_started_cpu)
                worker.start_command_wall_seconds += wall
                worker.start_command_cpu_seconds += cpu
                worker.start_command_wait_seconds += wait
                window_started = 0.0
                pending_window_batches = 0
                pending_window_rows = 0
                pending_window_parameter_bytes = 0

                def sync_window() -> None:
                    nonlocal window_started
                    nonlocal pending_window_batches
                    nonlocal pending_window_rows
                    nonlocal pending_window_parameter_bytes

                    if pending_window_batches == 0:
                        return

                    finish_started = perf_counter()
                    finish_started_cpu = thread_time()
                    pipeline.sync()
                    wall, cpu, wait = timed_delta(finish_started, finish_started_cpu)
                    worker.finish_seconds += wall
                    worker.finish_cpu_seconds += cpu
                    worker.finish_wait_seconds += wait
                    worker.pipeline_sync_count += 1
                    worker.logical_batch_count += pending_window_batches
                    worker.row_count += pending_window_rows
                    worker.parameter_payload_bytes += pending_window_parameter_bytes
                    worker.batch_latencies_ms.append((perf_counter() - window_started) * 1000.0)

                    window_started = 0.0
                    pending_window_batches = 0
                    pending_window_rows = 0
                    pending_window_parameter_bytes = 0

                for batch in chunk_rows(rows, batch_rows):
                    current_batch_rows = len(batch)

                    if pending_window_batches == 0:
                        window_started = perf_counter()

                    build_started = perf_counter()
                    build_started_cpu = thread_time()
                    query: str | None = prepared_queries.get(current_batch_rows)
                    prepare_query: bool | None = None
                    if query is None:
                        query = build_prepared_insert_statement(table_name, workload, current_batch_rows)
                        prepared_queries[current_batch_rows] = query
                        worker.statement_template_bytes += len(query.encode("utf-8"))
                        worker.prepared_statement_count += 1
                        prepare_query = True
                    params, parameter_payload_bytes = build_insert_parameters(batch)
                    wall, cpu, wait = timed_delta(build_started, build_started_cpu)
                    worker.build_seconds += wall
                    worker.build_cpu_seconds += cpu
                    worker.build_wait_seconds += wait

                    submit_started = perf_counter()
                    submit_started_cpu = thread_time()
                    cur.execute(query, params, prepare=prepare_query)
                    wall, cpu, wait = timed_delta(submit_started, submit_started_cpu)
                    worker.submit_seconds += wall
                    worker.submit_cpu_seconds += cpu
                    worker.submit_wait_seconds += wait
                    worker.insert_execute_count += 1

                    pending_window_batches += 1
                    pending_window_rows += len(batch)
                    pending_window_parameter_bytes += parameter_payload_bytes

                    if pending_window_batches >= pipeline_sync_batches:
                        sync_window()

                sync_window()
                protocol_teardown_started_wall = perf_counter()
                protocol_teardown_started_cpu = thread_time()

            if protocol_teardown_started_wall is not None and protocol_teardown_started_cpu is not None:
                wall, cpu, wait = timed_delta(protocol_teardown_started_wall, protocol_teardown_started_cpu)
                worker.protocol_teardown_wall_seconds += wall
                worker.protocol_teardown_cpu_seconds += cpu
                worker.protocol_teardown_wait_seconds += wait

            worker.elapsed_wall_seconds += perf_counter() - worker_started_wall
            worker.elapsed_cpu_seconds += thread_time() - worker_started_cpu

    return worker
