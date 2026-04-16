import json
from pathlib import Path
from types import SimpleNamespace

from picodata_ingest_bench import container as container_module
from picodata_ingest_bench import picodata
from picodata_ingest_bench import runner as runner_module
from picodata_ingest_bench.cli import main as cli_main
from picodata_ingest_bench.config import BenchmarkMethod, FairnessMode, RunMode, ServerProfileScope
from picodata_ingest_bench.container import build_parser, normalize_ingest_args, parse_container_args
from picodata_ingest_bench.planner import build_benchmark_plan, render_plan_text
from picodata_ingest_bench.profiling import (
    summarize_folded_stacks,
    summarize_macos_sample,
    summarize_perf_report_header,
)
from picodata_ingest_bench.results import BenchmarkReport, TrialMetrics, TrialResult, render_text_summary
from picodata_ingest_bench.runtime import build_container_run, rewrite_container_argv
from picodata_ingest_bench.workloads import (
    WORKLOADS,
    StridedRows,
    build_copy_payload,
    generate_rows,
    logical_payload_bytes,
    split_rows,
)

BENCHMARK_ROOT = Path(__file__).resolve().parents[1]
PICODATA_SOURCE = (BENCHMARK_ROOT.parent / "picodata").resolve()


def test_plan_rejects_invalid_numeric_inputs() -> None:
    plan = _plan(scale=0, concurrency=0, instance_count=0, server_profile_frequency=0)

    assert "--scale must be >= 1, got 0" in plan.errors
    assert "--concurrency must be >= 1, got 0" in plan.errors
    assert "--instance-count must be >= 1, got 0" in plan.errors
    assert "--server-profile-frequency must be >= 1, got 0" in plan.errors


def test_plan_rejects_concurrency_larger_than_rows() -> None:
    plan = _plan(scale=1, concurrency=8)

    assert "--concurrency must be <= measured rows (1), got 8" in plan.errors


def test_negative_scale_stays_visible_as_error() -> None:
    plan = _plan(scale=-1)

    assert plan.row_count == -1
    assert "--scale must be >= 1, got -1" in plan.errors


def test_split_rows_uses_lazy_strided_partitions() -> None:
    rows = generate_rows(WORKLOADS["narrow"], 10, seed=1)
    partitions = split_rows(rows, 3)

    assert all(isinstance(partition, StridedRows) for partition in partitions)
    assert [len(partition) for partition in partitions] == [4, 3, 3]
    assert [row[0] for row in partitions[0]] == [0, 3, 6, 9]


def test_tuned_batch_rows_do_not_exceed_measured_rows() -> None:
    plan = _plan(scale=2_048, method=BenchmarkMethod.INSERT, fairness=FairnessMode.TUNED)

    assert all(candidate.config.batch_rows <= plan.row_count for candidate in plan.candidates)
    assert 4_096 not in {candidate.config.batch_rows for candidate in plan.candidates}


def test_logical_payload_bytes_matches_copy_payload_without_full_payload_dependency() -> None:
    rows = generate_rows(WORKLOADS["narrow"], 8, seed=1)

    assert logical_payload_bytes(rows) == len(build_copy_payload(rows).encode("utf-8"))


def test_malformed_macos_sample_reports_parse_error(tmp_path: Path) -> None:
    sample = tmp_path / "sample.txt"
    sample.write_text("not a sample report\n")

    summary = summarize_macos_sample(sample)

    assert summary["total_samples"] == 0
    assert "parse_error" in summary


def test_malformed_folded_stacks_report_parse_error(tmp_path: Path) -> None:
    folded = tmp_path / "perf.folded"
    folded.write_text("not-a-folded-stack\nalso bad nope\n")

    summary = summarize_folded_stacks(folded)

    assert summary["total_samples"] == 0
    assert summary["malformed_line_count"] == 2
    assert "parse_error" in summary


def test_container_wrapper_keeps_plan_subcommand() -> None:
    assert normalize_ingest_args("run", ["--", "plan", "--profile", "multi-node-unsharded"]) == [
        "plan",
        "--profile",
        "multi-node-unsharded",
    ]
    assert normalize_ingest_args("run", ["--profile", "multi-node-unsharded"])[:2] == ["run", "--profile"]
    assert normalize_ingest_args("plan", ["--profile", "multi-node-unsharded"])[:2] == ["plan", "--profile"]
    parsed = parse_container_args(["plan", "--profile", "multi-node-unsharded"], build_parser())
    assert parsed.command == "plan"
    assert parsed.ingest_args == ["--profile", "multi-node-unsharded"]


def test_container_wrapper_accepts_global_container_image() -> None:
    parsed = parse_container_args(
        ["--container-image", "custom-image", "run", "--", "--profile", "multi-node-unsharded"],
        build_parser(),
    )

    assert parsed.command == "run"
    assert parsed.container_image == "custom-image"
    assert parsed.ingest_args == ["--", "--profile", "multi-node-unsharded"]


def test_container_describe_does_not_require_docker(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("describe should not inspect or build a container image")

    monkeypatch.setattr(container_module, "ensure_container_image", fail_if_called)

    assert container_module.main(["describe"]) == 0


def test_container_path_rewrite_tracks_output_and_profile_dir(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    profile_dir = tmp_path / "profiles"
    rewrite = rewrite_container_argv(
        argv=["run", "--output", str(report_path), "--server-profile-dir", str(profile_dir)],
        picodata_source=PICODATA_SOURCE,
    )

    source_index = rewrite.argv.index("--picodata-source")
    assert rewrite.argv[source_index + 1] == "/work/picodata"
    assert rewrite.report_paths == [report_path]
    assert any(target == str(report_path) for _, target in rewrite.path_rewrites)
    assert any(target == str(profile_dir) for _, target in rewrite.path_rewrites)


def test_container_default_profile_dir_stays_next_to_requested_output(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    profile_dir = tmp_path / "report-profiles"
    rewrite = rewrite_container_argv(
        argv=["run", "--server-profile", "--output", str(report_path)],
        picodata_source=PICODATA_SOURCE,
    )

    assert "--server-profile-dir" in rewrite.argv
    assert any(target == str(profile_dir) for _, target in rewrite.path_rewrites)


def test_container_plan_does_not_build_target() -> None:
    container_run = build_container_run(
        argv=["plan", "--profile", "multi-node-unsharded", "--workload", "narrow"],
        container_image="picodata-ingest-bench-base",
        picodata_source=PICODATA_SOURCE,
    )

    assert "INGEST_BENCH_BUILD_BEFORE_RUN=0" in container_run.command
    assert "PICODATA_SOURCE=/work/picodata" in container_run.command


def test_container_run_mounts_separate_picodata_source(tmp_path: Path) -> None:
    source = tmp_path / "picodata"
    (source / "test").mkdir(parents=True)
    (source / "Cargo.toml").write_text("[workspace]\n")
    (source / "test" / "conftest.py").write_text("")

    container_run = build_container_run(
        argv=["run", "--profile", "multi-node-unsharded", "--workload", "narrow"],
        container_image="picodata-ingest-bench-base",
        picodata_source=source,
    )

    assert f"{source.resolve()}:/work/picodata" in container_run.command
    assert f"{BENCHMARK_ROOT}:/work/picodata-ingest-bench:ro" in container_run.command
    assert "PYTHONPATH=/work/picodata-ingest-bench:/work/picodata/test:/work/picodata" in container_run.command
    command_text = " ".join(container_run.command)
    assert "'--picodata-source' '/work/picodata'" in command_text


def test_container_plan_does_not_require_docker(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("plan should not inspect or build a container image")

    monkeypatch.setattr(container_module, "ensure_container_image", fail_if_called)

    assert container_module.main(["plan", "--profile", "multi-node-unsharded", "--workload", "narrow"]) == 0


def test_native_plan_rejects_container_reuse_flag() -> None:
    plan = _plan(execution_runtime="native", reuse_container_build=True)
    text = render_plan_text(plan)

    assert any("--reuse-container-build requires container runtime" in error for error in plan.errors)
    assert "container build:" not in text
    assert "reuse existing target artifact" not in text


def test_plan_records_picodata_source() -> None:
    plan = _plan()
    text = render_plan_text(plan)

    assert plan.picodata_source == PICODATA_SOURCE
    assert f"Picodata source: {PICODATA_SOURCE}" in text


def test_plan_accepts_instance_count_override() -> None:
    plan = _plan(instance_count=5)
    payload = plan.to_dict()
    text = render_plan_text(plan)

    assert plan.instance_count == 5
    assert payload["profile"]["instance_count"] == 5
    assert payload["profile"]["default_instance_count"] == 3
    assert "instances per cluster: 5 (profile default: 3)" in text


def test_run_passes_instance_count_override_to_cluster(monkeypatch) -> None:
    cluster_kwargs: list[dict] = []

    class FakeCluster:
        def __init__(self, **kwargs) -> None:
            cluster_kwargs.append(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_run_candidate(*, profile, workload, mode, seed, row_count, **_kwargs):
        return TrialResult(
            profile=profile.key,
            workload=workload.key,
            mode=mode.value,
            seed=seed,
            row_count=row_count,
            trial_id="trial_1",
            cluster_setup_seconds=0.0,
            cluster_balance_seconds=0.0,
            metrics=_trial_metrics(total_rows=row_count),
        )

    monkeypatch.setattr(runner_module, "PicodataBenchmarkCluster", FakeCluster)
    monkeypatch.setattr(runner_module, "_run_candidate", fake_run_candidate)
    monkeypatch.setattr(
        runner_module,
        "load_runtime_modules",
        lambda _source: SimpleNamespace(cargo_build_profile=lambda: "release"),
    )
    monkeypatch.setattr(runner_module, "_git_metadata", lambda _source: {})
    monkeypatch.setattr(runner_module, "_host_metadata", lambda: {})

    report = runner_module.run_benchmark(
        runner_module.RunArgs(
            profile="multi-node-sharded",
            workload="narrow",
            method=BenchmarkMethod.COPY,
            fairness=FairnessMode.FIXED,
            mode=RunMode.SMOKE,
            scale=1,
            output=None,
            seed=1,
            concurrency=1,
            instance_count=5,
            execution_runtime="native",
            picodata_source=PICODATA_SOURCE,
        )
    )

    assert cluster_kwargs[0]["instance_count"] == 5
    assert report.profile_spec["instance_count"] == 5


def test_cli_accepts_nodes_per_cluster_alias(capsys) -> None:
    assert (
        cli_main(
            [
                "plan",
                "--format",
                "json",
                "--picodata-source",
                str(PICODATA_SOURCE),
                "--profile",
                "multi-node-unsharded",
                "--workload",
                "narrow",
                "--nodes-per-cluster",
                "4",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"]["instance_count"] == 4
    assert payload["profile"]["default_instance_count"] == 3


def test_describe_text_has_no_database_axis(capsys) -> None:
    assert cli_main(["describe"]) == 0

    text = capsys.readouterr().out
    assert "Databases:" not in text


def test_describe_text_explains_kafka_like_vs_narrow(capsys) -> None:
    assert cli_main(["describe"]) == 0

    text = capsys.readouterr().out
    assert "narrow: 2 columns, no secondary indexes; Narrow synthetic rows" in text
    assert "kafka-like: 4 columns, no secondary indexes; Event-shaped rows" in text
    assert "payload=232B" in text
    assert "partitions=16" in text
    assert "timestamp_step=1s" in text
    assert "narrow-indexed: 4 columns, 3 secondary indexes; Narrow rows plus tenant/stream" in text
    assert "kafka-like-indexed: 5 columns, 4 secondary indexes; Event-shaped rows plus tenant dimension" in text
    assert "tenants=512" in text
    assert "not a Kafka broker/protocol benchmark" in text
    assert "narrow means small rows for protocol and batching baseline" in text


def test_picodata_helpers_own_picodata_sql() -> None:
    workload = WORKLOADS["narrow"]

    create_sql = picodata.create_table_sql("t", workload, "multi-node-sharded")
    copy_sql = picodata.copy_from_stdin_sql("t", workload, batch_rows=256)

    assert "USING memtx" in create_sql
    assert 'DISTRIBUTED BY ("id")' in create_sql
    assert "SESSION_FLUSH_ROWS = 256" in copy_sql


def test_profile_category_names_are_current(tmp_path: Path) -> None:
    folded = tmp_path / "perf.folded"
    folded.write_text(
        "\n".join(
            [
                "root;prepare_copy_target 2",
                "root;CopyDestination::route 3",
                "root;copy_from_stdin;flush_batch 4",
                "root;CopySession::read 5",
                "root;TextRowParser::decode_record_into 6",
                "root;PreparedCopyTarget::prepare_pending_row;encode_row_with_builder 7",
                "root;PreparedCopyTarget::flush_sharded_batches;dispatch_remote_batches 8",
                "root;CopySession::process_record;insert_encoded_tuple;Space::insert;memtx_tree_index_replace 9",
                "root;pgproto::backend::copy::new_work 10",
            ]
        )
        + "\n"
    )

    summary = summarize_folded_stacks(folded)
    categories = {row["name"] for row in summary["top_categories"]}

    assert {
        "copy_target_setup",
        "copy_route",
        "copy_target_flush",
        "copy_row_decode",
        "copy_target_encode",
        "copy_target_dispatch_remote",
        "pgproto_copy_in",
        "direct_insert_storage",
        "copy_unclassified",
    } <= categories


def test_insert_profile_category_names_are_current(tmp_path: Path) -> None:
    folded = tmp_path / "perf.folded"
    folded.write_text(
        "\n".join(
            [
                "root;process_bind_message;Backend::bind;decode_parameters 2",
                "root;process_execute_message;Backend::execute;execute_bound_dml;step_portal 3",
                "root;dispatch_bound_statement;custom_plan_dispatch_dml;bucket_dispatch 4",
                "root;custom_plan_dispatch_dml;DmlRequest::into_message;write_insert_packet;write_tuples 5",
                "root;tuple_insert_from_proto;insert_encoded_tuple;Space::insert;memtx_tree_index_replace 6",
                "root;DmlRequest::new_insert_work 7",
            ]
        )
        + "\n"
    )

    summary = summarize_folded_stacks(folded)
    categories = {row["name"] for row in summary["top_categories"]}

    assert {
        "insert_bind_decode",
        "insert_execute",
        "insert_sql_dispatch",
        "insert_tuple_encode",
        "direct_insert_storage",
        "insert_unclassified",
    } <= categories


def test_perf_report_header_summary_exposes_raw_and_lost_samples(tmp_path: Path) -> None:
    report = tmp_path / "perf-report.txt"
    report.write_text(
        "\n".join(
            [
                "# Total Lost Samples: 0",
                "# Samples: 1,867  of event 'cpu-clock:pppH'",
                "# Event count (approx.): 18858585670",
                "# Overhead  Symbol  Shared Object",
            ]
        )
        + "\n"
    )

    summary = summarize_perf_report_header(report)

    assert summary == {
        "raw_sample_count": 1867,
        "lost_sample_count": 0,
        "event_count_approx": 18858585670,
        "perf_event": "cpu-clock:pppH",
    }


def test_perf_report_header_summary_accepts_perf_scaled_sample_count(tmp_path: Path) -> None:
    report = tmp_path / "perf-report.txt"
    report.write_text("# Samples: 1K of event 'cpu-clock:pppH'\n# Overhead  Symbol\n")

    summary = summarize_perf_report_header(report)

    assert summary["raw_sample_count"] == 1000


def test_text_summary_exposes_time_resources_and_profile_artifacts(tmp_path: Path) -> None:
    perf_data = tmp_path / "perf.data"
    perf_script = tmp_path / "perf.out"
    trial = TrialResult(
        profile="multi-node-unsharded",
        workload="narrow",
        mode="smoke",
        seed=1,
        row_count=128,
        trial_id="trial_1",
        cluster_setup_seconds=1.0,
        cluster_balance_seconds=0.0,
        metrics=_trial_metrics(
            resource_snapshots={
                "delta": {
                    "cpu_user_seconds": 1.5,
                    "cpu_system_seconds": 0.25,
                    "memory_data_bytes": 2048,
                    "memory_index_bytes": 1024,
                    "memory_net_bytes": -512,
                    "slab_arena_used_bytes": 4096,
                },
                "process": {
                    "instances": {
                        "i1": {
                            "before": {"pid": 123, "rss_bytes": 4096},
                            "after": {"pid": 123, "rss_bytes": 6144},
                            "delta": {
                                "cpu_user_seconds": 1.0,
                                "cpu_system_seconds": 0.25,
                                "rss_bytes": 2048,
                                "vms_bytes": 4096,
                            },
                        }
                    }
                },
            },
            server_profile={
                "tool": "perf",
                "output_dir": str(tmp_path),
                "aggregate": {
                    "sample_unit": "samples",
                    "total_samples": 10,
                    "raw_sample_count": 3,
                    "lost_sample_count": 0,
                    "top_categories": [{"name": "copy_row_decode", "percent": 70.0}],
                    "top_leaf_functions_from_instance_tops": [{"name": "leaf", "percent": 50.0}],
                },
                "instances": {
                    "i1": {
                        "pid": 123,
                        "perf_data": str(perf_data),
                        "perf_out": str(perf_script),
                        "perf_script_error": "short script output",
                        "samples": {"total_samples": 10, "raw_sample_count": 3, "lost_sample_count": 0},
                    }
                },
            },
        ),
    )
    report = _report(
        [trial], server_profile={"enabled": True, "scope": "selected", "frequency_hz": 99, "output_dir": str(tmp_path)}
    )

    text = render_text_summary(report)

    assert "Time/resource detail:" in text
    assert "Database:" not in text
    assert "worker_wall[start=0.010s, build=0.020s, submit=0.030s" in text
    assert "resource delta: cpu_user=1.500s, cpu_system=0.250s, data=+2.0KiB" in text
    assert "process delta: i1(pid=123) cpu=1.250s rss=+2.0KiB vms=+4.0KiB" in text
    assert "sample weight by category:" in text
    assert "samples: raw=3, weighted=10, lost=0" in text
    assert "i1 (raw=3, weighted=10, lost=0):" in text
    assert "sample unit: samples" in text
    assert f"data={perf_data}" in text
    assert f"script={perf_script}" in text
    assert "artifact errors: script=short script output" in text


def test_workload_shape_is_preserved_in_report_json() -> None:
    trial = TrialResult(
        profile="multi-node-unsharded",
        workload="kafka-like",
        mode="smoke",
        seed=1,
        row_count=128,
        trial_id="trial_1",
        cluster_setup_seconds=1.0,
        cluster_balance_seconds=0.0,
        metrics=_trial_metrics(),
    )
    report = _report([trial], workload_key="kafka-like")

    payload = report.to_dict()

    assert payload["workload_spec"]["shape"]["payload_total_bytes"] == 232
    assert payload["workload_spec"]["shape"]["partition_count"] == 16


def _plan(
    *,
    scale: int | None = 1,
    concurrency: int = 1,
    server_profile_frequency: int = 99,
    instance_count: int | None = None,
    method: BenchmarkMethod = BenchmarkMethod.COPY,
    fairness: FairnessMode = FairnessMode.FIXED,
    execution_runtime: str = "native",
    reuse_container_build: bool = False,
):
    return build_benchmark_plan(
        profile="multi-node-unsharded",
        workload="narrow",
        method=method,
        fairness=fairness,
        mode=RunMode.SMOKE,
        scale=scale,
        concurrency=concurrency,
        instance_count=instance_count,
        seed=1,
        execution_runtime=execution_runtime,
        picodata_source=PICODATA_SOURCE,
        reuse_container_build=reuse_container_build,
        base_port=None,
        candidate_index=None,
        custom_insert_configs=(),
        custom_copy_configs=(),
        output=None,
        server_profile=False,
        server_profile_scope=ServerProfileScope.ALL,
        server_profile_dir=None,
        server_profile_frequency=server_profile_frequency,
    )


def _trial_metrics(**overrides) -> TrialMetrics:
    defaults = {
        "method": "copy",
        "fairness": "fixed",
        "configured_batch_rows": 128,
        "configured_copy_chunk_bytes": 65_536,
        "configured_insert_pipeline_sync_batches": None,
        "batch_latency_label": "client_write_latency_ms",
        "concurrency": 1,
        "batch_count": 1,
        "pipeline_sync_count": 0,
        "total_rows": 128,
        "logical_payload_bytes": 1024,
        "statement_template_bytes": 0,
        "parameter_payload_bytes": 0,
        "prepared_statement_count": 0,
        "wall_seconds": 0.5,
        "worker_build_seconds": 0.02,
        "worker_submit_seconds": 0.03,
        "worker_finish_seconds": 0.04,
        "worker_elapsed_wall_seconds_sum": 0.12,
        "worker_elapsed_wall_seconds_max": 0.12,
        "worker_elapsed_cpu_seconds_sum": 0.09,
        "worker_build_cpu_seconds": 0.01,
        "worker_submit_cpu_seconds": 0.02,
        "worker_finish_cpu_seconds": 0.03,
        "worker_build_wait_seconds": 0.01,
        "worker_submit_wait_seconds": 0.01,
        "worker_finish_wait_seconds": 0.01,
        "worker_start_command_wall_seconds": 0.01,
        "worker_start_command_cpu_seconds": 0.005,
        "worker_start_command_wait_seconds": 0.005,
        "worker_protocol_teardown_wall_seconds": 0.01,
        "worker_protocol_teardown_cpu_seconds": 0.005,
        "worker_protocol_teardown_wait_seconds": 0.005,
        "insert_execute_count": 0,
        "copy_write_count": 1,
        "copy_write_bytes": 1024,
        "batch_latencies_ms": [1.0],
        "client_protocol_bytes": 2048,
        "resource_snapshots": {},
        "sharded_distribution": {},
        "log_validation": {},
        "server_metric_deltas": {},
        "server_profile": {},
    }
    defaults.update(overrides)
    return TrialMetrics(**defaults)


def _report(
    trials: list[TrialResult],
    *,
    server_profile: dict | None = None,
    workload_key: str = "narrow",
) -> BenchmarkReport:
    workload = WORKLOADS[workload_key]
    return BenchmarkReport.new(
        build_profile="release",
        execution_runtime="container",
        git_metadata={},
        runtime_host_metadata={},
        database="picodata",
        profile="multi-node-unsharded",
        profile_spec={"display_name": "3-node global table", "distribution_mode": "global"},
        workload=workload.key,
        workload_spec={
            "columns": [{"name": column.name} for column in workload.columns],
            "indexes": [{"name": index.name, "columns": list(index.columns)} for index in workload.secondary_indexes],
            "shape": dict(workload.shape),
        },
        mode="smoke",
        candidate_isolation="reused_cluster",
        topology_ready_before_timing=True,
        requested_method="copy",
        requested_fairness="fixed",
        concurrency=1,
        seed=1,
        requested_rows=128,
        warmup_rows=0,
        candidate_space={
            "fixed_insert_pipeline_sync_batches": 16,
            "fixed_copy_chunk_bytes": 65_536,
        },
        server_profile=server_profile or {"enabled": False},
        environment={"instance_memtx_memory": "256M"},
        trials=trials,
    )
