from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
from typing import Any


PROFILE_CATEGORY_DEFINITIONS = {
    "pgproto_copy_in": "COPY IN protocol session and CopyData/CopyDone handling.",
    "copy_target_setup": "COPY one-time target preparation before data streaming.",
    "copy_row_decode": "COPY record boundary scanning, text field decoding, and PG value conversion.",
    "copy_target_encode": "COPY per-row bucket id lookup, tuple conversion, and MsgPack encoding.",
    "copy_target_flush": "COPY batch flush bookkeeping and local batch handoff before storage insertion.",
    "copy_target_dispatch_remote": "COPY sharded remote batch dispatch and request packet construction.",
    "copy_route": "COPY destination routing for sharded or local writes.",
    "copy_unclassified": "COPY-like stack not matched to a more specific bucket.",
    "insert_bind_decode": "INSERT extended-query Bind parsing, parameter decoding, and portal binding.",
    "insert_execute": "INSERT extended-query Execute and portal control before SQL work.",
    "insert_sql_dispatch": "INSERT SQL execution, bucket routing, materialization, and DML dispatch.",
    "insert_tuple_encode": "INSERT tuple encoding and remote request packet construction.",
    "insert_unclassified": "INSERT-like stack not matched to a more specific bucket.",
    "direct_insert_storage": "Downstream Tarantool/memtx storage, index maintenance, and WAL write work.",
    "network": "Socket IO and network event-loop work.",
    "raft_control_plane": "Raft/CAS/control-plane work observed during the trial.",
    "lua_ffi_runtime": "LuaJIT, Lua runtime, and FFI bridge work.",
    "tarantool_fiber_runtime": "Tarantool fiber scheduling and wait primitives.",
    "unwind_runtime": "Stack unwinding and symbolication-related runtime work.",
    "idle_wait": "Kernel waits, condition variables, and idle event-loop time.",
    "unknown": "Samples not matched by the benchmark's coarse stack classifier.",
}
PERF_STOP_TIMEOUT_SECONDS = 30
MAC_SAMPLE_STOP_TIMEOUT_SECONDS = 120
KILL_TIMEOUT_SECONDS = 10


@dataclass
class ServerProfiler:
    output_dir: Path
    frequency: int
    recorders: list[Any]
    instance_metadata: dict[str, Any]

    @classmethod
    def start(cls, *, cluster, output_dir: Path, frequency: int) -> "ServerProfiler":
        cls.ensure_available()
        recorder_cls = _recorder_cls()

        output_dir.mkdir(parents=True, exist_ok=True)
        recorders = []
        instance_metadata = {}
        for index, instance in enumerate(cluster.instances, start=1):
            recorder = recorder_cls(
                name=instance.name,
                pid=_picodata_child_pid(instance),
                output_dir=output_dir,
                frequency=frequency,
            )
            recorder.start()
            recorders.append(recorder)
            instance_metadata[instance.name] = _instance_metadata(instance, index)

        return cls(
            output_dir=output_dir,
            frequency=frequency,
            recorders=recorders,
            instance_metadata=instance_metadata,
        )

    @staticmethod
    def ensure_available() -> None:
        tool = _profiler_tool()
        if shutil.which(tool) is None:
            raise RuntimeError(f"server profiling requested, but `{tool}` is not available")

    def stop(self) -> dict[str, Any]:
        instances: dict[str, Any] = {}
        for recorder in self.recorders:
            metadata = self.instance_metadata.get(recorder.name, {})
            result = recorder.stop_and_gather()
            result.update(metadata)
            instances[metadata.get("display_name", recorder.name)] = result

        return {
            "tool": _profiler_tool(),
            "frequency_hz": self.frequency,
            "output_dir": str(self.output_dir),
            "category_definitions": PROFILE_CATEGORY_DEFINITIONS,
            "instances": instances,
            "aggregate": _aggregate_instance_samples(instances),
        }


@dataclass
class PerfRecorder:
    name: str
    pid: int
    output_dir: Path
    frequency: int
    process: subprocess.Popen | None = None

    @property
    def perf_data(self) -> Path:
        return self.output_dir / f"{self.name}-perf.data"

    @property
    def perf_out(self) -> Path:
        return self.output_dir / f"{self.name}-perf.out"

    @property
    def perf_report(self) -> Path:
        return self.output_dir / f"{self.name}-perf-report.txt"

    @property
    def perf_folded(self) -> Path:
        return self.output_dir / f"{self.name}-perf.folded"

    @property
    def perf_svg(self) -> Path:
        return self.output_dir / f"{self.name}-perf.svg"

    def start(self) -> None:
        self.process = subprocess.Popen(
            [
                "perf",
                "record",
                "-p",
                str(self.pid),
                "-F",
                str(self.frequency),
                "--call-graph",
                "dwarf,65528",
                "-o",
                str(self.perf_data),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    def stop_and_gather(self) -> dict[str, Any]:
        assert self.process is not None

        stderr = _stop_recorder_process(self.process, interrupt_timeout=PERF_STOP_TIMEOUT_SECONDS)

        result: dict[str, Any] = {
            "pid": self.pid,
            "perf_data": str(self.perf_data),
            "perf_stderr": stderr.strip(),
            "returncode": self.process.returncode,
        }
        if self.process.returncode not in (0, -signal.SIGINT):
            result["error"] = "perf record failed"
            return result

        if not self.perf_data.exists() or self.perf_data.stat().st_size == 0:
            result["error"] = "perf did not produce data"
            return result

        result.update(_gather_perf_artifacts(self.perf_data, self.perf_out, self.perf_folded, self.perf_svg))
        if self.perf_folded.exists():
            samples = summarize_folded_stacks(self.perf_folded)
            if self.perf_report.exists():
                samples.update(summarize_perf_report_header(self.perf_report))
            result["samples"] = samples
        return result


@dataclass
class MacSampleRecorder:
    name: str
    pid: int
    output_dir: Path
    frequency: int
    process: subprocess.Popen | None = None

    @property
    def sample_report(self) -> Path:
        return self.output_dir / f"{self.name}-sample.txt"

    def start(self) -> None:
        interval_ms = max(1, round(1000 / max(self.frequency, 1)))
        self.process = subprocess.Popen(
            [
                "sample",
                str(self.pid),
                "3600",
                str(interval_ms),
                "-mayDie",
                "-file",
                str(self.sample_report),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

    def stop_and_gather(self) -> dict[str, Any]:
        assert self.process is not None

        stderr = _stop_recorder_process(self.process, interrupt_timeout=MAC_SAMPLE_STOP_TIMEOUT_SECONDS)

        result: dict[str, Any] = {
            "pid": self.pid,
            "sample_report": str(self.sample_report),
            "sample_stderr": stderr.strip(),
            "returncode": self.process.returncode,
        }
        if not self.sample_report.exists() or self.sample_report.stat().st_size == 0:
            result["error"] = "sample did not produce a report"
            return result

        result["samples"] = summarize_macos_sample(self.sample_report)
        return result


def _gather_perf_artifacts(perf_data: Path, perf_out: Path, perf_folded: Path, perf_svg: Path) -> dict[str, Any]:
    artifacts: dict[str, Any] = {"artifact_status": {}}

    report_path = perf_data.with_name(perf_data.name.replace("-perf.data", "-perf-report.txt"))
    with report_path.open("wb") as output:
        completed = subprocess.run(
            [
                "perf",
                "report",
                "--stdio",
                "--no-children",
                "--sort",
                "symbol,dso",
                "-i",
                str(perf_data),
            ],
            stdout=output,
            stderr=subprocess.PIPE,
            check=False,
        )
    _record_artifact_stage(
        artifacts,
        "report",
        path=report_path,
        returncode=completed.returncode,
        stderr=completed.stderr,
        error_key="perf_report_error",
    )

    with perf_out.open("wb") as output:
        completed = subprocess.run(
            ["perf", "script", "-i", str(perf_data)],
            stdout=output,
            stderr=subprocess.PIPE,
            check=False,
        )
    script_ok = _record_artifact_stage(
        artifacts,
        "script",
        path=perf_out,
        returncode=completed.returncode,
        stderr=completed.stderr,
        error_key="perf_script_error",
    )
    if not script_ok:
        return artifacts

    stackcollapse = _flamegraph_tool("stackcollapse-perf.pl")
    flamegraph = _flamegraph_tool("flamegraph.pl")
    if stackcollapse is None:
        artifacts["folded_error"] = "stackcollapse-perf.pl not found; set FLAMEGRAPH_DIR"
        artifacts["artifact_status"]["folded"] = {"ok": False, "error": artifacts["folded_error"]}
        return artifacts

    with perf_folded.open("wb") as output:
        completed = subprocess.run(
            [str(stackcollapse), str(perf_out)],
            stdout=output,
            stderr=subprocess.PIPE,
            check=False,
        )
    folded_ok = _record_artifact_stage(
        artifacts,
        "folded",
        path=perf_folded,
        returncode=completed.returncode,
        stderr=completed.stderr,
        error_key="folded_error",
    )
    if not folded_ok:
        return artifacts

    if flamegraph is None:
        artifacts["svg_error"] = "flamegraph.pl not found; set FLAMEGRAPH_DIR"
        artifacts["artifact_status"]["flamegraph"] = {"ok": False, "error": artifacts["svg_error"]}
        return artifacts

    with perf_svg.open("wb") as output:
        completed = subprocess.run(
            [str(flamegraph), str(perf_folded)],
            stdout=output,
            stderr=subprocess.PIPE,
            check=False,
        )
    _record_artifact_stage(
        artifacts,
        "flamegraph",
        path=perf_svg,
        returncode=completed.returncode,
        stderr=completed.stderr,
        error_key="svg_error",
    )
    if completed.returncode != 0:
        return artifacts
    return artifacts


def _record_artifact_stage(
    artifacts: dict[str, Any],
    stage: str,
    *,
    path: Path,
    returncode: int,
    stderr: str | bytes | None,
    error_key: str,
) -> bool:
    path_key = {
        "report": "perf_report",
        "script": "perf_out",
        "folded": "perf_folded",
        "flamegraph": "perf_svg",
    }[stage]
    has_output = path.exists() and path.stat().st_size > 0
    ok = returncode == 0 and has_output
    status: dict[str, Any] = {
        "ok": ok,
        "path": str(path),
        "returncode": returncode,
    }
    if ok:
        artifacts[path_key] = str(path)
    if returncode != 0:
        error = _process_stderr(stderr) or f"{stage} stage exited with {returncode}"
        artifacts[error_key] = error
        status["error"] = error
    elif not has_output:
        error = f"{stage} stage produced an empty artifact"
        artifacts[error_key] = error
        status["error"] = error
    artifacts["artifact_status"][stage] = status
    return ok


def summarize_macos_sample(path: Path, *, limit: int = 30) -> dict[str, Any]:
    top_samples: Counter[str] = Counter()
    in_top_section = False
    found_top_section = False

    for line in path.read_text(errors="replace").splitlines():
        stripped = line.strip()
        if stripped.startswith("Sort by top of stack"):
            in_top_section = True
            found_top_section = True
            continue
        if in_top_section and not stripped:
            break
        if not in_top_section:
            continue

        name, samples = _parse_sample_top_line(stripped)
        if name is not None:
            top_samples[name] += samples

    total = sum(top_samples.values())
    category_samples: Counter[str] = Counter()
    for name, samples in top_samples.items():
        category_samples[_classify_stack(name)] += samples

    summary = {
        "total_samples": total,
        "category_definitions": PROFILE_CATEGORY_DEFINITIONS,
        "top_categories": _counter_rows(category_samples, total, limit),
        "top_leaf_functions": _counter_rows(top_samples, total, limit),
    }
    if not found_top_section:
        summary["parse_error"] = "sample report did not contain a 'Sort by top of stack' section"
    elif total == 0:
        summary["parse_error"] = "sample report top-of-stack section did not contain sample rows"
    return summary


def summarize_folded_stacks(path: Path, *, limit: int = 30) -> dict[str, Any]:
    total = 0
    valid_line_count = 0
    malformed_line_count = 0
    leaf_samples: Counter[str] = Counter()
    stack_samples: Counter[str] = Counter()
    category_samples: Counter[str] = Counter()

    for line in path.read_text(errors="replace").splitlines():
        if not line:
            continue
        stack, _, count_text = line.rpartition(" ")
        if not stack or not count_text:
            malformed_line_count += 1
            continue
        try:
            count = int(count_text)
        except ValueError:
            malformed_line_count += 1
            continue

        valid_line_count += 1
        total += count
        stack_samples[stack] += count
        leaf_samples[stack.split(";")[-1]] += count
        category_samples[_classify_stack(stack)] += count

    summary = {
        "total_samples": total,
        "valid_line_count": valid_line_count,
        "malformed_line_count": malformed_line_count,
        "sample_unit": "perf folded sample-period weight",
        "sample_note": "Percentages are comparable within this profile; raw counts are perf period weights, not row counts.",
        "category_definitions": PROFILE_CATEGORY_DEFINITIONS,
        "top_categories": _counter_rows(category_samples, total, limit),
        "top_leaf_functions": _counter_rows(leaf_samples, total, limit),
        "top_stacks": _counter_rows(stack_samples, total, limit),
    }
    if total == 0:
        summary["parse_error"] = "folded stack file did not contain valid sample rows"
    return summary


def summarize_perf_report_header(path: Path) -> dict[str, Any]:
    raw_sample_count = None
    lost_sample_count = None
    event_count_approx = None
    perf_event = None

    for line in path.read_text(errors="replace").splitlines():
        stripped = line.strip()
        if stripped.startswith("# Total Lost Samples:"):
            lost_sample_count = _parse_int_after_colon(stripped)
            continue
        match = re.match(r"# Samples:\s+([\d,.]+[KMG]?)\s+of event '([^']+)'", stripped)
        if match:
            raw_sample_count = _parse_int_text(match.group(1))
            perf_event = match.group(2)
            continue
        if stripped.startswith("# Event count (approx.):"):
            event_count_approx = _parse_int_after_colon(stripped)
            continue
        if stripped.startswith("# Overhead"):
            break

    return {
        key: value
        for key, value in {
            "raw_sample_count": raw_sample_count,
            "lost_sample_count": lost_sample_count,
            "event_count_approx": event_count_approx,
            "perf_event": perf_event,
        }.items()
        if value is not None
    }


def _aggregate_instance_samples(instances: dict[str, Any], *, limit: int = 30) -> dict[str, Any]:
    total = 0
    raw_total = 0
    lost_total = 0
    raw_seen = False
    lost_seen = False
    category_samples: Counter[str] = Counter()
    leaf_samples: Counter[str] = Counter()

    for instance in instances.values():
        samples = instance.get("samples") or {}
        total += int(samples.get("total_samples") or 0)
        if samples.get("raw_sample_count") is not None:
            raw_total += int(samples["raw_sample_count"])
            raw_seen = True
        if samples.get("lost_sample_count") is not None:
            lost_total += int(samples["lost_sample_count"])
            lost_seen = True
        for row in samples.get("top_categories") or ():
            category_samples[row["name"]] += int(row["samples"])
        for row in samples.get("top_leaf_functions") or ():
            leaf_samples[row["name"]] += int(row["samples"])

    summary = {
        "total_samples": total,
        "sample_unit": "perf folded sample-period weight",
        "sample_note": "Percentages are comparable within this profile; raw counts are perf period weights, not row counts.",
        "category_definitions": PROFILE_CATEGORY_DEFINITIONS,
        "top_categories": _counter_rows(category_samples, total, limit),
        "top_leaf_functions_from_instance_tops": _counter_rows(leaf_samples, total, limit),
    }
    if raw_seen:
        summary["raw_sample_count"] = raw_total
    if lost_seen:
        summary["lost_sample_count"] = lost_total
    return summary


def _counter_rows(counter: Counter[str], total: int, limit: int) -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "samples": samples,
            "percent": 0.0 if total == 0 else samples * 100.0 / total,
        }
        for name, samples in counter.most_common(limit)
    ]


def _parse_sample_top_line(line: str) -> tuple[str | None, int]:
    if not line:
        return None, 0
    name, _, count_text = line.rpartition(" ")
    if not name or not count_text:
        return None, 0
    try:
        count = int(count_text)
    except ValueError:
        return None, 0
    return name.strip(), count


def _parse_int_after_colon(line: str) -> int | None:
    _, _, value = line.partition(":")
    return _parse_int_text(value.strip())


def _parse_int_text(value: str) -> int | None:
    value = value.strip()
    if value[-1:] in {"K", "M", "G"}:
        multiplier = {"K": 1_000, "M": 1_000_000, "G": 1_000_000_000}[value[-1]]
        try:
            return int(float(value[:-1].replace(",", "")) * multiplier)
        except ValueError:
            return None
    try:
        return int(value.replace(",", ""))
    except ValueError:
        return None


def _process_stderr(stderr: str | bytes | None) -> str:
    if isinstance(stderr, bytes):
        stderr = stderr.decode(errors="replace")
    if stderr:
        return stderr.strip()
    return ""


def _stop_recorder_process(process: subprocess.Popen, *, interrupt_timeout: int) -> str:
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            _, stderr = process.communicate(timeout=interrupt_timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            _, stderr = process.communicate(timeout=KILL_TIMEOUT_SECONDS)
    else:
        _, stderr = process.communicate(timeout=KILL_TIMEOUT_SECONDS)
    return stderr or ""


def _classify_stack(stack: str) -> str:
    storage_needles = (
        "insert_encoded_tuple",
        "insert_encoded_slices",
        "tuple_insert_from_proto",
        "Space::insert",
        "box_insert",
        "space_execute_dml",
        "memtx_",
        "tuple_compare",
        "tuple_field_raw",
        "bps_tree",
        "wal_write",
        "xlog_tx",
        "ZSTD_",
    )
    if any(needle in stack for needle in storage_needles):
        return "direct_insert_storage"

    copy_stack = any(
        needle in stack
        for needle in (
            "CopySession",
            "CopyData",
            "CopyDone",
            "CopyFail",
            "copy_from_stdin",
            "PreparedCopyTarget",
            "prepare_copy",
            "CopyTarget",
            "copy_target",
            "CopyDestination",
            "tuple_from_copy",
            "TextRecordReader",
            "TextRowParser",
            "pgproto::backend::copy",
            "sql::copy",
        )
    )
    if copy_stack:
        copy_categories = (
            (
                "copy_row_decode",
                (
                    "TextRecordReader",
                    "TextRowParser",
                    "decode_record",
                    "decode_text_field",
                    "PgValue::decode",
                    "try_parse_datetime",
                ),
            ),
            (
                "copy_target_dispatch_remote",
                (
                    "dispatch_remote_batches",
                    "dispatch_encoded_insert_batches",
                    "write_insert_packet",
                    "write_tuples",
                ),
            ),
            (
                "copy_target_flush",
                (
                    "flush_sharded_batches",
                    "flush_global_batch",
                    "flush_pending_rows",
                    "flush_pending_destination",
                    "CopyWriteSession::clear",
                    "PendingCopyBatch::clear",
                    "flush_batch",
                    "batch_flush",
                    "copy_from_stdin",
                ),
            ),
            (
                "copy_target_encode",
                (
                    "prepare_pending_row",
                    "encode_row_with_bucket",
                    "encode_row_with_builder",
                    "write_insert_args",
                    "determine_insert_bucket_id",
                    "bucket_id_for_row",
                    "CopyWriteSession::push",
                    "PendingCopyBatch::push",
                ),
            ),
            ("copy_route", ("ShardedCopyRouting", "destination_for_bucket", "CopyDestination", "route", "routing")),
            ("copy_target_setup", ("prepare_copy", "target_prepare", "CopyTarget", "copy_target")),
            (
                "pgproto_copy_in",
                (
                    "process_copy_in_message",
                    "CopySession",
                    "CopyData",
                    "CopyDone",
                    "CopyFail",
                ),
            ),
        )
        for category, needles in copy_categories:
            if any(needle in stack for needle in needles):
                return category
        return "copy_unclassified"

    insert_stack = any(
        needle in stack
        for needle in (
            "process_bind_message",
            "process_execute_message",
            "Backend::bind",
            "Backend::execute",
            "PreparedStatement::bind",
            "decode_parameter",
            "decode_parameters",
            "execute_bound_dml",
            "dispatch_bound_statement",
            "custom_plan_dispatch",
            "custom_plan_dispatch_dml",
            "single_plan_dispatch_dml",
            "build_dml_request",
            "DmlRequest",
            "write_insert_packet",
            "write_tuples",
            "write_insert_args",
            "InsertTupleEncoder",
            "bucket_dispatch",
            "calculate_bucket_id",
            "set_motion_vtable",
            "VirtualTable::reshard",
            "materialize_subtree",
        )
    )
    if insert_stack:
        insert_categories = (
            (
                "insert_bind_decode",
                (
                    "process_bind_message",
                    "Backend::bind",
                    "PreparedStatement::bind",
                    "decode_parameter",
                    "decode_parameters",
                    "PgValue::decode",
                    "Portal::new",
                ),
            ),
            (
                "insert_tuple_encode",
                (
                    "write_insert_packet",
                    "write_tuples",
                    "write_insert_args",
                    "encode_tuple_with_reservation",
                    "DmlRequest::into_message",
                    "InsertTupleEncoder",
                    "encode_row_with_builder",
                    "encode_row_with_bucket",
                ),
            ),
            (
                "insert_sql_dispatch",
                (
                    "dispatch_bound_statement",
                    "custom_plan_dispatch",
                    "custom_plan_dispatch_dml",
                    "single_plan_dispatch_dml",
                    "build_dml_request",
                    "bucket_dispatch",
                    "calculate_bucket_id",
                    "set_motion_vtable",
                    "VirtualTable::reshard",
                    "materialize_subtree",
                    "ExecutingQuery<C>::dispatch",
                    "materialize_values",
                ),
            ),
            (
                "insert_execute",
                (
                    "process_execute_message",
                    "Backend::execute",
                    "Portal::execute",
                    "execute_bound_dml",
                    "step_portal",
                    "command_complete_with_row_count",
                ),
            ),
        )
        for category, needles in insert_categories:
            if any(needle in stack for needle in needles):
                return category
        return "insert_unclassified"

    categories = (
        (
            "direct_insert_storage",
            storage_needles,
        ),
        ("network", ("coio", "ev_io", "read_cb", "write_cb", "net_msg", "sock_", "tcp_", "skb_", "inet_")),
        ("raft_control_plane", ("raft", "cas::", "propose")),
        ("lua_ffi_runtime", ("lua_", "lj_", "Lua")),
        ("tarantool_fiber_runtime", ("fiber_loop", "fiber_cond", "fiber_cxx_invoke", "coro_startup")),
        ("unwind_runtime", ("libunwind::", "_Unwind_", "lj_err_unwind")),
        ("idle_wait", ("__select", "__psynch_cvwait", "__workq_kernreturn", "kevent", "poll")),
    )
    for category, needles in categories:
        if any(needle in stack for needle in needles):
            return category
    return "unknown"


def _instance_metadata(instance, index: int) -> dict[str, Any]:
    return {
        "display_name": instance.name,
        "picodata_instance_name": instance.name,
        "ordinal": index,
        "replicaset_name": getattr(instance, "replicaset_name", None),
    }


def _profiler_tool() -> str:
    if sys.platform == "darwin":
        return "sample"
    return "perf"


def _recorder_cls():
    if sys.platform == "darwin":
        return MacSampleRecorder
    return PerfRecorder


def _flamegraph_tool(name: str) -> Path | None:
    if path := shutil.which(name):
        return Path(path)

    import os

    flamegraph_dir = Path(os.environ.get("FLAMEGRAPH_DIR", "../FlameGraph"))
    candidate = flamegraph_dir / name
    if candidate.exists():
        return candidate
    return None


def _picodata_child_pid(instance) -> int:
    from conftest import pgrep_tree, pid_alive  # type: ignore

    assert instance.process is not None
    assert instance.process.pid is not None
    assert pid_alive(instance.process.pid)

    pids = pgrep_tree(instance.process.pid)
    if len(pids) < 2:
        return instance.process.pid
    assert pid_alive(pids[1])
    return pids[1]
