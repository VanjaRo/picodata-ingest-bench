from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

from picodata_ingest_bench.config import ExecutionRuntime
from picodata_ingest_bench.harness import repo_root_path


DEFAULT_CONTAINER_IMAGE = "picodata-ingest-bench-base"
DEFAULT_RUST_VERSION = os.environ.get("RUST_VERSION", "1.89")
DEFAULT_CONTAINER_BUILD_PROFILE = "release"
CONTAINER_TARGET_DIR = "target/ingest-linux"
CONTAINER_REPO_ROOT = Path("/work/picodata")
CONTAINER_BENCHMARK_ROOT = Path("/work/picodata-ingest-bench")
CONTAINER_DOCKERFILE = Path(__file__).resolve().with_name("Dockerfile")
CONTAINER_CACHE_VOLUMES = (
    "picodata-bench-cargo-registry:/root/.cargo/registry",
    "picodata-bench-cargo-git:/root/.cargo/git",
    "picodata-bench-poetry-cache:/root/.cache/pypoetry",
    "picodata-bench-pip-cache:/root/.cache/pip",
)
CONTAINER_ENV_VARS = {
    "POETRY_VIRTUALENVS_IN_PROJECT": "false",
    "POETRY_VIRTUALENVS_PATH": "/root/.cache/pypoetry/virtualenvs",
    "TMPDIR": "/tmp",
}
CONTAINER_PATH_OPTIONS = {"--output", "--server-profile-dir"}
CONTAINER_TEXT_ARTIFACT_SUFFIXES = {".folded", ".out", ".svg", ".txt", ".json"}


@dataclass(frozen=True)
class ContainerRun:
    command: list[str]
    output_copies: tuple[tuple[Path, Path], ...]
    report_paths: tuple[Path, ...]
    path_rewrites: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ContainerArgRewrite:
    argv: list[str]
    output_copies: list[tuple[Path, Path]]
    report_paths: list[Path]
    path_rewrites: list[tuple[str, str]]


def maybe_delegate_run_to_container(
    *,
    argv: list[str],
    requested_runtime: ExecutionRuntime,
    container_image: str,
    picodata_source: Path,
) -> int | None:
    if os.environ.get("INGEST_BENCH_IN_CONTAINER") == "1":
        return None

    if not argv or argv[0] != "run":
        return None

    if resolve_runtime(requested_runtime) is not ExecutionRuntime.CONTAINER:
        return None

    ensure_container_image(container_image=container_image)
    container_run = build_container_run(
        argv=argv,
        container_image=container_image,
        picodata_source=picodata_source,
    )
    completed = subprocess.run(container_run.command)
    copy_container_outputs(container_run.output_copies, path_rewrites=container_run.path_rewrites)
    rewrite_container_report_paths(
        report_paths=container_run.report_paths,
        path_rewrites=container_run.path_rewrites,
    )
    return completed.returncode


def report_runtime_label(requested_runtime: ExecutionRuntime) -> str:
    if os.environ.get("INGEST_BENCH_IN_CONTAINER") == "1":
        return ExecutionRuntime.CONTAINER.value
    return resolve_runtime(requested_runtime).value


def resolve_runtime(requested_runtime: ExecutionRuntime) -> ExecutionRuntime:
    if requested_runtime is not ExecutionRuntime.AUTO:
        return requested_runtime
    if sys.platform.startswith("linux"):
        return ExecutionRuntime.NATIVE
    return ExecutionRuntime.CONTAINER


def picodata_source_from_argv(argv: list[str]) -> Path:
    for index, arg in enumerate(argv):
        if arg == "--picodata-source" and index + 1 < len(argv):
            return repo_root_path(Path(argv[index + 1]))
        if arg.startswith("--picodata-source="):
            return repo_root_path(Path(arg.split("=", 1)[1]))
    return repo_root_path()


def build_container_run(
    *,
    argv: list[str],
    container_image: str,
    picodata_source: Path,
) -> ContainerRun:
    source_root = repo_root_path(picodata_source)
    benchmark_root = benchmark_root_path()
    container_benchmark_root = CONTAINER_REPO_ROOT if source_root == benchmark_root else CONTAINER_BENCHMARK_ROOT
    build_profile = DEFAULT_CONTAINER_BUILD_PROFILE
    cargo_target_dir = CONTAINER_REPO_ROOT / CONTAINER_TARGET_DIR
    rewrite = rewrite_container_argv(argv=argv, picodata_source=source_root)
    inner_command = "python -m picodata_ingest_bench " + " ".join(_shell_quote(arg) for arg in rewrite.argv)
    benchmark_run = bool(rewrite.argv and rewrite.argv[0] == "run")
    enable_perf = benchmark_run and "--server-profile" in rewrite.argv
    reuse_container_build = benchmark_run and "--reuse-container-build" in rewrite.argv

    return ContainerRun(
        command=build_container_exec_command(
            picodata_source=source_root,
            benchmark_root=benchmark_root,
            container_benchmark_root=container_benchmark_root,
            container_image=container_image,
            build_profile=build_profile,
            cargo_target_dir=cargo_target_dir,
            inner_command=inner_command,
            path_rewrites=rewrite.path_rewrites,
            enable_perf=enable_perf,
            reuse_container_build=reuse_container_build,
            build_before_run=benchmark_run,
        ),
        output_copies=tuple(rewrite.output_copies),
        report_paths=tuple(rewrite.report_paths),
        path_rewrites=tuple(rewrite.path_rewrites),
    )


def build_container_exec_command(
    *,
    picodata_source: Path,
    benchmark_root: Path,
    container_benchmark_root: Path,
    container_image: str,
    build_profile: str,
    cargo_target_dir: Path,
    inner_command: str,
    path_rewrites: list[tuple[str, str]],
    enable_perf: bool = False,
    reuse_container_build: bool = False,
    build_before_run: bool = True,
) -> list[str]:
    docker_bin = os.environ.get("INGEST_BENCH_CONTAINER_BIN", "docker")

    command = [docker_bin, "run", "--rm", "-i"]
    if sys.stdin.isatty() and sys.stdout.isatty():
        command.append("-t")
    if enable_perf:
        command.extend(["--privileged", "--security-opt", "seccomp=unconfined"])

    env = {
        "INGEST_BENCH_IN_CONTAINER": "1",
        "PICODATA_SOURCE": str(CONTAINER_REPO_ROOT),
        "BUILD_PROFILE": build_profile,
        "CARGO_TARGET_DIR": str(cargo_target_dir),
        "PYTHONPATH": _benchmark_pythonpath(
            picodata_source=CONTAINER_REPO_ROOT,
            benchmark_root=container_benchmark_root,
        ),
        "INGEST_BENCH_DISPLAY_PATH_REWRITES": json.dumps(path_rewrites),
        "INGEST_BENCH_BUILD_BEFORE_RUN": "1" if build_before_run else "0",
        **CONTAINER_ENV_VARS,
    }
    if git_describe := _git_describe(picodata_source):
        env["GIT_DESCRIBE"] = git_describe
    if reuse_container_build:
        env["INGEST_BENCH_SKIP_CONTAINER_BUILD"] = "1"
    elif skip_container_build := os.environ.get("INGEST_BENCH_SKIP_CONTAINER_BUILD"):
        env["INGEST_BENCH_SKIP_CONTAINER_BUILD"] = skip_container_build
    if enable_perf:
        env["INGEST_BENCH_SERVER_PROFILE"] = "1"

    for key, value in env.items():
        command.extend(["-e", f"{key}={value}"])

    command.extend(["-v", f"{picodata_source}:{CONTAINER_REPO_ROOT}", "-w", str(CONTAINER_REPO_ROOT)])
    if benchmark_root != picodata_source:
        command.extend(["-v", f"{benchmark_root}:{container_benchmark_root}:ro"])

    for volume in CONTAINER_CACHE_VOLUMES:
        command.extend(["-v", volume])

    command.extend(
        [
            container_image,
            "bash",
            "-lc",
            _container_entrypoint(picodata_source=CONTAINER_REPO_ROOT, inner_command=inner_command),
        ]
    )
    return command


def ensure_container_image(*, container_image: str) -> None:
    docker_bin = os.environ.get("INGEST_BENCH_CONTAINER_BIN", "docker")
    try:
        inspected = subprocess.run(
            [docker_bin, "image", "inspect", container_image],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("docker is required for container-backed benchmark runs") from exc
    if inspected.returncode == 0:
        return

    if container_image != DEFAULT_CONTAINER_IMAGE:
        raise RuntimeError(
            f"container image '{container_image}' is not available locally; build or pull it before running the benchmark"
        )

    build_command = [
        docker_bin,
        "build",
        "-f",
        str(CONTAINER_DOCKERFILE),
        "--build-arg",
        f"RUST_VERSION={DEFAULT_RUST_VERSION}",
        "-t",
        container_image,
        ".",
    ]
    completed = subprocess.run(build_command, cwd=benchmark_root_path())
    if completed.returncode != 0:
        raise RuntimeError("failed to build the default benchmark container image")


def rewrite_container_argv(
    *,
    argv: list[str],
    picodata_source: Path,
) -> ContainerArgRewrite:
    rewritten = list(argv)
    _set_option_value(rewritten, "--picodata-source", str(CONTAINER_REPO_ROOT))
    _add_default_profile_dir(rewritten)
    output_copies: list[tuple[Path, Path]] = []
    report_paths: list[Path] = []
    source_root = repo_root_path(picodata_source)
    path_rewrites: list[tuple[str, str]] = [(str(CONTAINER_REPO_ROOT), str(source_root))]

    for index, arg in enumerate(rewritten):
        if arg not in CONTAINER_PATH_OPTIONS or index + 1 >= len(rewritten):
            continue

        output_path = Path(rewritten[index + 1]).expanduser()
        requested = (Path.cwd() / output_path) if not output_path.is_absolute() else output_path
        requested = requested.absolute()
        resolved = requested.resolve()
        if arg == "--output":
            report_paths.append(requested)
        if _is_relative_to(resolved, source_root):
            container_path = CONTAINER_REPO_ROOT / resolved.relative_to(source_root)
            rewritten[index + 1] = str(container_path)
            continue

        staging = source_root / "tmp" / f"ingest-container-output-{uuid.uuid4().hex}" / resolved.name
        container_path = CONTAINER_REPO_ROOT / staging.relative_to(source_root)
        rewritten[index + 1] = str(container_path)
        output_copies.append((staging, requested))
        path_rewrites.append((str(container_path), str(requested)))

    return ContainerArgRewrite(
        argv=rewritten,
        output_copies=output_copies,
        report_paths=report_paths,
        path_rewrites=path_rewrites,
    )


def _add_default_profile_dir(argv: list[str]) -> None:
    if "--server-profile" not in argv or "--server-profile-dir" in argv or "--output" not in argv:
        return
    output_index = argv.index("--output")
    if output_index + 1 >= len(argv):
        return
    output_path = Path(argv[output_index + 1]).expanduser()
    profile_dir = output_path.parent / f"{output_path.stem}-profiles"
    argv.extend(["--server-profile-dir", str(profile_dir)])


def _set_option_value(argv: list[str], option: str, value: str) -> None:
    for index, arg in enumerate(argv):
        if arg == option:
            if index + 1 < len(argv):
                argv[index + 1] = value
            else:
                argv.append(value)
            return
        if arg.startswith(option + "="):
            argv[index] = f"{option}={value}"
            return
    argv.extend([option, value])


def copy_container_outputs(
    output_copies: tuple[tuple[Path, Path], ...],
    *,
    path_rewrites: tuple[tuple[str, str], ...] = (),
) -> None:
    ordered_rewrites = sorted(path_rewrites, key=lambda item: len(item[0]), reverse=True)
    for staging, requested in output_copies:
        if not staging.exists():
            continue
        requested.parent.mkdir(parents=True, exist_ok=True)
        if staging.is_dir():
            if requested.exists():
                shutil.rmtree(requested)
            shutil.copytree(staging, requested)
        else:
            shutil.copy2(staging, requested)
        rewrite_exported_text_artifacts(requested, ordered_rewrites)
        _remove_staging_output(staging)


def rewrite_container_report_paths(
    *,
    report_paths: tuple[Path, ...],
    path_rewrites: tuple[tuple[str, str], ...],
) -> None:
    ordered_rewrites = sorted(path_rewrites, key=lambda item: len(item[0]), reverse=True)
    for report_path in report_paths:
        if not report_path.exists():
            continue
        payload = json.loads(report_path.read_text())
        payload = _rewrite_json_strings(payload, ordered_rewrites)
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        rewrite_report_artifacts(payload, ordered_rewrites)


def rewrite_exported_text_artifacts(path: Path, path_rewrites: list[tuple[str, str]]) -> None:
    if not path_rewrites:
        return
    paths = path.rglob("*") if path.is_dir() else (path,)
    for artifact in paths:
        if not artifact.is_file() or artifact.suffix not in CONTAINER_TEXT_ARTIFACT_SUFFIXES:
            continue
        try:
            text = artifact.read_text(errors="replace")
        except OSError:
            continue
        rewritten = _rewrite_text_paths(text, path_rewrites)
        if rewritten != text:
            artifact.write_text(rewritten)


def rewrite_report_artifacts(report_payload, path_rewrites: list[tuple[str, str]]) -> None:
    for artifact_path in _report_artifact_paths(report_payload):
        rewrite_exported_text_artifacts(artifact_path, path_rewrites)


def _report_artifact_paths(value) -> list[Path]:
    paths: list[Path] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"perf_report", "perf_out", "perf_folded", "perf_svg", "sample_report"} and isinstance(item, str):
                paths.append(Path(item))
            else:
                paths.extend(_report_artifact_paths(item))
    elif isinstance(value, list):
        for item in value:
            paths.extend(_report_artifact_paths(item))
    return paths


def _rewrite_text_paths(text: str, path_rewrites: list[tuple[str, str]]) -> str:
    rewritten = text
    for source, target in path_rewrites:
        rewritten = rewritten.replace(source, target)
    return rewritten


def _rewrite_json_strings(value, path_rewrites: list[tuple[str, str]]):
    if isinstance(value, str):
        for source, target in path_rewrites:
            if value == source:
                return target
            if value.startswith(source + "/"):
                return target + value[len(source) :]
        return value
    if isinstance(value, list):
        return [_rewrite_json_strings(item, path_rewrites) for item in value]
    if isinstance(value, dict):
        return {key: _rewrite_json_strings(item, path_rewrites) for key, item in value.items()}
    return value


def _remove_staging_output(path: Path) -> None:
    staging_root = path.parent
    if staging_root.parent.name != "tmp" or not staging_root.name.startswith("ingest-container-output-"):
        return
    shutil.rmtree(staging_root, ignore_errors=True)


def _container_entrypoint(*, picodata_source: Path, inner_command: str) -> str:
    source_root_quoted = _shell_quote(str(picodata_source))
    return f"""
set -euo pipefail
cd {source_root_quoted}
git config --global --add safe.directory {source_root_quoted} >/dev/null
poetry install --no-root --no-interaction >/dev/null
if [ "${{INGEST_BENCH_BUILD_BEFORE_RUN:-1}}" = "1" ]; then
    profile="${{BUILD_PROFILE:-{DEFAULT_CONTAINER_BUILD_PROFILE}}}"
    if [ "${{INGEST_BENCH_SKIP_CONTAINER_BUILD:-0}}" = "1" ]; then
        echo "[ingest] skip container build; using existing target artifact" >&2
    else
        make CARGO_FLAGS=--all "build-$profile"
    fi
fi
if [ "${{INGEST_BENCH_SERVER_PROFILE:-0}}" = "1" ] && {{ ! command -v perf >/dev/null 2>&1 || ! command -v pgrep >/dev/null 2>&1 || ! perl -e 'require q(open.pm)' >/dev/null 2>&1; }}; then
    dnf install -y --nobest perf perl-interpreter perl-open procps-ng >/dev/null
fi
if [ "${{INGEST_BENCH_SERVER_PROFILE:-0}}" = "1" ] && [ ! -x "${{FLAMEGRAPH_DIR:-../FlameGraph}}/stackcollapse-perf.pl" ]; then
    rm -rf /tmp/FlameGraph
    git clone --depth 1 https://github.com/brendangregg/FlameGraph.git /tmp/FlameGraph >/dev/null
    export FLAMEGRAPH_DIR=/tmp/FlameGraph
fi
exec poetry run {inner_command}
""".strip()


def _benchmark_pythonpath(*, picodata_source: Path, benchmark_root: Path) -> str:
    picodata_source = picodata_source.resolve()
    benchmark_root = benchmark_root.resolve()
    paths = [str(benchmark_root), str(picodata_source / "test")]
    if benchmark_root != picodata_source:
        paths.append(str(picodata_source))
    return os.pathsep.join(paths)


def benchmark_root_path() -> Path:
    return Path(__file__).resolve().parents[1]


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _git_describe(repo_root: Path) -> str | None:
    if value := os.environ.get("GIT_DESCRIBE"):
        return value

    completed = subprocess.run(
        ["git", "describe", "--always", "--dirty", "--tags"],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    value = completed.stdout.strip()
    if completed.returncode == 0 and value:
        return value
    return None


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"
