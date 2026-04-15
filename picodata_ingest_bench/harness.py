from __future__ import annotations

import logging
import os
import random
import sys
import tempfile
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

import psycopg

ADMIN_USER = "admin"
ADMIN_PASSWORD = "P@ssw0rd"
PICODATA_SOURCE_ENV = "PICODATA_SOURCE"


@dataclass(frozen=True)
class RuntimeModules:
    base_host: str
    cluster_cls: Any
    port_distributor_cls: Any
    executable_current: Callable[[], Any]
    cargo_build_profile: Callable[[], str]


@cache
def load_runtime_modules(picodata_source: Path | None = None) -> RuntimeModules:
    source_root = repo_root_path(picodata_source)
    test_root = source_root / "test"
    if str(test_root) not in sys.path:
        sys.path.insert(0, str(test_root))

    import conftest  # type: ignore
    from framework.log import log as framework_log  # type: ignore
    from framework.port_distributor import PortDistributor  # type: ignore
    from framework.util.build import Executable, cargo_build_profile  # type: ignore

    verbose = os.environ.get("INGEST_BENCH_VERBOSE", "").lower() in {"1", "true", "yes"}
    log_level = logging.WARNING if verbose else logging.CRITICAL
    framework_log.setLevel(log_level)
    for handler in framework_log.handlers:
        handler.setLevel(log_level)

    def executable_current():
        cwd = Path.cwd()
        try:
            os.chdir(source_root)
            return Executable.current()
        finally:
            os.chdir(cwd)

    return RuntimeModules(
        base_host=conftest.BASE_HOST,
        cluster_cls=conftest.Cluster,
        port_distributor_cls=PortDistributor,
        executable_current=executable_current,
        cargo_build_profile=cargo_build_profile,
    )


def repo_root_path(picodata_source: Path | None = None) -> Path:
    if picodata_source is not None:
        return validate_picodata_source_path(picodata_source)
    if env_source := os.environ.get(PICODATA_SOURCE_ENV):
        return validate_picodata_source_path(Path(env_source))

    for candidate in default_picodata_source_candidates():
        try:
            return validate_picodata_source_path(candidate)
        except ValueError:
            continue

    raise ValueError(
        f"Picodata source root was not provided; pass --picodata-source /path/to/picodata or set {PICODATA_SOURCE_ENV}"
    )


def default_picodata_source_candidates() -> tuple[Path, ...]:
    benchmark_root = benchmark_root_path()
    return (
        Path.cwd(),
        benchmark_root,
        benchmark_root.parent / "picodata",
    )


def benchmark_root_path() -> Path:
    return Path(__file__).resolve().parents[1]


def validate_picodata_source_path(path: Path) -> Path:
    source = path.expanduser().resolve()
    missing = [
        relative
        for relative in (
            Path("Cargo.toml"),
            Path("test") / "conftest.py",
        )
        if not (source / relative).exists()
    ]
    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise ValueError(f"Picodata source root {source} is missing required files: {missing_text}")
    return source


class PicodataBenchmarkCluster(AbstractContextManager["PicodataBenchmarkCluster"]):
    def __init__(
        self,
        *,
        instance_count: int,
        wait_balanced: bool,
        port_start: int | None = None,
        instance_memtx_memory: str | None = None,
        picodata_source: Path | None = None,
    ) -> None:
        self.picodata_source = repo_root_path(picodata_source)
        self.instance_count = instance_count
        self.wait_balanced_flag = wait_balanced
        self.port_start = port_start
        self.instance_memtx_memory = instance_memtx_memory
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None
        self.cluster: Any | None = None
        self.pg_host = ""
        self.pg_port = 0
        self.setup_seconds = 0.0
        self.balance_seconds = 0.0

    def __enter__(self) -> "PicodataBenchmarkCluster":
        runtime = load_runtime_modules(self.picodata_source)
        self._tmpdir = tempfile.TemporaryDirectory(
            prefix="pico-ingest-bench-",
            dir=_benchmark_tmp_root(),
        )
        cluster_id = f"ingest-bench-{uuid.uuid4().hex[:8]}"
        port_start = self.port_start if self.port_start is not None else _port_range_start()
        share_dir = self.picodata_source / "test" / "testplug"

        self.cluster = runtime.cluster_cls(
            id=cluster_id,
            data_dir=self._tmpdir.name,
            share_dir=str(share_dir),
            base_host=runtime.base_host,
            port_distributor=runtime.port_distributor_cls(port_start, port_start + 500),
            pytest_timeout=None,
        )
        self.cluster.set_service_password("password")
        if self.instance_memtx_memory is not None:
            self.cluster.set_config_file(
                yaml=_benchmark_cluster_config_yaml(
                    cluster_name=cluster_id,
                    instance_memtx_memory=self.instance_memtx_memory,
                )
            )

        # Build one real cluster deterministically: first iteration bootstraps,
        # later iterations join remaining nodes one by one.
        setup_started = perf_counter()
        for index in range(self.instance_count):
            ordinal = index + 1
            instance = self.cluster.add_instance(
                wait_online=False,
                name=f"ingest_{ordinal}",
                replicaset_name=f"ingest_rs_{ordinal}",
                executable=runtime.executable_current(),
                audit=False,
                log_to_console=False,
                log_to_file=True,
            )
            instance.env["PICODATA_LOG_LEVEL"] = _benchmark_picodata_log_level()
            instance.start()
            instance.wait_online()

        self.setup_seconds = perf_counter() - setup_started

        if self.wait_balanced_flag:
            balance_started = perf_counter()
            self.cluster.wait_balanced()
            self.balance_seconds = perf_counter() - balance_started

        first_instance = self.cluster.instances[0]
        first_instance.sql(f"ALTER USER \"{ADMIN_USER}\" WITH PASSWORD '{ADMIN_PASSWORD}' USING md5")

        self.pg_host = first_instance.pg_host
        self.pg_port = first_instance.pg_port
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.cluster is not None:
            self.cluster.kill()
        if self._tmpdir is not None:
            self._tmpdir.cleanup()

    def connect_admin(self) -> psycopg.Connection:
        conn = psycopg.connect(self.admin_dsn())
        conn.autocommit = True
        return conn

    def admin_dsn(self) -> str:
        return f"user={ADMIN_USER} password={ADMIN_PASSWORD} host={self.pg_host} port={self.pg_port} sslmode=disable"

    @property
    def instances(self):
        assert self.cluster is not None
        return self.cluster.instances


def _port_range_start() -> int:
    configured = os.environ.get("INGEST_BENCH_PORT_START")
    if configured is not None:
        return int(configured)

    random.seed(f"{os.getpid()}-{uuid.uuid4().hex}")
    return random.randrange(32_000, 50_000, 250)


def _benchmark_cluster_config_yaml(*, cluster_name: str, instance_memtx_memory: str) -> str:
    return f"""
cluster:
    name: {cluster_name}
    tier:
        default:
instance:
    memtx:
        memory: {instance_memtx_memory}
"""


def _benchmark_tmp_root() -> str | None:
    if sys.platform == "darwin":
        return "/tmp"
    return None


def _benchmark_picodata_log_level() -> str:
    return os.environ.get("INGEST_BENCH_PICODATA_LOG_LEVEL", "info")
