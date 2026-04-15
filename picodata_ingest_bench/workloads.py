from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Iterable, Sequence, overload


Row = tuple[object, ...]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sql_type: str
    nullable: bool = False
    primary_key: bool = False


@dataclass(frozen=True)
class IndexSpec:
    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class WorkloadSpec:
    key: str
    description: str
    columns: tuple[ColumnSpec, ...]
    secondary_indexes: tuple[IndexSpec, ...] = ()
    shape: tuple[tuple[str, object], ...] = ()

    def make_row(self, index: int, seed: int) -> Row:
        raise NotImplementedError


@dataclass(frozen=True)
class GeneratedRows(Sequence[Row]):
    workload: WorkloadSpec
    row_count: int
    seed: int

    def __len__(self) -> int:
        return self.row_count

    @overload
    def __getitem__(self, index: int) -> Row: ...

    @overload
    def __getitem__(self, index: slice) -> list[Row]: ...

    def __getitem__(self, index: int | slice) -> Row | list[Row]:
        if isinstance(index, slice):
            start, stop, step = index.indices(self.row_count)
            return [self.workload.make_row(row_index, self.seed) for row_index in range(start, stop, step)]

        if index < 0:
            index += self.row_count
        if index < 0 or index >= self.row_count:
            raise IndexError(index)
        return self.workload.make_row(index, self.seed)


@dataclass(frozen=True)
class StridedRows(Sequence[Row]):
    rows: Sequence[Row]
    offset: int
    step: int
    row_count: int

    def __len__(self) -> int:
        return self.row_count

    @overload
    def __getitem__(self, index: int) -> Row: ...

    @overload
    def __getitem__(self, index: slice) -> list[Row]: ...

    def __getitem__(self, index: int | slice) -> Row | list[Row]:
        if isinstance(index, slice):
            start, stop, stride = index.indices(self.row_count)
            return [self.rows[self.offset + row_index * self.step] for row_index in range(start, stop, stride)]

        if index < 0:
            index += self.row_count
        if index < 0 or index >= self.row_count:
            raise IndexError(index)
        return self.rows[self.offset + index * self.step]


@dataclass(frozen=True)
class NarrowWorkload(WorkloadSpec):
    def make_row(self, index: int, seed: int) -> Row:
        return (index, f"narrow_{seed}_{index:08d}")


@dataclass(frozen=True)
class KafkaLikeWorkload(WorkloadSpec):
    def make_row(self, index: int, seed: int) -> Row:
        created_at = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index)).strftime(
            "%Y-%m-%dT%H:%M:%S+0000"
        )
        key = f"topic-{seed % 97}-partition-{index % 16}-key-{index:08d}"
        payload = _deterministic_payload(seed, index, width=224)
        return (index, created_at, key, payload)


@dataclass(frozen=True)
class NarrowIndexedWorkload(WorkloadSpec):
    def make_row(self, index: int, seed: int) -> Row:
        tenant_id = (seed + index) % 512
        stream_id = index % 64
        value = f"narrow_{seed}_{index:08d}"
        return (index, tenant_id, stream_id, value)


@dataclass(frozen=True)
class KafkaLikeIndexedWorkload(WorkloadSpec):
    def make_row(self, index: int, seed: int) -> Row:
        created_at = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index)).strftime(
            "%Y-%m-%dT%H:%M:%S+0000"
        )
        tenant_id = (seed + index) % 512
        key = f"topic-{seed % 97}-partition-{index % 16}-key-{index:08d}"
        payload = _deterministic_payload(seed, index, width=224)
        return (index, tenant_id, created_at, key, payload)


WORKLOADS: dict[str, WorkloadSpec] = {
    "narrow": NarrowWorkload(
        key="narrow",
        description="Narrow synthetic rows: id plus short text. Use to isolate protocol batching and statement overhead.",
        columns=(
            ColumnSpec(name="id", sql_type="INTEGER", primary_key=True),
            ColumnSpec(name="value", sql_type="TEXT"),
        ),
        shape=(
            ("id", "monotonic integer"),
            ("value_pattern", "narrow_{seed}_{index:08d}"),
        ),
    ),
    "kafka-like": KafkaLikeWorkload(
        key="kafka-like",
        description="Event-shaped rows: id, timestamp, message key, and 232-byte payload. Use to model append/event ingestion.",
        columns=(
            ColumnSpec(name="id", sql_type="INTEGER", primary_key=True),
            ColumnSpec(name="created_at", sql_type="DATETIME"),
            ColumnSpec(name="message_key", sql_type="TEXT"),
            ColumnSpec(name="payload", sql_type="TEXT"),
        ),
        shape=(
            ("timestamp_base", "2026-01-01T00:00:00+0000"),
            ("timestamp_step_seconds", 1),
            ("partition_count", 16),
            ("payload_body_bytes", 224),
            ("payload_total_bytes", 232),
        ),
    ),
    "narrow-indexed": NarrowIndexedWorkload(
        key="narrow-indexed",
        description="Narrow rows plus tenant/stream dimensions and 3 secondary indexes. Use to isolate index write amplification.",
        columns=(
            ColumnSpec(name="id", sql_type="INTEGER", primary_key=True),
            ColumnSpec(name="tenant_id", sql_type="INTEGER"),
            ColumnSpec(name="stream_id", sql_type="INTEGER"),
            ColumnSpec(name="value", sql_type="TEXT"),
        ),
        secondary_indexes=(
            IndexSpec(name="tenant_id_idx", columns=("tenant_id",)),
            IndexSpec(name="stream_id_idx", columns=("stream_id",)),
            IndexSpec(name="tenant_stream_idx", columns=("tenant_id", "stream_id")),
        ),
        shape=(
            ("tenant_cardinality", 512),
            ("stream_cardinality", 64),
            ("value_pattern", "narrow_{seed}_{index:08d}"),
        ),
    ),
    "kafka-like-indexed": KafkaLikeIndexedWorkload(
        key="kafka-like-indexed",
        description="Event-shaped rows plus tenant dimension and 4 secondary indexes. Use to combine payload work with index amplification.",
        columns=(
            ColumnSpec(name="id", sql_type="INTEGER", primary_key=True),
            ColumnSpec(name="tenant_id", sql_type="INTEGER"),
            ColumnSpec(name="created_at", sql_type="DATETIME"),
            ColumnSpec(name="message_key", sql_type="TEXT"),
            ColumnSpec(name="payload", sql_type="TEXT"),
        ),
        secondary_indexes=(
            IndexSpec(name="tenant_id_idx", columns=("tenant_id",)),
            IndexSpec(name="created_at_idx", columns=("created_at",)),
            IndexSpec(name="message_key_idx", columns=("message_key",)),
            IndexSpec(name="tenant_created_at_idx", columns=("tenant_id", "created_at")),
        ),
        shape=(
            ("tenant_cardinality", 512),
            ("timestamp_base", "2026-01-01T00:00:00+0000"),
            ("timestamp_step_seconds", 1),
            ("partition_count", 16),
            ("payload_body_bytes", 224),
            ("payload_total_bytes", 232),
        ),
    ),
}


def get_workload(key: str) -> WorkloadSpec:
    return WORKLOADS[key]


def generate_rows(workload: WorkloadSpec, row_count: int, seed: int) -> Sequence[Row]:
    return GeneratedRows(workload=workload, row_count=row_count, seed=seed)


def chunk_rows(rows: Sequence[Row], batch_rows: int) -> Iterable[Sequence[Row]]:
    for start in range(0, len(rows), batch_rows):
        yield rows[start : start + batch_rows]


def split_rows(rows: Sequence[Row], concurrency: int) -> list[Sequence[Row]]:
    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")
    if concurrency == 1:
        return [rows]

    row_count = len(rows)
    return [
        StridedRows(rows=rows, offset=offset, step=concurrency, row_count=ceil((row_count - offset) / concurrency))
        for offset in range(min(concurrency, row_count))
    ]


def build_prepared_insert_statement(table_name: str, workload: WorkloadSpec, row_count: int) -> str:
    if row_count < 1:
        raise ValueError("row_count must be >= 1")
    column_list = ", ".join(f'"{column.name}"' for column in workload.columns)
    placeholder_row = "(" + ", ".join("%s" for _ in workload.columns) + ")"
    value_list = ",\n".join(placeholder_row for _ in range(row_count))
    return f'INSERT INTO "{table_name}" ({column_list}) VALUES\n{value_list}'


def build_insert_parameters(rows: Sequence[Row]) -> tuple[tuple[object, ...], int]:
    params: list[object] = []
    payload_bytes = 0
    for row in rows:
        for value in row:
            params.append(value)
            payload_bytes += parameter_payload_bytes(value)
    return tuple(params), payload_bytes


def build_copy_payload(rows: Sequence[Row]) -> str:
    return "".join("\t".join(copy_literal(value) for value in row) + "\n" for row in rows)


def logical_payload_bytes(rows: Sequence[Row]) -> int:
    total = 0
    for row in rows:
        total += sum(copy_literal_bytes(value) for value in row)
        total += max(len(row) - 1, 0)
        total += 1
    return total


def copy_literal(value: object) -> str:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
    raise TypeError(f"unsupported COPY literal type: {type(value)!r}")


def copy_literal_bytes(value: object) -> int:
    return len(copy_literal(value).encode("utf-8"))


def parameter_payload_bytes(value: object) -> int:
    if isinstance(value, int):
        return len(str(value).encode("utf-8"))
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    raise TypeError(f"unsupported parameter type: {type(value)!r}")


def _deterministic_payload(seed: int, index: int, width: int) -> str:
    base = f"{seed:08x}{index:08x}"
    repeats = (width // len(base)) + 1
    payload = (base * repeats)[:width]
    return f"payload-{payload}"
