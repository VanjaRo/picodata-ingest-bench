from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from picodata_ingest_bench.config import CLUSTER_PROFILES
from picodata_ingest_bench.workloads import Row, WorkloadSpec


DATABASE_NAME = "picodata"


def create_table_sql(table_name: str, workload: WorkloadSpec, profile_key: str) -> str:
    profile = CLUSTER_PROFILES[profile_key]
    columns: list[str] = []
    pk_columns: list[str] = []
    for column in workload.columns:
        nullability = "" if column.nullable else " NOT NULL"
        columns.append(f'"{column.name}" {column.sql_type}{nullability}')
        if column.primary_key:
            pk_columns.append(f'"{column.name}"')

    primary_key = ", ".join(pk_columns)
    columns_sql = ",\n            ".join(columns)
    return f'''
        CREATE TABLE "{table_name}" (
            {columns_sql},
            PRIMARY KEY ({primary_key})
        ) USING memtx {profile.distribution_sql}
        OPTION (TIMEOUT = 3)
    '''


def create_index_sql(table_name: str, workload: WorkloadSpec) -> list[str]:
    statements: list[str] = []
    for index in workload.secondary_indexes:
        columns_sql = ", ".join(f'"{column}"' for column in index.columns)
        statements.append(
            f'CREATE INDEX "{table_name}_{index.name}" ON "{table_name}" ({columns_sql}) OPTION (TIMEOUT = 10)'
        )
    return statements


def copy_from_stdin_sql(table_name: str, workload: WorkloadSpec, batch_rows: int) -> str:
    column_list = ", ".join(f'"{column.name}"' for column in workload.columns)
    return (
        f'COPY "{table_name}" ({column_list}) '
        f"FROM STDIN WITH (SESSION_FLUSH_ROWS = {batch_rows}, DESTINATION_FLUSH_ROWS = {batch_rows})"
    )


def normalize_result_row(workload: WorkloadSpec, row: Sequence[object]) -> Row:
    normalized: list[object] = []
    for column, value in zip(workload.columns, row, strict=True):
        if column.sql_type == "DATETIME":
            normalized.append(_normalize_datetime_result(value))
        else:
            normalized.append(value)
    return tuple(normalized)


def _normalize_datetime_result(value: object) -> object:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M:%S+0000")
    return value
