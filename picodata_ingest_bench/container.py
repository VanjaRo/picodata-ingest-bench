from __future__ import annotations

import argparse
from dataclasses import dataclass
import subprocess
import sys

from picodata_ingest_bench.runtime import (
    DEFAULT_CONTAINER_IMAGE,
    build_container_run,
    copy_container_outputs,
    ensure_container_image,
    picodata_source_from_argv,
    rewrite_container_report_paths,
)

SUPPORTED_INGEST_SUBCOMMANDS = {"run", "plan", "describe"}


@dataclass(frozen=True)
class ContainerCliArgs:
    command: str
    container_image: str
    ingest_args: list[str]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ingest benchmark tasks inside the benchmark container.")
    parser.add_argument("--container-image", default=DEFAULT_CONTAINER_IMAGE, help="container image to use")
    parser.add_argument("command", choices=sorted(SUPPORTED_INGEST_SUBCOMMANDS))
    parser.add_argument("ingest_args", nargs=argparse.REMAINDER, help="arguments passed to picodata_ingest_bench")
    return parser


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()
    if not argv or argv[0] in {"-h", "--help"}:
        args = parser.parse_args(argv)
    else:
        args = parse_container_args(argv, parser)
    if args.command in {"describe", "plan"}:
        ingest_args = normalize_ingest_args(args.command, args.ingest_args)
        return run_local_ingest_command(ingest_args)

    ingest_args = normalize_ingest_args(args.command, args.ingest_args)
    try:
        picodata_source = picodata_source_from_argv(ingest_args)
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")

    try:
        ensure_container_image(container_image=args.container_image)
    except RuntimeError as exc:
        parser.exit(2, f"error: {exc}\n")

    if not ingest_args:
        parser.exit(2, "error: container run requires picodata_ingest_bench arguments after '--'\n")

    container_run = build_container_run(
        argv=ingest_args,
        container_image=args.container_image,
        picodata_source=picodata_source,
    )
    completed = subprocess.run(container_run.command)
    copy_container_outputs(container_run.output_copies, path_rewrites=container_run.path_rewrites)
    rewrite_container_report_paths(
        report_paths=container_run.report_paths,
        path_rewrites=container_run.path_rewrites,
    )
    return completed.returncode


def parse_container_args(argv: list[str], parser: argparse.ArgumentParser) -> ContainerCliArgs:
    tokens = list(argv)
    container_image = DEFAULT_CONTAINER_IMAGE
    if tokens and tokens[0] == "--container-image":
        if len(tokens) < 2:
            parser.exit(2, "error: --container-image expects a value\n")
        container_image = tokens[1]
        tokens = tokens[2:]
    elif tokens and tokens[0].startswith("--container-image="):
        container_image = tokens[0].split("=", 1)[1]
        tokens = tokens[1:]

    if not tokens:
        parser.exit(2, "error: missing command; expected run, plan, or describe\n")

    command = tokens[0]
    if command not in SUPPORTED_INGEST_SUBCOMMANDS:
        parser.exit(2, f"error: invalid command {command!r}; expected run, plan, or describe\n")

    ingest_args = list(tokens[1:])
    if ingest_args and ingest_args[0] == "--container-image":
        if len(ingest_args) < 2:
            parser.exit(2, "error: --container-image expects a value\n")
        container_image = ingest_args[1]
        ingest_args = ingest_args[2:]
    elif ingest_args and ingest_args[0].startswith("--container-image="):
        container_image = ingest_args[0].split("=", 1)[1]
        ingest_args = ingest_args[1:]

    return ContainerCliArgs(command=command, container_image=container_image, ingest_args=ingest_args)


def run_local_ingest_command(argv: list[str]) -> int:
    from picodata_ingest_bench.cli import main as ingest_main

    return ingest_main(argv)


def normalize_ingest_args(command: str, args: list[str]) -> list[str]:
    normalized = list(args)
    if normalized and normalized[0] == "--":
        normalized = normalized[1:]
    if command == "plan":
        if "--runtime" not in normalized and not any(arg.startswith("--runtime=") for arg in normalized):
            normalized.extend(["--runtime", "container"])
        return [command, *normalized]
    if command == "describe":
        return [command, *normalized]
    if normalized and normalized[0] not in SUPPORTED_INGEST_SUBCOMMANDS:
        normalized.insert(0, "run")
    return normalized


if __name__ == "__main__":
    raise SystemExit(main())
