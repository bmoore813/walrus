#!/usr/bin/env python3
"""Local performance run metadata, sampling, summaries, and comparisons for Walrus."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import platform
import re
import signal
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2
METRIC_COLUMNS = (
    "extractor_rows",
    "raw_append_lag_bytes",
    "transform_lag_bytes",
    "files_ready",
    "raw_rows",
    "extractor_inflight_bytes",
    "extractor_spill_total",
    "replication_lag_bytes",
)
CSV_COLUMNS = (
    "t_seconds",
    *METRIC_COLUMNS,
    "extractor_cpu_seconds",
    "transformer_cpu_seconds",
    "extractor_rss_bytes",
    "transformer_rss_bytes",
)
PROMETHEUS_NAMES = {
    "extractor_rows": "walrus_extractor_parquet_rows_written_total",
    "raw_append_lag_bytes": "walrus_transformer_raw_append_lag_bytes",
    "transform_lag_bytes": "walrus_transformer_transform_lag_bytes",
    "files_ready": "walrus_transformer_files_ready",
    "raw_rows": "walrus_transformer_raw_row_count",
    "extractor_inflight_bytes": "walrus_extractor_inflight_bytes",
    "extractor_spill_total": "walrus_extractor_spill_total",
    "replication_lag_bytes": "walrus_extractor_replication_lag_bytes",
}

RELOAD_SAMPLE_COLUMNS = (
    "implementation",
    "fixture",
    "matrix",
    "tables_requested",
    "max_concurrent_reloads",
    "workers_per_table",
    "chunk_rows",
    "iteration",
    "warmup",
    "status",
    "failure_reason",
    "rows_expected",
    "rows_exported",
    "source_bytes",
    "export_seconds",
    "publish_seconds",
    "rows_per_second",
    "source_mib_per_second",
    "extractor_cpu_seconds",
    "extractor_peak_rss_bytes",
    "transformer_peak_rss_bytes",
    "source_blks_read",
    "source_blks_hit",
    "peak_copy_connections",
    "peak_copy_tables",
    "peak_wal_lag_bytes",
    "chunk_files",
    "slot_count_min",
    "slot_count_max",
    "walsenders_min",
    "walsenders_max",
    "mirror_diff_rows",
)
RELOAD_TEXT_COLUMNS = {
    "implementation",
    "fixture",
    "matrix",
    "status",
    "failure_reason",
}
RELOAD_INTEGER_COLUMNS = {
    "tables_requested",
    "max_concurrent_reloads",
    "workers_per_table",
    "chunk_rows",
    "iteration",
    "rows_expected",
    "rows_exported",
    "source_bytes",
    "extractor_peak_rss_bytes",
    "transformer_peak_rss_bytes",
    "source_blks_read",
    "source_blks_hit",
    "peak_copy_connections",
    "peak_copy_tables",
    "peak_wal_lag_bytes",
    "chunk_files",
    "slot_count_min",
    "slot_count_max",
    "walsenders_min",
    "walsenders_max",
    "mirror_diff_rows",
}
RELOAD_GROUP_COLUMNS = (
    "implementation",
    "fixture",
    "matrix",
    "tables_requested",
    "max_concurrent_reloads",
    "workers_per_table",
    "chunk_rows",
)
RELOAD_MATRICES = ("workers", "tables", "chunks")
RELOAD_MEDIAN_COLUMNS = (
    "export_seconds",
    "publish_seconds",
    "rows_per_second",
    "source_mib_per_second",
    "extractor_cpu_seconds",
    "extractor_peak_rss_bytes",
    "transformer_peak_rss_bytes",
    "source_blks_read",
    "source_blks_hit",
    "peak_copy_connections",
    "peak_copy_tables",
    "peak_wal_lag_bytes",
    "chunk_files",
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def command_output(*command: str) -> str | None:
    try:
        result = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    value = result.stdout.strip()
    return value or None


def cpu_model() -> str | None:
    value = command_output("sysctl", "-n", "machdep.cpu.brand_string")
    if value:
        return value
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith(("model name", "hardware")):
                return line.split(":", 1)[-1].strip()
    except OSError:
        pass
    return platform.processor() or None


def tool_version(tool: str) -> str | None:
    value = command_output(tool, "--version")
    return value.splitlines()[0] if value else None


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path_or_dir: str) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "summary.json"
    return json.loads(path.read_text(encoding="utf-8"))


def start_run(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=False)
    commit = command_output("git", "rev-parse", "HEAD")
    dirty_output = command_output("git", "status", "--porcelain")
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_dir.name,
        "status": "running",
        "started_at": utc_now(),
        "ended_at": None,
        "mode": args.mode,
        "diagnostic_target": args.target,
        "build_profile": args.profile,
        "comparable": args.mode == "measure" and args.profile == "release",
        "scenario": args.scenario,
        "workload": {
            "duration_seconds": args.duration,
            "clients": args.clients,
            "max_fill": args.max_fill,
            "max_rows": args.max_rows,
            "max_bytes": args.max_bytes,
            "max_inflight_bytes": args.max_inflight,
            "poll_interval": args.poll_interval,
            "sample_interval_seconds": args.sample_interval,
        },
        "source": {"git_commit": commit, "git_dirty": bool(dirty_output)},
        "host": {
            "os": platform.system(),
            "kernel": platform.release(),
            "architecture": platform.machine(),
            "cpu_model": cpu_model(),
        },
        "toolchain": {
            "rustc": tool_version("rustc"),
            "cargo": tool_version("cargo"),
            "python": platform.python_version(),
            "samply": tool_version("samply") if args.mode == "cpu" else None,
            "tokio_console": tool_version("tokio-console") if args.mode == "async" else None,
            "console_subscriber": "0.5.0" if args.mode == "async" else None,
            "dhat": "0.3.3" if args.mode == "heap" else None,
        },
    }
    write_json(run_dir / "metadata.json", metadata)
    return 0


def parse_prometheus(text: str, name: str) -> float:
    total = 0.0
    matched = False
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        metric, separator, value = line.rpartition(" ")
        if not separator:
            continue
        bare_name = metric.split("{", 1)[0]
        if bare_name != name:
            continue
        try:
            total += float(value)
            matched = True
        except ValueError:
            continue
    return total if matched else 0.0


def scrape(url: str) -> str:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.read().decode("utf-8", errors="replace")
    except (OSError, TimeoutError):
        return ""


def parse_cpu_time(value: str) -> float:
    value = value.strip()
    days = 0
    if "-" in value:
        day_text, value = value.split("-", 1)
        days = int(day_text)
    parts = value.split(":")
    if len(parts) == 3:
        hours, minutes, seconds = parts
    elif len(parts) == 2:
        hours = "0"
        minutes, seconds = parts
    else:
        raise ValueError(f"unsupported CPU time: {value!r}")
    return days * 86400 + int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def process_usage(pid: int) -> tuple[float | None, int | None]:
    output = command_output("ps", "-o", "time=", "-o", "rss=", "-p", str(pid))
    if not output:
        return None, None
    fields = output.split()
    if len(fields) < 2:
        return None, None
    try:
        return parse_cpu_time(fields[-2]), int(fields[-1]) * 1024
    except ValueError:
        return None, None


def sample_run(args: argparse.Namespace) -> int:
    keep_running = True

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal keep_running
        keep_running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    output = Path(args.output)
    started = time.monotonic()
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        while keep_running:
            extractor_metrics = scrape(args.extractor_url)
            transformer_metrics = scrape(args.transformer_url)
            extractor_cpu, extractor_rss = process_usage(args.extractor_pid)
            transformer_cpu, transformer_rss = process_usage(args.transformer_pid)
            merged = extractor_metrics + "\n" + transformer_metrics
            row: dict[str, Any] = {"t_seconds": round(time.monotonic() - started, 3)}
            for column in METRIC_COLUMNS:
                row[column] = parse_prometheus(merged, PROMETHEUS_NAMES[column])
            row.update(
                {
                    "extractor_cpu_seconds": extractor_cpu,
                    "transformer_cpu_seconds": transformer_cpu,
                    "extractor_rss_bytes": extractor_rss,
                    "transformer_rss_bytes": transformer_rss,
                }
            )
            writer.writerow(row)
            handle.flush()
            deadline = time.monotonic() + args.interval
            while keep_running and time.monotonic() < deadline:
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    return 0


def numeric_rows(path: Path) -> list[dict[str, float | None]]:
    rows: list[dict[str, float | None]] = []
    if not path.exists():
        return rows
    with path.open(newline="", encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            parsed: dict[str, float | None] = {}
            for key, value in raw.items():
                try:
                    parsed[key] = float(value) if value not in (None, "") else None
                except ValueError:
                    parsed[key] = None
            rows.append(parsed)
    return rows


def first_last_delta(rows: list[dict[str, float | None]], key: str) -> float | None:
    values = [row[key] for row in rows if row.get(key) is not None]
    if len(values) < 2:
        return None
    return max(0.0, float(values[-1]) - float(values[0]))


def peak(rows: list[dict[str, float | None]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return max(values) if values else None


def per_thousand(cpu_seconds: float | None, rows: int) -> float | None:
    if cpu_seconds is None or rows <= 0:
        return None
    return cpu_seconds * 1000 / rows


def finish_run(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    metadata_path = run_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    samples = numeric_rows(run_dir / "samples.csv")
    row_count = max(0, round(args.rows_end - args.rows_start))
    extractor_cpu = first_last_delta(samples, "extractor_cpu_seconds")
    transformer_cpu = first_last_delta(samples, "transformer_cpu_seconds")
    total_cpu = None if extractor_cpu is None or transformer_cpu is None else extractor_cpu + transformer_cpu
    flush_count = max(0, round(args.flush_count_end - args.flush_count_start))
    flush_latency = None
    if flush_count > 0:
        flush_latency = (args.flush_sum_end - args.flush_sum_start) / flush_count
    spill_count = max(0, round(args.spill_end - args.spill_start))
    reasons = [reason for reason in args.failure_reason if reason]
    status = "success" if not reasons else "failed"
    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": metadata["run_id"],
        "status": status,
        "failure_reasons": reasons,
        "comparable": metadata["comparable"] and status == "success",
        "mode": metadata["mode"],
        "diagnostic_target": metadata.get("diagnostic_target"),
        "build_profile": metadata["build_profile"],
        "scenario": metadata["scenario"],
        "workload": metadata["workload"],
        "host": metadata["host"],
        "toolchain": metadata["toolchain"],
        "throughput": {
            "rows": row_count,
            "elapsed_seconds": args.elapsed,
            "rows_per_second": row_count / args.elapsed if args.elapsed > 0 else None,
        },
        "cpu": {
            "extractor_seconds": extractor_cpu,
            "transformer_seconds": transformer_cpu,
            "total_seconds": total_cpu,
            "extractor_seconds_per_1000_rows": per_thousand(extractor_cpu, row_count),
            "transformer_seconds_per_1000_rows": per_thousand(transformer_cpu, row_count),
            "total_seconds_per_1000_rows": per_thousand(total_cpu, row_count),
        },
        "memory": {
            "extractor_peak_rss_bytes": peak(samples, "extractor_rss_bytes"),
            "transformer_peak_rss_bytes": peak(samples, "transformer_rss_bytes"),
        },
        "pipeline": {
            "mean_flush_latency_seconds": flush_latency,
            "flush_count": flush_count,
            "spill_count": spill_count,
            "extractor_inflight_peak_bytes": peak(samples, "extractor_inflight_bytes"),
            "raw_append_lag_peak_bytes": peak(samples, "raw_append_lag_bytes"),
            "transform_lag_peak_bytes": peak(samples, "transform_lag_bytes"),
            "files_ready_peak": peak(samples, "files_ready"),
            "raw_rows_peak": peak(samples, "raw_rows"),
            "replication_lag_peak_bytes": peak(samples, "replication_lag_bytes"),
        },
        "artifacts": {
            "metadata": "metadata.json",
            "samples": "samples.csv",
            "extractor_log": "extractor.log",
            "transformer_log": "transformer.log",
            "diagnostics": sorted(
                path.name
                for path in run_dir.iterdir()
                if path.is_file()
                and any(
                    marker in path.name
                    for marker in ("samply.json", "dhat-heap", "tokio-console")
                )
            ),
        },
    }
    metadata["status"] = status
    metadata["ended_at"] = utc_now()
    write_json(metadata_path, metadata)
    write_json(run_dir / "summary.json", summary)
    print_summary(summary, run_dir)
    return 0 if status == "success" else 1


def fail_run(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        return 0
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if metadata.get("status") != "running":
        return 0
    metadata["status"] = "failed"
    metadata["ended_at"] = utc_now()
    write_json(metadata_path, metadata)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": metadata["run_id"],
        "status": "failed",
        "failure_reasons": [args.reason],
        "comparable": False,
        "mode": metadata["mode"],
        "diagnostic_target": metadata.get("diagnostic_target"),
        "build_profile": metadata["build_profile"],
        "scenario": metadata["scenario"],
        "workload": metadata["workload"],
        "host": metadata["host"],
        "toolchain": metadata["toolchain"],
    }
    write_json(run_dir / "summary.json", summary)
    return 0


def resolve_bench(args: argparse.Namespace) -> int:
    matches: list[str] = []
    with Path(args.cargo_json).open(encoding="utf-8") as handle:
        for line in handle:
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            target = message.get("target", {})
            if (
                message.get("reason") == "compiler-artifact"
                and target.get("name") == args.bench
                and "bench" in target.get("kind", [])
                and message.get("executable")
            ):
                matches.append(str(message["executable"]))
    unique = sorted(set(matches))
    if len(unique) != 1:
        print(
            f"expected one executable for benchmark {args.bench!r}, found {unique}",
            file=sys.stderr,
        )
        return 2
    print(unique[0])
    return 0


def complete_artifact(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir)
    metadata_path = run_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    artifact = run_dir / args.artifact
    status = "success" if artifact.is_file() and artifact.stat().st_size > 0 else "failed"
    reasons = [] if status == "success" else [f"missing or empty artifact: {args.artifact}"]
    metadata["status"] = status
    metadata["ended_at"] = utc_now()
    write_json(metadata_path, metadata)
    write_json(
        run_dir / "summary.json",
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": metadata["run_id"],
            "status": status,
            "failure_reasons": reasons,
            "comparable": False,
            "mode": metadata["mode"],
            "diagnostic_target": metadata.get("diagnostic_target"),
            "build_profile": metadata["build_profile"],
            "scenario": metadata["scenario"],
            "workload": metadata["workload"],
            "host": metadata["host"],
            "toolchain": metadata["toolchain"],
            "artifacts": {"profile": args.artifact, "cargo_messages": "cargo-artifacts.json"},
        },
    )
    print(f"profile bundle: {run_dir}")
    return 0 if status == "success" else 1


def format_number(value: Any, unit: str = "") -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if unit == "bytes":
        for suffix in ("B", "KiB", "MiB", "GiB"):
            if abs(number) < 1024 or suffix == "GiB":
                return f"{number:.2f} {suffix}"
            number /= 1024
    return f"{number:.4f}{unit}"


def print_summary(summary: dict[str, Any], run_dir: Path) -> None:
    throughput = summary["throughput"]
    cpu = summary["cpu"]
    memory = summary["memory"]
    pipeline = summary["pipeline"]
    print("\n==========================================================================")
    print(f"  perf SUMMARY — scenario={summary['scenario']} mode={summary['mode']}")
    print(f"  status={summary['status']} profile={summary['build_profile']}")
    print("--------------------------------------------------------------------------")
    print(
        f"  rows={throughput['rows']}  rows/s={format_number(throughput['rows_per_second'])}  "
        f"elapsed={format_number(throughput['elapsed_seconds'], 's')}"
    )
    print(
        "  CPU seconds / 1k rows: "
        f"extractor={format_number(cpu['extractor_seconds_per_1000_rows'])}  "
        f"transformer={format_number(cpu['transformer_seconds_per_1000_rows'])}  "
        f"total={format_number(cpu['total_seconds_per_1000_rows'])}"
    )
    print(
        "  peak RSS: "
        f"extractor={format_number(memory['extractor_peak_rss_bytes'], 'bytes')}  "
        f"transformer={format_number(memory['transformer_peak_rss_bytes'], 'bytes')}"
    )
    print(
        "  peak lag: "
        f"raw={format_number(pipeline['raw_append_lag_peak_bytes'], 'bytes')}  "
        f"transform={format_number(pipeline['transform_lag_peak_bytes'], 'bytes')}  "
        f"inflight={format_number(pipeline['extractor_inflight_peak_bytes'], 'bytes')}"
    )
    if summary["failure_reasons"]:
        print(f"  failures: {'; '.join(summary['failure_reasons'])}")
    print(f"  run bundle: {run_dir}")
    print("==========================================================================\n")


def nested(value: dict[str, Any], path: str) -> Any:
    current: Any = value
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def comparison_mismatches(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
    paths = (
        "mode",
        "diagnostic_target",
        "build_profile",
        "scenario",
        "workload.duration_seconds",
        "workload.clients",
        "workload.max_fill",
        "workload.max_rows",
        "workload.max_bytes",
        "workload.max_inflight_bytes",
        "workload.poll_interval",
        "workload.sample_interval_seconds",
        "host.os",
        "host.architecture",
        "host.cpu_model",
        "toolchain.rustc",
    )
    mismatches = [path for path in paths if nested(left, path) != nested(right, path)]
    if not left.get("comparable") or not right.get("comparable"):
        mismatches.append("run.comparable")
    if left.get("status") != "success" or right.get("status") != "success":
        mismatches.append("run.status")
    return mismatches


def compare_runs(args: argparse.Namespace) -> int:
    baseline = read_json(args.baseline)
    candidate = read_json(args.candidate)
    mismatches = comparison_mismatches(baseline, candidate)
    if mismatches and not args.allow_mismatch:
        print("incompatible performance runs:", file=sys.stderr)
        for path in mismatches:
            print(
                f"  {path}: {nested(baseline, path)!r} != {nested(candidate, path)!r}",
                file=sys.stderr,
            )
        print("use --allow-mismatch only for an explicitly non-authoritative comparison", file=sys.stderr)
        return 2
    if mismatches:
        print("WARNING: comparing mismatched runs: " + ", ".join(mismatches))
    metrics = (
        ("rows/s", "throughput.rows_per_second", True),
        ("elapsed seconds", "throughput.elapsed_seconds", False),
        ("total CPU s/1k rows", "cpu.total_seconds_per_1000_rows", False),
        ("extractor CPU s/1k rows", "cpu.extractor_seconds_per_1000_rows", False),
        ("transformer CPU s/1k rows", "cpu.transformer_seconds_per_1000_rows", False),
        ("extractor peak RSS", "memory.extractor_peak_rss_bytes", False),
        ("transformer peak RSS", "memory.transformer_peak_rss_bytes", False),
        ("raw lag peak", "pipeline.raw_append_lag_peak_bytes", False),
        ("transform lag peak", "pipeline.transform_lag_peak_bytes", False),
        ("extractor inflight peak", "pipeline.extractor_inflight_peak_bytes", False),
        ("mean flush latency", "pipeline.mean_flush_latency_seconds", False),
        ("spill count", "pipeline.spill_count", False),
    )
    print(f"baseline:  {baseline['run_id']}")
    print(f"candidate: {candidate['run_id']}")
    print(f"{'metric':27} {'baseline':>14} {'candidate':>14} {'change':>12}  result")
    for label, path, higher_is_better in metrics:
        before = nested(baseline, path)
        after = nested(candidate, path)
        if before is None or after is None:
            print(f"{label:27} {'n/a':>14} {'n/a':>14} {'n/a':>12}  unknown")
            continue
        if float(before) == 0:
            change = None
        else:
            change = (float(after) - float(before)) / float(before) * 100
        if change is None:
            change_text = "n/a"
            result = "unknown"
        else:
            improvement = change if higher_is_better else -change
            result = "better" if improvement > 0 else "worse" if improvement < 0 else "same"
            change_text = f"{change:+.2f}%"
        print(f"{label:27} {float(before):14.4f} {float(after):14.4f} {change_text:>12}  {result}")
    return 0


def parse_csv_numbers(value: str) -> list[int]:
    """Parse a non-empty, positive, comma-separated integer list."""
    parts = value.split(",")
    if not value or any(not part.strip() for part in parts):
        raise ValueError(f"expected positive comma-separated integers, got {value!r}")
    try:
        numbers = [int(part) for part in parts]
    except ValueError as error:
        raise ValueError(f"expected comma-separated integers, got {value!r}") from error
    if not numbers or any(number <= 0 for number in numbers):
        raise ValueError(f"expected positive comma-separated integers, got {value!r}")
    if len(numbers) != len(set(numbers)):
        raise ValueError(f"expected distinct comma-separated integers, got {value!r}")
    return numbers


def selected_reload_matrices(matrix: str) -> tuple[str, ...]:
    return RELOAD_MATRICES if matrix == "all" else (matrix,)


def expected_effective_reload_settings(metadata: dict[str, Any]) -> dict[str, Any]:
    """Return the exact candidate settings exercised, including the bootstrap settings."""
    workload = metadata["workload"]
    matrices = selected_reload_matrices(metadata["matrix"])
    max_concurrent = {workload["base_concurrent_tables"]}
    workers = {workload["base_workers_per_table"]}
    chunks = {workload["base_chunk_rows"]}
    if "tables" in matrices:
        max_concurrent.update(workload["max_concurrent_reloads"])
    if "workers" in matrices:
        workers.update(workload["workers_per_table"])
    if "chunks" in matrices:
        chunks.update(workload["chunk_rows"])
    return {
        "max_concurrent_reloads": sorted(max_concurrent),
        "reload_workers_per_table": sorted(workers),
        "reload_chunk_rows": sorted(chunks),
        "max_inflight_bytes": workload["max_inflight_bytes"],
        "max_bytes": workload["max_bytes"],
        "max_rows": workload["max_rows"],
        "max_fill": workload["max_fill"],
        "heartbeat_idle_after": workload["heartbeat_idle_after"],
    }


def reload_metadata_failures(metadata: dict[str, Any]) -> list[str]:
    """Validate the matrix envelope needed to reproduce and audit a reload run."""
    failures: list[str] = []
    if metadata.get("schema_version") != SCHEMA_VERSION:
        failures.append(
            f"metadata schema_version is {metadata.get('schema_version')!r}, "
            f"expected {SCHEMA_VERSION}"
        )
    if metadata.get("kind") != "reload_matrix":
        failures.append(f"metadata kind is invalid: {metadata.get('kind')!r}")
    matrix = metadata.get("matrix")
    if matrix not in (*RELOAD_MATRICES, "all"):
        failures.append(f"metadata matrix is invalid: {matrix!r}")

    fixtures = metadata.get("fixtures")
    if (
        not isinstance(fixtures, list)
        or not fixtures
        or any(
            not isinstance(fixture, str) or fixture not in {"narrow", "wide"}
            for fixture in fixtures
        )
        or len(fixtures) != len(set(fixtures))
    ):
        failures.append(
            "metadata fixtures must be a distinct non-empty narrow/wide list: "
            f"{fixtures!r}"
        )

    workload = metadata.get("workload")
    if not isinstance(workload, dict):
        return failures + ["metadata workload is missing"]

    list_fields = (
        "workers_per_table",
        "max_concurrent_reloads",
        "chunk_rows",
    )
    for field in list_fields:
        values = workload.get(field)
        if (
            not isinstance(values, list)
            or not values
            or any(type(value) is not int or value <= 0 for value in values)
            or len(values) != len(set(values))
        ):
            failures.append(f"metadata workload.{field} must contain distinct positive integers")

    positive_fields = (
        "narrow_rows",
        "narrow_payload_bytes",
        "wide_rows",
        "wide_payload_bytes",
        "base_workers_per_table",
        "base_concurrent_tables",
        "base_chunk_rows",
        "samples",
        "timeout_seconds",
        "max_inflight_bytes",
        "max_bytes",
        "max_rows",
    )
    for field in positive_fields:
        value = workload.get(field)
        if type(value) is not int or value <= 0:
            failures.append(f"metadata workload.{field} must be a positive integer")
    warmups = workload.get("warmups")
    if type(warmups) is not int or warmups < 0:
        failures.append("metadata workload.warmups must be a non-negative integer")
    sample_interval = workload.get("sample_interval_seconds")
    if (
        not isinstance(sample_interval, (int, float))
        or isinstance(sample_interval, bool)
        or sample_interval <= 0
    ):
        failures.append("metadata workload.sample_interval_seconds must be positive")
    for field in ("max_fill", "heartbeat_idle_after"):
        if not isinstance(workload.get(field), str) or not workload[field].strip():
            failures.append(f"metadata workload.{field} must be a non-empty duration")

    if matrix in ("workers", "all"):
        workers = workload.get("workers_per_table")
        if isinstance(workers, list) and 1 not in workers:
            failures.append("workers matrix requires workers_per_table=1 serial baseline")
    if matrix in ("tables", "all"):
        table_caps = workload.get("max_concurrent_reloads")
        if isinstance(table_caps, list) and 1 not in table_caps:
            failures.append("tables matrix requires max_concurrent_reloads=1 serial baseline")

    binaries = metadata.get("binaries")
    if not isinstance(binaries, dict):
        failures.append("metadata binaries are missing")
    else:
        for implementation in ("candidate", "baseline"):
            binary = binaries.get(implementation)
            if implementation == "baseline" and binary is None:
                continue
            if (
                not isinstance(binary, dict)
                or not isinstance(binary.get("path"), str)
                or not binary["path"]
            ):
                failures.append(f"metadata binaries.{implementation}.path is missing")
                continue
            digest = binary.get("sha256")
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                failures.append(f"metadata binaries.{implementation}.sha256 is invalid")

    effective = metadata.get("effective_settings")
    required_workload_fields = {
        *list_fields,
        *positive_fields,
        "warmups",
        "sample_interval_seconds",
        "max_fill",
        "heartbeat_idle_after",
    }
    if not isinstance(effective, dict):
        failures.append("metadata effective_settings are missing")
    elif required_workload_fields <= workload.keys() and matrix in (*RELOAD_MATRICES, "all"):
        try:
            expected = expected_effective_reload_settings(metadata)
        except (KeyError, TypeError, ValueError):
            pass
        else:
            for field, expected_value in expected.items():
                if effective.get(field) != expected_value:
                    failures.append(
                        f"metadata effective_settings.{field} is {effective.get(field)!r}, "
                        f"expected {expected_value!r}"
                    )
    return failures


def expected_reload_configs(metadata: dict[str, Any]) -> set[tuple[Any, ...]]:
    """Derive every raw CSV configuration promised by valid matrix metadata."""
    workload = metadata["workload"]
    fixtures = metadata["fixtures"]
    matrices = selected_reload_matrices(metadata["matrix"])
    base_tables = workload["base_concurrent_tables"]
    base_workers = workload["base_workers_per_table"]
    base_chunk = workload["base_chunk_rows"]
    table_workload = max(workload["max_concurrent_reloads"])
    has_baseline = metadata["binaries"].get("baseline") is not None
    configs: set[tuple[Any, ...]] = set()

    def add(
        implementation: str,
        fixture: str,
        matrix: str,
        tables_requested: int,
        max_concurrent_reloads: int,
        workers_per_table: int,
        chunk_rows: int,
    ) -> None:
        configs.add(
            (
                implementation,
                fixture,
                matrix,
                tables_requested,
                max_concurrent_reloads,
                workers_per_table,
                chunk_rows,
            )
        )

    for fixture in fixtures:
        if "workers" in matrices:
            for workers in workload["workers_per_table"]:
                add("candidate", fixture, "workers", base_tables, base_tables, workers, base_chunk)
            if has_baseline:
                add("baseline", fixture, "workers", base_tables, base_tables, 1, base_chunk)
        if "tables" in matrices:
            for table_cap in workload["max_concurrent_reloads"]:
                add(
                    "candidate",
                    fixture,
                    "tables",
                    table_workload,
                    table_cap,
                    base_workers,
                    base_chunk,
                )
                if has_baseline:
                    add(
                        "baseline",
                        fixture,
                        "tables",
                        table_workload,
                        table_cap,
                        1,
                        base_chunk,
                    )
        if "chunks" in matrices:
            for chunk_rows in workload["chunk_rows"]:
                add(
                    "candidate",
                    fixture,
                    "chunks",
                    base_tables,
                    base_tables,
                    base_workers,
                    chunk_rows,
                )
                if has_baseline:
                    add(
                        "baseline",
                        fixture,
                        "chunks",
                        base_tables,
                        base_tables,
                        1,
                        chunk_rows,
                    )
    return configs


def reload_start_run(args: argparse.Namespace) -> int:
    """Create the self-describing envelope for a reload benchmark matrix."""
    run_dir = Path(args.run_dir)
    try:
        workers = parse_csv_numbers(args.workers)
        tables = parse_csv_numbers(args.tables)
        chunks = parse_csv_numbers(args.chunks)
        effective_max_concurrent = parse_csv_numbers(args.effective_max_concurrent_reloads)
        effective_workers = parse_csv_numbers(args.effective_workers_per_table)
        effective_chunks = parse_csv_numbers(args.effective_chunk_rows)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    fixtures = [fixture.strip() for fixture in args.fixtures.split(",")]
    candidate_sha256 = args.candidate_sha256.lower()
    baseline_sha256 = args.baseline_sha256.lower() if args.baseline_sha256 else ""
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "kind": "reload_matrix",
        "run_id": run_dir.name,
        "status": "running",
        "started_at": utc_now(),
        "ended_at": None,
        "build_profile": "release",
        "matrix": args.matrix,
        "fixtures": fixtures,
        "workload": {
            "narrow_rows": args.narrow_rows,
            "narrow_payload_bytes": args.narrow_payload_bytes,
            "wide_rows": args.wide_rows,
            "wide_payload_bytes": args.wide_payload_bytes,
            "workers_per_table": workers,
            "max_concurrent_reloads": tables,
            "chunk_rows": chunks,
            "base_workers_per_table": args.base_workers,
            "base_concurrent_tables": args.base_tables,
            "base_chunk_rows": args.base_chunk_rows,
            "warmups": args.warmups,
            "samples": args.samples,
            "sample_interval_seconds": args.sample_interval,
            "timeout_seconds": args.timeout_seconds,
            "max_inflight_bytes": args.max_inflight_bytes,
            "max_bytes": args.max_bytes,
            "max_rows": args.max_rows,
            "max_fill": args.max_fill,
            "heartbeat_idle_after": args.heartbeat_idle_after,
        },
        "effective_settings": {
            "max_concurrent_reloads": sorted(effective_max_concurrent),
            "reload_workers_per_table": sorted(effective_workers),
            "reload_chunk_rows": sorted(effective_chunks),
            "max_inflight_bytes": args.max_inflight_bytes,
            "max_bytes": args.max_bytes,
            "max_rows": args.max_rows,
            "max_fill": args.max_fill,
            "heartbeat_idle_after": args.heartbeat_idle_after,
        },
        "binaries": {
            "candidate": {"path": args.candidate_bin, "sha256": candidate_sha256},
            "baseline": (
                {"path": args.baseline_bin, "sha256": baseline_sha256}
                if args.baseline_bin
                else None
            ),
        },
        "source": {
            "git_commit": command_output("git", "rev-parse", "HEAD"),
            "git_dirty": bool(command_output("git", "status", "--porcelain")),
        },
        "host": {
            "os": platform.system(),
            "kernel": platform.release(),
            "architecture": platform.machine(),
            "cpu_model": cpu_model(),
        },
        "toolchain": {
            "rustc": tool_version("rustc"),
            "cargo": tool_version("cargo"),
            "python": platform.python_version(),
            "docker": tool_version("docker"),
        },
    }
    failures = reload_metadata_failures(metadata)
    if args.baseline_bin and not baseline_sha256:
        failures.append("--baseline-sha256 is required when --baseline-bin is set")
    if not args.baseline_bin and baseline_sha256:
        failures.append("--baseline-sha256 requires --baseline-bin")
    if failures:
        for failure in dict.fromkeys(failures):
            print(f"invalid reload benchmark metadata: {failure}", file=sys.stderr)
        return 2
    run_dir.mkdir(parents=True, exist_ok=False)
    write_json(run_dir / "metadata.json", metadata)
    with (run_dir / "reload-samples.csv").open("w", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=RELOAD_SAMPLE_COLUMNS).writeheader()
    return 0


def read_reload_samples(path: Path) -> list[dict[str, Any]]:
    """Read the reload harness's raw CSV into typed values."""
    samples: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != RELOAD_SAMPLE_COLUMNS:
            raise ValueError(
                "reload sample CSV columns differ: "
                f"expected {list(RELOAD_SAMPLE_COLUMNS)!r}, got {reader.fieldnames!r}"
            )
        for raw in reader:
            parsed: dict[str, Any] = {}
            for column in RELOAD_SAMPLE_COLUMNS:
                value = raw.get(column, "")
                if column in RELOAD_TEXT_COLUMNS:
                    parsed[column] = value or ""
                elif column == "warmup":
                    normalized = value.lower()
                    if normalized not in {"0", "1", "false", "true", "no", "yes"}:
                        raise ValueError(f"invalid warmup value {value!r}")
                    parsed[column] = normalized in {"1", "true", "yes"}
                elif value in (None, ""):
                    parsed[column] = None
                elif column in RELOAD_INTEGER_COLUMNS:
                    parsed[column] = int(value)
                else:
                    parsed[column] = float(value)
            for column, allowed in (
                ("implementation", {"candidate", "baseline"}),
                ("fixture", {"narrow", "wide"}),
                ("matrix", set(RELOAD_MATRICES)),
                ("status", {"success", "failed"}),
            ):
                if parsed[column] not in allowed:
                    raise ValueError(
                        f"line {reader.line_num}: invalid {column} value {parsed[column]!r}"
                    )
            for column in (
                "tables_requested",
                "max_concurrent_reloads",
                "workers_per_table",
                "chunk_rows",
                "iteration",
            ):
                if type(parsed[column]) is not int or parsed[column] <= 0:
                    raise ValueError(
                        f"line {reader.line_num}: {column} must be a positive integer"
                    )
            samples.append(parsed)
    return samples


def median_present(samples: list[dict[str, Any]], column: str) -> float | None:
    values = [float(sample[column]) for sample in samples if sample.get(column) is not None]
    return statistics.median(values) if values else None


def format_reload_config(config: tuple[Any, ...]) -> str:
    values = dict(zip(RELOAD_GROUP_COLUMNS, config, strict=True))
    return (
        f"{values['implementation']}/{values['fixture']}/{values['matrix']} "
        f"tables_requested={values['tables_requested']} "
        f"max_concurrent_reloads={values['max_concurrent_reloads']} "
        f"workers={values['workers_per_table']} chunk={values['chunk_rows']}"
    )


def reload_matrix_failures(
    metadata: dict[str, Any], samples: list[dict[str, Any]]
) -> list[str]:
    """Check that the CSV contains exactly the configurations and rounds promised."""
    if reload_metadata_failures(metadata):
        return []
    expected_configs = expected_reload_configs(metadata)
    actual_by_config: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for sample in samples:
        config = tuple(sample[column] for column in RELOAD_GROUP_COLUMNS)
        actual_by_config.setdefault(config, []).append(sample)

    failures: list[str] = []
    actual_configs = set(actual_by_config)
    for config in sorted(expected_configs - actual_configs, key=repr):
        failures.append(f"missing benchmark configuration: {format_reload_config(config)}")
    for config in sorted(actual_configs - expected_configs, key=repr):
        failures.append(f"unexpected benchmark configuration: {format_reload_config(config)}")

    warmups = metadata["workload"]["warmups"]
    measured = metadata["workload"]["samples"]
    expected_rounds = {
        *((True, iteration) for iteration in range(1, warmups + 1)),
        *((False, iteration) for iteration in range(1, measured + 1)),
    }
    for config in sorted(expected_configs & actual_configs, key=repr):
        round_counts: dict[tuple[bool, Any], int] = {}
        for sample in actual_by_config[config]:
            round_key = (sample["warmup"], sample["iteration"])
            round_counts[round_key] = round_counts.get(round_key, 0) + 1
        for round_key, count in sorted(round_counts.items()):
            if count > 1:
                kind = "warmup" if round_key[0] else "measured"
                failures.append(
                    f"duplicate {kind} iteration {round_key[1]} ({count} rows): "
                    f"{format_reload_config(config)}"
                )
        for warmup, iteration in sorted(expected_rounds - set(round_counts)):
            kind = "warmup" if warmup else "measured"
            failures.append(
                f"missing {kind} iteration {iteration}: {format_reload_config(config)}"
            )
        for warmup, iteration in sorted(set(round_counts) - expected_rounds):
            kind = "warmup" if warmup else "measured"
            failures.append(
                f"unexpected {kind} iteration {iteration}: {format_reload_config(config)}"
            )

    row_counts = {
        "narrow": metadata["workload"]["narrow_rows"],
        "wide": metadata["workload"]["wide_rows"],
    }
    for sample in samples:
        fixture = sample.get("fixture")
        tables_requested = sample.get("tables_requested")
        if fixture not in row_counts or type(tables_requested) is not int:
            continue
        expected_rows = row_counts[fixture] * tables_requested
        if sample.get("rows_expected") != expected_rows:
            config = tuple(sample[column] for column in RELOAD_GROUP_COLUMNS)
            failures.append(
                f"{format_reload_config(config)} iteration={sample['iteration']}: "
                f"rows_expected is {sample.get('rows_expected')}, expected {expected_rows}"
            )
    return list(dict.fromkeys(failures))


def reload_sample_failures(samples: list[dict[str, Any]]) -> list[str]:
    """Return correctness/invariant failures observed by the benchmark, without duplicates."""
    failures: list[str] = []
    for sample in samples:
        identity = (
            f"{sample['implementation']}/{sample['fixture']}/{sample['matrix']} "
            f"tables_requested={sample['tables_requested']} "
            f"max_concurrent_reloads={sample['max_concurrent_reloads']} "
            f"workers={sample['workers_per_table']} "
            f"chunk={sample['chunk_rows']} iteration={sample['iteration']}"
        )
        if sample["status"] != "success":
            reason = sample["failure_reason"] or "harness marked sample failed"
            failures.append(f"{identity}: {reason}")
        expected = sample.get("rows_expected")
        exported = sample.get("rows_exported")
        if expected is None or exported is None:
            failures.append(
                f"{identity}: exported/expected row count is missing "
                f"(exported={exported}, expected={expected})"
            )
        elif expected != exported:
            failures.append(f"{identity}: exported {exported} rows, expected {expected}")
        mirror_diff = sample.get("mirror_diff_rows")
        if not sample.get("warmup") and sample.get("status") == "success" and mirror_diff is None:
            failures.append(f"{identity}: exact mirror comparison is missing")
        elif mirror_diff not in (None, 0):
            failures.append(f"{identity}: mirror differs by {mirror_diff} rows")
        for low, high, label in (
            ("slot_count_min", "slot_count_max", "replication slot"),
            ("walsenders_min", "walsenders_max", "active walsender"),
        ):
            if sample.get(low) != 1 or sample.get(high) != 1:
                failures.append(
                    f"{identity}: {label} count left 1 "
                    f"(min={sample.get(low)}, max={sample.get(high)})"
                )
        tables_requested = sample.get("tables_requested")
        max_concurrent = sample.get("max_concurrent_reloads")
        workers = sample.get("workers_per_table")
        peak_copy = sample.get("peak_copy_connections")
        peak_copy_tables = sample.get("peak_copy_tables")
        if all(
            type(value) is int and value > 0
            for value in (tables_requested, max_concurrent, workers)
        ):
            allowed_tables = min(tables_requested, max_concurrent)
            allowed_copy = min(tables_requested, max_concurrent) * workers
            if type(peak_copy) is not int:
                failures.append(f"{identity}: peak COPY connection count is missing")
            elif peak_copy > allowed_copy:
                failures.append(
                    f"{identity}: peak COPY connections {peak_copy} exceeded allowed {allowed_copy}"
                )
            elif allowed_copy > 1 and peak_copy < 2:
                failures.append(
                    f"{identity}: no parallel COPY activity observed "
                    f"(peak={peak_copy}, allowed={allowed_copy})"
                )
            if type(peak_copy_tables) is not int or peak_copy_tables < 0:
                failures.append(f"{identity}: peak COPY table count is missing or invalid")
            elif peak_copy_tables > allowed_tables:
                failures.append(
                    f"{identity}: peak COPY tables {peak_copy_tables} exceeded allowed "
                    f"{allowed_tables}"
                )
            elif (
                sample.get("matrix") == "tables"
                and allowed_tables > 1
                and peak_copy_tables < 2
            ):
                failures.append(
                    f"{identity}: table-cap concurrency was not observed "
                    f"(peak_tables={peak_copy_tables}, allowed_tables={allowed_tables})"
                )
            if (
                type(peak_copy) is int
                and type(peak_copy_tables) is int
                and peak_copy_tables > peak_copy
            ):
                failures.append(
                    f"{identity}: peak COPY tables {peak_copy_tables} exceeded peak COPY "
                    f"connections {peak_copy}"
                )
    return list(dict.fromkeys(failures))


def aggregate_reload_samples(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group measured samples, compute medians, and attach serial/baseline speedups."""
    measured = [sample for sample in samples if not sample["warmup"]]
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for sample in measured:
        key = tuple(sample[column] for column in RELOAD_GROUP_COLUMNS)
        grouped.setdefault(key, []).append(sample)

    aggregates: list[dict[str, Any]] = []
    for key, group in sorted(grouped.items(), key=lambda item: item[0]):
        successful = [sample for sample in group if sample["status"] == "success"]
        aggregate = dict(zip(RELOAD_GROUP_COLUMNS, key, strict=True))
        aggregate["sample_count"] = len(group)
        aggregate["failed_count"] = len(group) - len(successful)
        for column in RELOAD_MEDIAN_COLUMNS:
            aggregate[f"median_{column}"] = median_present(successful, column)
        aggregate["speedup_vs_serial"] = None
        aggregate["speedup_vs_baseline"] = None
        aggregates.append(aggregate)

    def index_key(aggregate: dict[str, Any]) -> tuple[Any, ...]:
        return (
            aggregate["implementation"],
            aggregate["fixture"],
            aggregate["matrix"],
            aggregate["tables_requested"],
            aggregate["max_concurrent_reloads"],
            aggregate["workers_per_table"],
            aggregate["chunk_rows"],
        )

    index = {index_key(aggregate): aggregate for aggregate in aggregates}
    for aggregate in aggregates:
        duration = aggregate["median_export_seconds"]
        if duration in (None, 0):
            continue
        serial_key = None
        if aggregate["matrix"] == "workers":
            serial_key = (
                aggregate["implementation"],
                aggregate["fixture"],
                aggregate["matrix"],
                aggregate["tables_requested"],
                aggregate["max_concurrent_reloads"],
                1,
                aggregate["chunk_rows"],
            )
        elif aggregate["matrix"] == "tables":
            serial_key = (
                aggregate["implementation"],
                aggregate["fixture"],
                aggregate["matrix"],
                aggregate["tables_requested"],
                1,
                aggregate["workers_per_table"],
                aggregate["chunk_rows"],
            )
        serial = index.get(serial_key) if serial_key is not None else None
        baseline = index.get(
            (
                "baseline",
                aggregate["fixture"],
                aggregate["matrix"],
                aggregate["tables_requested"],
                aggregate["max_concurrent_reloads"],
                1,
                aggregate["chunk_rows"],
            )
        )
        if serial and serial["median_export_seconds"] is not None:
            aggregate["speedup_vs_serial"] = serial["median_export_seconds"] / duration
        if baseline and baseline["median_export_seconds"] is not None:
            aggregate["speedup_vs_baseline"] = baseline["median_export_seconds"] / duration
    return aggregates


def reload_finish_run(args: argparse.Namespace) -> int:
    """Finalize raw reload samples into JSON and CSV matrix summaries."""
    run_dir = Path(args.run_dir)
    metadata_path = run_dir / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    try:
        samples = read_reload_samples(run_dir / "reload-samples.csv")
    except (OSError, ValueError) as error:
        print(f"cannot read reload samples: {error}", file=sys.stderr)
        return 2
    failures = [reason for reason in args.failure_reason if reason]
    metadata_failures = reload_metadata_failures(metadata)
    failures.extend(metadata_failures)
    failures.extend(reload_sample_failures(samples))
    if not metadata_failures:
        failures.extend(reload_matrix_failures(metadata, samples))
    failures = list(dict.fromkeys(failures))
    if not samples:
        failures.append("benchmark produced no samples")
    aggregates = aggregate_reload_samples(samples)
    status = "success" if not failures else "failed"
    measured = [sample for sample in samples if not sample["warmup"]]
    summary = {
        "schema_version": SCHEMA_VERSION,
        "kind": "reload_matrix",
        "run_id": metadata["run_id"],
        "status": status,
        "failure_reasons": failures,
        "comparable": status == "success",
        "matrix": metadata["matrix"],
        "fixtures": metadata["fixtures"],
        "workload": metadata["workload"],
        "effective_settings": metadata["effective_settings"],
        "binaries": metadata["binaries"],
        "source": metadata["source"],
        "host": metadata["host"],
        "toolchain": metadata["toolchain"],
        "sample_count": len(measured),
        "warmup_count": len(samples) - len(measured),
        "aggregates": aggregates,
        "artifacts": {
            "metadata": "metadata.json",
            "samples": "reload-samples.csv",
            "summary_csv": "summary.csv",
            "extractor_log": "extractor.log",
            "transformer_log": "transformer.log",
        },
    }
    csv_columns = (
        *RELOAD_GROUP_COLUMNS,
        "sample_count",
        "failed_count",
        *(f"median_{column}" for column in RELOAD_MEDIAN_COLUMNS),
        "speedup_vs_serial",
        "speedup_vs_baseline",
    )
    with (run_dir / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(aggregates)
    metadata["status"] = status
    metadata["ended_at"] = utc_now()
    write_json(metadata_path, metadata)
    write_json(run_dir / "summary.json", summary)
    print(f"reload benchmark: status={status} measured={len(measured)} bundle={run_dir}")
    for aggregate in aggregates:
        print(
            f"  {aggregate['implementation']}/{aggregate['fixture']}/{aggregate['matrix']} "
            f"tables_requested={aggregate['tables_requested']} "
            f"max_concurrent_reloads={aggregate['max_concurrent_reloads']} "
            f"workers={aggregate['workers_per_table']} "
            f"chunk={aggregate['chunk_rows']}: "
            f"median F→H={format_number(aggregate['median_export_seconds'], 's')} "
            f"rows/s={format_number(aggregate['median_rows_per_second'])} "
            f"serial×={format_number(aggregate['speedup_vs_serial'])} "
            f"baseline×={format_number(aggregate['speedup_vs_baseline'])}"
        )
    if failures:
        print("reload benchmark failures:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
    return 0 if status == "success" else 1


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start")
    start.add_argument("--run-dir", required=True)
    start.add_argument("--mode", choices=("measure", "cpu", "heap", "async"), required=True)
    start.add_argument("--target", choices=("extractor", "transformer"))
    start.add_argument("--profile", required=True)
    start.add_argument("--scenario", required=True)
    start.add_argument("--duration", type=int, required=True)
    start.add_argument("--clients", type=int, required=True)
    start.add_argument("--max-fill", required=True)
    start.add_argument("--max-rows", type=int, required=True)
    start.add_argument("--max-bytes", type=int, required=True)
    start.add_argument("--max-inflight", type=int, required=True)
    start.add_argument("--poll-interval", required=True)
    start.add_argument("--sample-interval", type=float, required=True)
    start.set_defaults(function=start_run)

    sample = commands.add_parser("sample")
    sample.add_argument("--extractor-pid", type=int, required=True)
    sample.add_argument("--transformer-pid", type=int, required=True)
    sample.add_argument("--extractor-url", required=True)
    sample.add_argument("--transformer-url", required=True)
    sample.add_argument("--output", required=True)
    sample.add_argument("--interval", type=float, default=1.0)
    sample.set_defaults(function=sample_run)

    finish = commands.add_parser("finish")
    finish.add_argument("--run-dir", required=True)
    finish.add_argument("--elapsed", type=float, required=True)
    finish.add_argument("--rows-start", type=float, required=True)
    finish.add_argument("--rows-end", type=float, required=True)
    finish.add_argument("--flush-sum-start", type=float, required=True)
    finish.add_argument("--flush-sum-end", type=float, required=True)
    finish.add_argument("--flush-count-start", type=float, required=True)
    finish.add_argument("--flush-count-end", type=float, required=True)
    finish.add_argument("--spill-start", type=float, required=True)
    finish.add_argument("--spill-end", type=float, required=True)
    finish.add_argument("--failure-reason", action="append", default=[])
    finish.set_defaults(function=finish_run)

    fail = commands.add_parser("fail")
    fail.add_argument("--run-dir", required=True)
    fail.add_argument("--reason", required=True)
    fail.set_defaults(function=fail_run)

    resolve = commands.add_parser("resolve-bench")
    resolve.add_argument("--cargo-json", required=True)
    resolve.add_argument("--bench", required=True)
    resolve.set_defaults(function=resolve_bench)

    complete = commands.add_parser("complete-artifact")
    complete.add_argument("--run-dir", required=True)
    complete.add_argument("--artifact", required=True)
    complete.set_defaults(function=complete_artifact)

    compare = commands.add_parser("compare")
    compare.add_argument("baseline")
    compare.add_argument("candidate")
    compare.add_argument("--allow-mismatch", action="store_true")
    compare.set_defaults(function=compare_runs)

    reload_start = commands.add_parser("reload-start")
    reload_start.add_argument("--run-dir", required=True)
    reload_start.add_argument(
        "--matrix", choices=("workers", "tables", "chunks", "all"), required=True
    )
    reload_start.add_argument("--fixtures", required=True)
    reload_start.add_argument("--narrow-rows", type=int, required=True)
    reload_start.add_argument("--narrow-payload-bytes", type=int, required=True)
    reload_start.add_argument("--wide-rows", type=int, required=True)
    reload_start.add_argument("--wide-payload-bytes", type=int, required=True)
    reload_start.add_argument("--workers", required=True)
    reload_start.add_argument("--tables", required=True)
    reload_start.add_argument("--chunks", required=True)
    reload_start.add_argument("--base-workers", type=int, required=True)
    reload_start.add_argument("--base-tables", type=int, required=True)
    reload_start.add_argument("--base-chunk-rows", type=int, required=True)
    reload_start.add_argument("--warmups", type=int, required=True)
    reload_start.add_argument("--samples", type=int, required=True)
    reload_start.add_argument("--sample-interval", type=float, required=True)
    reload_start.add_argument("--timeout-seconds", type=int, required=True)
    reload_start.add_argument("--max-inflight-bytes", type=int, required=True)
    reload_start.add_argument("--max-bytes", type=int, required=True)
    reload_start.add_argument("--max-rows", type=int, required=True)
    reload_start.add_argument("--max-fill", required=True)
    reload_start.add_argument("--heartbeat-idle-after", required=True)
    reload_start.add_argument("--effective-max-concurrent-reloads", required=True)
    reload_start.add_argument("--effective-workers-per-table", required=True)
    reload_start.add_argument("--effective-chunk-rows", required=True)
    reload_start.add_argument("--candidate-bin", required=True)
    reload_start.add_argument("--candidate-sha256", required=True)
    reload_start.add_argument("--baseline-bin", default="")
    reload_start.add_argument("--baseline-sha256", default="")
    reload_start.set_defaults(function=reload_start_run)

    reload_finish = commands.add_parser("reload-finish")
    reload_finish.add_argument("--run-dir", required=True)
    reload_finish.add_argument("--failure-reason", action="append", default=[])
    reload_finish.set_defaults(function=reload_finish_run)
    return root


def main() -> int:
    args = parser().parse_args()
    return int(args.function(args))


if __name__ == "__main__":
    raise SystemExit(main())
