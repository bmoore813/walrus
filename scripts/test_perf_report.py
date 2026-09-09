#!/usr/bin/env python3
"""Unit tests for the dependency-free local performance report helper."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("perf_report.py")
SPEC = importlib.util.spec_from_file_location("perf_report", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
perf_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(perf_report)


class PerfReportTest(unittest.TestCase):
    @staticmethod
    def reload_sample(**overrides: object) -> dict[str, object]:
        sample: dict[str, object] = {
            "implementation": "candidate",
            "fixture": "narrow",
            "matrix": "workers",
            "tables_requested": 1,
            "max_concurrent_reloads": 1,
            "workers_per_table": 1,
            "chunk_rows": 10_000,
            "iteration": 1,
            "warmup": False,
            "status": "success",
            "failure_reason": "",
            "rows_expected": 10_000,
            "rows_exported": 10_000,
            "source_bytes": 1_048_576,
            "export_seconds": 10.0,
            "publish_seconds": 11.0,
            "rows_per_second": 1_000.0,
            "source_mib_per_second": 0.1,
            "extractor_cpu_seconds": 5.0,
            "extractor_peak_rss_bytes": 100,
            "transformer_peak_rss_bytes": 200,
            "source_blks_read": 10,
            "source_blks_hit": 20,
            "peak_copy_connections": 1,
            "peak_copy_tables": 1,
            "peak_wal_lag_bytes": 30,
            "chunk_files": 1,
            "slot_count_min": 1,
            "slot_count_max": 1,
            "walsenders_min": 1,
            "walsenders_max": 1,
            "mirror_diff_rows": 0,
        }
        sample.update(overrides)
        return sample

    @staticmethod
    def reload_metadata(**overrides: object) -> dict[str, object]:
        digest = "a" * 64
        metadata: dict[str, object] = {
            "schema_version": 2,
            "kind": "reload_matrix",
            "run_id": "reload-run",
            "status": "running",
            "matrix": "workers",
            "fixtures": ["narrow"],
            "workload": {
                "narrow_rows": 10_000,
                "narrow_payload_bytes": 128,
                "wide_rows": 500,
                "wide_payload_bytes": 4096,
                "workers_per_table": [1],
                "max_concurrent_reloads": [1],
                "chunk_rows": [10_000],
                "base_workers_per_table": 1,
                "base_concurrent_tables": 1,
                "base_chunk_rows": 10_000,
                "warmups": 0,
                "samples": 1,
                "sample_interval_seconds": 0.1,
                "timeout_seconds": 60,
                "max_inflight_bytes": 536_870_912,
                "max_bytes": 134_217_728,
                "max_rows": 100_000,
                "max_fill": "5s",
                "heartbeat_idle_after": "1s",
            },
            "effective_settings": {
                "max_concurrent_reloads": [1],
                "reload_workers_per_table": [1],
                "reload_chunk_rows": [10_000],
                "max_inflight_bytes": 536_870_912,
                "max_bytes": 134_217_728,
                "max_rows": 100_000,
                "max_fill": "5s",
                "heartbeat_idle_after": "1s",
            },
            "binaries": {
                "candidate": {"path": "/tmp/candidate", "sha256": digest},
                "baseline": None,
            },
            "source": {},
            "host": {},
            "toolchain": {},
        }
        metadata.update(overrides)
        return metadata

    @classmethod
    def complete_reload_samples(cls, metadata: dict[str, object]) -> list[dict[str, object]]:
        samples: list[dict[str, object]] = []
        workload = metadata["workload"]
        assert isinstance(workload, dict)
        fixture_rows = {
            "narrow": workload["narrow_rows"],
            "wide": workload["wide_rows"],
        }
        for config in perf_report.expected_reload_configs(metadata):
            values = dict(zip(perf_report.RELOAD_GROUP_COLUMNS, config, strict=True))
            expected_rows = fixture_rows[values["fixture"]] * values["tables_requested"]
            for warmup, count in (
                (True, workload["warmups"]),
                (False, workload["samples"]),
            ):
                for iteration in range(1, count + 1):
                    samples.append(
                        cls.reload_sample(
                            **values,
                            warmup=warmup,
                            iteration=iteration,
                            rows_expected=expected_rows,
                            rows_exported=expected_rows,
                        )
                    )
        return samples

    def test_cpu_time_parses_macos_and_linux_ps_shapes(self) -> None:
        self.assertEqual(perf_report.parse_cpu_time("01:02"), 62)
        self.assertEqual(perf_report.parse_cpu_time("02:03:04"), 7384)
        self.assertEqual(perf_report.parse_cpu_time("1-02:03:04.5"), 93784.5)

    def test_prometheus_sums_label_sets_and_ignores_prefixes(self) -> None:
        text = """
# TYPE walrus_transformer_raw_append_lag_bytes gauge
walrus_transformer_raw_append_lag_bytes{table="a"} 3
walrus_transformer_raw_append_lag_bytes{table="b"} 4.5
walrus_transformer_raw_append_lag_bytes_extra 100
"""
        self.assertEqual(
            perf_report.parse_prometheus(text, "walrus_transformer_raw_append_lag_bytes"), 7.5
        )

    def test_finish_writes_efficiency_and_peaks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            metadata = {
                "schema_version": 2,
                "run_id": "run",
                "status": "running",
                "mode": "measure",
                "diagnostic_target": None,
                "build_profile": "release",
                "comparable": True,
                "scenario": "mixed",
                "workload": {},
                "host": {},
                "toolchain": {},
            }
            perf_report.write_json(run_dir / "metadata.json", metadata)
            with (run_dir / "samples.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=perf_report.CSV_COLUMNS)
                writer.writeheader()
                base = {column: 0 for column in perf_report.CSV_COLUMNS}
                writer.writerow(base | {"extractor_cpu_seconds": 10, "transformer_cpu_seconds": 20})
                writer.writerow(
                    base
                    | {
                        "t_seconds": 5,
                        "extractor_cpu_seconds": 12,
                        "transformer_cpu_seconds": 24,
                        "extractor_rss_bytes": 100,
                        "transformer_rss_bytes": 200,
                        "raw_append_lag_bytes": 300,
                    }
                )
            args = argparse.Namespace(
                run_dir=str(run_dir),
                elapsed=5,
                rows_start=100,
                rows_end=2100,
                flush_sum_start=1,
                flush_sum_end=3,
                flush_count_start=2,
                flush_count_end=4,
                spill_start=0,
                spill_end=2,
                failure_reason=[],
            )
            self.assertEqual(perf_report.finish_run(args), 0)
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["throughput"]["rows_per_second"], 400)
            self.assertEqual(summary["cpu"]["total_seconds_per_1000_rows"], 3)
            self.assertEqual(summary["memory"]["transformer_peak_rss_bytes"], 200)
            self.assertEqual(summary["pipeline"]["raw_append_lag_peak_bytes"], 300)

    def test_comparison_rejects_workload_and_host_drift(self) -> None:
        base = {
            "status": "success",
            "comparable": True,
            "mode": "measure",
            "diagnostic_target": None,
            "build_profile": "release",
            "scenario": "mixed",
            "workload": {
                "duration_seconds": 30,
                "clients": 4,
                "max_fill": "2s",
                "max_rows": 5000,
                "max_bytes": 2_000_000,
                "max_inflight_bytes": 4_000_000,
                "poll_interval": "1s",
                "sample_interval_seconds": 1,
            },
            "host": {"os": "Darwin", "architecture": "arm64", "cpu_model": "M2"},
            "toolchain": {"rustc": "rustc 1.95.0"},
        }
        candidate = json.loads(json.dumps(base))
        candidate["workload"]["clients"] = 8
        candidate["host"]["cpu_model"] = "M3"
        self.assertEqual(
            perf_report.comparison_mismatches(base, candidate),
            ["workload.clients", "host.cpu_model"],
        )

    def test_resolve_bench_requires_one_matching_executable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            messages = Path(temporary) / "messages.json"
            messages.write_text(
                json.dumps(
                    {
                        "reason": "compiler-artifact",
                        "target": {"name": "decode", "kind": ["bench"]},
                        "executable": "/tmp/decode-123",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            args = argparse.Namespace(cargo_json=str(messages), bench="decode")
            self.assertEqual(perf_report.resolve_bench(args), 0)

    def test_reload_start_records_effective_settings_and_binary_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary) / "run"
            digest = "a" * 64
            args = perf_report.parser().parse_args(
                [
                    "reload-start",
                    "--run-dir",
                    str(run_dir),
                    "--matrix",
                    "workers",
                    "--fixtures",
                    "narrow",
                    "--narrow-rows",
                    "10000",
                    "--narrow-payload-bytes",
                    "128",
                    "--wide-rows",
                    "500",
                    "--wide-payload-bytes",
                    "4096",
                    "--workers",
                    "1,2",
                    "--tables",
                    "1,2,4",
                    "--chunks",
                    "100,1000",
                    "--base-workers",
                    "4",
                    "--base-tables",
                    "2",
                    "--base-chunk-rows",
                    "10000",
                    "--warmups",
                    "1",
                    "--samples",
                    "2",
                    "--sample-interval",
                    "0.1",
                    "--timeout-seconds",
                    "60",
                    "--max-inflight-bytes",
                    "536870912",
                    "--max-bytes",
                    "134217728",
                    "--max-rows",
                    "100000",
                    "--max-fill",
                    "5s",
                    "--heartbeat-idle-after",
                    "1s",
                    "--effective-max-concurrent-reloads",
                    "2",
                    "--effective-workers-per-table",
                    "1,2,4",
                    "--effective-chunk-rows",
                    "10000",
                    "--candidate-bin",
                    "/tmp/candidate",
                    "--candidate-sha256",
                    digest.upper(),
                ]
            )
            self.assertEqual(args.function(args), 0)
            metadata = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(
                metadata["effective_settings"],
                {
                    "max_concurrent_reloads": [2],
                    "reload_workers_per_table": [1, 2, 4],
                    "reload_chunk_rows": [10_000],
                    "max_inflight_bytes": 536_870_912,
                    "max_bytes": 134_217_728,
                    "max_rows": 100_000,
                    "max_fill": "5s",
                    "heartbeat_idle_after": "1s",
                },
            )
            self.assertEqual(metadata["workload"]["max_inflight_bytes"], 536_870_912)
            self.assertEqual(metadata["binaries"]["candidate"]["sha256"], digest)
            self.assertIsNone(metadata["binaries"]["baseline"])
            with (run_dir / "reload-samples.csv").open(newline="", encoding="utf-8") as handle:
                self.assertEqual(
                    tuple(csv.DictReader(handle).fieldnames or ()),
                    perf_report.RELOAD_SAMPLE_COLUMNS,
                )

    def test_reload_metadata_requires_serial_axes_and_exact_effective_settings(self) -> None:
        metadata = self.reload_metadata()
        metadata["matrix"] = "all"
        workload = metadata["workload"]
        assert isinstance(workload, dict)
        workload["workers_per_table"] = [2, 4]
        workload["max_concurrent_reloads"] = [2, 4]
        metadata["effective_settings"] = {
            "max_concurrent_reloads": [1, 2, 4],
            "reload_workers_per_table": [1, 2, 4],
            "reload_chunk_rows": [10_000],
            "max_inflight_bytes": 536_870_912,
            "max_bytes": 134_217_728,
            "max_rows": 100_000,
            "max_fill": "5s",
            "heartbeat_idle_after": "1s",
        }
        failures = perf_report.reload_metadata_failures(metadata)
        self.assertIn(
            "workers matrix requires workers_per_table=1 serial baseline", failures
        )
        self.assertIn(
            "tables matrix requires max_concurrent_reloads=1 serial baseline", failures
        )

        metadata = self.reload_metadata()
        metadata["effective_settings"] = {
            "max_concurrent_reloads": [1, 2],
            "reload_workers_per_table": [1],
            "reload_chunk_rows": [10_000],
            "max_inflight_bytes": 536_870_912,
            "max_bytes": 134_217_728,
            "max_rows": 100_000,
            "max_fill": "5s",
            "heartbeat_idle_after": "1s",
        }
        self.assertTrue(
            any(
                "effective_settings.max_concurrent_reloads" in failure
                for failure in perf_report.reload_metadata_failures(metadata)
            )
        )

    def test_reload_metadata_validates_sha256_for_each_configured_binary(self) -> None:
        metadata = self.reload_metadata()
        binaries = metadata["binaries"]
        assert isinstance(binaries, dict)
        binaries["candidate"] = {"path": "/tmp/candidate", "sha256": "not-a-digest"}
        binaries["baseline"] = {"path": "/tmp/baseline", "sha256": "b" * 63}
        failures = perf_report.reload_metadata_failures(metadata)
        self.assertIn("metadata binaries.candidate.sha256 is invalid", failures)
        self.assertIn("metadata binaries.baseline.sha256 is invalid", failures)

    def test_reload_table_matrix_has_fixed_workload_and_distinct_caps(self) -> None:
        metadata = self.reload_metadata()
        metadata["matrix"] = "tables"
        workload = metadata["workload"]
        assert isinstance(workload, dict)
        workload["max_concurrent_reloads"] = [1, 2, 4]
        workload["base_workers_per_table"] = 4
        metadata["effective_settings"] = {
            "max_concurrent_reloads": [1, 2, 4],
            "reload_workers_per_table": [4],
            "reload_chunk_rows": [10_000],
            "max_inflight_bytes": 536_870_912,
            "max_bytes": 134_217_728,
            "max_rows": 100_000,
            "max_fill": "5s",
            "heartbeat_idle_after": "1s",
        }
        configs = perf_report.expected_reload_configs(metadata)
        self.assertEqual(len(configs), 3)
        by_cap = {config[4]: config for config in configs}
        self.assertEqual(set(by_cap), {1, 2, 4})
        self.assertEqual({config[3] for config in configs}, {4})

        samples = [
            self.reload_sample(
                matrix="tables",
                tables_requested=4,
                max_concurrent_reloads=1,
                workers_per_table=4,
                rows_expected=40_000,
                rows_exported=40_000,
                export_seconds=20,
            ),
            self.reload_sample(
                matrix="tables",
                tables_requested=4,
                max_concurrent_reloads=4,
                workers_per_table=4,
                rows_expected=40_000,
                rows_exported=40_000,
                export_seconds=5,
            ),
        ]
        aggregates = perf_report.aggregate_reload_samples(samples)
        cap_four = next(row for row in aggregates if row["max_concurrent_reloads"] == 4)
        self.assertEqual(cap_four["speedup_vs_serial"], 4)

    def test_reload_matrix_completeness_detects_missing_duplicate_and_unexpected_rows(self) -> None:
        metadata = self.reload_metadata()
        workload = metadata["workload"]
        assert isinstance(workload, dict)
        workload["workers_per_table"] = [1, 4]
        workload["base_workers_per_table"] = 4
        workload["warmups"] = 1
        workload["samples"] = 2
        metadata["effective_settings"] = {
            "max_concurrent_reloads": [1],
            "reload_workers_per_table": [1, 4],
            "reload_chunk_rows": [10_000],
            "max_inflight_bytes": 536_870_912,
            "max_bytes": 134_217_728,
            "max_rows": 100_000,
            "max_fill": "5s",
            "heartbeat_idle_after": "1s",
        }
        samples = self.complete_reload_samples(metadata)
        self.assertEqual(perf_report.reload_matrix_failures(metadata, samples), [])

        missing = samples[:-1]
        self.assertTrue(
            any(
                failure.startswith("missing measured iteration")
                for failure in perf_report.reload_matrix_failures(metadata, missing)
            )
        )

        duplicate = samples + [dict(samples[0])]
        self.assertTrue(
            any(
                failure.startswith("duplicate warmup iteration")
                for failure in perf_report.reload_matrix_failures(metadata, duplicate)
            )
        )

        unexpected = samples + [self.reload_sample(workers_per_table=8)]
        self.assertTrue(
            any(
                failure.startswith("unexpected benchmark configuration")
                for failure in perf_report.reload_matrix_failures(metadata, unexpected)
            )
        )

    def test_reload_matrix_completeness_requires_every_expected_configuration(self) -> None:
        metadata = self.reload_metadata()
        workload = metadata["workload"]
        assert isinstance(workload, dict)
        workload["workers_per_table"] = [1, 4]
        metadata["effective_settings"] = {
            "max_concurrent_reloads": [1],
            "reload_workers_per_table": [1, 4],
            "reload_chunk_rows": [10_000],
            "max_inflight_bytes": 536_870_912,
            "max_bytes": 134_217_728,
            "max_rows": 100_000,
            "max_fill": "5s",
            "heartbeat_idle_after": "1s",
        }
        failures = perf_report.reload_matrix_failures(
            metadata, [self.reload_sample(workers_per_table=4)]
        )
        self.assertTrue(
            any(
                failure.startswith("missing benchmark configuration")
                and "workers=1" in failure
                for failure in failures
            )
        )

    def test_reload_matrix_validates_declared_row_workload(self) -> None:
        metadata = self.reload_metadata()
        sample = self.reload_sample(rows_expected=1, rows_exported=1)
        failures = perf_report.reload_matrix_failures(metadata, [sample])
        self.assertTrue(any("rows_expected is 1, expected 10000" in item for item in failures))

    def test_reload_aggregate_uses_measured_medians_and_computes_speedups(self) -> None:
        samples = [
            self.reload_sample(warmup=True, export_seconds=1000, rows_per_second=10),
            self.reload_sample(iteration=1, export_seconds=10, rows_per_second=1000),
            self.reload_sample(iteration=2, export_seconds=14, rows_per_second=800),
            self.reload_sample(
                workers_per_table=4,
                iteration=1,
                export_seconds=4,
                rows_per_second=2500,
                peak_copy_connections=4,
            ),
            self.reload_sample(
                workers_per_table=4,
                iteration=2,
                export_seconds=6,
                rows_per_second=2000,
                peak_copy_connections=4,
            ),
            self.reload_sample(
                implementation="baseline",
                iteration=1,
                export_seconds=20,
                rows_per_second=500,
            ),
        ]
        grouped = perf_report.aggregate_reload_samples(samples)
        parallel = next(
            row
            for row in grouped
            if row["implementation"] == "candidate" and row["workers_per_table"] == 4
        )
        self.assertEqual(parallel["sample_count"], 2)
        self.assertEqual(parallel["median_export_seconds"], 5)
        self.assertEqual(parallel["median_rows_per_second"], 2250)
        self.assertEqual(parallel["median_peak_copy_tables"], 1)
        self.assertEqual(parallel["speedup_vs_serial"], 12 / 5)
        self.assertEqual(parallel["speedup_vs_baseline"], 20 / 5)

    def test_reload_aggregate_does_not_cross_matrix_baselines(self) -> None:
        samples = [
            self.reload_sample(matrix="workers", workers_per_table=1, export_seconds=12),
            self.reload_sample(matrix="workers", workers_per_table=4, export_seconds=3),
            self.reload_sample(matrix="tables", workers_per_table=1, export_seconds=99),
        ]
        aggregates = perf_report.aggregate_reload_samples(samples)
        parallel = next(
            row
            for row in aggregates
            if row["matrix"] == "workers" and row["workers_per_table"] == 4
        )
        self.assertEqual(parallel["speedup_vs_serial"], 4)

    def test_reload_sample_reader_rejects_schema_and_identity_corruption(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "samples.csv"
            path.write_text("implementation,fixture\ncandidate,narrow\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "columns differ"):
                perf_report.read_reload_samples(path)

            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=perf_report.RELOAD_SAMPLE_COLUMNS)
                writer.writeheader()
                writer.writerow(self.reload_sample(iteration=0))
            with self.assertRaisesRegex(ValueError, "iteration must be a positive integer"):
                perf_report.read_reload_samples(path)

    def test_reload_failures_enforce_rows_mirror_and_one_slot(self) -> None:
        sample = self.reload_sample(
            rows_exported=9999,
            mirror_diff_rows=2,
            slot_count_max=2,
            walsenders_min=0,
        )
        failures = perf_report.reload_sample_failures([sample])
        self.assertTrue(any("exported 9999 rows" in failure for failure in failures))
        self.assertTrue(any("mirror differs by 2 rows" in failure for failure in failures))
        self.assertTrue(any("replication slot count left 1" in failure for failure in failures))
        self.assertTrue(any("active walsender count left 1" in failure for failure in failures))

    def test_reload_failures_enforce_copy_cap_and_parallel_evidence(self) -> None:
        no_parallelism = self.reload_sample(
            tables_requested=4,
            max_concurrent_reloads=2,
            workers_per_table=4,
            rows_expected=40_000,
            rows_exported=40_000,
            peak_copy_connections=1,
        )
        over_cap = self.reload_sample(
            tables_requested=2,
            max_concurrent_reloads=1,
            workers_per_table=2,
            rows_expected=20_000,
            rows_exported=20_000,
            peak_copy_connections=3,
        )
        no_table_parallelism = self.reload_sample(
            matrix="tables",
            tables_requested=4,
            max_concurrent_reloads=2,
            workers_per_table=4,
            rows_expected=40_000,
            rows_exported=40_000,
            peak_copy_connections=4,
            peak_copy_tables=1,
        )
        too_many_tables = self.reload_sample(
            matrix="tables",
            tables_requested=4,
            max_concurrent_reloads=2,
            workers_per_table=4,
            rows_expected=40_000,
            rows_exported=40_000,
            peak_copy_connections=3,
            peak_copy_tables=3,
        )
        missing_tables = self.reload_sample(peak_copy_tables=None)
        failures = perf_report.reload_sample_failures(
            [
                no_parallelism,
                over_cap,
                no_table_parallelism,
                too_many_tables,
                missing_tables,
            ]
        )
        self.assertTrue(any("no parallel COPY activity observed" in item for item in failures))
        self.assertTrue(
            any("peak COPY connections 3 exceeded allowed 2" in item for item in failures)
        )
        self.assertTrue(any("table-cap concurrency was not observed" in item for item in failures))
        self.assertTrue(any("peak COPY tables 3 exceeded allowed 2" in item for item in failures))
        self.assertTrue(any("peak COPY table count is missing" in item for item in failures))

    def test_reload_finish_writes_json_and_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            perf_report.write_json(
                run_dir / "metadata.json",
                self.reload_metadata(),
            )
            with (run_dir / "reload-samples.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=perf_report.RELOAD_SAMPLE_COLUMNS)
                writer.writeheader()
                writer.writerow(self.reload_sample())
            args = argparse.Namespace(run_dir=str(run_dir), failure_reason=[])
            self.assertEqual(perf_report.reload_finish_run(args), 0)
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "success")
            self.assertEqual(summary["sample_count"], 1)
            self.assertEqual(summary["aggregates"][0]["median_export_seconds"], 10)
            self.assertEqual(summary["effective_settings"]["max_inflight_bytes"], 536_870_912)
            self.assertTrue((run_dir / "summary.csv").is_file())
            with (run_dir / "summary.csv").open(newline="", encoding="utf-8") as handle:
                columns = csv.DictReader(handle).fieldnames or []
            self.assertIn("tables_requested", columns)
            self.assertIn("max_concurrent_reloads", columns)
            self.assertNotIn("tables", columns)

    def test_reload_finish_fails_an_incomplete_declared_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_dir = Path(temporary)
            metadata = self.reload_metadata()
            workload = metadata["workload"]
            assert isinstance(workload, dict)
            workload["workers_per_table"] = [1, 4]
            metadata["effective_settings"] = {
                "max_concurrent_reloads": [1],
                "reload_workers_per_table": [1, 4],
                "reload_chunk_rows": [10_000],
                "max_inflight_bytes": 536_870_912,
                "max_bytes": 134_217_728,
                "max_rows": 100_000,
                "max_fill": "5s",
                "heartbeat_idle_after": "1s",
            }
            perf_report.write_json(run_dir / "metadata.json", metadata)
            with (run_dir / "reload-samples.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=perf_report.RELOAD_SAMPLE_COLUMNS)
                writer.writeheader()
                writer.writerow(self.reload_sample())
            args = argparse.Namespace(run_dir=str(run_dir), failure_reason=[])
            self.assertEqual(perf_report.reload_finish_run(args), 1)
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "failed")
            self.assertTrue(
                any(
                    reason.startswith("missing benchmark configuration")
                    for reason in summary["failure_reasons"]
                )
            )


if __name__ == "__main__":
    unittest.main()
