from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import Base, ThetaDataOptionHistory
from parquet_analytics.config import ParquetConfig, ParquetConfigurationError
from parquet_analytics.schema import OUTPUT_SCHEMA, source_row_to_dict
from parquet_analytics.service import (
    BACKUP_DIRECTORY,
    MAINTENANCE_MARKER_NAME,
    MANIFEST_NAME,
    REBUILD_CHECKPOINT_NAME,
    PartitionKey,
    ParquetLayerService,
    ParquetSchemaMismatch,
    encode_partition_value,
    next_scheduled_run,
)


def add_history_row(session, **overrides):
    values = {
        "symbol": "SPX",
        "expiry": date(2026, 9, 18),
        "strike": 6500.0,
        "right": "C",
        "interval": "1m",
        "timestamp": datetime(2026, 8, 10, 10, 0),
        "open": 10.0,
        "high": 11.0,
        "low": None,
        "close": 10.5,
        "volume": 100,
        "trade_count": 4,
        "vwap": 10.4,
        "bid": 10.2,
        "ask": 10.8,
        "bid_size": 3,
        "ask_size": 5,
        "open_interest": 500,
        "bid_implied_vol": 0.19,
        "ask_implied_vol": 0.21,
        "implied_volatility": 0.20,
        "iv_error": 0.001,
        "underlying_price": 6490.0,
        "delta": 0.5,
        "theta": -0.1,
        "vega": 0.2,
        "rho": 0.05,
        "epsilon": 0.01,
        "lambda_": 2.5,
        "dte": 39,
        "collection_batch": "batch-a",
        "created_at": datetime(2026, 8, 10, 10, 1),
        "updated_at": datetime(2026, 8, 10, 10, 1),
    }
    values.update(overrides)
    row = ThetaDataOptionHistory(**values)
    session.add(row)
    session.commit()
    return row


class ParquetLayerTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name) / "parquet"
        self.database_path = Path(self.temporary_directory.name) / "source.sqlite"
        self.engine = create_engine(f"sqlite:///{self.database_path}")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.config = ParquetConfig(
            enabled=True,
            root=self.root,
            target_file_size_bytes=1024 * 1024,
            refresh_batch_size=2,
        )
        self.service = ParquetLayerService(self.engine, self.Session, self.config)

    def tearDown(self):
        self.engine.dispose()
        self.temporary_directory.cleanup()

    def test_rebuild_writes_stable_hive_dataset_with_source_types(self):
        session = self.Session()
        try:
            add_history_row(session)
            add_history_row(
                session,
                symbol="SPX W/TEST",
                interval="30m",
                strike=6550.0,
                timestamp=datetime(2026, 9, 1, 0, 0),
                updated_at=datetime(2026, 9, 1, 0, 1),
            )
        finally:
            session.close()

        result = self.service.rebuild()

        self.assertEqual(result["rows"], 2)
        encoded = encode_partition_value("SPX W/TEST")
        self.assertTrue(
            (
                self.root
                / f"symbol={encoded}"
                / "interval=30m"
                / "year=2026"
                / "month=09"
                / "part-00000.parquet"
            ).exists()
        )
        first_file = self.root / "symbol=SPX" / "interval=1m" / "year=2026" / "month=08" / "part-00000.parquet"
        self.assertEqual(
            pq.ParquetFile(first_file).metadata.row_group(0).column(0).compression,
            "ZSTD",
        )
        dataset = ds.dataset(
            self.root,
            format="parquet",
            partitioning="hive",
            ignore_prefixes=[".", "_"],
        )
        table = dataset.to_table()
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.schema.field("timestamp").type, pa.timestamp("us", tz="UTC"))
        self.assertEqual(table.schema.field("strike").type, pa.float64())
        self.assertEqual(table.schema.field("volume").type, pa.int32())
        self.assertEqual(set(table.column("symbol").to_pylist()), {"SPX", "SPX W/TEST"})
        self.assertEqual(set(table.column("year").to_pylist()), {2026})
        self.assertIsNone(table.column("low")[0].as_py())
        fragments = list(
            dataset.get_fragments(
                filter=(ds.field("symbol") == "SPX")
                & (ds.field("interval") == "1m")
                & (ds.field("year") == 2026)
                & (ds.field("month") == 8)
            )
        )
        self.assertEqual(len(fragments), 1)
        self.assertIn("month=08", fragments[0].path)
        unusual_fragments = list(
            dataset.get_fragments(filter=ds.field("symbol") == "SPX W/TEST")
        )
        self.assertEqual(len(unusual_fragments), 1)
        self.assertIn(f"symbol={encoded}", unusual_fragments[0].path)

        parquet_glob = str(
            self.root / "symbol=*" / "interval=*" / "year=*" / "month=*" / "*.parquet"
        ).replace("\\", "/")
        duckdb_rows = duckdb.sql(
            "SELECT count(*) FROM read_parquet(?, hive_partitioning=true)",
            params=[parquet_glob],
        ).fetchone()[0]
        self.assertEqual(duckdb_rows, 2)

        manifest = json.loads((self.root / MANIFEST_NAME).read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["source_table"], "thetadata_option_history")
        self.assertFalse((self.root / MAINTENANCE_MARKER_NAME).exists())

    def test_schema_mismatch_requires_rebuild(self):
        self.root.mkdir(parents=True)
        (self.root / MANIFEST_NAME).write_text(
            json.dumps({"schema_version": 999, "partitions": []}), encoding="utf-8"
        )

        with self.assertRaisesRegex(ParquetSchemaMismatch, "rebuild"):
            self.service.refresh()

        self.assertFalse((self.root / MAINTENANCE_MARKER_NAME).exists())

    def test_refresh_rebuilds_only_partition_changed_after_watermark(self):
        session = self.Session()
        try:
            august = add_history_row(session)
            august_id = august.id
            add_history_row(
                session,
                strike=6600.0,
                timestamp=datetime(2026, 9, 1, 10, 0),
                updated_at=datetime(2026, 9, 1, 10, 1),
            )
        finally:
            session.close()
        self.service.rebuild()

        session = self.Session()
        try:
            august = session.get(ThetaDataOptionHistory, august_id)
            august.close = 12.25
            august.updated_at = datetime(2026, 9, 2, 10, 0)
            session.commit()
        finally:
            session.close()

        result = self.service.refresh()
        self.assertEqual(result["partitions"], 1)
        self.assertEqual(result["rows"], 1)
        dataset = ds.dataset(self.root, format="parquet", partitioning="hive")
        changed = dataset.to_table(
            filter=(ds.field("symbol") == "SPX")
            & (ds.field("interval") == "1m")
            & (ds.field("year") == 2026)
            & (ds.field("month") == 8)
        )
        self.assertEqual(changed.column("close")[0].as_py(), 12.25)

    def test_manifest_failure_restores_partition_and_watermark(self):
        session = self.Session()
        try:
            row = add_history_row(session)
            row_id = row.id
        finally:
            session.close()
        self.service.rebuild()
        manifest_before = (self.root / MANIFEST_NAME).read_text(encoding="utf-8")

        session = self.Session()
        try:
            row = session.get(ThetaDataOptionHistory, row_id)
            row.close = 99.0
            row.updated_at = datetime(2026, 8, 11, 10, 0)
            session.commit()
        finally:
            session.close()

        original_atomic_write = self.service._atomic_write_json

        def fail_manifest(path, value):
            if path.name == MANIFEST_NAME:
                raise OSError("simulated manifest failure")
            return original_atomic_write(path, value)

        with patch.object(self.service, "_atomic_write_json", side_effect=fail_manifest):
            with self.assertRaisesRegex(OSError, "simulated manifest failure"):
                self.service.refresh()

        dataset = ds.dataset(self.root, format="parquet", partitioning="hive")
        self.assertEqual(dataset.to_table().column("close")[0].as_py(), 10.5)
        self.assertEqual(
            (self.root / MANIFEST_NAME).read_text(encoding="utf-8"), manifest_before
        )
        self.assertFalse((self.root / MAINTENANCE_MARKER_NAME).exists())

    def test_small_target_produces_deterministically_named_multiple_files(self):
        session = self.Session()
        try:
            for minute in range(3):
                add_history_row(
                    session,
                    strike=6500.0 + minute,
                    timestamp=datetime(2026, 8, 10, 10, minute),
                    updated_at=datetime(2026, 8, 10, 11, minute),
                )
        finally:
            session.close()
        tiny_config = ParquetConfig(
            enabled=True,
            root=self.root,
            target_file_size_bytes=1,
            refresh_batch_size=1,
        )
        service = ParquetLayerService(self.engine, self.Session, tiny_config)

        service.rebuild()

        files = sorted(
            path.name
            for path in (
                self.root / "symbol=SPX" / "interval=1m" / "year=2026" / "month=08"
            ).glob("*.parquet")
        )
        self.assertEqual(files, ["part-00000.parquet", "part-00001.parquet", "part-00002.parquet"])
        dataset = ds.dataset(self.root, format="parquet", partitioning="hive")
        self.assertEqual(
            [value.minute for value in dataset.to_table().column("timestamp").to_pylist()],
            [0, 1, 2],
        )

    def test_stale_marker_rolls_back_published_partition(self):
        key = PartitionKey("SPX", "1m", 2026, 8)
        live = self.root / key.relative_path()
        backup = self.root / BACKUP_DIRECTORY / "stale-job" / key.relative_path()
        live.mkdir(parents=True)
        backup.mkdir(parents=True)
        (live / "new.txt").write_text("new", encoding="utf-8")
        (backup / "old.txt").write_text("old", encoding="utf-8")
        self.root.mkdir(parents=True, exist_ok=True)
        marker = {
            "job_id": "stale-job",
            "actions": [
                {"relative_path": key.relative_path().as_posix(), "had_live": True, "remove_only": False}
            ],
        }
        (self.root / MAINTENANCE_MARKER_NAME).write_text(json.dumps(marker), encoding="utf-8")

        self.service._recover_interrupted_publication(self.root)

        self.assertTrue((live / "old.txt").exists())
        self.assertFalse((live / "new.txt").exists())
        self.assertFalse((self.root / MAINTENANCE_MARKER_NAME).exists())

    def test_stale_marker_for_committed_manifest_keeps_new_partition(self):
        key = PartitionKey("SPX", "1m", 2026, 8)
        live = self.root / key.relative_path()
        backup = self.root / BACKUP_DIRECTORY / "committed-job" / key.relative_path()
        live.mkdir(parents=True)
        backup.mkdir(parents=True)
        (live / "new.txt").write_text("new", encoding="utf-8")
        (backup / "old.txt").write_text("old", encoding="utf-8")
        marker = {
            "job_id": "committed-job",
            "actions": [
                {"relative_path": key.relative_path().as_posix(), "had_live": True, "remove_only": False}
            ],
        }
        (self.root / MAINTENANCE_MARKER_NAME).write_text(json.dumps(marker), encoding="utf-8")
        (self.root / MANIFEST_NAME).write_text(
            json.dumps({"last_successful_job": {"job_id": "committed-job"}}), encoding="utf-8"
        )

        self.service._recover_interrupted_publication(self.root)

        self.assertTrue((live / "new.txt").exists())
        self.assertFalse(backup.exists())
        self.assertFalse((self.root / MAINTENANCE_MARKER_NAME).exists())

    def test_reconcile_range_is_inclusive_and_rebuilds_intersecting_months(self):
        session = self.Session()
        try:
            add_history_row(session, timestamp=datetime(2026, 8, 31, 23, 59))
            add_history_row(
                session,
                strike=6600.0,
                timestamp=datetime(2026, 9, 1, 0, 0),
                updated_at=datetime(2026, 9, 1, 0, 1),
            )
        finally:
            session.close()
        self.service.rebuild()

        result = self.service.reconcile(date(2026, 8, 31), date(2026, 9, 1))

        self.assertEqual(result["partitions"], 2)
        self.assertEqual(result["rows"], 2)

    def test_interrupted_rebuild_resumes_validated_partitions(self):
        session = self.Session()
        try:
            add_history_row(session, timestamp=datetime(2026, 8, 10, 10, 0))
            add_history_row(
                session,
                strike=6600.0,
                timestamp=datetime(2026, 9, 10, 10, 0),
                updated_at=datetime(2026, 9, 10, 10, 1),
            )
        finally:
            session.close()

        original_write = self.service._write_partition
        calls = 0

        def interrupt_second_partition(stage_root, key):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise KeyboardInterrupt()
            return original_write(stage_root, key)

        with patch.object(self.service, "_write_partition", side_effect=interrupt_second_partition):
            with self.assertRaises(KeyboardInterrupt):
                self.service.rebuild()

        self.assertTrue((self.root / REBUILD_CHECKPOINT_NAME).exists())
        resumed = ParquetLayerService(self.engine, self.Session, self.config)
        with patch.object(resumed, "_write_partition", wraps=resumed._write_partition) as write:
            result = resumed.rebuild()

        self.assertEqual(write.call_count, 1)
        self.assertEqual(result["rows"], 2)
        self.assertFalse((self.root / REBUILD_CHECKPOINT_NAME).exists())

    def test_schema_conversion_attaches_utc_and_exposes_lambda_column(self):
        session = self.Session()
        try:
            row = add_history_row(session)
            converted = source_row_to_dict(row)
        finally:
            session.close()
        self.assertEqual(tuple(converted), tuple(OUTPUT_SCHEMA.names))
        self.assertEqual(converted["timestamp"].tzinfo, timezone.utc)
        self.assertEqual(converted["lambda"], 2.5)
        self.assertEqual(
            [column.name for column in ThetaDataOptionHistory.__table__.columns],
            OUTPUT_SCHEMA.names,
        )


class ParquetConfigurationTests(unittest.TestCase):
    def test_rejects_disabled_and_relative_root(self):
        with self.assertRaises(ParquetConfigurationError):
            ParquetConfig(enabled=False, root=Path("C:/parquet")).validate()
        with self.assertRaises(ParquetConfigurationError):
            ParquetConfig(enabled=True, root=Path("relative/path")).validate()


class ParquetSchedulerTests(unittest.TestCase):
    def test_start_after_three_waits_until_following_day(self):
        current = datetime(2026, 8, 26, 7, 1, tzinfo=timezone.utc)
        scheduled = next_scheduled_run(current)
        self.assertEqual(scheduled.isoformat(), "2026-08-27T03:00:00-04:00")

    def test_dst_boundaries_keep_three_am_toronto(self):
        spring = next_scheduled_run(datetime(2026, 3, 7, 12, 0, tzinfo=timezone.utc))
        fall = next_scheduled_run(datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc))
        self.assertEqual((spring.hour, spring.utcoffset().total_seconds()), (3, -4 * 3600))
        self.assertEqual((fall.hour, fall.utcoffset().total_seconds()), (3, -5 * 3600))


if __name__ == "__main__":
    unittest.main()
