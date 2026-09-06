"""Extraction, validation, checkpointing, and publication service."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import time as time_module
from typing import Any, Callable, Iterable, Iterator
from urllib.parse import quote, unquote
from uuid import uuid4
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import extract, func, select, text

from database import ThetaDataOptionHistory
from .config import ParquetConfig, ParquetConfigurationError
from .schema import OUTPUT_SCHEMA, SCHEMA_VERSION, SOURCE_TABLE, source_row_to_dict


logger = logging.getLogger(__name__)

MANIFEST_NAME = "_parquet_manifest.json"
FAILURE_NAME = "_parquet_last_failure.json"
MAINTENANCE_MARKER_NAME = "_PARQUET_REFRESH_IN_PROGRESS"
REBUILD_CHECKPOINT_NAME = "_parquet_rebuild_checkpoint.json"
REBUILD_CHECKPOINT_VERSION = 2
STAGING_DIRECTORY = "_staging"
BACKUP_DIRECTORY = "_backup"
ADVISORY_LOCK_KEY = 0x50415251554554


class ParquetLayerError(RuntimeError):
    """Base error for Parquet layer operations."""


class ParquetLockUnavailable(ParquetLayerError):
    """Raised when another writer owns the advisory lock."""


class ParquetSchemaMismatch(ParquetLayerError):
    """Raised when an incremental operation encounters a different schema version."""


class ParquetValidationError(ParquetLayerError):
    """Raised when staged Parquet content does not match its source rows."""


@dataclass(frozen=True, order=True)
class PartitionKey:
    symbol: str
    interval: str
    year: int
    month: int

    def relative_path(self) -> Path:
        return Path(
            f"symbol={encode_partition_value(self.symbol)}",
            f"interval={encode_partition_value(self.interval)}",
            f"year={self.year:04d}",
            f"month={self.month:02d}",
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "year": self.year,
            "month": self.month,
        }


@dataclass(frozen=True)
class PartitionResult:
    key: PartitionKey
    row_count: int
    files: tuple[dict[str, Any], ...]

    def as_manifest_entry(self) -> dict[str, Any]:
        return {
            **self.key.as_dict(),
            "status": "committed",
            "row_count": self.row_count,
            "files": list(self.files),
        }

    @classmethod
    def from_manifest_entry(cls, value: dict[str, Any]) -> "PartitionResult":
        return cls(
            key=PartitionKey(
                symbol=str(value["symbol"]),
                interval=str(value["interval"]),
                year=int(value["year"]),
                month=int(value["month"]),
            ),
            row_count=int(value["row_count"]),
            files=tuple(value.get("files") or ()),
        )


@dataclass(frozen=True, order=True)
class SymbolIntervalKey:
    symbol: str
    interval: str

    def as_dict(self) -> dict[str, str]:
        return {"symbol": self.symbol, "interval": self.interval}


class _PartitionStreamWriter:
    """Bounded writer for one monthly partition."""

    def __init__(self, output_directory: Path, key: PartitionKey, config: ParquetConfig) -> None:
        self.output_directory = output_directory
        self.key = key
        self.config = config
        if output_directory.exists():
            shutil.rmtree(output_directory)
        output_directory.mkdir(parents=True, exist_ok=True)
        self.writer: pq.ParquetWriter | None = None
        self.file_path: Path | None = None
        self.file_index = 0
        self.file_uncompressed_bytes = 0
        self.total_rows = 0
        self.files: list[dict[str, Any]] = []
        self.records: list[dict[str, Any]] = []

    def add(self, row: Any) -> None:
        self.records.append(source_row_to_dict(row))
        self.total_rows += 1
        if len(self.records) >= self.config.refresh_batch_size:
            self._write_records()

    def finish(self) -> PartitionResult:
        if self.records:
            self._write_records()
        self._close_file()
        return PartitionResult(
            key=self.key,
            row_count=self.total_rows,
            files=tuple(self.files),
        )

    def abort(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def _write_records(self) -> None:
        table = pa.Table.from_pylist(self.records, schema=OUTPUT_SCHEMA)
        if (
            self.writer is not None
            and self.file_uncompressed_bytes + table.nbytes
            > self.config.target_file_size_bytes
        ):
            self._close_file()
        if self.writer is None:
            self.file_path = self.output_directory / f"part-{self.file_index:05d}.parquet"
            self.file_index += 1
            self.writer = pq.ParquetWriter(
                self.file_path,
                OUTPUT_SCHEMA,
                compression=self.config.compression,
                compression_level=self.config.compression_level,
                use_dictionary=True,
                write_statistics=True,
            )
        self.writer.write_table(table, row_group_size=len(self.records))
        self.file_uncompressed_bytes += table.nbytes
        self.records = []

    def _close_file(self) -> None:
        if self.writer is None or self.file_path is None:
            return
        self.writer.close()
        parquet_file = pq.ParquetFile(self.file_path)
        self.files.append(
            {
                "name": self.file_path.name,
                "rows": parquet_file.metadata.num_rows,
                "bytes": self.file_path.stat().st_size,
                "sha256": _sha256(self.file_path),
            }
        )
        self.writer = None
        self.file_path = None
        self.file_uncompressed_bytes = 0


def encode_partition_value(value: str) -> str:
    """URI-encode a Hive path segment while leaving common ticker characters readable."""
    return quote(value, safe="-._~")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_watermark(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _month_bounds(key: PartitionKey) -> tuple[datetime, datetime]:
    start = datetime(key.year, key.month, 1)
    if key.month == 12:
        return start, datetime(key.year + 1, 1, 1)
    return start, datetime(key.year, key.month + 1, 1)


def _log_event(event: str, **fields: Any) -> None:
    logger.info(json.dumps({"event": event, **fields}, default=str, sort_keys=True))


class ParquetLayerService:
    def __init__(
        self,
        engine: Any,
        session_factory: Callable[[], Any],
        config: ParquetConfig,
        *,
        now: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.engine = engine
        self.session_factory = session_factory
        self.config = config
        self.now = now
        self.root: Path | None = None

    def validate_configuration(self, *, require_enabled: bool = True) -> Path:
        self.root = self.config.validate(require_enabled=require_enabled)
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
        except Exception as exc:
            raise ParquetConfigurationError(f"PostgreSQL connection failed: {exc}") from exc
        return self.root

    def refresh(self) -> dict[str, Any]:
        return self._run_job("refresh")

    def reconcile(self, start_date: date, end_date: date) -> dict[str, Any]:
        if start_date > end_date:
            raise ParquetConfigurationError("Reconciliation start date must not follow end date.")
        return self._run_job("reconcile", start_date=start_date, end_date=end_date)

    def rebuild(self) -> dict[str, Any]:
        return self._run_job("rebuild")

    def _run_job(
        self,
        job_type: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        root = self.validate_configuration()
        started_at = self.now()
        job_id = f"{started_at.strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:10]}"
        started_clock = time_module.monotonic()

        with self._advisory_lock():
            self._recover_interrupted_publication(root)
            manifest = self._load_manifest(root)
            if job_type != "rebuild" and manifest and manifest.get("schema_version") != SCHEMA_VERSION:
                raise ParquetSchemaMismatch(
                    "Parquet schema version mismatch: "
                    f"manifest={manifest.get('schema_version')}, expected={SCHEMA_VERSION}. "
                    "Run `python parquet_layer.py rebuild`."
                )

            marker = {
                "job_id": job_id,
                "job_type": job_type,
                "started_at": _iso_utc(started_at),
                "host": os.getenv("COMPUTERNAME") or os.getenv("HOSTNAME") or "unknown",
                "pid": os.getpid(),
                "actions": [],
            }
            self._atomic_write_json(root / MAINTENANCE_MARKER_NAME, marker)
            _log_event("parquet_job_started", job_id=job_id, job_type=job_type)

            stage_root = root / STAGING_DIRECTORY / job_id
            backup_root = root / BACKUP_DIRECTORY / job_id
            staged_results: list[PartitionResult] = []
            actions: list[dict[str, Any]] = marker["actions"]
            manifest_committed = False
            try:
                old_watermark = _parse_watermark(manifest.get("source_watermark") if manifest else None)
                upper_watermark = self._maximum_updated_at()
                if job_type == "refresh":
                    partitions = self._affected_partitions(old_watermark, upper_watermark)
                elif job_type == "reconcile":
                    assert start_date is not None and end_date is not None
                    partitions = self._partitions_in_date_range(start_date, end_date)
                else:
                    symbol_intervals = self._symbol_interval_pairs()
                    self._validate_rebuild_disk_space(root)

                if job_type == "rebuild":
                    marker["symbol_intervals"] = [item.as_dict() for item in symbol_intervals]
                    stage_root, staged_results, completed_pairs = self._prepare_rebuild_staging(
                        root, stage_root, symbol_intervals, upper_watermark
                    )
                    marker["staging_job_id"] = stage_root.name
                else:
                    marker["partitions"] = [item.as_dict() for item in partitions]
                self._atomic_write_json(root / MAINTENANCE_MARKER_NAME, marker)
                if job_type == "rebuild":
                    for pair in symbol_intervals:
                        if pair in completed_pairs:
                            _log_event(
                                "parquet_symbol_interval_resumed",
                                job_id=job_id,
                                **pair.as_dict(),
                            )
                            continue
                        pair_started = time_module.monotonic()
                        rows_before = sum(item.row_count for item in staged_results)
                        self._stream_rebuild_pair(
                            root=root,
                            stage_root=stage_root,
                            pair=pair,
                            symbol_intervals=symbol_intervals,
                            results=staged_results,
                            completed_pairs=completed_pairs,
                            source_watermark=upper_watermark,
                            job_id=job_id,
                        )
                        _log_event(
                            "parquet_symbol_interval_staged",
                            job_id=job_id,
                            **pair.as_dict(),
                            rows=sum(item.row_count for item in staged_results) - rows_before,
                            duration_seconds=round(
                                time_module.monotonic() - pair_started, 3
                            ),
                        )
                else:
                    for partition in partitions:
                        partition_started = time_module.monotonic()
                        result = self._write_partition(stage_root, partition)
                        staged_results.append(result)
                        _log_event(
                            "parquet_partition_staged",
                            job_id=job_id,
                            **partition.as_dict(),
                            rows=result.row_count,
                            files=len(result.files),
                            duration_seconds=round(
                                time_module.monotonic() - partition_started, 3
                            ),
                        )

                self._publish_results(root, stage_root, backup_root, staged_results, marker, actions)
                if job_type == "rebuild":
                    self._stage_obsolete_partition_removals(
                        root, backup_root, {item.key for item in staged_results}, marker, actions
                    )

                updated_manifest = self._build_manifest(
                    prior=manifest,
                    job_id=job_id,
                    job_type=job_type,
                    results=staged_results,
                    source_watermark=(
                        upper_watermark if job_type in {"refresh", "rebuild"} else old_watermark
                    ),
                    started_at=started_at,
                )
                self._atomic_write_json(root / MANIFEST_NAME, updated_manifest)
                manifest_committed = True
                try:
                    (root / FAILURE_NAME).unlink(missing_ok=True)
                    (root / REBUILD_CHECKPOINT_NAME).unlink(missing_ok=True)
                    (root / MAINTENANCE_MARKER_NAME).unlink(missing_ok=True)
                    self._cleanup_job_paths(stage_root, backup_root)
                except OSError:
                    logger.exception("Committed Parquet job left removable staging or backup artifacts")

                committed_watermark = (
                    upper_watermark if job_type in {"refresh", "rebuild"} else old_watermark
                )
                summary = {
                    "job_id": job_id,
                    "job_type": job_type,
                    "status": "completed",
                    "partitions": len(staged_results),
                    "rows": sum(item.row_count for item in staged_results),
                    "files": sum(len(item.files) for item in staged_results),
                    "source_watermark": _iso_utc(committed_watermark),
                    "duration_seconds": round(time_module.monotonic() - started_clock, 3),
                }
                _log_event("parquet_job_completed", **summary)
                return summary
            except BaseException as exc:
                preserve_rebuild = job_type == "rebuild" and not actions and stage_root.exists()
                rollback_error = (
                    None
                    if manifest_committed or preserve_rebuild
                    else self._rollback_actions(root, actions)
                )
                failure = {
                    "job_id": job_id,
                    "job_type": job_type,
                    "status": "failed",
                    "failed_at": _iso_utc(self.now()),
                    "error": str(exc),
                    "rollback_error": str(rollback_error) if rollback_error else None,
                }
                self._atomic_write_json(root / FAILURE_NAME, failure)
                if preserve_rebuild:
                    (root / MAINTENANCE_MARKER_NAME).unlink(missing_ok=True)
                elif rollback_error is None and not manifest_committed:
                    self._cleanup_job_paths(stage_root, backup_root)
                    (root / REBUILD_CHECKPOINT_NAME).unlink(missing_ok=True)
                    (root / MAINTENANCE_MARKER_NAME).unlink(missing_ok=True)
                _log_event("parquet_job_failed", **failure)
                raise

    @contextmanager
    def _advisory_lock(self) -> Iterator[None]:
        with self.engine.connect() as connection:
            if connection.dialect.name != "postgresql":
                yield
                return
            acquired = bool(
                connection.execute(
                    text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": ADVISORY_LOCK_KEY},
                ).scalar_one()
            )
            if not acquired:
                raise ParquetLockUnavailable("Another Parquet writer already holds the advisory lock.")
            try:
                yield
            finally:
                connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": ADVISORY_LOCK_KEY},
                )

    def _maximum_updated_at(self) -> datetime | None:
        session = self.session_factory()
        try:
            return session.execute(select(func.max(ThetaDataOptionHistory.updated_at))).scalar_one()
        finally:
            session.close()

    def _symbol_interval_pairs(self) -> list[SymbolIntervalKey]:
        statement = (
            select(
                ThetaDataOptionHistory.symbol,
                ThetaDataOptionHistory.interval,
            )
            .distinct()
            .order_by(
                ThetaDataOptionHistory.symbol,
                ThetaDataOptionHistory.interval,
            )
        )
        session = self.session_factory()
        try:
            return [
                SymbolIntervalKey(str(row.symbol), str(row.interval))
                for row in session.execute(statement)
            ]
        finally:
            session.close()

    def _affected_partitions(
        self, previous: datetime | None, upper: datetime | None
    ) -> list[PartitionKey]:
        if upper is None:
            return []
        filters = [ThetaDataOptionHistory.updated_at <= upper]
        if previous is not None:
            filters.append(ThetaDataOptionHistory.updated_at > previous)
        return self._partition_query(*filters)

    def _partitions_in_date_range(self, start_date: date, end_date: date) -> list[PartitionKey]:
        start = datetime.combine(start_date, time.min)
        end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min)
        return self._partition_query(
            ThetaDataOptionHistory.timestamp >= start,
            ThetaDataOptionHistory.timestamp < end_exclusive,
        )

    def _partition_query(self, *filters: Any) -> list[PartitionKey]:
        year = extract("year", ThetaDataOptionHistory.timestamp)
        month = extract("month", ThetaDataOptionHistory.timestamp)
        statement = select(
            ThetaDataOptionHistory.symbol,
            ThetaDataOptionHistory.interval,
            year.label("year"),
            month.label("month"),
        ).distinct()
        if filters:
            statement = statement.where(*filters)
        statement = statement.order_by(
            ThetaDataOptionHistory.symbol,
            ThetaDataOptionHistory.interval,
            year,
            month,
        )
        session = self.session_factory()
        try:
            return [
                PartitionKey(str(row.symbol), str(row.interval), int(row.year), int(row.month))
                for row in session.execute(statement)
            ]
        finally:
            session.close()

    def _iter_partition_rows(self, key: PartitionKey) -> Iterator[Any]:
        start, end = _month_bounds(key)
        statement = (
            select(ThetaDataOptionHistory)
            .where(
                ThetaDataOptionHistory.symbol == key.symbol,
                ThetaDataOptionHistory.interval == key.interval,
                ThetaDataOptionHistory.timestamp >= start,
                ThetaDataOptionHistory.timestamp < end,
            )
            .order_by(
                ThetaDataOptionHistory.timestamp,
                ThetaDataOptionHistory.expiry,
                ThetaDataOptionHistory.strike,
                ThetaDataOptionHistory.right,
                ThetaDataOptionHistory.id,
            )
            .execution_options(yield_per=self.config.refresh_batch_size)
        )
        session = self.session_factory()
        try:
            yield from session.execute(statement).scalars()
        finally:
            session.close()

    def _iter_symbol_interval_rows(
        self,
        pair: SymbolIntervalKey,
        start_timestamp: datetime | None = None,
    ) -> Iterator[Any]:
        statement = select(ThetaDataOptionHistory).where(
            ThetaDataOptionHistory.symbol == pair.symbol,
            ThetaDataOptionHistory.interval == pair.interval,
        )
        if start_timestamp is not None:
            statement = statement.where(
                ThetaDataOptionHistory.timestamp >= start_timestamp
            )
        statement = statement.order_by(
            ThetaDataOptionHistory.timestamp,
            ThetaDataOptionHistory.expiry,
            ThetaDataOptionHistory.strike,
            ThetaDataOptionHistory.right,
            ThetaDataOptionHistory.id,
        ).execution_options(yield_per=self.config.refresh_batch_size)
        session = self.session_factory()
        try:
            yield from session.execute(statement).scalars()
        finally:
            session.close()

    def _prepare_rebuild_staging(
        self,
        root: Path,
        default_stage_root: Path,
        symbol_intervals: list[SymbolIntervalKey],
        source_watermark: datetime | None,
    ) -> tuple[Path, list[PartitionResult], set[SymbolIntervalKey]]:
        checkpoint = self._read_json(root / REBUILD_CHECKPOINT_NAME)
        expected_pairs = [item.as_dict() for item in symbol_intervals]
        expected_watermark = _iso_utc(source_watermark)
        if checkpoint:
            staging_job_id = str(checkpoint.get("staging_job_id") or "")
            safe_staging_job_id = (
                bool(staging_job_id)
                and Path(staging_job_id).name == staging_job_id
                and staging_job_id not in {".", ".."}
            )
            checkpoint_stage = (
                root / STAGING_DIRECTORY / staging_job_id
                if safe_staging_job_id
                else default_stage_root
            )
            compatible = (
                checkpoint.get("checkpoint_version") == REBUILD_CHECKPOINT_VERSION
                and checkpoint.get("schema_version") == SCHEMA_VERSION
                and checkpoint.get("source_watermark") == expected_watermark
                and checkpoint.get("symbol_intervals") == expected_pairs
                and safe_staging_job_id
                and checkpoint_stage.is_dir()
            )
            if compatible:
                try:
                    staged_entries = checkpoint.get("staged_partitions", [])
                    if any(
                        item.get("status") != "committed"
                        for item in staged_entries
                    ):
                        raise ParquetValidationError(
                            "Rebuild checkpoint contains an uncommitted partition"
                        )
                    results = [
                        PartitionResult.from_manifest_entry(item)
                        for item in staged_entries
                    ]
                    completed_pairs = {
                        SymbolIntervalKey(
                            symbol=str(item["symbol"]),
                            interval=str(item["interval"]),
                        )
                        for item in checkpoint.get("completed_symbol_intervals", [])
                    }
                    expected_pair_set = set(symbol_intervals)
                    if not completed_pairs.issubset(expected_pair_set):
                        raise ParquetValidationError(
                            "Rebuild checkpoint contains an unknown completed symbol/interval pair"
                        )
                    result_keys: set[PartitionKey] = set()
                    for result in results:
                        pair = SymbolIntervalKey(result.key.symbol, result.key.interval)
                        if pair not in expected_pair_set:
                            raise ParquetValidationError(
                                f"Rebuild checkpoint contains an unknown partition {result.key}"
                            )
                        if result.key in result_keys:
                            raise ParquetValidationError(
                                f"Rebuild checkpoint contains a duplicate partition {result.key}"
                            )
                        result_keys.add(result.key)
                        self._validate_partition(checkpoint_stage / result.key.relative_path(), result)
                    return checkpoint_stage, results, completed_pairs
                except Exception:
                    logger.exception("Discarding an invalid Parquet rebuild checkpoint")
            if safe_staging_job_id and checkpoint_stage.is_dir():
                shutil.rmtree(checkpoint_stage)
            (root / REBUILD_CHECKPOINT_NAME).unlink(missing_ok=True)

        self._write_rebuild_checkpoint(
            root,
            default_stage_root,
            symbol_intervals,
            [],
            set(),
            source_watermark,
        )
        return default_stage_root, [], set()

    def _write_rebuild_checkpoint(
        self,
        root: Path,
        stage_root: Path,
        symbol_intervals: list[SymbolIntervalKey],
        results: list[PartitionResult],
        completed_pairs: set[SymbolIntervalKey],
        source_watermark: datetime | None,
    ) -> None:
        self._atomic_write_json(
            root / REBUILD_CHECKPOINT_NAME,
            {
                "checkpoint_version": REBUILD_CHECKPOINT_VERSION,
                "schema_version": SCHEMA_VERSION,
                "source_watermark": _iso_utc(source_watermark),
                "staging_job_id": stage_root.name,
                "symbol_intervals": [item.as_dict() for item in symbol_intervals],
                "completed_symbol_intervals": [
                    item.as_dict() for item in sorted(completed_pairs)
                ],
                "staged_partitions": [item.as_manifest_entry() for item in results],
            },
        )

    def _stream_rebuild_pair(
        self,
        *,
        root: Path,
        stage_root: Path,
        pair: SymbolIntervalKey,
        symbol_intervals: list[SymbolIntervalKey],
        results: list[PartitionResult],
        completed_pairs: set[SymbolIntervalKey],
        source_watermark: datetime | None,
        job_id: str,
    ) -> None:
        pair_results = [
            result
            for result in results
            if result.key.symbol == pair.symbol and result.key.interval == pair.interval
        ]
        start_timestamp = None
        if pair_results:
            last_key = max(result.key for result in pair_results)
            start_timestamp = _month_bounds(last_key)[1]

        rows = self._iter_symbol_interval_rows(pair, start_timestamp)
        writer: _PartitionStreamWriter | None = None
        current_key: PartitionKey | None = None
        partition_started = time_module.monotonic()

        def complete_partition() -> None:
            nonlocal writer, current_key, partition_started
            if writer is None or current_key is None:
                return
            result = writer.finish()
            writer = None
            self._validate_partition(stage_root / current_key.relative_path(), result)
            results.append(result)
            self._write_rebuild_checkpoint(
                root,
                stage_root,
                symbol_intervals,
                results,
                completed_pairs,
                source_watermark,
            )
            _log_event(
                "parquet_partition_staged",
                job_id=job_id,
                **current_key.as_dict(),
                rows=result.row_count,
                files=len(result.files),
                duration_seconds=round(time_module.monotonic() - partition_started, 3),
            )
            current_key = None

        try:
            for row in rows:
                timestamp = row.timestamp
                if timestamp.tzinfo is not None:
                    timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
                key = PartitionKey(pair.symbol, pair.interval, timestamp.year, timestamp.month)
                if key != current_key:
                    complete_partition()
                    current_key = key
                    partition_started = time_module.monotonic()
                    writer = _PartitionStreamWriter(
                        stage_root / key.relative_path(),
                        key,
                        self.config,
                    )
                assert writer is not None
                writer.add(row)
            complete_partition()
            completed_pairs.add(pair)
            self._write_rebuild_checkpoint(
                root,
                stage_root,
                symbol_intervals,
                results,
                completed_pairs,
                source_watermark,
            )
        except BaseException:
            if writer is not None:
                writer.abort()
            raise
        finally:
            close = getattr(rows, "close", None)
            if close is not None:
                close()

    def _write_partition(self, stage_root: Path, key: PartitionKey) -> PartitionResult:
        output_directory = stage_root / key.relative_path()
        writer = _PartitionStreamWriter(output_directory, key, self.config)
        try:
            for row in self._iter_partition_rows(key):
                writer.add(row)
            result = writer.finish()
        except BaseException:
            writer.abort()
            raise
        self._validate_partition(output_directory, result)
        return result

    @staticmethod
    def _validate_partition(directory: Path, expected: PartitionResult) -> None:
        files = sorted(directory.glob("part-*.parquet"))
        row_count = 0
        actual_files: list[dict[str, Any]] = []
        for path in files:
            parquet_file = pq.ParquetFile(path)
            if not parquet_file.schema_arrow.equals(OUTPUT_SCHEMA, check_metadata=True):
                raise ParquetValidationError(f"Schema mismatch in staged file {path}")
            row_count += parquet_file.metadata.num_rows
            actual_files.append(
                {
                    "name": path.name,
                    "rows": parquet_file.metadata.num_rows,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
        if row_count != expected.row_count:
            raise ParquetValidationError(
                f"Staged row count mismatch for {expected.key}: expected {expected.row_count}, got {row_count}"
            )
        if len(files) != len(expected.files):
            raise ParquetValidationError(f"Staged file count mismatch for {expected.key}")
        if tuple(actual_files) != expected.files:
            raise ParquetValidationError(f"Staged file metadata mismatch for {expected.key}")

    def _publish_results(
        self,
        root: Path,
        stage_root: Path,
        backup_root: Path,
        results: Iterable[PartitionResult],
        marker: dict[str, Any],
        actions: list[dict[str, Any]],
    ) -> None:
        for result in results:
            relative = result.key.relative_path()
            staged = stage_root / relative
            live = root / relative
            backup = backup_root / relative
            actions.append(
                {
                    "relative_path": relative.as_posix(),
                    "had_live": live.exists(),
                    "remove_only": False,
                }
            )
            self._atomic_write_json(root / MAINTENANCE_MARKER_NAME, marker)
            live.parent.mkdir(parents=True, exist_ok=True)
            if live.exists():
                backup.parent.mkdir(parents=True, exist_ok=True)
                live.rename(backup)
            try:
                staged.rename(live)
            except Exception:
                if backup.exists() and not live.exists():
                    backup.rename(live)
                raise

    def _stage_obsolete_partition_removals(
        self,
        root: Path,
        backup_root: Path,
        staged_keys: set[PartitionKey],
        marker: dict[str, Any],
        actions: list[dict[str, Any]],
    ) -> None:
        for key in self._live_partitions(root):
            if key in staged_keys:
                continue
            relative = key.relative_path()
            live = root / relative
            backup = backup_root / relative
            actions.append(
                {
                    "relative_path": relative.as_posix(),
                    "had_live": True,
                    "remove_only": True,
                }
            )
            self._atomic_write_json(root / MAINTENANCE_MARKER_NAME, marker)
            backup.parent.mkdir(parents=True, exist_ok=True)
            live.rename(backup)

    def _rollback_actions(self, root: Path, actions: list[dict[str, Any]]) -> Exception | None:
        try:
            marker = self._read_json(root / MAINTENANCE_MARKER_NAME) or {}
            job_id = marker.get("job_id")
            if not job_id:
                return None
            backup_root = root / BACKUP_DIRECTORY / job_id
            for action in reversed(actions):
                relative = Path(action["relative_path"])
                live = root / relative
                backup = backup_root / relative
                if backup.exists():
                    if live.exists():
                        shutil.rmtree(live)
                    live.parent.mkdir(parents=True, exist_ok=True)
                    backup.rename(live)
                elif not action.get("had_live") and live.exists():
                    shutil.rmtree(live)
            return None
        except Exception as exc:
            logger.exception("Failed to roll back Parquet publication")
            return exc

    def _recover_interrupted_publication(self, root: Path) -> None:
        marker = self._read_json(root / MAINTENANCE_MARKER_NAME)
        if not marker:
            checkpoint = self._read_json(root / REBUILD_CHECKPOINT_NAME) or {}
            self._cleanup_orphan_workdirs(
                root, preserve_stage_id=checkpoint.get("staging_job_id")
            )
            return
        manifest = self._load_manifest(root)
        committed_job_id = (manifest.get("last_successful_job") or {}).get("job_id")
        if marker.get("manifest_committed") or committed_job_id == marker.get("job_id"):
            job_id = marker.get("job_id")
            staging_job_id = marker.get("staging_job_id") or job_id
            if job_id:
                self._cleanup_job_paths(
                    root / STAGING_DIRECTORY / staging_job_id,
                    root / BACKUP_DIRECTORY / job_id,
                )
            (root / REBUILD_CHECKPOINT_NAME).unlink(missing_ok=True)
            (root / MAINTENANCE_MARKER_NAME).unlink(missing_ok=True)
            _log_event("parquet_committed_job_cleanup_completed", job_id=job_id)
            return
        actions = list(marker.get("actions") or [])
        if marker.get("job_type") == "rebuild" and not actions:
            checkpoint = self._read_json(root / REBUILD_CHECKPOINT_NAME)
            if checkpoint:
                (root / MAINTENANCE_MARKER_NAME).unlink(missing_ok=True)
                _log_event("parquet_rebuild_staging_preserved", job_id=marker.get("job_id"))
                return
        rollback_error = self._rollback_actions(root, actions)
        if rollback_error is not None:
            raise ParquetLayerError(
                f"Could not recover interrupted Parquet job {marker.get('job_id')}: {rollback_error}"
            )
        job_id = marker.get("job_id")
        staging_job_id = marker.get("staging_job_id") or job_id
        if job_id:
            self._cleanup_job_paths(
                root / STAGING_DIRECTORY / staging_job_id,
                root / BACKUP_DIRECTORY / job_id,
            )
        (root / REBUILD_CHECKPOINT_NAME).unlink(missing_ok=True)
        (root / MAINTENANCE_MARKER_NAME).unlink(missing_ok=True)
        _log_event("parquet_stale_job_recovered", job_id=job_id)

    def _build_manifest(
        self,
        *,
        prior: dict[str, Any],
        job_id: str,
        job_type: str,
        results: list[PartitionResult],
        source_watermark: datetime | None,
        started_at: datetime,
    ) -> dict[str, Any]:
        prior_partitions = {
            _partition_identity(entry): entry for entry in prior.get("partitions", [])
        }
        if job_type == "rebuild":
            prior_partitions = {}
        for result in results:
            prior_partitions[_partition_identity(result.key.as_dict())] = result.as_manifest_entry()
        return {
            "manifest_version": 1,
            "schema_version": SCHEMA_VERSION,
            "source_table": SOURCE_TABLE,
            "source_watermark": _iso_utc(source_watermark),
            "last_successful_job": {
                "job_id": job_id,
                "job_type": job_type,
                "started_at": _iso_utc(started_at),
                "completed_at": _iso_utc(self.now()),
                "partitions": len(results),
                "rows": sum(result.row_count for result in results),
                "files": sum(len(result.files) for result in results),
            },
            "partitions": [prior_partitions[key] for key in sorted(prior_partitions)],
        }

    def _load_manifest(self, root: Path) -> dict[str, Any]:
        return self._read_json(root / MANIFEST_NAME) or {}

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as handle:
                json.dump(value, handle, indent=2, sort_keys=True, default=str)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _cleanup_job_paths(stage_root: Path, backup_root: Path) -> None:
        for path in (stage_root, backup_root):
            if path.exists():
                shutil.rmtree(path)

    @staticmethod
    def _cleanup_orphan_workdirs(root: Path, *, preserve_stage_id: str | None = None) -> None:
        staging = root / STAGING_DIRECTORY
        if staging.exists():
            for child in staging.iterdir():
                if child.name != preserve_stage_id:
                    shutil.rmtree(child)
        backup = root / BACKUP_DIRECTORY
        if backup.exists():
            shutil.rmtree(backup)

    def _validate_rebuild_disk_space(self, root: Path) -> None:
        live_bytes = sum(
            path.stat().st_size
            for path in root.glob("symbol=*/interval=*/year=*/month=*/*.parquet")
            if path.is_file()
        )
        source_bytes = 0
        if self.engine.dialect.name == "postgresql":
            with self.engine.connect() as connection:
                source_bytes = int(
                    connection.execute(
                        text("SELECT pg_total_relation_size(CAST(:table_name AS regclass))"),
                        {"table_name": SOURCE_TABLE},
                    ).scalar_one()
                    or 0
                )
        required = int(max(live_bytes, source_bytes) * 1.10)
        available = shutil.disk_usage(root).free
        if required and available < required:
            raise ParquetLayerError(
                f"Insufficient disk space for rebuild: required approximately {required} bytes, "
                f"available {available} bytes."
            )

    @staticmethod
    def _live_partitions(root: Path) -> list[PartitionKey]:
        results: list[PartitionKey] = []
        for month_path in root.glob("symbol=*/interval=*/year=*/month=*"):
            try:
                symbol = unquote(month_path.parents[2].name.split("=", 1)[1])
                interval = unquote(month_path.parents[1].name.split("=", 1)[1])
                year = int(month_path.parent.name.split("=", 1)[1])
                month = int(month_path.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            results.append(PartitionKey(symbol, interval, year, month))
        return sorted(results)


def _partition_identity(value: dict[str, Any]) -> tuple[str, str, int, int]:
    return (
        str(value["symbol"]),
        str(value["interval"]),
        int(value["year"]),
        int(value["month"]),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


TORONTO_TIMEZONE = ZoneInfo("America/Toronto")


def next_scheduled_run(now: datetime) -> datetime:
    """Return the next 03:00 Toronto run; startup after 03:00 never catches up."""
    local_now = now.astimezone(TORONTO_TIMEZONE)
    candidate = datetime.combine(local_now.date(), time(3, 0), tzinfo=TORONTO_TIMEZONE)
    if local_now >= candidate:
        candidate += timedelta(days=1)
    return candidate


def run_scheduler(
    service: ParquetLayerService,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(TORONTO_TIMEZONE),
    sleep: Callable[[float], None] = time_module.sleep,
) -> None:
    """Continuously poll for the next 03:00 Toronto incremental refresh."""
    scheduled_for = next_scheduled_run(now())
    _log_event("parquet_scheduler_started", next_run=scheduled_for.isoformat())
    while True:
        current = now().astimezone(TORONTO_TIMEZONE)
        if current >= scheduled_for:
            try:
                service.refresh()
            except ParquetLockUnavailable as exc:
                _log_event("parquet_scheduled_run_skipped", reason=str(exc))
            except Exception:
                logger.exception("Scheduled Parquet refresh failed")
            scheduled_for = next_scheduled_run(now())
            _log_event("parquet_scheduler_next_run", next_run=scheduled_for.isoformat())
        seconds_until_run = max(0.1, (scheduled_for - current).total_seconds())
        sleep(min(float(service.config.scheduler_poll_seconds), seconds_until_run))
