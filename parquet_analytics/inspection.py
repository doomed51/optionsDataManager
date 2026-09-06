"""Read-only analytics for visually inspecting the Parquet option dataset."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote, unquote

import duckdb
import pandas as pd
import pandas_market_calendars as mcal

from .config import ParquetConfig
from .service import MAINTENANCE_MARKER_NAME, MANIFEST_NAME


NEW_YORK_TIMEZONE = "America/New_York"
INTERVAL_PATTERN = re.compile(r"^(?P<count>\d+)(?P<unit>[mh])$")


class ParquetInspectionError(RuntimeError):
    """Raised when the dataset cannot be inspected safely."""


@dataclass(frozen=True)
class InspectionFilters:
    symbol: str
    interval: str
    dte: int
    start_date: date
    end_date: date

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.interval:
            raise ValueError("interval is required")
        if self.dte < 0:
            raise ValueError("dte must be non-negative")
        if self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")


@dataclass(frozen=True)
class DatasetSelection:
    dtes: tuple[int, ...]
    first_date: date | None
    last_date: date | None


class ParquetInspectionService:
    """Query the published Hive dataset without modifying it."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._validate_root()

    @classmethod
    def from_env(cls) -> "ParquetInspectionService":
        config = ParquetConfig.from_env()
        if not config.enabled:
            raise ParquetInspectionError(
                "The Parquet layer is disabled. Set PARQUET_ENABLED=true."
            )
        if config.root is None:
            raise ParquetInspectionError("PARQUET_ROOT is required.")
        return cls(config.root)

    def _validate_root(self) -> None:
        if not self.root.is_absolute():
            raise ParquetInspectionError("PARQUET_ROOT must be absolute.")
        if not self.root.is_dir():
            raise ParquetInspectionError(f"Parquet root does not exist: {self.root}")
        self.assert_available()

    def assert_available(self) -> None:
        if (self.root / MAINTENANCE_MARKER_NAME).exists():
            raise ParquetInspectionError(
                "A Parquet refresh is in progress. Wait for publication to complete."
            )

    def signature(self) -> tuple[int, int]:
        """Return a cheap cache key that changes after dataset publication."""
        self.assert_available()
        manifest = self.root / MANIFEST_NAME
        if manifest.exists():
            stat = manifest.stat()
            return stat.st_mtime_ns, stat.st_size
        files = list(self.root.glob("symbol=*/interval=*/year=*/month=*/*.parquet"))
        if not files:
            return 0, 0
        return max(path.stat().st_mtime_ns for path in files), len(files)

    def symbols(self) -> tuple[str, ...]:
        self.assert_available()
        return tuple(sorted(
            unquote(path.name.removeprefix("symbol="))
            for path in self.root.glob("symbol=*") if path.is_dir()
        ))

    def intervals(self, symbol: str) -> tuple[str, ...]:
        self.assert_available()
        base = self.root / f"symbol={quote(symbol, safe='-._~')}"
        return tuple(sorted(
            unquote(path.name.removeprefix("interval="))
            for path in base.glob("interval=*") if path.is_dir()
        ))

    def selection(self, symbol: str, interval: str) -> DatasetSelection:
        source = self._source_glob(symbol, interval)
        trade_date = self._trade_date_sql()
        frame = self._execute(
            f"""SELECT list_sort(list(DISTINCT dte) FILTER (WHERE dte IS NOT NULL)) AS dtes,
                       min({trade_date}) AS first_date, max({trade_date}) AS last_date
                FROM read_parquet(?, hive_partitioning=true, union_by_name=true)""",
            [source],
        )
        row = frame.iloc[0]
        raw_dtes = row["dtes"]
        if raw_dtes is None:
            dtes: tuple[int, ...] = ()
        else:
            dtes = tuple(int(value) for value in raw_dtes if value is not None)
        return DatasetSelection(
            dtes, self._as_date(row["first_date"]), self._as_date(row["last_date"])
        )

    def date_bounds(
        self, symbol: str, interval: str, dte: int
    ) -> tuple[date | None, date | None]:
        source = self._source_glob(symbol, interval)
        trade_date = self._trade_date_sql()
        frame = self._execute(
            f"""SELECT min({trade_date}) AS first_date, max({trade_date}) AS last_date
                FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
                WHERE dte = ?""",
            [source, dte],
        )
        row = frame.iloc[0]
        return self._as_date(row["first_date"]), self._as_date(row["last_date"])

    def summary(self, filters: InspectionFilters) -> dict[str, Any]:
        base, params = self._filtered_cte(filters)
        frame = self._execute(base + """
            SELECT count(*) AS row_count, count(DISTINCT trade_date) AS available_days,
                   min(trade_date) AS first_date, max(trade_date) AS last_date
            FROM filtered""", params)
        result = frame.iloc[0].to_dict()
        expected = self.expected_sessions(filters.start_date, filters.end_date)
        available = set(self.available_dates(filters))
        missing = tuple(day for day in expected if day not in available)
        result.update(
            expected_sessions=len(expected), missing_sessions=len(missing),
            coverage_percent=(100.0 * len(available) / len(expected)) if expected else 0.0,
            missing_dates=missing,
            first_date=self._as_date(result.get("first_date")),
            last_date=self._as_date(result.get("last_date")),
        )
        return result

    def available_dates(self, filters: InspectionFilters) -> tuple[date, ...]:
        base, params = self._filtered_cte(filters)
        frame = self._execute(
            base + " SELECT DISTINCT trade_date FROM filtered ORDER BY trade_date", params
        )
        return tuple(
            self._as_date(value) for value in frame["trade_date"] if pd.notna(value)
        )

    @staticmethod
    def expected_sessions(start_date: date, end_date: date) -> tuple[date, ...]:
        schedule = mcal.get_calendar("NYSE").schedule(
            start_date=start_date, end_date=end_date
        )
        return tuple(timestamp.date() for timestamp in schedule.index)

    def daily_strikes(self, filters: InspectionFilters) -> pd.DataFrame:
        base, params = self._filtered_cte(filters)
        return self._execute(base + """
            SELECT trade_date, "right", count(DISTINCT strike) AS strike_count
            FROM filtered WHERE "right" IN ('C', 'P')
            GROUP BY trade_date, "right" ORDER BY trade_date, "right"
            """, params)

    def delta_coverage(self, filters: InspectionFilters) -> pd.DataFrame:
        base, params = self._filtered_cte(filters)
        return self._execute(base + """
            , contract_delta AS (
                SELECT trade_date, "right", expiry, strike, median(abs(delta)) AS abs_delta
                FROM filtered
                WHERE ("right" = 'C' AND delta BETWEEN 0 AND 1)
                   OR ("right" = 'P' AND delta BETWEEN -1 AND 0)
                GROUP BY trade_date, "right", expiry, strike
            ), bucketed AS (
                SELECT *, least(floor(abs_delta * 10) / 10, 0.9) AS bucket_start
                FROM contract_delta
            )
            SELECT trade_date, "right", bucket_start, count(*) AS contract_count,
                   min(abs_delta) AS min_abs_delta, max(abs_delta) AS max_abs_delta
            FROM bucketed GROUP BY trade_date, "right", bucket_start
            ORDER BY trade_date, "right", bucket_start
            """, params)

    def atm_series(self, filters: InspectionFilters) -> pd.DataFrame:
        base, params = self._filtered_cte(filters)
        return self._execute(base + """
            , ranked AS (
                SELECT *, row_number() OVER (
                    PARTITION BY timestamp, "right"
                    ORDER BY abs(strike - underlying_price),
                             CASE WHEN bid IS NOT NULL AND ask IS NOT NULL
                                       AND isfinite(bid) AND isfinite(ask)
                                       AND bid >= 0 AND ask >= bid THEN 0 ELSE 1 END,
                             CASE WHEN bid IS NOT NULL AND ask IS NOT NULL
                                  THEN ask - bid ELSE NULL END NULLS LAST,
                             expiry, strike, id
                ) AS atm_rank
                FROM filtered
                WHERE "right" IN ('C', 'P') AND underlying_price IS NOT NULL
                  AND isfinite(underlying_price)
            )
            SELECT timestamp, trade_date, "right", expiry, strike, dte,
                   underlying_price, bid, ask, close,
                   CASE WHEN bid IS NOT NULL AND ask IS NOT NULL
                              AND isfinite(bid) AND isfinite(ask)
                              AND bid >= 0 AND ask >= bid
                        THEN (bid + ask) / 2 ELSE NULL END AS mid_price
            FROM ranked WHERE atm_rank = 1 ORDER BY timestamp, "right"
            """, params)

    def atm_gaps(self, filters: InspectionFilters) -> pd.DataFrame:
        columns = ["right", "trade_date", "gap_start", "gap_end", "missing_intervals"]
        cadence = self._interval_timedelta(filters.interval)
        if cadence is None:
            return pd.DataFrame(columns=columns)
        frame = self.atm_series(filters)
        if frame.empty:
            return pd.DataFrame(columns=columns)
        valid = frame[frame["mid_price"].notna()].copy()
        valid["timestamp"] = pd.to_datetime(valid["timestamp"], utc=True)
        rows: list[dict[str, Any]] = []
        for (right, trade_date), group in valid.groupby(["right", "trade_date"]):
            timestamps = group["timestamp"].sort_values().drop_duplicates().tolist()
            for previous, current in zip(timestamps, timestamps[1:]):
                gap = current - previous
                if gap > cadence * 1.5:
                    rows.append({
                        "right": right, "trade_date": self._as_date(trade_date),
                        "gap_start": previous, "gap_end": current,
                        "missing_intervals": max(1, math.ceil(gap / cadence) - 1),
                    })
        return pd.DataFrame(rows, columns=columns)

    def exception_counts(self, filters: InspectionFilters) -> pd.DataFrame:
        union, params = self._exception_union(filters, count_only=True)
        return self._execute(
            f"SELECT rule, severity, count(*) AS exception_count FROM ({union}) "
            "GROUP BY rule, severity ORDER BY severity DESC, rule", params
        )

    def exceptions(
        self, filters: InspectionFilters, *, limit: int | None = None
    ) -> pd.DataFrame:
        union, params = self._exception_union(filters, count_only=False)
        query = f"SELECT * FROM ({union}) ORDER BY timestamp, \"right\", strike, rule"
        if limit is not None:
            if limit <= 0:
                raise ValueError("limit must be positive")
            query += f" LIMIT {int(limit)}"
        return self._execute(query, params)

    def _exception_union(
        self, filters: InspectionFilters, *, count_only: bool
    ) -> tuple[str, list[Any]]:
        base, params = self._filtered_cte(filters)
        payload = "rule, severity" if count_only else (
            "timestamp, trade_date, \"right\", expiry, strike, dte, underlying_price, "
            "open, high, low, close, bid, ask, delta, rule, severity"
        )
        rules = [
            ("missing_underlying", "error", "underlying_price IS NULL"),
            ("missing_delta", "warning", "delta IS NULL"),
            ("missing_all_prices", "error", "bid IS NULL AND ask IS NULL AND close IS NULL"),
            ("non_finite_numeric", "error", "(strike IS NOT NULL AND NOT isfinite(strike)) OR (underlying_price IS NOT NULL AND NOT isfinite(underlying_price)) OR (delta IS NOT NULL AND NOT isfinite(delta)) OR (bid IS NOT NULL AND NOT isfinite(bid)) OR (ask IS NOT NULL AND NOT isfinite(ask)) OR (close IS NOT NULL AND NOT isfinite(close))"),
            ("negative_price", "error", "coalesce(bid < 0, false) OR coalesce(ask < 0, false) OR coalesce(close < 0, false)"),
            ("crossed_quote", "error", "bid IS NOT NULL AND ask IS NOT NULL AND bid > ask"),
            ("invalid_right", "error", "\"right\" IS NULL OR \"right\" NOT IN ('C', 'P')"),
            ("invalid_delta", "error", "delta IS NOT NULL AND ((\"right\" = 'C' AND delta NOT BETWEEN 0 AND 1) OR (\"right\" = 'P' AND delta NOT BETWEEN -1 AND 0))"),
            ("ohlc_ordering", "error", "(high IS NOT NULL AND low IS NOT NULL AND high < low) OR (open IS NOT NULL AND high IS NOT NULL AND open > high) OR (open IS NOT NULL AND low IS NOT NULL AND open < low) OR (close IS NOT NULL AND high IS NOT NULL AND close > high) OR (close IS NOT NULL AND low IS NOT NULL AND close < low)"),
            ("dte_mismatch", "warning", "expiry IS NOT NULL AND dte IS NOT NULL AND date_diff('day', trade_date, expiry) <> dte"),
        ]
        selects = [
            f"SELECT {payload} FROM (SELECT *, '{name}' AS rule, '{severity}' AS severity FROM filtered WHERE {predicate})"
            for name, severity, predicate in rules
        ]
        selects.append(f"""SELECT {payload} FROM (
            SELECT f.*, 'duplicate_key' AS rule, 'error' AS severity
            FROM filtered f INNER JOIN (
                SELECT symbol, expiry, strike, "right", interval, timestamp
                FROM filtered GROUP BY symbol, expiry, strike, "right", interval, timestamp
                HAVING count(*) > 1
            ) d USING (symbol, expiry, strike, "right", interval, timestamp)
        )""")
        return base + " " + " UNION ALL ".join(selects), params

    def _filtered_cte(self, filters: InspectionFilters) -> tuple[str, list[Any]]:
        source = self._source_glob(filters.symbol, filters.interval)
        trade_date = self._trade_date_sql()
        return f"""WITH filtered AS (
            SELECT *, {trade_date} AS trade_date
            FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
            WHERE dte = ? AND {trade_date} BETWEEN ? AND ?
        )""", [source, filters.dte, filters.start_date, filters.end_date]

    def _source_glob(self, symbol: str, interval: str) -> str:
        self.assert_available()
        symbol_value = quote(symbol, safe="-._~")
        interval_value = quote(interval, safe="-._~")
        base = self.root / f"symbol={symbol_value}" / f"interval={interval_value}"
        if not any(base.glob("year=*/month=*/*.parquet")):
            raise ParquetInspectionError(f"No Parquet files found for {symbol} / {interval}.")
        return (base / "year=*" / "month=*" / "*.parquet").as_posix()

    def _execute(self, query: str, params: list[Any]) -> pd.DataFrame:
        self.assert_available()
        try:
            with duckdb.connect(database=":memory:") as connection:
                connection.execute("SET TimeZone='UTC'")
                return connection.execute(query, params).fetchdf()
        except duckdb.Error as exc:
            raise ParquetInspectionError(f"Parquet query failed: {exc}") from exc

    @staticmethod
    def _trade_date_sql() -> str:
        return f"CAST(timezone('{NEW_YORK_TIMEZONE}', timestamp) AS DATE)"

    @staticmethod
    def _interval_timedelta(interval: str) -> pd.Timedelta | None:
        match = INTERVAL_PATTERN.fullmatch(interval.strip().lower())
        if not match or int(match.group("count")) <= 0:
            return None
        unit = "minutes" if match.group("unit") == "m" else "hours"
        return pd.Timedelta(**{unit: int(match.group("count"))})

    @staticmethod
    def _as_date(value: Any) -> date | None:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.date()
        if isinstance(value, date):
            return value
        return pd.Timestamp(value).date()
