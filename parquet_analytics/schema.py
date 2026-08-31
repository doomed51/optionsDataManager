"""Stable Arrow schema and source-row conversion."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa


SCHEMA_VERSION = 1
SOURCE_TABLE = "thetadata_option_history"

# Output name, SQLAlchemy attribute name, Arrow type, nullable.
SOURCE_FIELDS: tuple[tuple[str, str, pa.DataType, bool], ...] = (
    ("id", "id", pa.int32(), False),
    ("symbol", "symbol", pa.string(), False),
    ("expiry", "expiry", pa.date32(), False),
    ("strike", "strike", pa.float64(), False),
    ("right", "right", pa.string(), False),
    ("interval", "interval", pa.string(), False),
    ("timestamp", "timestamp", pa.timestamp("us", tz="UTC"), False),
    ("open", "open", pa.float64(), True),
    ("high", "high", pa.float64(), True),
    ("low", "low", pa.float64(), True),
    ("close", "close", pa.float64(), True),
    ("volume", "volume", pa.int32(), True),
    ("trade_count", "trade_count", pa.int32(), True),
    ("vwap", "vwap", pa.float64(), True),
    ("bid", "bid", pa.float64(), True),
    ("ask", "ask", pa.float64(), True),
    ("bid_size", "bid_size", pa.int32(), True),
    ("ask_size", "ask_size", pa.int32(), True),
    ("open_interest", "open_interest", pa.int32(), True),
    ("bid_implied_vol", "bid_implied_vol", pa.float64(), True),
    ("ask_implied_vol", "ask_implied_vol", pa.float64(), True),
    ("implied_volatility", "implied_volatility", pa.float64(), True),
    ("iv_error", "iv_error", pa.float64(), True),
    ("underlying_price", "underlying_price", pa.float64(), True),
    ("delta", "delta", pa.float64(), True),
    ("theta", "theta", pa.float64(), True),
    ("vega", "vega", pa.float64(), True),
    ("rho", "rho", pa.float64(), True),
    ("epsilon", "epsilon", pa.float64(), True),
    ("lambda", "lambda_", pa.float64(), True),
    ("dte", "dte", pa.int32(), True),
    ("collection_batch", "collection_batch", pa.string(), False),
    ("created_at", "created_at", pa.timestamp("us", tz="UTC"), True),
    ("updated_at", "updated_at", pa.timestamp("us", tz="UTC"), True),
)

OUTPUT_SCHEMA = pa.schema(
    [pa.field(name, data_type, nullable=nullable) for name, _, data_type, nullable in SOURCE_FIELDS],
    metadata={
        b"parquet_schema_version": str(SCHEMA_VERSION).encode("ascii"),
        b"source_table": SOURCE_TABLE.encode("ascii"),
        b"timestamp_semantics": b"UTC",
    },
)


def as_utc(value: datetime | None) -> datetime | None:
    """Attach UTC semantics to naive source datetimes and normalize aware values."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def source_row_to_dict(row: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for output_name, attribute_name, data_type, _ in SOURCE_FIELDS:
        value = getattr(row, attribute_name)
        if pa.types.is_timestamp(data_type):
            value = as_utc(value)
        elif pa.types.is_date(data_type) and isinstance(value, datetime):
            value = value.date()
        elif value is not None and pa.types.is_integer(data_type):
            value = int(value)
        elif value is not None and pa.types.is_floating(data_type):
            value = float(value)
        result[output_name] = value
    return result
