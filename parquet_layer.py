"""Command-line interface for the ThetaData PostgreSQL-to-Parquet layer."""

from __future__ import annotations

import argparse
from datetime import date
import json
import logging
import sys

from database import DatabaseManager
from parquet_analytics import ParquetConfig, ParquetLayerService
from parquet_analytics.service import run_scheduler


def _date_argument(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Maintain the ThetaData option-history Parquet analytics dataset."
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("refresh", help="incrementally refresh changed monthly partitions")
    reconcile = subparsers.add_parser(
        "reconcile", help="rebuild monthly partitions intersecting an inclusive UTC date range"
    )
    reconcile.add_argument("--start", required=True, type=_date_argument)
    reconcile.add_argument("--end", required=True, type=_date_argument)
    subparsers.add_parser("rebuild", help="stage and publish a complete replacement dataset")
    subparsers.add_parser("schedule", help="run the continuous 03:00 Toronto scheduler")
    subparsers.add_parser("config-check", help="validate configuration and database access")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    database = None
    try:
        database = DatabaseManager()
        service = ParquetLayerService(
            engine=database.engine,
            session_factory=database.get_session,
            config=ParquetConfig.from_env(),
        )
        if args.command == "config-check":
            root = service.validate_configuration()
            print(json.dumps({"status": "ok", "parquet_root": str(root)}))
            return 0
        if args.command == "refresh":
            result = service.refresh()
        elif args.command == "reconcile":
            result = service.reconcile(args.start, args.end)
        elif args.command == "rebuild":
            result = service.rebuild()
        else:
            service.validate_configuration()
            run_scheduler(service)
            return 0
        print(json.dumps(result, sort_keys=True))
        return 0
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Parquet operation interrupted by user")
        return 130
    except Exception as exc:
        logging.getLogger(__name__).exception("Parquet operation failed: %s", exc)
        return 1
    finally:
        if database is not None:
            database.close()


if __name__ == "__main__":
    sys.exit(main())
