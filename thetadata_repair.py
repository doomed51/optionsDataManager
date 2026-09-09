"""Audit and repair incomplete ThetaData history checkpoints."""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable

import polars as pl

import config as cfg
from database import DatabaseManager, ThetaDataCollectionCheckpoint, ThetaDataOptionHistory
from thetadata_collector import ThetaDataContract, ThetaDataEndpointUnavailable, ThetaDataOptionsBackfillCollector

logger = logging.getLogger(__name__)


@dataclass
class RepairSummary:
    audited: int = 0
    valid: int = 0
    queued: int = 0
    repaired: int = 0
    retryable: int = 0


def _configured_symbols() -> set[str]:
    return {symbol.upper() for symbol in (*cfg.THETADATA_SYMBOLS, *cfg.COLLECTION_SYMBOLS_METADATA)}


def _frame(rows: Iterable[ThetaDataOptionHistory]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"timestamp": row.timestamp, "bid": row.bid, "ask": row.ask, "close": row.close} for row in rows],
        schema={"timestamp": pl.Datetime, "bid": pl.Float64, "ask": pl.Float64, "close": pl.Float64},
    )


def repair_thetadata_data(
    symbols: Iterable[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    collector: ThetaDataOptionsBackfillCollector | None = None,
    db_manager: DatabaseManager | None = None,
) -> RepairSummary:
    """Audit COMPLETE checkpoints and immediately recollect deficient contract-days."""
    owns_db = db_manager is None
    db_manager = db_manager or DatabaseManager()
    db_manager.create_tables()
    session = db_manager.get_session()
    collector = collector or ThetaDataOptionsBackfillCollector()
    summary = RepairSummary()
    try:
        selected_symbols = {symbol.upper() for symbol in symbols} if symbols else _configured_symbols()
        cutoff = date.today() - timedelta(days=cfg.THETADATA_MAX_LOOKBACK_YEARS * 365)
        start_date = max(start_date or cutoff, cutoff)
        query = session.query(ThetaDataCollectionCheckpoint).filter(
            ThetaDataCollectionCheckpoint.dataset == "full_history",
            ThetaDataCollectionCheckpoint.status == "COMPLETE",
            ThetaDataCollectionCheckpoint.symbol.in_(selected_symbols),
            ThetaDataCollectionCheckpoint.interval.in_(cfg.THETADATA_INTERVALS),
            ThetaDataCollectionCheckpoint.trade_date >= start_date,
        )
        if end_date:
            query = query.filter(ThetaDataCollectionCheckpoint.trade_date <= end_date)

        for checkpoint in query.order_by(ThetaDataCollectionCheckpoint.trade_date).all():
            summary.audited += 1
            day_start = datetime.combine(checkpoint.trade_date, time.min)
            day_end = day_start + timedelta(days=1)
            rows = session.query(ThetaDataOptionHistory).filter(
                ThetaDataOptionHistory.symbol == checkpoint.symbol,
                ThetaDataOptionHistory.expiry == checkpoint.expiry,
                ThetaDataOptionHistory.strike == checkpoint.strike,
                ThetaDataOptionHistory.right == checkpoint.right,
                ThetaDataOptionHistory.interval == checkpoint.interval,
                ThetaDataOptionHistory.timestamp >= day_start,
                ThetaDataOptionHistory.timestamp < day_end,
            ).all()
            coverage = collector.assess_history_frame(
                _frame(rows), checkpoint.trade_date, checkpoint.interval
            )
            if coverage.complete:
                summary.valid += 1
                continue

            summary.queued += 1
            checkpoint.status = "RETRY"
            checkpoint.completed_at = None
            checkpoint.next_retry_at = None
            checkpoint.last_error = f"Repair audit: {coverage.reason}"[:500]
            session.commit()
            contract = ThetaDataContract(
                checkpoint.symbol, checkpoint.expiry, checkpoint.strike, checkpoint.right
            )
            try:
                collector.collect_contract_day(
                    session, contract, checkpoint.trade_date, checkpoint.interval,
                    checkpoint.collection_batch or f"repair:{checkpoint.symbol}:{checkpoint.trade_date}",
                )
            except ThetaDataEndpointUnavailable:
                logger.exception("Repair endpoint failure for %s", contract)
            session.refresh(checkpoint)
            if checkpoint.status == "COMPLETE":
                summary.repaired += 1
            else:
                summary.retryable += 1
        return summary
    finally:
        session.close()
        if owns_db:
            db_manager.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit and repair ThetaData missing-price history")
    parser.add_argument("--symbols", help="Comma-separated symbols; defaults to configured ThetaData symbols")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    symbols = args.symbols.split(",") if args.symbols else None
    print(json.dumps(asdict(repair_thetadata_data(symbols, args.start_date, args.end_date)), default=str))


if __name__ == "__main__":
    main()
