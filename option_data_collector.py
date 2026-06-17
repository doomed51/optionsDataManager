from collections import deque
from typing import Deque, List, Optional, Dict, Tuple
from ib_insync import IB, Contract, Option, Stock, Index, Future, util
import pandas as pd
from datetime import datetime, timedelta, date as dt_date
import logging
import math
from pathlib import Path
import hashlib
from options_data_retriever import OptionsDataRetriever
from skew_data_collector import OptionsSkewDataCollector
from database import (
    DatabaseManager,
    CollectionProgress,
    ContractCheckpoint,
    OptionsHistoricalData,
    UnderlyingPriceHistory,
)
import config as cfg

class OptionsDataCollector:
    def __init__(self, ib_connection: IB, checkpoint_dir: str = "checkpoints"):
        """
        Initialize the Options Data Collector
        
        Args:
            ib_connection (IB): Active IB connection
            checkpoint_dir (str): Directory to store checkpoint files
        """
        self.ib = ib_connection
        self.options_retriever = OptionsDataRetriever(ib_connection)
        self.logger = logging.getLogger(__name__)

        # PostgreSQL connection for checkpoint/status tracking.
        self.db_manager = DatabaseManager()
        self.db_manager.create_tables()
        self.pg_session = self.db_manager.get_session()

        # Shared pacing state for IB historical-data limits.
        self._request_window = timedelta(minutes=10)
        self._request_limit = 56
        self._request_events: Deque[Tuple[datetime, int]] = deque()
        self._head_timestamp_cache: Dict[Tuple[str, str, float, str, str, bool], Optional[datetime]] = {}
        
        # Checkpoint configuration
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)

    def _prune_request_window(self, now: datetime) -> None:
        """Drop pacing events that have aged out of the rolling window."""
        while self._request_events and now - self._request_events[0][0] >= self._request_window:
            self._request_events.popleft()

    def _get_request_cost(self, what_to_show: str) -> int:
        """Return the weighted pacing cost for a historical request."""
        return 2 if what_to_show == 'BID_ASK' else 1

    def _acquire_request_budget(self, what_to_show: str, request_name: str) -> None:
        """Wait until enough rolling-window budget is available for a request."""
        request_cost = self._get_request_cost(what_to_show)

        while True:
            now = datetime.now()
            self._prune_request_window(now)
            used_budget = sum(cost for _, cost in self._request_events)

            if used_budget + request_cost <= self._request_limit:
                self._request_events.append((now, request_cost))
                return

            oldest_timestamp, _ = self._request_events[0]
            wait_seconds = max(
                0.5,
                (oldest_timestamp + self._request_window - now).total_seconds(),
            )
            self.logger.info(
                "Pacing %s request for %.1fs (cost=%s, used=%s/%s)",
                request_name,
                wait_seconds,
                request_cost,
                used_budget,
                self._request_limit,
            )
            self.ib.sleep(wait_seconds)

    def _paced_req_historical_data(
        self,
        contract: Contract,
        endDateTime,
        durationStr: str,
        barSizeSetting: str,
        whatToShow: str,
        useRTH: bool,
        formatDate: int = 1,
    ):
        """Submit a historical-data request after acquiring pacing budget."""
        self._acquire_request_budget(whatToShow, 'historical-data')
        return self.ib.reqHistoricalData(
            contract,
            endDateTime=endDateTime,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=useRTH,
            formatDate=formatDate,
        )

    def _get_contract_cache_key(
        self,
        contract: Contract,
        what_to_show: str,
        use_rth: bool,
    ) -> Tuple[str, str, float, str, str, bool]:
        """Build a stable cache key for per-run head timestamp lookups."""
        return (
            contract.symbol,
            str(contract.lastTradeDateOrContractMonth),
            float(getattr(contract, 'strike', 0.0) or 0.0),
            getattr(contract, 'right', ''),
            what_to_show,
            use_rth,
        )

    def _paced_req_head_timestamp(
        self,
        contract: Contract,
        what_to_show: str,
        use_rth: bool,
    ) -> Optional[datetime]:
        """Return a cached head timestamp or acquire budget before requesting it."""
        cache_key = self._get_contract_cache_key(contract, what_to_show, use_rth)
        if cache_key in self._head_timestamp_cache:
            self.logger.info("Head timestamp cache hit for %s", contract.localSymbol)
            return self._head_timestamp_cache[cache_key]

        self._acquire_request_budget(what_to_show, 'head-timestamp')
        head_timestamp = self.ib.reqHeadTimeStamp(contract, what_to_show, use_rth)
        self._head_timestamp_cache[cache_key] = head_timestamp
        return head_timestamp

    def _should_skip_head_timestamp(
        self,
        requested_start_date: Optional[dt_date],
        requested_end_date: Optional[dt_date],
    ) -> bool:
        """Skip head timestamp when the caller is collecting a specific known day."""
        if requested_start_date is None or requested_end_date is None:
            return False

        return (requested_end_date - requested_start_date).days <= 1

    def _resolve_contract_start_date(
        self,
        contract: Contract,
        requested_start_date: Optional[dt_date],
        requested_end_date: dt_date,
        what_to_show: str,
        use_rth: bool,
    ) -> Optional[dt_date]:
        """Choose the earliest request date without issuing redundant head timestamp calls."""
        latest_timestamp_row = self.pg_session.query(OptionsHistoricalData.date).filter(
            OptionsHistoricalData.symbol == contract.symbol,
            OptionsHistoricalData.strike == float(contract.strike),
            OptionsHistoricalData.right == contract.right,
            OptionsHistoricalData.expiry == pd.to_datetime(contract.lastTradeDateOrContractMonth).date(),
            OptionsHistoricalData.what_to_show == what_to_show,
        ).order_by(OptionsHistoricalData.date.desc()).first()

        latest_timestamp = latest_timestamp_row[0] if latest_timestamp_row else None
        if latest_timestamp is not None and not pd.isna(latest_timestamp):
            contract_start_date = latest_timestamp.date()
        elif self._should_skip_head_timestamp(requested_start_date, requested_end_date):
            contract_start_date = requested_start_date
        else:
            head_timestamp = self._paced_req_head_timestamp(contract, what_to_show, use_rth)
            if head_timestamp is None:
                return requested_start_date

            contract_start_date = pd.to_datetime(head_timestamp).date()

        if requested_start_date is not None and requested_start_date > contract_start_date:
            return requested_start_date

        return contract_start_date
        
    def _initialize_database(self):
        """Deprecated: PostgreSQL tables are managed via SQLAlchemy Base metadata."""
        return
        
    def _generate_batch_id(self, symbol: str, start_date: datetime, 
                          end_date: datetime) -> str:
        """Generate a unique batch ID for a collection run"""
        input_string = f"{symbol}_{start_date.isoformat()}_{end_date.isoformat()}"
        return hashlib.md5(input_string.encode()).hexdigest()
        
    def _save_checkpoint(self, batch_id: str, last_processed_date: datetime,
                        status: str = 'IN_PROGRESS'):
        """Update collection progress in database"""
        checkpoint = self.pg_session.query(CollectionProgress).filter_by(batch_id=batch_id).first()
        if checkpoint is None:
            return

        checkpoint.snapshot_time = last_processed_date
        checkpoint.status = status
        self.pg_session.commit()
        
    def _get_collection_status(self, batch_id: str) -> Optional[Dict]:
        """Get the status of a collection batch"""
        checkpoint = self.pg_session.query(CollectionProgress).filter_by(batch_id=batch_id).first()
        if checkpoint:
            return {
                'batch_id': checkpoint.batch_id,
                'symbol': checkpoint.symbol,
                'status': checkpoint.status,
                'last_processed_date': checkpoint.snapshot_time,
                'created_at': checkpoint.created_at,
                'updated_at': checkpoint.updated_at,
            }
        return None
        
    def _initialize_collection_batch(self, symbol: str, start_date: datetime,
                                   end_date: datetime) -> str:
        """Initialize or resume a collection batch"""
        batch_id = self._generate_batch_id(symbol, start_date, end_date)
        
        # Check if batch exists
        existing_batch = self._get_collection_status(batch_id)
        if existing_batch:
            if existing_batch['status'] == 'COMPLETED':
                self.logger.info(f"Batch {batch_id} already completed")
                return batch_id
            
            # Resume from last processed date
            self.logger.info(f"Resuming batch {batch_id} from {existing_batch['last_processed_date']}")
            return batch_id

        # Create new batch in PostgreSQL progress table.
        checkpoint = CollectionProgress(
            batch_id=batch_id,
            symbol=symbol,
            snapshot_time=start_date,
            status='STARTED',
        )
        self.pg_session.add(checkpoint)
        self.pg_session.commit()
        
        return batch_id

    def _seed_contract_checkpoints(
        self,
        batch_id: str,
        symbol: str,
        trade_date: dt_date,
        strikes: List[float],
        expirations: List[str],
        bar_size: str,
        what_to_show: str = 'TRADES',
        use_rth: bool = True,
    ) -> None:
        """Create checkpoint targets for all contracts to be collected on a trade date."""
        for expiration_str in expirations:
            expiry_date = datetime.strptime(expiration_str, '%Y%m%d').date()
            for strike in strikes:
                for right in ('C', 'P'):
                    existing = self.pg_session.query(ContractCheckpoint).filter_by(
                        batch_id=batch_id,
                        symbol=symbol,
                        trade_date=trade_date,
                        expiry=expiry_date,
                        strike=float(strike),
                        right=right,
                        bar_size=bar_size,
                        what_to_show=what_to_show,
                        use_rth=use_rth,
                    ).first()
                    if existing:
                        continue

                    checkpoint = ContractCheckpoint(
                        batch_id=batch_id,
                        symbol=symbol,
                        trade_date=trade_date,
                        expiry=expiry_date,
                        strike=float(strike),
                        right=right,
                        bar_size=bar_size,
                        what_to_show=what_to_show,
                        use_rth=use_rth,
                        status='PENDING',
                    )
                    self.pg_session.add(checkpoint)

        self.pg_session.commit()

    def _get_prioritized_contract_queue(
        self,
        batch_id: str,
        symbol: str,
        trade_date: dt_date,
        reference_price: float,
    ) -> List[ContractCheckpoint]:
        """Return queued contracts sorted by nearest expiry, then nearest strike."""
        now = datetime.now()
        checkpoints = self.pg_session.query(ContractCheckpoint).filter(
            ContractCheckpoint.batch_id == batch_id,
            ContractCheckpoint.symbol == symbol,
            ContractCheckpoint.trade_date == trade_date,
            ContractCheckpoint.status.in_(['PENDING', 'RETRY', 'IN_PROGRESS']),
        ).all()

        checkpoints = [
            cp for cp in checkpoints if cp.backoff_until is None or cp.backoff_until <= now
        ]

        return sorted(
            checkpoints,
            key=lambda cp: (
                (cp.expiry - trade_date).days,
                abs(cp.strike - float(reference_price)),
            ),
        )

    def _set_contract_checkpoint_status(
        self,
        checkpoint: ContractCheckpoint,
        status: str,
        last_error: Optional[str] = None,
        unavailable_reason: Optional[str] = None,
        probe_evidence: Optional[str] = None,
        with_backoff: bool = False,
    ) -> None:
        """Update a checkpoint status with optional retry metadata."""
        checkpoint.status = status
        checkpoint.last_attempt_at = datetime.now()

        if status in ('IN_PROGRESS', 'RETRY', 'FAILED'):
            checkpoint.attempts = (checkpoint.attempts or 0) + 1

        if status in ('COMPLETE', 'VERIFIED_UNAVAILABLE_EXPIRED'):
            checkpoint.completed_at = datetime.now()

        checkpoint.last_error = last_error
        checkpoint.unavailable_reason = unavailable_reason
        checkpoint.probe_evidence = probe_evidence

        if with_backoff:
            delay_minutes = min(30, max(1, checkpoint.attempts))
            checkpoint.backoff_until = datetime.now() + timedelta(minutes=delay_minutes)
        elif status in ('COMPLETE', 'VERIFIED_UNAVAILABLE_EXPIRED'):
            checkpoint.backoff_until = None

        if status == 'RETRY' and checkpoint.attempts >= checkpoint.retry_budget:
            checkpoint.status = 'FAILED'

        self.pg_session.commit()

    def _is_contract_data_saved(
        self,
        symbol: str,
        strike: float,
        expiry: dt_date,
        right: str,
        what_to_show: str,
        trade_date: dt_date,
    ) -> bool:
        """Verify historical rows exist for a specific target contract and date."""
        start_dt = datetime.combine(trade_date, datetime.min.time())
        end_dt = start_dt + timedelta(days=1)

        row = self.pg_session.query(OptionsHistoricalData.id).filter(
            OptionsHistoricalData.symbol == symbol,
            OptionsHistoricalData.strike == float(strike),
            OptionsHistoricalData.expiry == expiry,
            OptionsHistoricalData.right == right,
            OptionsHistoricalData.what_to_show == what_to_show,
            OptionsHistoricalData.date >= start_dt,
            OptionsHistoricalData.date < end_dt,
        ).first()
        return row is not None

    def _store_underlying_price_history(self, price_history: pd.DataFrame, batch_id: str) -> None:
        """Persist underlying daily history rows in PostgreSQL with duplicate protection."""
        for _, row in price_history.iterrows():
            row_date = pd.to_datetime(row['date']).to_pydatetime().replace(tzinfo=None)
            exists = self.pg_session.query(UnderlyingPriceHistory.id).filter(
                UnderlyingPriceHistory.symbol == row['symbol'],
                UnderlyingPriceHistory.date == row_date,
            ).first()
            if exists:
                continue

            self.pg_session.add(
                UnderlyingPriceHistory(
                    symbol=row['symbol'],
                    date=row_date,
                    open=float(row['open']) if not pd.isna(row['open']) else None,
                    high=float(row['high']) if not pd.isna(row['high']) else None,
                    low=float(row['low']) if not pd.isna(row['low']) else None,
                    price=float(row['close']) if not pd.isna(row['close']) else None,
                    collection_batch=batch_id,
                )
            )
        self.pg_session.commit()

    def _probe_expired_contract_unavailable(
        self,
        contract: Contract,
        trade_date: dt_date,
        bar_size: str,
        what_to_show: str,
        use_rth: bool,
    ) -> Tuple[bool, str]:
        """Probe API to classify whether an expired contract is truly unavailable."""
        try:
            self.ib.qualifyContracts(contract)
            bars = self._paced_req_historical_data(
                contract,
                endDateTime=trade_date + timedelta(days=1),
                durationStr='1 D',
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=use_rth,
                formatDate=1,
            )
            head = self._paced_req_head_timestamp(contract, what_to_show, use_rth)

            if (not bars) and (head is None):
                return True, 'probe:no-bars-and-no-head-timestamp'

            return False, 'probe:data-available-or-head-timestamp-present'
        except Exception as exc:
            message = str(exc)
            lowered = message.lower()
            if 'no security definition' in lowered or 'no historical market data' in lowered:
                return True, f'probe:explicit-unavailable:{message}'

            return False, f'probe:transient-or-unknown:{message}'

    def _finalize_batch_status(self, batch_id: str, as_of_date: datetime) -> str:
        """Mark batch complete only if all checkpoints are in terminal states."""
        total_count = self.pg_session.query(ContractCheckpoint).filter(
            ContractCheckpoint.batch_id == batch_id,
        ).count()

        terminal_count = self.pg_session.query(ContractCheckpoint).filter(
            ContractCheckpoint.batch_id == batch_id,
            ContractCheckpoint.status.in_(['COMPLETE', 'VERIFIED_UNAVAILABLE_EXPIRED', 'FAILED']),
        ).count()

        pending_count = self.pg_session.query(ContractCheckpoint).filter(
            ContractCheckpoint.batch_id == batch_id,
            ContractCheckpoint.status.in_(['PENDING', 'IN_PROGRESS', 'RETRY']),
        ).count()

        progress_row = self.pg_session.query(CollectionProgress).filter_by(batch_id=batch_id).first()
        if progress_row is not None:
            progress_row.num_contracts_total = total_count
            progress_row.num_contracts_processed = terminal_count
            self.pg_session.commit()

        if total_count > 0 and pending_count == 0:
            self._save_checkpoint(batch_id, as_of_date, 'COMPLETED')
            return 'COMPLETED'

        self._save_checkpoint(batch_id, as_of_date, 'IN_PROGRESS')
        return 'IN_PROGRESS'
        
    def get_date_chunks(self, start_date: datetime, end_date: datetime, 
                       bar_size: str) -> List[Tuple[datetime, datetime]]:
        """
        Break down date range into chunks that respect the 2000 bar limit
        """
        bars_per_day = {
            '1 min': 390,
            '5 mins': 78,
            '15 mins': 26,
            '1 hour': 7,
            '1 day': 1
        }[bar_size]
        
        max_days_per_chunk = math.floor(1200 / bars_per_day)
        
        chunks = []
        current_start = start_date
        while current_start < end_date:
            chunk_end = min(
                current_start + timedelta(days=max_days_per_chunk),
                end_date
            )
            chunks.append((current_start, chunk_end))
            current_start = chunk_end + timedelta(days=1)
            
        return chunks
    
    def get_underlying_price_history(self, symbol: str, start_date: datetime, 
                                   end_date: datetime) -> pd.DataFrame:
        """Get historical 1d frequency underlying prices for the date range"""
        # stock = Stock(symbol, 'SMART', 'USD')
        if symbol == 'SPXW':
            symbol = 'SPX'
        if symbol in cfg.INDEX_LIST:
            stock = Index(symbol, cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART'), 'USD')
        else:
            stock = Stock(symbol, 'SMART', 'USD')
        self.ib.qualifyContracts(stock)
        
        chunks = self.get_date_chunks(start_date, end_date, '1 day')
        price_data = []
        
        for chunk_start, chunk_end in chunks:
            bars = self._paced_req_historical_data(
                stock,
                endDateTime=chunk_end,
                durationStr=f'{(chunk_end - chunk_start).days + 1} D',
                barSizeSetting='1 day',
                whatToShow='TRADES',
                useRTH=True,
                formatDate=1
            )
            
            if bars:
                df = util.df(bars)
                price_data.append(df)
        
        if price_data:
            ym = pd.concat(price_data)
            ym['symbol'] = symbol
            return ym
        return pd.DataFrame()

    def get_option_strikes_between_range(self, symbol:str, high_price, low_price, num_strikes:int = 5) -> List[float]:
        """Get n strikes between high and low price"""
        exchange = cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART')
        if symbol == 'SPXW':
            symbol = 'SPX'
        if symbol in cfg.INDEX_LIST:
            stock = Index(symbol, exchange, 'USD')
        else:
            stock = Stock(symbol, exchange, 'USD')
        # qualify 
        self.ib.qualifyContracts(stock)
        chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        
        if not chains:
            raise ValueError(f"No option chains found for {symbol}")
            
        chain = next(c for c in chains if c.exchange == exchange)
        strikes = sorted(chain.strikes)
        # drop any strikes that are not ##.0 
        # strikes = [strike for strike in strikes if strike % 1 == 0]

        # get index of strike that is num_strikes above the strike closest to the high price 
        high_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - high_price))
        low_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - low_price))
        
        start_idx = max(0, low_idx - num_strikes)
        end_idx = min(len(strikes)-1, high_idx + num_strikes)
        return strikes[start_idx:end_idx+1]

    def get_underlying_contract(self, symbol: str, type: str = 'STK', exchange:str = 'default', currency: str = 'USD') -> Contract:
        """Get the underlying contract for a symbol"""
        if type == 'STK':
            contract = Stock(symbol.upper(), "SMART" if exchange == 'default' else exchange, currency)
        elif type == 'FUT':
            contract = Future(symbol.upper(), "SMART" if exchange == 'default' else exchange, currency)
        elif type == 'IND':
            contract = Index(symbol.upper(), "SMART" if exchange == 'default' else exchange, currency)
        else:
            raise ValueError(f"Unsupported contract type: {type}")
        
        self.ib.qualifyContracts(contract)
        return contract

    def get_option_strikes_and_expirations(
        self,
        symbol: str,
        start_date: datetime,
        num_strikes: int = 5,
        num_expiries: int = 3,
        current_price: Optional[float] = None,
        high_price: Optional[float] = None,
        low_price: Optional[float] = None
    ) -> Tuple[List[float], List[str]]:
        """Get strikes and expirations with a single chain lookup."""
        if symbol == 'SPXW':
            symbol = 'SPX'
        exchange = cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART')
        if symbol in cfg.INDEX_LIST:
            stock = Index(symbol, exchange, 'USD')
        else:
            stock = Stock(symbol, exchange, 'USD')
        self.ib.qualifyContracts(stock)
        chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)

        if not chains:
            raise ValueError(f"No option chains found for {symbol}")

        chain = next(c for c in chains)
        all_strikes = sorted(chain.strikes)
        # all_strikes = [strike for strike in all_strikes if strike % 1 == 0]

        if high_price is not None and low_price is not None:
            high_idx = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - high_price))
            low_idx = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - low_price))
            start_idx = max(0, low_idx - num_strikes)
            end_idx = min(len(all_strikes) - 1, high_idx + num_strikes)
            selected_strikes = all_strikes[start_idx:end_idx + 1]
        elif current_price is not None:
            closest_idx = min(range(len(all_strikes)), key=lambda i: abs(all_strikes[i] - current_price))
            selected_strikes = all_strikes[closest_idx + 1:closest_idx + 1 + num_strikes]
        else:
            raise ValueError("Provide either current_price or both high_price and low_price")

        valid_expirations = []
        for exp in chain.expirations:
            exp_date = datetime.strptime(exp, '%Y%m%d')
            # if exp_date >= start_date:
            valid_expirations.append(exp)

        valid_expirations.sort()
        return selected_strikes, valid_expirations[:num_expiries]
    
    def get_chains(self, symbol: str) -> List:
        """Get option chains for a symbol"""
        if symbol == 'SPXW':
            symbol = 'SPX'
        exchange = cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART')
        if symbol in cfg.INDEX_LIST:
            stock = Index(symbol, exchange, 'USD')
        else:
            stock = Stock(symbol, exchange, 'USD')
        self.ib.qualifyContracts(stock)
        chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        
        if not chains:
            raise ValueError(f"No option chains found for {symbol}")
            
        return chains

    def create_option_contracts(self, symbol: str, strikes: List[float], 
                              expirations: List[str]) -> List[Contract]:
        """Create option contracts for given strikes and expirations"""
        contracts = []
        for expiry in expirations:
            for strike in strikes:
                try:
                    call = Option(symbol, expiry, strike, 'C', 'SMART')
                    put = Option(symbol, expiry, strike, 'P', 'SMART')
                    contracts.extend([call, put])
                except Exception as e:
                    self.logger.error(f"Error creating contract: {e}")
        
        return self.ib.qualifyContracts(*contracts)
    
    def get_missing_price_history(self, symbol: str, start_date: datetime,
                                end_date: datetime, batch_id: str) -> List[Tuple[datetime, datetime]]:
        """Get date ranges where price history is missing"""
        existing_rows = self.pg_session.query(UnderlyingPriceHistory.date).filter(
            UnderlyingPriceHistory.symbol == symbol,
            UnderlyingPriceHistory.date >= start_date,
            UnderlyingPriceHistory.date <= end_date,
            UnderlyingPriceHistory.collection_batch == batch_id,
        ).order_by(UnderlyingPriceHistory.date.asc()).all()
        # convert end date to datetime since it is usually just the date component 
        # end_date = datetime.combine(end_date, datetime.min.time())
        # set time to 1700
        # create datetime object with time set to 1700
        end_date = datetime.combine(end_date, datetime.min.time()).replace(hour=23, minute=59)

        existing_dates = [row[0] for row in existing_rows]
        
        if not existing_dates:
            return [(start_date, end_date)]
            
        missing_ranges = []
        current_date = start_date
        
        for existing_date in existing_dates:
            if current_date < existing_date:
                missing_ranges.append((current_date, existing_date - timedelta(days=1)))
            current_date = existing_date + timedelta(days=1)
            
        if current_date <= end_date:
            missing_ranges.append((current_date, (end_date)))
            
        return missing_ranges
    
    def collect_historical_data(
        self,
        contracts: List[Contract],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        bar_size: str = '1 min',
        whatToShow: str = "TRADES",
        useRTH: bool = True,
    ) -> Dict:
        """
        Collect historical data for contracts in chunks
        """
        requested_start_date = start_date.date() if isinstance(start_date, datetime) else start_date
        requested_end_date = end_date.date() if isinstance(end_date, datetime) else end_date
        if requested_end_date is None:
            requested_end_date = datetime.now().date()

        # set start date to the earliest available timestamp 
        all_data = {}
        i = 0
        for contract in contracts:
            i += 1
            logging.info(f"Getting contract {i} of {len(contracts)}, {contract.symbol}-{contract.strike}{contract.right}-{contract.lastTradeDateOrContractMonth}")
            contract_start_date = self._resolve_contract_start_date(
                contract=contract,
                requested_start_date=requested_start_date,
                requested_end_date=requested_end_date,
                what_to_show=whatToShow,
                use_rth=useRTH,
            )

            if contract_start_date is None:
                logging.info(f"Skipping {contract.localSymbol}; no resolvable start date")
                continue

            if contract_start_date > requested_end_date:
                logging.info(
                    f"Skipping {contract.localSymbol}; start {contract_start_date} is after end {requested_end_date}"
                )
                continue

            chunks = self.get_date_chunks(contract_start_date, requested_end_date, bar_size)
            contract_data = []
            
            for chunk_start, chunk_end in chunks:
                try:
                    duration = f'{(chunk_end - chunk_start).days + 1} D'
                    logging.info(f"Getting data for chunk {chunk_start} to {chunk_end} @ {bar_size}")
                    bars = self._paced_req_historical_data(
                        contract,
                        endDateTime=chunk_end,
                        durationStr=duration,
                        barSizeSetting=bar_size,
                        whatToShow=whatToShow,
                        useRTH=useRTH,
                        formatDate=1
                    )
                    
                    if bars:
                        df = util.df(bars)
                        df['symbol'] = contract.symbol
                        df['strike'] = contract.strike
                        df['expiry'] = pd.to_datetime(contract.lastTradeDateOrContractMonth)
                        df['right'] = contract.right
                        df['whattoshow'] = whatToShow
                        contract_data.append(df)
                    
                except Exception as e:
                    self.logger.warning(
                        f"Failed to get data for {contract.localSymbol} "
                        f"chunk {chunk_start} to {chunk_end}: {e}"
                    )
                
            if contract_data:
                all_data[contract.conId] = {
                    'contract': contract,
                    'df': pd.concat(contract_data)
                }
        return all_data
    
    def collect_and_store_data(self, symbol: str, start_date: datetime,
                              end_date: datetime, num_strikes: int = 5,
                              num_expiries: int = 3, bar_size: str = '1 min'):
        """
        Main method to collect and store options data with resumption capability
        -
        num_strikes (int): Number of strikes to consider above/below current price
        num_expiries (int): Number of expiries to consider
        bar_size (str): Bar size for historical data e.g. '1 min', '1 day'
        """
        try:
            # Initialize or resume collection batch
            batch_id = self._initialize_collection_batch(symbol, start_date, end_date)
            logging.info(f"Starting collection batch {batch_id}")

            # Get collection status
            status = self._get_collection_status(batch_id)
            if status['status'] == 'COMPLETED':
                return
            logging.info(f"Resuming collection {status['batch_id']} from {status['last_processed_date']}")


            # Resume from last processed date if exists
            last_processed_date = status['last_processed_date']
            

            # Get missing price history ranges
            missing_ranges = self.get_missing_price_history(
                symbol, last_processed_date, end_date, batch_id
            )
            logging.info(f"Missing price history ranges: {missing_ranges}")
            
            for date_range_start, date_range_end in missing_ranges:
                try:
                    # Collect underlying price history for missing range
                    price_history = self.get_underlying_price_history(
                        symbol, date_range_start, date_range_end
                    )                    
                    logging.info(f"Price history for {symbol} from {date_range_start} to {date_range_end}")

                    if not price_history.empty:
                        # Store underlying prices
                        price_history['collection_batch'] = batch_id
                        self._store_underlying_price_history(price_history, batch_id)

                    # Process each day
                    for idx, price_row in price_history.iterrows():
                        iteration_date = price_row['date']
                        try:
                            current_price = price_row['close']
                            current_date = pd.to_datetime(iteration_date)

                            # Collect and store options data for current date
                            logging.info(f"Processing data for {symbol} with {num_strikes} strikes and {num_expiries} expiries on {current_date}")
                            self._process_single_date(
                                symbol, current_date, current_price, price_row['high'], price_row['low'],
                                num_strikes, num_expiries, bar_size, batch_id
                            )
                            
                            # Update checkpoint
                            self._save_checkpoint(batch_id, current_date)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing date {current_date}: {e}")
                            # Continue with next date
                            continue
                            
                except Exception as e:
                    self.logger.error(
                        f"Error processing date range {date_range_start} to {date_range_end}: {e}"
                    )
                    # Continue with next range
                    continue
                    
            final_status = self._finalize_batch_status(batch_id, end_date)
            self.logger.info(f"Batch {batch_id} finalized with status {final_status}")
            
        except Exception as e:
            self.logger.error(f"Error in collection process: {e}")
            raise
            
    def _process_single_date(self, symbol: str, current_date: datetime,
                            current_price: float, high_price:float, low_price:float, num_strikes: int,
                            num_expiries: int, bar_size: str, batch_id: str):
        """Process data collection for a single date"""
        strikes, expirations = self.get_option_strikes_and_expirations(
            symbol=symbol,
            start_date=current_date,
            num_strikes=num_strikes,
            num_expiries=num_expiries,
            high_price=high_price,
            low_price=low_price
        )
        trade_date = current_date.date() if isinstance(current_date, datetime) else current_date
        self._seed_contract_checkpoints(
            batch_id=batch_id,
            symbol=symbol,
            trade_date=trade_date,
            strikes=strikes,
            expirations=expirations,
            bar_size=bar_size,
            what_to_show=cfg.WHATTOSHOW_MAPPING.get(symbol, 'TRADES'),
            use_rth=True,
        )

        queue = self._get_prioritized_contract_queue(
            batch_id=batch_id,
            symbol=symbol,
            trade_date=trade_date,
            reference_price=float(current_price),
        )

        logging.info(f"Queued {len(queue)} contracts for {symbol} on {trade_date}")

        for checkpoint in queue:
            try:
                self._set_contract_checkpoint_status(checkpoint, 'IN_PROGRESS')

                expiry_str = checkpoint.expiry.strftime('%Y%m%d')
                contract = Option(symbol, expiry_str, checkpoint.strike, checkpoint.right, 'SMART')
                qualified_contracts = self.ib.qualifyContracts(contract)
                if not qualified_contracts:
                    self._set_contract_checkpoint_status(
                        checkpoint,
                        'RETRY',
                        last_error='Unable to qualify contract',
                        with_backoff=True,
                    )
                    continue

                historical_data = self.collect_historical_data(
                    contracts=[qualified_contracts[0]],
                    start_date=trade_date,
                    end_date=trade_date + timedelta(days=1),
                    bar_size=bar_size,
                    whatToShow=checkpoint.what_to_show,
                    useRTH=checkpoint.use_rth,
                )

                if not historical_data:
                    is_unavailable, evidence = self._probe_expired_contract_unavailable(
                        qualified_contracts[0],
                        trade_date,
                        bar_size,
                        checkpoint.what_to_show,
                        checkpoint.use_rth,
                    )
                    if is_unavailable and checkpoint.expiry < datetime.now().date():
                        self._set_contract_checkpoint_status(
                            checkpoint,
                            'VERIFIED_UNAVAILABLE_EXPIRED',
                            unavailable_reason='expired_contract_historical_unavailable',
                            probe_evidence=evidence,
                        )
                    else:
                        self._set_contract_checkpoint_status(
                            checkpoint,
                            'RETRY',
                            last_error=evidence,
                            with_backoff=True,
                        )
                    continue

                contract_data = next(iter(historical_data.values()))
                df = contract_data['df']
                if df.empty:
                    self._set_contract_checkpoint_status(
                        checkpoint,
                        'RETRY',
                        last_error='No rows returned for contract request',
                        with_backoff=True,
                    )
                    continue

                df['underlying_price'] = current_price
                df['collection_batch'] = batch_id
                df.drop(columns=['average', 'barCount'], inplace=True, errors='ignore')
                df['date'] = df['date'].dt.tz_localize(None)
                df.drop_duplicates(inplace=True, ignore_index=True)

                for _, row in df.iterrows():
                    row_dt = pd.to_datetime(row['date']).to_pydatetime().replace(tzinfo=None)
                    row_expiry = pd.to_datetime(row['expiry']).date()
                    existing = self.pg_session.query(OptionsHistoricalData.id).filter(
                        OptionsHistoricalData.symbol == row['symbol'],
                        OptionsHistoricalData.strike == float(row['strike']),
                        OptionsHistoricalData.expiry == row_expiry,
                        OptionsHistoricalData.right == row['right'],
                        OptionsHistoricalData.what_to_show == row['whattoshow'],
                        OptionsHistoricalData.date == row_dt,
                    ).first()
                    if existing:
                        continue

                    self.pg_session.add(
                        OptionsHistoricalData(
                            symbol=row['symbol'],
                            strike=float(row['strike']),
                            expiry=row_expiry,
                            right=row['right'],
                            what_to_show=row['whattoshow'],
                            date=row_dt,
                            open=float(row['open']) if not pd.isna(row['open']) else None,
                            high=float(row['high']) if not pd.isna(row['high']) else None,
                            low=float(row['low']) if not pd.isna(row['low']) else None,
                            close=float(row['close']) if not pd.isna(row['close']) else None,
                            volume=int(row['volume']) if not pd.isna(row['volume']) else None,
                            underlying_price=float(row['underlying_price']) if not pd.isna(row['underlying_price']) else None,
                            collection_batch=batch_id,
                        )
                    )
                self.pg_session.commit()

                if self._is_contract_data_saved(
                    symbol=symbol,
                    strike=checkpoint.strike,
                    expiry=checkpoint.expiry,
                    right=checkpoint.right,
                    what_to_show=checkpoint.what_to_show,
                    trade_date=trade_date,
                ):
                    self._set_contract_checkpoint_status(checkpoint, 'COMPLETE')
                else:
                    self._set_contract_checkpoint_status(
                        checkpoint,
                        'RETRY',
                        last_error='Insert completed but verification found no rows',
                        with_backoff=True,
                    )
            except Exception as e:
                self._set_contract_checkpoint_status(
                    checkpoint,
                    'RETRY',
                    last_error=f'Contract processing error: {e}',
                    with_backoff=True,
                )
            
    def close(self):
        """Close the database connection"""
        self.pg_session.close()
        self.db_manager.close()

def main():
    # logging.basicConfig(level=logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(message)s',
        datefmt='%H:%M:%S'
    )
    
    ib = IB()
    try:
        ib.connect('127.0.0.1', cfg.IB_TWS_PORT, clientId=cfg.IB_CLIENT_ID)
        
        

        # _________________________________ REGULAR DATA COLLECTION ___________________________________________
        collector = OptionsDataCollector(ib)
        start_date = datetime.now() - timedelta(days=1) # set start date to yesterday 
        # Only collect new data after 4:15 PM on weekdays 
        end_date = datetime.now()
        if end_date.weekday() == 6 : 
            start_date = datetime.now() - timedelta(days=2) # set start date to friday

        # if start and end dates are the same, nothing to update 
        if start_date.date() == end_date.date():
            logging.info("start and end dates are the same")
            return

        
        for key, values in cfg.COLLECTION_SYMBOLS_METADATA.items():
            collector.collect_and_store_data(
                key,
                start_date.date(),
                end_date,
                num_strikes=values.get('strikes', cfg.DEFAULT_NUM_STRIKES),
                num_expiries=values.get('expiries', cfg.DEFAULT_NUM_EXPIRIES),
                bar_size='1 min'
            )
        collector.close()


        #__________________________________ SKEW DATA COLLECTION ___________________________________________
        # skewcollector = OptionsSkewDataCollector(ib)
        # skewdata = skewcollector.collect_skew_data(
        #     symbols=cfg.SKEW_DATA_SYMBOLS,
        #     force=True,
        #     # strike_wing=cfg.SKEW_DATA_STRIKE_WING
        # )
        
        # print(skewdata) 

        # skewcollector.close() 
    finally:
        # collector.close()
        ib.disconnect()


# ___________________________________________ DEBUG AND TESTING ___________________________________________



def getstrikes(symbol="AVGO", strike=420, num_strikes=5, high_price=None, low_price=None):
    ib = IB()
    try:
        ib.connect('127.0.0.1', 7496, clientId=cfg.IB_CLIENT_ID)
        
        collector = OptionsDataCollector(ib)

        strikes, expirations = collector.get_option_strikes_and_expirations(
            symbol=symbol,
            start_date=datetime.now(),
            num_strikes=num_strikes,
            num_expiries=0,
            current_price=strike,
            high_price=high_price,
            low_price=low_price
        )

        return strikes, expirations
    finally:        ib.disconnect()

def getchains(symbol="AVGO"):
    ib = IB()
    try:
        ib.connect('127.0.0.1', 7496, clientId=cfg.IB_CLIENT_ID)
        
        collector = OptionsDataCollector(ib)

        chains = collector.get_chains(symbol)

        return chains
    finally:
        ib.disconnect()


def gethist(symbol, strike=420, bar_size='1 min', whatToShow="TRADES", useRTH=True):
    ib  = IB()
    
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S'
    )
    try:
        ib.connect('127.0.0.1', 7496, clientId=cfg.IB_CLIENT_ID)
        collector = OptionsDataCollector(ib)


        # strikes and exps 
        strikes, expirations = collector.get_option_strikes_and_expirations(
            symbol=symbol,
            start_date=datetime.now(),
            num_strikes=5,
            num_expiries=1,
            current_price=strike,
            high_price=440,
            low_price=400
        )
        # return strikes, expirations 
        # contracts 
        cons = collector.create_option_contracts(symbol, strikes[:1], expirations[:1])
        # return cons

        return collector.collect_historical_data(
            contracts = cons,
            bar_size=bar_size,
            whatToShow=whatToShow,
            useRTH=useRTH
        )
    finally:
        collector.close()
        ib.disconnect()

if __name__ == "__main__":
    main()