"""ThetaData option-history access with provider-safe failure handling."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import math
import os
from typing import Any, Callable, Dict, List, Optional
from thetadata import ThetaClient

import polars as pl
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import config as cfg

from database import ThetaDataCollectionCheckpoint, ThetaDataOptionHistory


logger = logging.getLogger(__name__)


class ThetaDataEndpointUnavailable(Exception):
    """Raised when the ThetaData service or subscription cannot serve an endpoint."""


@dataclass(frozen=True)
class ThetaDataContract:
    """Contract identity accepted by ThetaData option-history methods."""

    symbol: str
    expiration: date
    strike: float
    right: str

    def endpoint_right(self) -> str:
        normalized_right = self.right.upper()
        if normalized_right == 'C':
            return 'call'
        if normalized_right == 'P':
            return 'put'
        raise ValueError(f"Unsupported option right: {self.right}")


@dataclass(frozen=True)
class EndpointAvailability:
    """Result of checking whether a required ThetaData endpoint is reachable."""

    available: bool
    endpoint: Optional[str] = None
    reason: Optional[str] = None


class ThetaDataOptionsBackfillCollector:
    """Thin ThetaData client adapter used by the historical backfill workflow."""

    PERSIST_CHUNK_SIZE = 1000

    REQUIRED_ENDPOINTS = {
        'quote': 'option_history_quote',
        'ohlc': 'option_history_ohlc',
        'open_interest': 'option_history_open_interest',
        'implied_volatility': 'option_history_greeks_implied_volatility',
        'first_order_greeks': 'option_history_greeks_first_order',
    }
    DISCOVERY_METHODS = ('option_list_dates', 'option_list_expirations', 'option_list_strikes')
    IDENTITY_COLUMNS = ('symbol', 'expiry', 'strike', 'right', 'timestamp', 'interval')
    FIRST_ORDER_GREEKS_COLUMNS = {
        'symbol',
        'expiration',
        'strike',
        'right',
        'timestamp',
        'bid',
        'ask',
        'delta',
        'theta',
        'vega',
        'rho',
        'epsilon',
        'lambda',
        'implied_vol',
    }
    IMPLIED_VOLATILITY_COLUMNS = {
        'symbol',
        'expiration',
        'strike',
        'right',
        'timestamp',
        'bid_implied_vol',
        'ask_implied_vol',
        'implied_vol',
        'iv_error',
        'underlying_price',
    }
    QUOTE_COLUMNS = {
        'symbol', 'expiration', 'strike', 'right', 'timestamp', 'bid', 'ask', 'bid_size', 'ask_size',
    }
    OHLC_COLUMNS = {
        'symbol', 'expiration', 'strike', 'right', 'timestamp', 'open', 'high', 'low', 'close',
        'volume', 'count', 'vwap',
    }
    OPEN_INTEREST_COLUMNS = {
        'symbol', 'expiration', 'strike', 'right', 'timestamp', 'open_interest',
    }

    def __init__(self, client: Optional[Any] = None, client_factory: Optional[Callable[..., Any]] = None):
        self.client = client or self._create_client(client_factory)
        self._validate_client_methods()
        self.AVAILABLE_DATES_BY_EXPIRATION_CACHE: Dict[str, List[date]] = {}

    @staticmethod
    def _create_client(client_factory: Optional[Callable[..., Any]]) -> Any:
        api_key = os.getenv('THETADATA_API_KEY')
        if not api_key:
            raise ThetaDataEndpointUnavailable(
                'ThetaData API key is unavailable; set THETADATA_API_KEY.'
            )

        if client_factory is None:
            client_factory = ThetaClient

        try:
            return client_factory(api_key=api_key, dataframe_type='polars')
        except Exception as exc:
            raise ThetaDataEndpointUnavailable(
                f"Unable to initialize the ThetaData client: {ThetaDataOptionsBackfillCollector._safe_error(exc)}"
            ) from exc

    def _validate_client_methods(self) -> None:
        missing_methods = [
            method_name
            for method_name in self.REQUIRED_ENDPOINTS.values()
            if not callable(getattr(self.client, method_name, None))
        ]
        if missing_methods:
            raise ThetaDataEndpointUnavailable(
                'Installed ThetaData client is missing required methods: ' + ', '.join(missing_methods)
            )

        missing_discovery_methods = [
            method_name
            for method_name in self.DISCOVERY_METHODS
            if not callable(getattr(self.client, method_name, None))
        ]
        if missing_discovery_methods:
            raise ThetaDataEndpointUnavailable(
                'Installed ThetaData client is missing discovery methods: '
                + ', '.join(missing_discovery_methods)
            )

    def discover_contracts(
        self,
        symbol: str,
        reference_price: float,
        num_strikes: int,
        num_expiries: int,
    ) -> List[ThetaDataContract]:
        """Select configured strike and expiration subsets from ThetaData listings."""
        try:
            expiration_frame = self.client.option_list_expirations(symbol=symbol)
        except Exception as exc:
            raise ThetaDataEndpointUnavailable(
                f"option_list_expirations unavailable: {self._safe_error(exc)}"
            ) from exc

        if not isinstance(expiration_frame, pl.DataFrame) or 'expiration' not in expiration_frame.columns:
            raise ThetaDataEndpointUnavailable('option_list_expirations returned an invalid Polars schema.')

        expirations = sorted(
            expiration_frame.get_column('expiration').cast(pl.Date).unique().to_list()
        )[:num_expiries]
        contracts: List[ThetaDataContract] = []
        for expiration in expirations:
            try:
                strike_frame = self.client.option_list_strikes(symbol=symbol, expiration=expiration)
            except Exception as exc:
                raise ThetaDataEndpointUnavailable(
                    f"option_list_strikes unavailable: {self._safe_error(exc)}"
                ) from exc

            if not isinstance(strike_frame, pl.DataFrame) or 'strike' not in strike_frame.columns:
                raise ThetaDataEndpointUnavailable('option_list_strikes returned an invalid Polars schema.')

            strikes = sorted(strike_frame.get_column('strike').cast(pl.Float64).unique().to_list())
            closest_index = min(range(len(strikes)), key=lambda index: abs(strikes[index] - reference_price))
            start_index = max(0, closest_index - num_strikes)
            end_index = min(len(strikes), closest_index + num_strikes + 1)
            for strike in strikes[start_index:end_index]:
                contracts.extend(
                    [
                        ThetaDataContract(symbol, expiration, strike, 'C'),
                        ThetaDataContract(symbol, expiration, strike, 'P'),
                    ]
                )
        return contracts

    def first_available_backfill_date(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> date:
        """Return vendor-confirmed dates from the earliest available expiration."""
        try:
            expiration_frame = self.client.option_list_expirations(symbol=symbol)
            # truncate to earliest available date 
            cutoffdate = date.fromisoformat(cfg.THETADATA_EARLIEST_AVILABLE_DATE) if not start_date else max(date.fromisoformat(cfg.THETADATA_EARLIEST_AVILABLE_DATE), start_date)
            expiration_frame = expiration_frame.filter(pl.col('expiration').cast(pl.Date) >= cutoffdate)

        except Exception as exc:
            raise ThetaDataEndpointUnavailable(
                f"option_list_expirations unavailable: {self._safe_error(exc)}"
            ) from exc

        if not isinstance(expiration_frame, pl.DataFrame) or 'expiration' not in expiration_frame.columns:
            raise ThetaDataEndpointUnavailable('option_list_expirations returned an invalid Polars schema.')

        expirations = sorted(expiration_frame.get_column('expiration').cast(pl.Date).unique().to_list())

        if not expirations:
            return []


        try:
            date_frame = self.client.option_list_dates(
                request_type='quote',
                symbol=symbol,
                expiration=expirations[0],
                strike='*',
                right='both',
            )
        except Exception as exc:
            raise ThetaDataEndpointUnavailable(
                f"option_list_dates unavailable: {self._safe_error(exc)}"
            ) from exc

        if not isinstance(date_frame, pl.DataFrame) or 'date' not in date_frame.columns:
            raise ThetaDataEndpointUnavailable('option_list_dates returned an invalid Polars schema.')

        # truncate to earliest available date
        date_frame = date_frame.filter(pl.col('date').cast(pl.Date) >= cutoffdate)

        # return [
        #     available_date
        #     for available_date in sorted(date_frame.get_column('date').cast(pl.Date).unique().to_list())
        #     if (start_date is None or available_date >= start_date)
        #     and (end_date is None or available_date <= end_date)
        # ]
        return date_frame.drop_nans().drop_nulls().get_column('date').cast(pl.Date).min()

    def _check_and_update_available_dates_cache(
        self,
        symbol: str,
        expiration: date,
    ) -> List[date]:
        """Check the cache for available dates, and update it if necessary."""
        cache_key = f"{symbol}_{expiration.isoformat()}"
        if cache_key in self.AVAILABLE_DATES_BY_EXPIRATION_CACHE:
            return self.AVAILABLE_DATES_BY_EXPIRATION_CACHE[cache_key]

        try:
            # date_frame = self.client.option_list_dates(
            #     request_type='quote',
            #     symbol=symbol,
            #     expiration=expiration,
            #     strike='*',
            #     right='both',
            # )
            # available_dates = date_frame.drop_nans().drop_nulls().get_column('date').cast(pl.Date).to_list()
            available_dates = self.available_quote_dates(symbol=symbol, expiration=expiration)
            self.AVAILABLE_DATES_BY_EXPIRATION_CACHE[cache_key] = available_dates
            return available_dates
        except Exception as exc:
            raise ThetaDataEndpointUnavailable(
                f"option_list_dates unavailable: {self._safe_error(exc)}"
            ) from exc

    def discover_contracts_for_day(
        self,
        symbol: str,
        trade_date: date,
        # high_price: float,
        # low_price: float,
        num_strikes: int,
        num_expiries: int,
    ) -> List[ThetaDataContract]:
        """Select the next expiries and strike wings for one underlying trading day."""
        logging.debug('Discovering ThetaData contracts for %s on %s', symbol, trade_date)
        try:
            expiration_frame = self.client.option_list_expirations(symbol=symbol)
            expiration_frame = expiration_frame.filter(pl.col('expiration').cast(pl.Date) >= trade_date)
        except Exception as exc:
            raise ThetaDataEndpointUnavailable(
                f"option_list_expirations unavailable: {self._safe_error(exc)}"
            ) from exc

        if not isinstance(expiration_frame, pl.DataFrame) or 'expiration' not in expiration_frame.columns:
            raise ThetaDataEndpointUnavailable('option_list_expirations returned an invalid Polars schema.')

        expirations = [
            expiration
            for expiration in sorted(expiration_frame.get_column('expiration').cast(pl.Date).unique().to_list())
        ]#[:num_expiries]

        # check if vendor has data for the target date and expiry
        expirations_with_data = [] 
        logging.debug('Checking available data for expirations for %s on %s', symbol, trade_date)
        for exp in sorted(expirations): 
            try:
                available_dates = self._check_and_update_available_dates_cache(symbol=symbol, expiration=exp)
                if trade_date in available_dates:
                    expirations_with_data.append(exp)

                if len(expirations_with_data) >= num_expiries:
                    break
            except Exception as exc:
                raise ThetaDataEndpointUnavailable(
                    f"Error checking available dates: {self._safe_error(exc)}"
                ) from exc

        if len(expirations_with_data) == 0:
            logging.warning('No expirations with available data for %s on %s', symbol, trade_date)
            exit() 
            return None 
        
        logging.debug('Found %d expirations with available data for %s on %s', len(expirations_with_data), symbol, trade_date)
        expirations = sorted(expirations_with_data[:num_expiries])

        # determine high/low prices to establish strike bounds 
        strike_frame = self.client.option_list_strikes(symbol=symbol, expiration=expirations[0])
        strikes = sorted(strike_frame.get_column('strike').cast(pl.Float64).unique().to_list())
        middle_strike = strikes[len(strikes) // 2]
        contract = ThetaDataContract(symbol, expirations[0], middle_strike, 'C')

        implied_volatility = self.fetch_implied_volatility(contract, trade_date, '1m')
        high_price = implied_volatility.get_column('underlying_price').drop_nulls().drop_nans().max()
        low_price = implied_volatility.get_column('underlying_price').drop_nulls().drop_nans().min()

        contracts: List[ThetaDataContract] = []
        for expiration in expirations:
            try:
                strike_frame = self.client.option_list_strikes(symbol=symbol, expiration=expiration)
            except Exception as exc:
                raise ThetaDataEndpointUnavailable(
                    f"option_list_strikes unavailable: {self._safe_error(exc)}"
                ) from exc

            if not isinstance(strike_frame, pl.DataFrame) or 'strike' not in strike_frame.columns:
                raise ThetaDataEndpointUnavailable('option_list_strikes returned an invalid Polars schema.')

            strikes = sorted(strike_frame.get_column('strike').cast(pl.Float64).unique().to_list())
            if not strikes:
                continue

            low_index = min(range(len(strikes)), key=lambda index: abs(strikes[index] - low_price))
            high_index = min(range(len(strikes)), key=lambda index: abs(strikes[index] - high_price))
            start_index = max(0, low_index - num_strikes)
            end_index = min(len(strikes), high_index + num_strikes + 1)

            # print(strikes[start_index:end_index])
            # print(low_price, high_price) 
            
            for strike in strikes[start_index:end_index]:
                contracts.extend(
                    [
                        ThetaDataContract(symbol, expiration, strike, 'C'),
                        ThetaDataContract(symbol, expiration, strike, 'P'),
                    ]
                )
        return contracts

    def backfill_daily_subsets(
        self,
        session: Any,
        symbol: str,
        daily_prices: Dict[date, Dict[str, float]],
        num_strikes: int,
        num_expiries: int,
        intervals: List[str],
        collection_batch: str,
        session_factory: Optional[Callable[[], Any]] = None,
        max_workers: int = 4,
    ) -> int:
        """Backfill contracts selected independently for every vendor-confirmed day."""
        stored_count = 0

        def collect_one(contract: ThetaDataContract, trade_date: date, interval: str) -> int:
            worker_session = session_factory()
            try:
                return self.collect_contract_day(
                    session=worker_session,
                    contract=contract,
                    request_date=trade_date,
                    interval=interval,
                    collection_batch=collection_batch,
                )
            finally:
                worker_session.close()

        executor = None
        if session_factory is not None:
            executor = ThreadPoolExecutor(max_workers=max_workers)

        try:
            for trade_date, prices in sorted(daily_prices.items()):
                start_time = datetime.now()
                logging.info('Starting backfill for %s on %s', symbol, trade_date)
                contracts = self.discover_contracts_for_day(
                    symbol=symbol,
                    trade_date=trade_date,
                    num_strikes=num_strikes,
                    num_expiries=num_expiries,
                )
                logging.info('Backfilling %d contracts for %s on %s', len(contracts), symbol, trade_date)
                # print(len(contracts))
                # print(num_expiries, num_strikes)
                # exit() 

                if not contracts:
                    logging.warning('No ThetaData contracts discovered for %s on %s.', symbol, trade_date)
                    continue

                work_items = [
                    (contract, interval)
                    for contract in contracts
                    for interval in intervals
                ]

                if executor is None:
                    for contract, interval in work_items:
                        stored_count += self.collect_contract_day(
                            session=session,
                            contract=contract,
                            request_date=trade_date,
                            interval=interval,
                            collection_batch=collection_batch,
                        )
                else:
                    futures = [
                        executor.submit(collect_one, contract, trade_date, interval)
                        for contract, interval in work_items
                    ]
                    stored_count += sum(future.result() for future in futures)

                elapsed = datetime.now() - start_time
                logging.info(
                    'Finished backfill for %s on %s in %s (seconds=%.2f)',
                    symbol,
                    trade_date,
                    elapsed,
                    elapsed.total_seconds(),
                )
        finally:
            if executor is not None:
                executor.shutdown(wait=True)

        return stored_count

    def available_quote_dates(
        self,
        # contract: ThetaDataContract,
        symbol: str,
        expiration: date,
        request_type: str = 'quote',
        strike: int = '*',
        right: str = 'both',
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[date]:
        """Return actual available quote dates for a contract, optionally bounded by callers."""
        try:
            date_frame = self.client.option_list_dates(
                request_type=request_type,
                symbol=symbol,
                expiration=expiration,
                strike=strike,
                right=right,
            )

            # truncate to earliest available date
            date_frame = date_frame.filter(pl.col('date').cast(pl.Date) >= date.fromisoformat(cfg.THETADATA_EARLIEST_AVILABLE_DATE))
        except Exception as exc:
            raise ThetaDataEndpointUnavailable(
                f"option_list_dates unavailable: {self._safe_error(exc)}"
            ) from exc

        if not isinstance(date_frame, pl.DataFrame) or 'date' not in date_frame.columns:
            raise ThetaDataEndpointUnavailable('option_list_dates returned an invalid Polars schema.')

        available_dates = sorted(date_frame.get_column('date').cast(pl.Date).unique().to_list())
        return [
            available_date
            for available_date in available_dates
            if (start_date is None or available_date >= start_date)
            and (end_date is None or available_date <= end_date)
        ]

    def backfill_contracts(
        self,
        session: Any,
        contracts: List[ThetaDataContract],
        intervals: List[str],
        collection_batch: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> int:
        """Backfill requested contracts only on vendor-confirmed quote dates."""
        stored_count = 0
        for contract in contracts:
            quote_dates = self.available_quote_dates(contract, start_date, end_date)
            for request_date in quote_dates:
                for interval in intervals:
                    stored_count += self.collect_contract_day(
                        session=session,
                        contract=contract,
                        request_date=request_date,
                        interval=interval,
                        collection_batch=collection_batch,
                    )
        return stored_count

    def backfill_symbol(
        self,
        session: Any,
        symbol: str,
        reference_price: float,
        num_strikes: int,
        num_expiries: int,
        intervals: List[str],
        collection_batch: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> int:
        """Discover and backfill the configured ThetaData contract subset for one symbol."""
        contracts = self.discover_contracts(
            symbol=symbol,
            reference_price=reference_price,
            num_strikes=num_strikes,
            num_expiries=num_expiries,
        )
        if not contracts:
            logger.warning('No ThetaData contracts discovered for %s.', symbol)
            return 0
        return self.backfill_contracts(
            session=session,
            contracts=contracts,
            intervals=intervals,
            collection_batch=collection_batch,
            start_date=start_date,
            end_date=end_date,
        )

    def preflight(self, contract: ThetaDataContract, request_date: date) -> EndpointAvailability:
        """Verify every required endpoint for a known contract before backfill work starts."""
        for endpoint, method_name in self.REQUIRED_ENDPOINTS.items():
            try:
                self._call_history_endpoint(
                    method_name=method_name,
                    contract=contract,
                    request_date=request_date,
                    interval='1m',
                )
            except ThetaDataEndpointUnavailable as exc:
                logger.warning('ThetaData endpoint unavailable: endpoint=%s reason=%s', endpoint, exc)
                return EndpointAvailability(available=False, endpoint=endpoint, reason=str(exc))
            except Exception as exc:
                if self._is_no_data_error(exc):
                    continue
                reason = self._safe_error(exc)
                logger.warning('ThetaData endpoint unavailable: endpoint=%s reason=%s', endpoint, reason)
                return EndpointAvailability(available=False, endpoint=endpoint, reason=reason)

        return EndpointAvailability(available=True)

    def fetch_first_order_greeks(
        self,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
    ) -> pl.DataFrame:
        """Fetch and normalize documented first-order Greeks for one option contract/day."""
        raw_frame = self._call_history_endpoint(
            method_name=self.REQUIRED_ENDPOINTS['first_order_greeks'],
            contract=contract,
            request_date=request_date,
            interval=interval,
        )
        return self._normalize_first_order_greeks(raw_frame, interval)

    def fetch_implied_volatility(
        self,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
    ) -> pl.DataFrame:
        """Fetch documented bid, ask, and midpoint implied-volatility values."""
        raw_frame = self._call_history_endpoint(
            method_name=self.REQUIRED_ENDPOINTS['implied_volatility'],
            contract=contract,
            request_date=request_date,
            interval=interval,
        )
        return self._normalize_implied_volatility(raw_frame, interval)

    def fetch_iv_and_first_order_greeks(
        self,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
    ) -> pl.DataFrame:
        """Fetch and merge the documented IV and first-order Greek endpoint values."""
        implied_volatility = self.fetch_implied_volatility(contract, request_date, interval)
        first_order_greeks = self.fetch_first_order_greeks(contract, request_date, interval)
        return self._merge_history_frames(implied_volatility, first_order_greeks)

    def fetch_contract_history(
        self,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
    ) -> pl.DataFrame:
        """Fetch every requested option-history dataset for one contract and trading day."""
        quote = self._fetch_and_normalize(
            'quote', contract, request_date, interval, self.QUOTE_COLUMNS,
            {'bid': 'bid', 'ask': 'ask', 'bid_size': 'bid_size', 'ask_size': 'ask_size'},
        )
        ohlc = self._fetch_and_normalize(
            'ohlc', contract, request_date, interval, self.OHLC_COLUMNS,
            {'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume',
             'count': 'trade_count', 'vwap': 'vwap'},
        )
        combined = self._merge_history_frames(quote, ohlc)
        combined = self._merge_history_frames(
            combined,
            self.fetch_iv_and_first_order_greeks(contract, request_date, interval),
        )
        open_interest = self._fetch_and_normalize(
            'open_interest', contract, request_date, interval, self.OPEN_INTEREST_COLUMNS,
            {'open_interest': 'open_interest'},
        )
        return self._attach_daily_open_interest(combined, open_interest)

    def persist_first_order_greeks(
        self,
        session: Any,
        frame: pl.DataFrame,
        collection_batch: str,
    ) -> int:
        """Insert or enrich first-order Greek rows without replacing stored values with nulls."""
        records = [
            {
                **{
                    column_name: (
                        value if not isinstance(value, float) or math.isfinite(value) else None
                    )
                    for column_name, value in record.items()
                },
                'collection_batch': collection_batch,
            }
            for record in frame.to_dicts()
        ]
        if not records:
            return 0

        dialect_name = session.get_bind().dialect.name
        if dialect_name == 'postgresql':
            insert_factory = postgresql_insert
        elif dialect_name == 'sqlite':
            insert_factory = sqlite_insert
        else:
            raise NotImplementedError(
                f'Bulk ThetaData history upserts are not supported for {dialect_name}.'
            )

        identity_columns = {'symbol', 'expiry', 'strike', 'right', 'interval', 'timestamp'}
        update_columns = [
            column_name
            for column_name in records[0]
            if column_name not in identity_columns and column_name != 'collection_batch'
        ]

        for start_index in range(0, len(records), self.PERSIST_CHUNK_SIZE):
            chunk = records[start_index:start_index + self.PERSIST_CHUNK_SIZE]
            statement = insert_factory(ThetaDataOptionHistory).values(chunk)
            update_values = {}
            for column_name in update_columns:
                model_attribute = getattr(ThetaDataOptionHistory, column_name)
                database_column = model_attribute.property.columns[0]
                update_values[database_column.name] = func.coalesce(
                    statement.excluded[database_column.name],
                    database_column,
                )

            statement = statement.on_conflict_do_update(
                index_elements=[
                    ThetaDataOptionHistory.symbol,
                    ThetaDataOptionHistory.expiry,
                    ThetaDataOptionHistory.strike,
                    ThetaDataOptionHistory.right,
                    ThetaDataOptionHistory.interval,
                    ThetaDataOptionHistory.timestamp,
                ],
                set_={
                    **update_values,
                    'collection_batch': statement.excluded.collection_batch,
                    'updated_at': func.now(),
                },
            )
            session.execute(statement)

        return len(records)

    def collect_contract_day(   
        self,
        session: Any,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
        collection_batch: str,
    ) -> int:
        """Collect a contract/day/interval once and checkpoint only a successful merged write."""
        logging.debug('Collecting ThetaData for %s on %s at interval %s', contract, request_date, interval)
        checkpoint = self._get_or_create_contract_checkpoint(
            session=session,
            contract=contract,
            request_date=request_date,
            interval=interval,
            collection_batch=collection_batch,
        )
        if checkpoint.status == 'COMPLETE':
            logging.info('ThetaData already collected for %s on %s at interval %s', contract, request_date, interval)
            return 0
        if checkpoint.next_retry_at is not None and checkpoint.next_retry_at > datetime.now():
            return 0

        checkpoint.status = 'IN_PROGRESS'
        checkpoint.attempts = (checkpoint.attempts or 0) + 1
        checkpoint.next_retry_at = None
        # session.commit()

        try:
            fetch_start = datetime.now()
            frame = self.fetch_contract_history(contract, request_date, interval)
            fetch_elapsed = datetime.now() - fetch_start
            logging.debug(
                'ThetaData fetch for %s on %s at interval %s took %s (seconds=%.2f)',
                contract,
                request_date,
                interval,
                fetch_elapsed,
                fetch_elapsed.total_seconds(),
            )

            persist_start = datetime.now()
            stored_count = self.persist_first_order_greeks(session, frame, collection_batch)
            persist_elapsed = datetime.now() - persist_start
            logging.debug(
                'ThetaData persist for %s on %s at interval %s took %s (seconds=%.2f)',
                contract,
                request_date,
                interval,
                persist_elapsed,
                persist_elapsed.total_seconds(),
            )

        except ThetaDataEndpointUnavailable as exc:
            checkpoint.status = 'RETRY'
            checkpoint.last_error = str(exc)[:500]
            checkpoint.next_retry_at = datetime.now() + timedelta(
                minutes=min(60, 2 ** checkpoint.attempts)
            )
            session.commit()
            raise
        except Exception as exc:
            checkpoint.status = 'RETRY'
            checkpoint.last_error = self._safe_error(exc)
            checkpoint.next_retry_at = datetime.now() + timedelta(
                minutes=min(60, 2 ** checkpoint.attempts)
            )
            session.commit()
            raise

        checkpoint.status = 'COMPLETE'
        checkpoint.completed_at = datetime.now()
        checkpoint.last_error = None
        checkpoint.next_retry_at = None
        session.commit()
        return stored_count

    @staticmethod
    def _get_or_create_contract_checkpoint(
        session: Any,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
        collection_batch: str,
    ) -> ThetaDataCollectionCheckpoint:
        checkpoint = session.query(ThetaDataCollectionCheckpoint).filter_by(
            symbol=contract.symbol,
            expiry=contract.expiration,
            strike=contract.strike,
            right=contract.right,
            interval=interval,
            trade_date=request_date,
            dataset='full_history',
        ).first()
        if checkpoint is not None:
            return checkpoint

        checkpoint = ThetaDataCollectionCheckpoint(
            symbol=contract.symbol,
            expiry=contract.expiration,
            strike=contract.strike,
            right=contract.right,
            interval=interval,
            trade_date=request_date,
            dataset='full_history',
            status='PENDING',
            collection_batch=collection_batch,
        )
        session.add(checkpoint)
        session.commit()
        return checkpoint

    def record_endpoint_unavailable(
        self,
        session: Any,
        symbol: str,
        endpoint: str,
        reason: str,
        now: Optional[datetime] = None,
    ) -> ThetaDataCollectionCheckpoint:
        """Persist endpoint unavailability without completing any outstanding data work."""
        now = now or datetime.now()
        checkpoint = session.query(ThetaDataCollectionCheckpoint).filter_by(
            symbol=symbol,
            dataset=f'endpoint:{endpoint}',
        ).order_by(ThetaDataCollectionCheckpoint.id.desc()).first()

        if checkpoint is None:
            checkpoint = ThetaDataCollectionCheckpoint(
                symbol=symbol,
                dataset=f'endpoint:{endpoint}',
                status='ENDPOINT_UNAVAILABLE',
                attempts=0,
            )
            session.add(checkpoint)

        checkpoint.attempts = (checkpoint.attempts or 0) + 1
        checkpoint.status = 'ENDPOINT_UNAVAILABLE'
        checkpoint.last_error = reason[:500]
        checkpoint.next_retry_at = now + timedelta(minutes=min(60, 2 ** checkpoint.attempts))
        checkpoint.completed_at = None
        session.commit()
        return checkpoint

    def _call_history_endpoint(
        self,
        method_name: str,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
    ) -> pl.DataFrame:
        method = getattr(self.client, method_name)
        parameters: Dict[str, Any] = {
            'symbol': contract.symbol,
            'expiration': contract.expiration,
            'date': request_date,
            'strike': f'{contract.strike:.6f}',
            'right': contract.endpoint_right(),
        }
        if method_name != self.REQUIRED_ENDPOINTS['open_interest']:
            parameters['interval'] = interval
        if method_name == self.REQUIRED_ENDPOINTS['implied_volatility']:
            parameters['version'] = 'latest'

        try:
            result = method(**parameters)
        except Exception as exc:
            if self._is_no_data_error(exc):
                return pl.DataFrame()
            raise ThetaDataEndpointUnavailable(
                f"{method_name} unavailable: {self._safe_error(exc)}"
            ) from exc

        if not isinstance(result, pl.DataFrame):
            raise ThetaDataEndpointUnavailable(
                f"{method_name} returned {type(result).__name__}; expected a Polars DataFrame."
            )
        return result

    def _normalize_first_order_greeks(self, frame: pl.DataFrame, interval: str) -> pl.DataFrame:
        if frame.is_empty():
            return self._empty_first_order_greeks_frame()

        missing_columns = self.FIRST_ORDER_GREEKS_COLUMNS - set(frame.columns)
        if missing_columns:
            raise ThetaDataEndpointUnavailable(
                'First-order Greeks response is missing required columns: '
                + ', '.join(sorted(missing_columns))
            )

        return (
            frame.select(
                pl.col('symbol').cast(pl.String),
                pl.col('expiration').cast(pl.Date).alias('expiry'),
                pl.col('strike').cast(pl.Float64),
                pl.when(pl.col('right').cast(pl.String).str.to_lowercase() == 'call')
                .then(pl.lit('C'))
                .when(pl.col('right').cast(pl.String).str.to_lowercase() == 'put')
                .then(pl.lit('P'))
                .otherwise(pl.col('right').cast(pl.String))
                .alias('right'),
                pl.col('timestamp').cast(pl.Datetime),
                pl.col('bid').cast(pl.Float64),
                pl.col('ask').cast(pl.Float64),
                pl.col('delta').cast(pl.Float64),
                pl.col('theta').cast(pl.Float64),
                pl.col('vega').cast(pl.Float64),
                pl.col('rho').cast(pl.Float64),
                pl.col('epsilon').cast(pl.Float64),
                pl.col('lambda').cast(pl.Float64).alias('lambda_'),
                pl.col('implied_vol').cast(pl.Float64).alias('implied_volatility'),
            )
            .with_columns(pl.lit(interval).alias('interval'))
        )

    def _normalize_implied_volatility(self, frame: pl.DataFrame, interval: str) -> pl.DataFrame:
        if frame.is_empty():
            return self._empty_implied_volatility_frame()

        missing_columns = self.IMPLIED_VOLATILITY_COLUMNS - set(frame.columns)
        if missing_columns:
            raise ThetaDataEndpointUnavailable(
                'Implied-volatility response is missing required columns: '
                + ', '.join(sorted(missing_columns))
            )

        return (
            frame.select(
                pl.col('symbol').cast(pl.String),
                pl.col('expiration').cast(pl.Date).alias('expiry'),
                pl.col('strike').cast(pl.Float64),
                pl.when(pl.col('right').cast(pl.String).str.to_lowercase() == 'call')
                .then(pl.lit('C'))
                .when(pl.col('right').cast(pl.String).str.to_lowercase() == 'put')
                .then(pl.lit('P'))
                .otherwise(pl.col('right').cast(pl.String))
                .alias('right'),
                pl.col('timestamp').cast(pl.Datetime),
                pl.col('bid_implied_vol').cast(pl.Float64),
                pl.col('ask_implied_vol').cast(pl.Float64),
                pl.col('implied_vol').cast(pl.Float64).alias('implied_volatility'),
                pl.col('iv_error').cast(pl.Float64),
                pl.col('underlying_price').cast(pl.Float64),
            )
            .with_columns(pl.lit(interval).alias('interval'))
        )

    def _fetch_and_normalize(
        self,
        endpoint: str,
        contract: ThetaDataContract,
        request_date: date,
        interval: str,
        required_columns: set[str],
        field_mapping: Dict[str, str],
    ) -> pl.DataFrame:
        raw_frame = self._call_history_endpoint(
            method_name=self.REQUIRED_ENDPOINTS[endpoint],
            contract=contract,
            request_date=request_date,
            interval=interval,
        )
        return self._normalize_endpoint_frame(raw_frame, interval, endpoint, required_columns, field_mapping)

    def _normalize_endpoint_frame(
        self,
        frame: pl.DataFrame,
        interval: str,
        endpoint: str,
        required_columns: set[str],
        field_mapping: Dict[str, str],
    ) -> pl.DataFrame:
        if frame.is_empty():
            return pl.DataFrame(schema=self._history_schema(field_mapping.values()))

        missing_columns = required_columns - set(frame.columns)
        if missing_columns:
            raise ThetaDataEndpointUnavailable(
                f"{endpoint} response is missing required columns: " + ', '.join(sorted(missing_columns))
            )

        return frame.select(
            pl.col('symbol').cast(pl.String),
            pl.col('expiration').cast(pl.Date).alias('expiry'),
            pl.col('strike').cast(pl.Float64),
            pl.when(pl.col('right').cast(pl.String).str.to_lowercase() == 'call')
            .then(pl.lit('C'))
            .when(pl.col('right').cast(pl.String).str.to_lowercase() == 'put')
            .then(pl.lit('P'))
            .otherwise(pl.col('right').cast(pl.String))
            .alias('right'),
            pl.col('timestamp').cast(pl.Datetime),
            *[
                pl.col(source_column).cast(pl.Float64).alias(target_column)
                for source_column, target_column in field_mapping.items()
            ],
        ).with_columns(pl.lit(interval).alias('interval'))

    @staticmethod
    def _history_schema(fields: Any) -> Dict[str, pl.DataType]:
        schema: Dict[str, pl.DataType] = {
            'symbol': pl.String,
            'expiry': pl.Date,
            'strike': pl.Float64,
            'right': pl.String,
            'timestamp': pl.Datetime,
            'interval': pl.String,
        }
        schema.update({field: pl.Float64 for field in fields})
        return schema

    def _attach_daily_open_interest(self, history: pl.DataFrame, open_interest: pl.DataFrame) -> pl.DataFrame:
        if history.is_empty() or open_interest.is_empty():
            return history

        latest_open_interest = (
            open_interest.sort('timestamp', descending=True).get_column('open_interest').drop_nulls()
        )
        if latest_open_interest.is_empty():
            return history
        return history.with_columns(pl.lit(latest_open_interest[0]).alias('open_interest'))

    def _merge_history_frames(self, left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        """Outer-join normalized endpoint frames while keeping the provider identity stable."""
        if left.is_empty():
            return right
        if right.is_empty():
            return left

        joined = left.join(right, on=list(self.IDENTITY_COLUMNS), how='full', suffix='_right')
        return joined.select(
            *[pl.coalesce([pl.col(column), pl.col(f'{column}_right')]).alias(column)
              if f'{column}_right' in joined.columns else pl.col(column)
              for column in self.IDENTITY_COLUMNS],
            *[
                pl.col(column)
                for column in left.columns
                if column not in self.IDENTITY_COLUMNS
            ],
            *[
                pl.col(column)
                for column in right.columns
                if column not in self.IDENTITY_COLUMNS and column not in left.columns
            ],
        )

    @staticmethod
    def _empty_first_order_greeks_frame() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                'symbol': pl.String,
                'expiry': pl.Date,
                'strike': pl.Float64,
                'right': pl.String,
                'timestamp': pl.Datetime,
                'bid': pl.Float64,
                'ask': pl.Float64,
                'delta': pl.Float64,
                'theta': pl.Float64,
                'vega': pl.Float64,
                'rho': pl.Float64,
                'epsilon': pl.Float64,
                'lambda_': pl.Float64,
                'implied_volatility': pl.Float64,
                'interval': pl.String,
            }
        )

    @staticmethod
    def _empty_implied_volatility_frame() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                'symbol': pl.String,
                'expiry': pl.Date,
                'strike': pl.Float64,
                'right': pl.String,
                'timestamp': pl.Datetime,
                'bid_implied_vol': pl.Float64,
                'ask_implied_vol': pl.Float64,
                'implied_volatility': pl.Float64,
                'iv_error': pl.Float64,
                'underlying_price': pl.Float64,
                'interval': pl.String,
            }
        )

    @staticmethod
    def _is_no_data_error(exc: Exception) -> bool:
        return exc.__class__.__name__ == 'NoDataFoundError'

    @staticmethod
    def _safe_error(exc: Exception) -> str:
        message = str(exc).strip().replace('\n', ' ')
        return message[:500] or exc.__class__.__name__
