from datetime import date, datetime
from unittest.mock import patch
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

import polars as pl
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import Base, ThetaDataCollectionCheckpoint, ThetaDataOptionHistory
from option_data_collector import ThetaDataBackfillService
from thetadata_collector import ThetaDataContract, ThetaDataOptionsBackfillCollector


class FakeThetaClient:
    def __init__(self, unavailable_method=None):
        self.calls = []
        self.unavailable_method = unavailable_method

    def option_history_quote(self, **kwargs):
        return self._response('option_history_quote', **kwargs)

    def option_history_ohlc(self, **kwargs):
        return self._response('option_history_ohlc', **kwargs)

    def option_history_open_interest(self, **kwargs):
        return self._response('option_history_open_interest', **kwargs)

    def option_history_greeks_implied_volatility(self, **kwargs):
        return self._response('option_history_greeks_implied_volatility', **kwargs)

    def option_history_greeks_first_order(self, **kwargs):
        return self._response('option_history_greeks_first_order', **kwargs)

    def option_list_expirations(self, **kwargs):
        self.calls.append(('option_list_expirations', kwargs))
        return pl.DataFrame(
            {
                'symbol': ['SPX', 'SPX'],
                'expiration': [date(2026, 9, 18), date(2026, 10, 16)],
            }
        )

    def option_list_strikes(self, **kwargs):
        self.calls.append(('option_list_strikes', kwargs))
        return pl.DataFrame({'symbol': ['SPX'] * 3, 'strike': [6400.0, 6500.0, 6600.0]})

    def option_list_dates(self, **kwargs):
        self.calls.append(('option_list_dates', kwargs))
        return pl.DataFrame({'date': [date(2026, 8, 7), date(2026, 8, 10), date(2026, 8, 11)]})

    def _response(self, method_name, **kwargs):
        self.calls.append((method_name, kwargs))
        if method_name == self.unavailable_method:
            raise RuntimeError('endpoint is unavailable')
        right = kwargs.get('right', 'call')
        if method_name == 'option_history_quote':
            return pl.DataFrame(
                {
                    'symbol': ['SPX'],
                    'expiration': [date(2026, 9, 18)],
                    'strike': [6500.0],
                    'right': [right],
                    'timestamp': [datetime(2026, 8, 10, 10, 0)],
                    'bid': [10.0],
                    'ask': [11.0],
                    'bid_size': [20],
                    'ask_size': [25],
                }
            )
        if method_name == 'option_history_ohlc':
            return pl.DataFrame(
                {
                    'symbol': ['SPX'],
                    'expiration': [date(2026, 9, 18)],
                    'strike': [6500.0],
                    'right': [right],
                    'timestamp': [datetime(2026, 8, 10, 10, 0)],
                    'open': [10.25],
                    'high': [11.5],
                    'low': [10.0],
                    'close': [11.0],
                    'volume': [100],
                    'count': [10],
                    'vwap': [10.75],
                }
            )
        if method_name == 'option_history_open_interest':
            return pl.DataFrame(
                {
                    'symbol': ['SPX'],
                    'expiration': [date(2026, 9, 18)],
                    'strike': [6500.0],
                    'right': [right],
                    'timestamp': [datetime(2026, 8, 10, 6, 30)],
                    'open_interest': [500],
                }
            )
        if method_name == 'option_history_greeks_implied_volatility':
            return pl.DataFrame(
                {
                    'symbol': ['SPX'],
                    'expiration': [date(2026, 9, 18)],
                    'strike': [6500.0],
                    'right': [right],
                    'timestamp': [datetime(2026, 8, 10, 10, 0)],
                    'bid_implied_vol': [0.19],
                    'ask_implied_vol': [0.21],
                    'implied_vol': [0.2],
                    'iv_error': [0.001],
                    'underlying_price': [6490.0],
                }
            )
        if method_name != 'option_history_greeks_first_order':
            return pl.DataFrame()

        return pl.DataFrame(
            {
                'symbol': ['SPX'],
                'expiration': [date(2026, 9, 18)],
                'strike': [6500.0],
                'right': [right],
                'timestamp': [datetime(2026, 8, 10, 10, 0)],
                'bid': [10.0],
                'ask': [11.0],
                'delta': [0.5],
                'theta': [-0.1],
                'vega': [0.2],
                'rho': [0.05],
                'epsilon': [0.01],
                'lambda': [2.5],
                'implied_vol': [0.2],
            }
        )


class ThetaDataOptionsBackfillCollectorTests(unittest.TestCase):
    def setUp(self):
        self.contract = ThetaDataContract(
            symbol='SPX',
            expiration=date(2026, 9, 18),
            strike=6500.0,
            right='C',
        )
        self.request_date = date(2026, 8, 10)

    def test_preflight_uses_python_client_contract_arguments(self):
        client = FakeThetaClient()
        collector = ThetaDataOptionsBackfillCollector(client=client)

        result = collector.preflight(self.contract, self.request_date)

        self.assertTrue(result.available)
        self.assertEqual(len(client.calls), 5)
        for method_name, kwargs in client.calls:
            self.assertEqual(kwargs['expiration'], self.contract.expiration)
            self.assertEqual(kwargs['date'], self.request_date)
            self.assertEqual(kwargs['strike'], '6500.000000')
            self.assertEqual(kwargs['right'], 'call')
            if method_name == 'option_history_open_interest':
                self.assertNotIn('interval', kwargs)
            else:
                self.assertEqual(kwargs['interval'], '1m')

    def test_creates_polars_client_with_api_key(self):
        created_with = {}

        def client_factory(**kwargs):
            created_with.update(kwargs)
            return FakeThetaClient()

        with patch.dict('os.environ', {'THETADATA_API_KEY': 'test-key'}, clear=True):
            ThetaDataOptionsBackfillCollector(client_factory=client_factory)

        self.assertEqual(created_with['api_key'], 'test-key')
        self.assertEqual(created_with['dataframe_type'], 'polars')
        self.assertNotIn('email', created_with)
        self.assertNotIn('password', created_with)

    def test_normalizes_documented_first_order_greek_columns(self):
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())

        result = collector.fetch_first_order_greeks(self.contract, self.request_date, '1h')

        self.assertEqual(result.item(0, 'right'), 'C')
        self.assertEqual(result.item(0, 'lambda_'), 2.5)
        self.assertEqual(result.item(0, 'implied_volatility'), 0.2)
        self.assertEqual(result.item(0, 'interval'), '1h')

    def test_preflight_returns_unavailable_status_without_raising(self):
        collector = ThetaDataOptionsBackfillCollector(
            client=FakeThetaClient(unavailable_method='option_history_ohlc')
        )

        result = collector.preflight(self.contract, self.request_date)

        self.assertFalse(result.available)
        self.assertEqual(result.endpoint, 'ohlc')
        self.assertIn('option_history_ohlc unavailable', result.reason)

    def test_discovers_configured_contract_subset(self):
        client = FakeThetaClient()
        collector = ThetaDataOptionsBackfillCollector(client=client)

        contracts = collector.discover_contracts('SPX', 6500.0, num_strikes=1, num_expiries=1)

        self.assertEqual(len(contracts), 6)
        self.assertEqual({contract.right for contract in contracts}, {'C', 'P'})
        self.assertEqual({contract.strike for contract in contracts}, {6400.0, 6500.0, 6600.0})
        self.assertEqual(contracts[0].expiration, date(2026, 9, 18))

    def test_anchors_backfill_dates_to_earliest_expiry(self):
        client = FakeThetaClient()
        collector = ThetaDataOptionsBackfillCollector(client=client)

        dates = collector.first_available_backfill_date('SPX')

        self.assertEqual(dates[0], date(2026, 8, 7))
        _, request = client.calls[-1]
        self.assertEqual(request['expiration'], date(2026, 9, 18))
        self.assertEqual(request['strike'], '*')
        self.assertEqual(request['right'], 'both')

    def test_discovers_contracts_per_day_from_underlying_price_range(self):
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())

        contracts = collector.discover_contracts_for_day(
            symbol='SPX',
            trade_date=date(2026, 8, 10),
            low_price=6400.0,
            high_price=6600.0,
            num_strikes=0,
            num_expiries=1,
        )

        self.assertEqual(len(contracts), 6)
        self.assertEqual({contract.expiration for contract in contracts}, {date(2026, 9, 18)})
        self.assertEqual({contract.strike for contract in contracts}, {6400.0, 6500.0, 6600.0})
        self.assertEqual({contract.right for contract in contracts}, {'C', 'P'})

    def test_backfills_daily_contract_subset(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())

        stored_count = collector.backfill_daily_subsets(
            session=session,
            symbol='SPX',
            daily_prices={self.request_date: {'high': 6500.0, 'low': 6500.0}},
            num_strikes=0,
            num_expiries=1,
            intervals=['1m'],
            collection_batch='daily-subset',
        )

        self.assertEqual(stored_count, 2)
        self.assertEqual(session.query(ThetaDataOptionHistory).count(), 2)

    def test_backfills_contracts_with_at_most_four_workers(self):
        from threading import Barrier, Lock

        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())
        contracts = [
            ThetaDataContract('SPX', date(2026, 9, 18), 6400.0 + strike, 'C')
            for strike in range(8)
        ]
        barrier = Barrier(4)
        lock = Lock()
        state = {'active': 0, 'max_active': 0, 'closed_sessions': 0}

        def collect_contract_day(**kwargs):
            with lock:
                state['active'] += 1
                state['max_active'] = max(state['max_active'], state['active'])
            barrier.wait(timeout=2)
            with lock:
                state['active'] -= 1
            return 1

        class WorkerSession:
            def close(self):
                with lock:
                    state['closed_sessions'] += 1

        with patch.object(collector, 'discover_contracts_for_day', return_value=contracts), patch.object(
            collector,
            'collect_contract_day',
            side_effect=collect_contract_day,
        ):
            stored_count = collector.backfill_daily_subsets(
                session=None,
                symbol='SPX',
                daily_prices={self.request_date: {'high': 6500.0, 'low': 6500.0}},
                num_strikes=0,
                num_expiries=1,
                intervals=['1m'],
                collection_batch='parallel-subset',
                session_factory=WorkerSession,
                max_workers=4,
            )

        self.assertEqual(stored_count, 8)
        self.assertEqual(state['max_active'], 4)
        self.assertEqual(state['closed_sessions'], 8)

    def test_lists_and_bounds_available_quote_dates(self):
        client = FakeThetaClient()
        collector = ThetaDataOptionsBackfillCollector(client=client)

        dates = collector.available_quote_dates(
            self.contract,
            start_date=date(2026, 8, 10),
            end_date=date(2026, 8, 10),
        )

        self.assertEqual(dates, [date(2026, 8, 10)])
        _, request = client.calls[-1]
        self.assertEqual(request['request_type'], 'quote')
        self.assertEqual(request['expiration'], self.contract.expiration)
        self.assertEqual(request['right'], 'call')

    def test_backfills_only_vendor_available_dates_and_requested_intervals(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())

        stored_count = collector.backfill_contracts(
            session=session,
            contracts=[self.contract],
            intervals=['1m', '1h'],
            collection_batch='batch-backfill',
            start_date=date(2026, 8, 10),
            end_date=date(2026, 8, 10),
        )

        self.assertEqual(stored_count, 2)
        checkpoints = session.query(ThetaDataCollectionCheckpoint).filter_by(dataset='full_history').all()
        self.assertEqual(len(checkpoints), 2)
        self.assertEqual({checkpoint.interval for checkpoint in checkpoints}, {'1m', '1h'})

    def test_backfills_symbol_using_discovered_contract_subset(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())

        stored_count = collector.backfill_symbol(
            session=session,
            symbol='SPX',
            reference_price=6500.0,
            num_strikes=0,
            num_expiries=1,
            intervals=['1m'],
            collection_batch='batch-symbol',
            start_date=date(2026, 8, 10),
            end_date=date(2026, 8, 10),
        )

        self.assertEqual(stored_count, 2)

    def test_merges_documented_implied_volatility_fields(self):
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())

        result = collector.fetch_iv_and_first_order_greeks(self.contract, self.request_date, '1m')

        self.assertEqual(result.item(0, 'bid_implied_vol'), 0.19)
        self.assertEqual(result.item(0, 'ask_implied_vol'), 0.21)
        self.assertEqual(result.item(0, 'implied_volatility'), 0.2)
        self.assertEqual(result.item(0, 'iv_error'), 0.001)
        self.assertEqual(result.item(0, 'underlying_price'), 6490.0)
        self.assertEqual(result.item(0, 'delta'), 0.5)

    def test_fetches_and_merges_all_requested_history_datasets(self):
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())

        result = collector.fetch_contract_history(self.contract, self.request_date, '1m')

        self.assertEqual(result.item(0, 'bid'), 10.0)
        self.assertEqual(result.item(0, 'ask_size'), 25.0)
        self.assertEqual(result.item(0, 'open'), 10.25)
        self.assertEqual(result.item(0, 'volume'), 100.0)
        self.assertEqual(result.item(0, 'trade_count'), 10.0)
        self.assertEqual(result.item(0, 'vwap'), 10.75)
        self.assertEqual(result.item(0, 'open_interest'), 500.0)
        self.assertEqual(result.item(0, 'bid_implied_vol'), 0.19)
        self.assertEqual(result.item(0, 'delta'), 0.5)

    def test_persists_greeks_by_contract_interval_and_timestamp(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())
        frame = collector.fetch_first_order_greeks(self.contract, self.request_date, '1m')

        self.assertEqual(collector.persist_first_order_greeks(session, frame, 'batch-a'), 1)
        self.assertEqual(collector.persist_first_order_greeks(session, frame, 'batch-b'), 1)
        self.assertEqual(session.query(ThetaDataOptionHistory).count(), 1)
        self.assertEqual(session.query(ThetaDataOptionHistory).one().collection_batch, 'batch-b')

        hourly_frame = frame.with_columns(pl.lit('1h').alias('interval'))
        self.assertEqual(collector.persist_first_order_greeks(session, hourly_frame, 'batch-c'), 1)
        self.assertEqual(session.query(ThetaDataOptionHistory).count(), 2)

    def test_persists_implied_volatility_endpoint_fields(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())
        frame = collector.fetch_iv_and_first_order_greeks(self.contract, self.request_date, '1m')

        self.assertEqual(collector.persist_first_order_greeks(session, frame, 'batch-iv'), 1)
        stored = session.query(ThetaDataOptionHistory).one()
        self.assertEqual(stored.bid_implied_vol, 0.19)
        self.assertEqual(stored.ask_implied_vol, 0.21)
        self.assertEqual(stored.implied_volatility, 0.2)
        self.assertEqual(stored.iv_error, 0.001)
        self.assertEqual(stored.underlying_price, 6490.0)

    def test_persists_full_contract_history(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())
        frame = collector.fetch_contract_history(self.contract, self.request_date, '1m')

        self.assertEqual(collector.persist_first_order_greeks(session, frame, 'batch-full'), 1)
        stored = session.query(ThetaDataOptionHistory).one()
        self.assertEqual(stored.volume, 100)
        self.assertEqual(stored.trade_count, 10)
        self.assertEqual(stored.vwap, 10.75)
        self.assertEqual(stored.open_interest, 500)

    def test_collect_contract_day_checkpoints_and_skips_completed_work(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        client = FakeThetaClient()
        collector = ThetaDataOptionsBackfillCollector(client=client)

        self.assertEqual(
            collector.collect_contract_day(session, self.contract, self.request_date, '1m', 'batch-day'),
            1,
        )
        calls_after_first_run = len(client.calls)
        self.assertEqual(
            collector.collect_contract_day(session, self.contract, self.request_date, '1m', 'batch-day'),
            0,
        )
        checkpoint = session.query(ThetaDataCollectionCheckpoint).filter_by(dataset='full_history').one()
        self.assertEqual(checkpoint.status, 'COMPLETE')
        self.assertEqual(len(client.calls), calls_after_first_run)

    def test_collect_contract_day_keeps_unavailable_endpoint_retryable(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(
            client=FakeThetaClient(unavailable_method='option_history_quote')
        )

        with self.assertRaises(Exception):
            collector.collect_contract_day(session, self.contract, self.request_date, '1m', 'batch-day')

        checkpoint = session.query(ThetaDataCollectionCheckpoint).filter_by(dataset='full_history').one()
        self.assertEqual(checkpoint.status, 'RETRY')
        self.assertIsNotNone(checkpoint.next_retry_at)
        self.assertIsNone(checkpoint.completed_at)

    def test_persists_unavailable_endpoint_with_backoff(self):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        session = sessionmaker(bind=engine)()
        collector = ThetaDataOptionsBackfillCollector(client=FakeThetaClient())
        first_attempt = datetime(2026, 8, 11, 16, 15)

        first = collector.record_endpoint_unavailable(
            session,
            symbol='SPX',
            endpoint='quote',
            reason='service unavailable',
            now=first_attempt,
        )
        second = collector.record_endpoint_unavailable(
            session,
            symbol='SPX',
            endpoint='quote',
            reason='service unavailable',
            now=first_attempt,
        )

        self.assertEqual(session.query(ThetaDataCollectionCheckpoint).count(), 1)
        self.assertEqual(second.status, 'ENDPOINT_UNAVAILABLE')
        self.assertIsNone(second.completed_at)
        self.assertEqual(second.attempts, 2)
        self.assertGreater(second.next_retry_at, first_attempt)

    def test_reads_underlying_ohlc_from_sqlite_cache(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            sqlite_path = Path(temporary_directory) / 'underlying_prices.db'
            connection = sqlite3.connect(sqlite_path)
            try:
                connection.execute(
                    '''CREATE TABLE SPX_INDEX_1day (
                        date TEXT, open REAL, high REAL, low REAL, price REAL
                    )'''
                )
                connection.execute(
                    '''INSERT INTO SPX_INDEX_1day
                    VALUES ('2026-08-10 00:00:00', 6490, 6510, 6480, 6500)'''
                )
                connection.commit()
            finally:
                connection.close()

            with patch('option_data_collector.cfg.UNDERLYING_PRICE_SQLITE_PATH', str(sqlite_path)):
                prices = ThetaDataBackfillService()._load_underlying_prices_from_sqlite(
                    'SPX',
                    [date(2026, 8, 10)],
                )

        self.assertEqual(
            prices,
            {date(2026, 8, 10): {'open': 6490.0, 'high': 6510.0, 'low': 6480.0, 'close': 6500.0}},
        )


if __name__ == '__main__':
    unittest.main()
