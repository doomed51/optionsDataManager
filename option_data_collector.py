from typing import List, Optional, Dict, Tuple
from ib_insync import IB, Contract, Option, Stock, util
import duckdb
from duckdb.typing import *
import pandas as pd
from datetime import datetime, timedelta
import logging
import math
from pathlib import Path
import hashlib
from options_data_retriever import OptionsDataRetriever

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
        
        # Checkpoint configuration
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        
        # Initialize DuckDB connection
        self.db = duckdb.connect('data/options_data.db')
        self._initialize_database()

        # get opttions data in the db 
        self.options_historical_data = self.db.execute("SELECT * FROM options_historical_data").fetchdf()
        
    def _initialize_database(self):
        """Create the necessary tables if they don't exist"""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS options_historical_data (
                symbol VARCHAR,
                strike DECIMAL,
                expiry DATE,
                "right" VARCHAR(1),
                date TIMESTAMP,
                open DECIMAL,
                "high" DECIMAL,
                low DECIMAL,
                close DECIMAL,
                volume INTEGER,
                underlying_price DECIMAL,
                collection_batch VARCHAR,
                UNIQUE (symbol, strike, expiry, "right", date)
            )
        """)
        
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS underlying_price_history (
                symbol VARCHAR,
                date TIMESTAMP,
                open DECIMAL,
                "high" DECIMAL,
                low DECIMAL,
                price DECIMAL,
                collection_batch VARCHAR,
                PRIMARY KEY (symbol, date)
            )
        """)
        
        # Table to track collection progress
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS collection_progress (
                batch_id VARCHAR PRIMARY KEY,
                symbol VARCHAR,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                status VARCHAR,
                last_processed_date TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        """)
        
    def _generate_batch_id(self, symbol: str, start_date: datetime, 
                          end_date: datetime) -> str:
        """Generate a unique batch ID for a collection run"""
        input_string = f"{symbol}_{start_date.isoformat()}_{end_date.isoformat()}"
        return hashlib.md5(input_string.encode()).hexdigest()
        
    def _save_checkpoint(self, batch_id: str, last_processed_date: datetime,
                        status: str = 'IN_PROGRESS'):
        """Update collection progress in database"""
        self.db.execute("""
            UPDATE collection_progress
            SET last_processed_date = ?,
                status = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE batch_id = ?
        """, [last_processed_date, status, batch_id])
        
    def _get_collection_status(self, batch_id: str) -> Optional[Dict]:
        """Get the status of a collection batch"""
        query_result = self.db.execute("""
            SELECT * FROM collection_progress
            WHERE batch_id = ?
        """, [batch_id])
        result = query_result.fetchone()
        if result:
            colnames = [desc[0] for desc in query_result.description]
            # pv = dict(zip(colnames, result))
            # print(pv)
            # exit()
            return dict(zip(colnames, result))
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

        # Create new batch
        self.db.execute("""
            INSERT INTO collection_progress (
                batch_id, symbol, start_date, end_date, status,
                last_processed_date, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'STARTED', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, [batch_id, symbol, start_date, end_date, start_date])
        
        return batch_id
        
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
        """Get historical underlying prices for the date range"""
        stock = Stock(symbol, 'SMART', 'USD')
        self.ib.qualifyContracts(stock)
        
        chunks = self.get_date_chunks(start_date, end_date, '1 day')
        price_data = []
        
        for chunk_start, chunk_end in chunks:
            bars = self.ib.reqHistoricalData(
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
            
            self.ib.sleep(1)  # Rate limiting
        
        if price_data:
            ym = pd.concat(price_data)
            ym['symbol'] = symbol
            return ym
        return pd.DataFrame()

    def get_option_strikes_between_range(self, symbol:str, high_price, low_price, num_strikes:int = 5) -> List[float]:
        """Get n strikes between high and low price"""
        stock = Stock(symbol, 'SMART', 'USD')
        # qualify 
        self.ib.qualifyContracts(stock)
        chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        
        if not chains:
            raise ValueError(f"No option chains found for {symbol}")
            
        chain = next(c for c in chains)
        strikes = sorted(chain.strikes)
        # drop any strikes that are not ##.0 
        strikes = [strike for strike in strikes if strike % 1 == 0]

        # get index of strike that is num_strikes above the strike closest to the high price 
        high_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - high_price))
        low_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - low_price))
        
        start_idx = max(0, low_idx - num_strikes)
        end_idx = min(len(strikes)-1, high_idx + num_strikes)
        return strikes[start_idx:end_idx+1]

    def get_option_strikes(self, symbol: str, current_price: float, 
                          num_strikes: int = 5) -> List[float]:
        """Get n strikes above current price"""
        stock = Stock(symbol, 'SMART', 'USD')
        # qualify 
        self.ib.qualifyContracts(stock)
        chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        
        if not chains:
            raise ValueError(f"No option chains found for {symbol}")
            
        chain = next(c for c in chains)
        strikes = sorted(chain.strikes)
        # drop any strikes that are not ##.0 
        strikes = [strike for strike in strikes if strike % 1 == 0]

        closest_idx = min(range(len(strikes)), 
                         key=lambda i: abs(strikes[i] - current_price))
        
        return strikes[closest_idx+1:closest_idx+1+num_strikes]
        
    def get_valid_expirations(self, symbol: str, start_date: datetime,
                            num_expiries: int = 3) -> List[str]:
        """Get valid expiration dates from a given date"""
        stock = Stock(symbol, 'SMART', 'USD')
        self.ib.qualifyContracts(stock)
        chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
        
        if not chains:
            raise ValueError(f"No option chains found for {symbol}")
            
        chain = next(c for c in chains)
        
        # Filter and sort expirations that are after current_date
        valid_expirations = []
        for exp in chain.expirations:
            exp_date = datetime.strptime(exp, '%Y%m%d')
            if exp_date > start_date:
                valid_expirations.append(exp)

        valid_expirations.sort()
        return valid_expirations[:num_expiries]
    
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
        existing_dates = self.db.execute("""
            SELECT date
            FROM underlying_price_history
            WHERE symbol = ?
            AND date BETWEEN ? AND ?
            AND collection_batch = ?
            ORDER BY date
        """, [symbol, start_date, end_date, batch_id]).fetchall()
        # convert end date to datetime since it is usually just the date component 
        # end_date = datetime.combine(end_date, datetime.min.time())
        # set time to 1700
        # create datetime object with time set to 1700
        end_date = datetime.combine(end_date, datetime.min.time()).replace(hour=17)

        existing_dates = [row[0] for row in existing_dates]
        
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
    
    def collect_historical_data(self, contracts: List[Contract], 
                              start_date: datetime, end_date: datetime,
                              bar_size: str) -> Dict:
        """
        Collect historical data for contracts in chunks
        """
        # set start date to the earliest available timestamp 
        all_data = {}
        i = 0
        for contract in contracts:
            i += 1
            logging.info(f"Getting contract {i} of {len(contracts)}, {contract.symbol}-{contract.strike}{contract.right}-{contract.lastTradeDateOrContractMonth}")
            # set the start date to the max of the matching contract in self.options_historical_data (if it exits) otherwise we will be querying for data we already have 
            start_date = self.options_historical_data[(self.options_historical_data['symbol'] == contract.symbol) & (self.options_historical_data['strike'] == contract.strike) & (self.options_historical_data['right'] == contract.right) & (self.options_historical_data['expiry'] == pd.to_datetime(contract.lastTradeDateOrContractMonth))]['date'].max().date()

            
            if start_date is pd.NaT: # if we don't have any data then start from the earliest available
                start_date = self.ib.reqHeadTimeStamp(contract, 'TRADES', False).date()
            # end_date is today 
            end_date = datetime.now().date()

            chunks = self.get_date_chunks(start_date, end_date, bar_size)
            contract_data = []
            
            for chunk_start, chunk_end in chunks:
                try:
                    duration = f'{(chunk_end - chunk_start).days + 1} D'
                    logging.info(f"Getting data for chunk {chunk_start} to {chunk_end} @ {bar_size}")
                    bars = self.ib.reqHistoricalData(
                        contract,
                        endDateTime=chunk_end,
                        durationStr=duration,
                        barSizeSetting=bar_size,
                        whatToShow='TRADES',
                        useRTH=True,
                        formatDate=1
                    )
                    
                    if bars:
                        df = util.df(bars)
                        df['symbol'] = contract.symbol
                        df['strike'] = contract.strike
                        df['expiry'] = pd.to_datetime(contract.lastTradeDateOrContractMonth)
                        df['right'] = contract.right
                        contract_data.append(df)
                    

                    self.ib.sleep(1)  # Rate limiting
                    
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
                    price_history = price_history.tail(2)
                    logging.info(f"Price history for {symbol} from {date_range_start} to {date_range_end}")

                    if not price_history.empty:
                        # Store underlying prices
                        price_history['collection_batch'] = batch_id
                        self.db.execute(f"""
                            INSERT INTO underlying_price_history 
                            SELECT symbol, date, open, high, low, close as price, collection_batch 
                            FROM price_history
                            ON CONFLICT DO NOTHING
                        """)

                    # Process each day
                    for idx, price_row in price_history.iterrows():
                        iteration_date = price_row['date']
                        try:
                            current_price = price_row['close']
                            current_date = pd.to_datetime(iteration_date)
                            
                            # Skip if we already have data for this date
                            existing_data = self.db.execute("""
                                SELECT 1 FROM options_historical_data
                                WHERE symbol = ? AND date = ? AND collection_batch = ?
                                LIMIT 1
                            """, [symbol, current_date, batch_id]).fetchone()
                            
                            if existing_data:
                                logging.info(f"Data already exists for {symbol} on {current_date}, continuing to next record")
                                continue
                            
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
                    
            # Mark batch as completed
            self._save_checkpoint(batch_id, end_date, 'COMPLETED')
            self.logger.info(f"Successfully completed batch {batch_id}")
            
        except Exception as e:
            self.logger.error(f"Error in collection process: {e}")
            raise
            
    def _process_single_date(self, symbol: str, current_date: datetime,
                            current_price: float, high_price:float, low_price:float, num_strikes: int,
                            num_expiries: int, bar_size: str, batch_id: str):
        """Process data collection for a single date"""
        strikes = self.get_option_strikes_between_range(
            symbol, high_price=high_price, low_price=low_price, num_strikes=num_strikes
        )
        expirations = self.get_valid_expirations(
            symbol, current_date, num_expiries
        )
        
        # Create and qualify contracts
        contracts = self.create_option_contracts(symbol, strikes, expirations)
        logging.info(f"Created {len(contracts)} contracts for {symbol} on {current_date}, collecting historical data...")
        
        # Collect data for these contracts
        historical_data = self.collect_historical_data(
            contracts,
            current_date,
            current_date + timedelta(days=1),
            bar_size
        )
        
        # Process and store data
        for contract_data in historical_data.values():
            contract = contract_data['contract']
            df = contract_data['df']
            
            if df.empty:
                continue
                
            # Add additional columns
            df['underlying_price'] = current_price
            df['collection_batch'] = batch_id
            # drop average and barCount columns 
            df.drop(columns=['average', 'barCount'], inplace=True)
            df['date'] = df['date'].dt.tz_localize(None)
            # log 
            logging.info(f"Storing data for {contract.symbol}-{contract.strike}{contract.right}-{contract.lastTradeDateOrContractMonth} on {current_date}")

            df.drop_duplicates( inplace=True, ignore_index=True) # drop dupes caused by overlap in date batches 

            # Store in DuckDB
            try:
                self.db.execute("""
                    INSERT INTO options_historical_data 
                    SELECT symbol, strike, CAST(expiry AS DATE), "right", date, open, "high", low, close, volume, underlying_price, collection_batch  FROM df
                    ON CONFLICT DO NOTHING
                """)
            except Exception as e:
                self.logger.error(f"Error storing data for {contract.localSymbol} on {current_date}: {e}")
                exit() 
            logging.info("Done\n")
            
    def close(self):
        """Close the database connection"""
        self.db.close()

def main():
    logging.basicConfig(level=logging.INFO)
    
    ib = IB()
    try:
        ib.connect('127.0.0.1', 7496, clientId=10)
        
        collector = OptionsDataCollector(ib)
        
        symbol = "SPY"
        
        start_date = datetime.now() - timedelta(days=1) # set start date to yesterday 

        # Only collect new data after 4:15 PM on weekdays 
        end_date = datetime.now().date()
        if datetime.now().weekday() <= 5 :
            if datetime.now().hour < 16 and datetime.now().minute < 15:
                end_date = datetime.now() - timedelta(days=1)

        collector.collect_and_store_data(
            symbol,
            start_date.date(),
            end_date,
            num_strikes=10,
            num_expiries=5,
            bar_size='1 min'
        )
        
    finally:
        collector.close()
        ib.disconnect()

if __name__ == "__main__":
    main()