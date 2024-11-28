from typing import List, Optional, Callable
from ib_insync import Contract, IB, util
import pandas as pd
import logging
from datetime import datetime

class OptionsDataRetriever:
    """
    Manages retrieval of historical and real-time options data
    """
    
    def __init__(self, ib_connection: IB):
        """
        Initialize with an Interactive Brokers connection
        
        Args:
            ib_connection (IB): Active IB connection
        """
        self.ib = ib_connection
        self.logger = logging.getLogger(__name__)
    
    def get_historical_data(self, 
                             contracts: List[Contract], 
                             duration: str = '1 D',
                             bar_size: str = '1 min') -> pd.DataFrame:
        """
        Retrieve historical data for option contracts
        
        Args:
            contracts (List[Contract]): List of option contracts
            duration (str): Historical data duration
            bar_size (str): Bar size for historical data
        
        Returns:
            pd.DataFrame: Consolidated historical data
        """
        dfs = {}
        for con in contracts:
            try:
                print(f"Requesting historical data for {con}")
                print(f"Duration: {duration}, Bar Size: {bar_size}")
                bars = self.ib.reqHistoricalData(
                    con,
                    endDateTime='',
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow='TRADES',
                    useRTH=True,
                    formatDate=1
                )
                if bars:
                    df = util.df(bars)
                    print(df)
                    exit()
                    df['right'] = con.right  # Add option type (C/P)
                    df['strike'] = con.strike
                    # dfs.append(df)
                    dfs[len(dfs)] = {'contract': con, 'df': df}
            
            except Exception as e:
                self.logger.warning(f"Failed to get historical data for {con.localSymbol}: {e}")
            
            # pause so we dont hit a rate limiter 
            self.ib.sleep(0.5)
        
        return dfs # pd.concat(dfs) if dfs else pd.DataFrame()
    
    def stream_market_data(self, 
                            contracts: List[Contract], 
                            callback: Optional[Callable] = None):
        """
        Stream real-time market data for contracts
        
        Args:
            contracts (List[Contract]): Contracts to stream
            callback (Optional[Callable]): Function to process incoming data
        """
        market_data_list = []
        
        def process_ticker(ticker):
            """Internal ticker processing"""
            timestamp = datetime.now()
            data = {
                'timestamp': timestamp,
                'symbol': ticker.contract.symbol,
                'last': ticker.last,
                'volume': ticker.volume,
                'bid': ticker.bid,
                'ask': ticker.ask
            }
            
            market_data_list.append(data)
            
            if callback:
                callback(data)
        
        # Request market data for each contract
        for contract in contracts:
            try:
                market_data = self.ib.reqMktData(contract)
                market_data.updateEvent += process_ticker
            except Exception as e:
                self.logger.warning(f"Failed to stream data for {contract.localSymbol}: {e}")
        
        return market_data_list