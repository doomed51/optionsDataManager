"""
Enhanced Options Data Collector with PostgreSQL, IV, Greeks, and hourly scheduling
"""
from typing import Any, List, Optional, Dict, Tuple
from unittest.mock import call
from ib_insync import IB, Contract, Option, Stock, util, Index
from sqlalchemy import and_
import pandas as pd
from datetime import datetime, timedelta, time
from time import sleep 
import logging
import hashlib
import os
from pathlib import Path
import numpy as np 

import config as cfg
from database import (
    DatabaseManager,
    OptionsData,
    UnderlyingPriceData,
    CollectionProgress,
    TenorDeltaOptionsSnapshot,
    TenorDeltaCollectionProgress,
    TenorDeltaTargetStatus,
)
from options_calculator import OptionsCalculator
from options_data_retriever import OptionsDataRetriever

logger = logging.getLogger(__name__)

class OptionsSkewDataCollector:
    """
    Enhanced options data collector with IV, Greeks, and PostgreSQL storage
    """
    
    def __init__(self, ib_connection: IB, risk_free_rate: float = 0.05):
        """
        Initialize Data Collector
        
        Args:
            ib_connection (IB): Active IB connection
            risk_free_rate (float): Risk-free rate for Greeks calculation
        """
        self.ib = ib_connection
        self.db_manager = DatabaseManager()
        self.calculator = OptionsCalculator(risk_free_rate)
        self.options_retriever = OptionsDataRetriever(ib_connection)
        
        # Create tables if they don't exist
        self.db_manager.create_tables()
        
    def _generate_batch_id(self, symbol: str, snapshot_time: datetime) -> str:
        """Generate a unique batch ID for a collection run"""
        input_string = f"{symbol}_{snapshot_time.isoformat()}"
        return hashlib.md5(input_string.encode()).hexdigest()
    
    def _should_collect_data(self, current_time: datetime) -> bool:
        """
        Check if we should collect data based on time constraints
        Data should be collected at the top of the hour during market hours
        """
        # Check if it's a weekday (Monday=0, Sunday=6)
        if current_time.weekday() > 4:  # Saturday or Sunday
            return False
        
        # Market hours: 9:30 AM to 4:00 PM ET
        market_open = time(9, 30)
        market_close = time(16, 15)
        current_time_only = current_time.time()
        
        if not (market_open <= current_time_only <= market_close):
            return False
        
        # Check if it's at the top of the hour (within 5 minutes)
        # if current_time.minute > 5:
        #     return False
        
        return True
    
    def _get_last_collection_time(self, symbol: str) -> Optional[datetime]:
        """Get the last successful collection time for a symbol"""
        with self.db_manager.get_session() as session:
            last_collection = session.query(CollectionProgress).filter(
                and_(
                    CollectionProgress.symbol == symbol,
                    CollectionProgress.status == 'COMPLETED'
                )
            ).order_by(CollectionProgress.snapshot_time.desc()).first()
            
            return last_collection.snapshot_time if last_collection else None
    
    def _check_minimum_interval(self, symbol: str, current_time: datetime, 
                              min_interval_hours: int = 1) -> bool:
        """Check if minimum time interval has passed since last collection"""
        last_time = self._get_last_collection_time(symbol)
        if last_time is None:
            return True
        
        return (current_time - last_time).total_seconds() >= (min_interval_hours * 3600)

    def _get_last_tenor_delta_collection_time(self, symbol: str) -> Optional[datetime]:
        """Get the last successful tenor-delta collection time for a symbol."""
        with self.db_manager.get_session() as session:
            last_collection = session.query(TenorDeltaCollectionProgress).filter(
                and_(
                    TenorDeltaCollectionProgress.symbol == symbol,
                    TenorDeltaCollectionProgress.status == 'COMPLETED'
                )
            ).order_by(TenorDeltaCollectionProgress.snapshot_time.desc()).first()

            return last_collection.snapshot_time if last_collection else None

    def _check_tenor_delta_minimum_interval(
        self,
        symbol: str,
        current_time: datetime,
        min_interval_hours: int = 1,
    ) -> bool:
        """Check if minimum time interval has passed for tenor-delta collections."""
        last_time = self._get_last_tenor_delta_collection_time(symbol)
        if last_time is None:
            return True

        return (current_time - last_time).total_seconds() >= (min_interval_hours * 3600)

    def _get_option_chain(self, symbol: str, exchange="SMART") -> List[Contract]:
        """Fetch and return the primary option chain definition for a symbol."""
        if symbol in cfg.INDEX_LIST:
            exchange = cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART')
            stock = Index(symbol, exchange, 'USD')
        else:
            stock = Stock(symbol, exchange, 'USD')
        self.ib.qualifyContracts(stock)
        chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)

        if not chains:
            raise ValueError(f"No option chains found for {symbol}")
        
        chains_ = []
        for idx, c in enumerate(chains):
            if c.exchange == exchange: 
                logger.info(f"Retained Chain {idx}: exchange={c.exchange}, tradingClass={c.tradingClass}, multiplier={c.multiplier}, strikes={len(c.strikes)}, expirations={len(c.expirations)}")
                chains_.append(c)

        return chains_

    def _get_strikes_from_chain(self, chain) -> List[float]:
        """Extract and return sorted strikes from an option chain."""
        try:
            if len(chain) == 0:
                logger.warning("No chains available to extract strikes.")
                return []
            elif len(chain) <= 1:
                strikes = sorted([float(strike) for strike in chain.strikes])
            else:
                # if multiple chains are returned, flatten strikes from all chains and deduplicate
                all_strikes = set()
                for c in chain:
                    all_strikes.update(float(strike) for strike in c.strikes)
                strikes = sorted(all_strikes)

                
            return strikes
        except Exception as e:
            logger.error(f"Error extracting strikes from chain: {e}")
            return []

    def _get_expiries_from_chain(self, chain) -> List[str]:
        """Extract and return sorted expirations from an option chain."""
        try:
            if len(chain) == 0:
                logger.warning("No chains available to extract expirations.")
                return []
            elif len(chain) <= 1:
                expiries = sorted(chain.expirations)
            else:
                # if multiple chains are returned, flatten expirations from all chains and deduplicate
                all_expiries = set()
                for c in chain:
                    all_expiries.update(c.expirations)
                expiries = sorted(all_expiries)

            return expiries
        except Exception as e:
            logger.error(f"Error extracting expirations from chain: {e}")
            return []

    def get_expirations_near_tenors(
        self,
        symbol: str,
        tenor_targets: List[int],
        chain=None,
    ) -> Dict[int, str]:
        """Map each target DTE to the nearest available expiration."""
        try:
            chain = chain or self._get_option_chain(symbol)
            chain_expiries = self._get_expiries_from_chain(chain)
            today = datetime.now().date()
            expiries = []
            for exp in chain_expiries:
                exp_date = datetime.strptime(exp, '%Y%m%d').date()
                if exp_date > today:
                    expiries.append((exp, exp_date))

            if not expiries:
                return {}

            expiries.sort(key=lambda item: item[1])
            mappings: Dict[int, str] = {}
            for tenor in tenor_targets:
                target_date = today + timedelta(days=int(tenor))
                best_exp, _ = min(expiries, key=lambda item: abs((item[1] - target_date).days))
                mappings[int(tenor)] = best_exp

            return mappings
        except Exception as e:
            logger.error(f"Error finding expirations near tenors for {symbol}: {e}")
            return {}

    def get_candidate_strikes_from_deltas(
        self,
        symbol: str,
        spot_price: float,
        deltas_abs: List[float],
        strike_wing: int = 1,
        chain=None,
    ) -> List[float]:
        """
        ** DEPRECATED ** 
        Guess likely strikes for target deltas to reduce chain coverage.
        """
        try:
            chain = chain or self._get_option_chain(symbol)
            strikes = sorted([float(strike) for strike in chain.strikes])
            if not strikes:
                return []

            selected_indices = set()

            # Include ATM neighborhood as a baseline.
            atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - spot_price))
            for i in range(max(0, atm_idx - strike_wing), min(len(strikes), atm_idx + strike_wing + 1)):
                selected_indices.add(i)

            for delta_abs in deltas_abs:
                rounded_delta = round(float(delta_abs), 2)
                offset = cfg.DELTA_TO_MONEYNESS_OFFSET.get(rounded_delta, 0.12)
                guess_call = spot_price * (1.0 + offset)
                guess_put = spot_price * (1.0 - offset)

                for guess in (guess_call, guess_put):
                    idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - guess))
                    for i in range(max(0, idx - strike_wing), min(len(strikes), idx + strike_wing + 1)):
                        selected_indices.add(i)

            return sorted(strikes[i] for i in selected_indices)
        except Exception as e:
            logger.error(f"Error finding candidate strikes for {symbol}: {e}")
            return []

    def get_candidate_strikes(
            self, 
            symbol: str, 
            spot_price: float,
            deltas_abs: List[float],
            expiries: List[str],
            strike_wing: int = 1,
            chain = None
    ) -> Dict[str, List[float]]:
        """
            Guess likely strikes as a function of target deltas and target expiries to reduce chain coverage for tenor-delta snapshots.

            strikes are for both puts and calls. with the lower half strikes for puts, and upper half strikes for calls. the "strike_wing" parameter controls how many additional strikes on either side of the initial guess are included to ensure we have a good chance of capturing the target delta even if our initial guess is off.
        """
        try: 
            chain = chain or self._get_option_chain(symbol)
            strikes = sorted([float(strike) for strike in chain.strikes])
            if not strikes:
                return {}

            candidates = {} 
            for expiry in expiries:
                selected_indices = set()
                for delta_abs in deltas_abs:
                    # rounded_delta = round(float(delta_abs), 2)
                    # offset = cfg.DELTA_TO_MONEYNESS_OFFSET.get(rounded_delta, 0.12)
                    offset = self._calculate_moneyness_offset_for_delta_and_dte(delta_abs, (datetime.strptime(expiry, '%Y%m%d').date() - datetime.now().date()).days)
                    guess_call = spot_price * (1.0 + offset)
                    guess_put = spot_price * (1.0 - offset)

                    for guess in (guess_call, guess_put):
                        idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - guess))
                        for i in range(max(0, idx - strike_wing), min(len(strikes), idx + strike_wing + 1)):
                            selected_indices.add(i)

                candidates[expiry] = sorted(strikes[i] for i in selected_indices)

            return candidates
        except Exception as e:
            logger.error(f"Error finding candidate strikes for {symbol}: {e}")
            return {}

    @staticmethod
    def _calculate_moneyness_offset_for_delta_and_dte(delta: float, dte: int) -> float:
        """
        Heuristic to calculate moneyness offset based on delta and DTE.
        This can be refined with empirical data or a more sophisticated model.
        """
        # if delta >= 0.20:
        #     base_offset = 0.06
        # elif delta >= 0.10:
        #     base_offset = 0.12
        # elif delta >= 0.05:
        #     base_offset = 0.18
        # else:
        #     base_offset = 0.30

        # # Adjust offset based on DTE (longer DTE generally means wider strikes for same delta)
        # if dte > 90:
        #     return base_offset * 1.5
        # elif dte > 30:
        #     return base_offset * 1.2
        # else:
        #     return base_offset

        # base_offset = cfg.DELTA_TO_MONEYNESS_OFFSET.get(round(float(delta), 2), 0.12)
        # get the closes delta baseline in the config 
        # base_offset = cfg.DELTA_TO_MONEYNESS_OFFSET.get(round(float(delta), 2), 0.12)
        delta = abs(float(delta))
        for delta_threshold, offset in sorted(cfg.DELTA_TO_MONEYNESS_OFFSET.items(), reverse=True):
            if delta >= delta_threshold:
                base_offset = offset
                break
        else:
            base_offset = 0.3  # Default offset if no threshold is met


        scale = ((max(1, dte) / cfg.TENOR_DELTA_DTE_REF_DAYS)) ** cfg.TENOR_DELTA_DTE_OFFSET_ALPHA

        logger.info(f"Calculated moneyness offset for delta {delta} and DTE {dte}: base_offset={base_offset}, scale={scale}, final_offset={base_offset * scale}")

        return base_offset * scale

    @staticmethod
    def _nearest_strike_index(strikes: List[float], target_strike: float) -> int:
        if not strikes:
            raise ValueError("No strikes available")
        return min(range(len(strikes)), key=lambda i: abs(float(strikes[i]) - float(target_strike)))

    @staticmethod
    def _market_cache_key(expiry: str, strike: float, option_type: str) -> Tuple[str, float, str]:
        return (str(expiry), round(float(strike), 6), str(option_type))

    def _choose_starting_strike(
        self,
        candidate_strikes: List[float],
        chain_strikes: List[float],
        spot_price: float,
        target_delta_abs: float,
        option_type: str,
        expiry: str,
    ) -> float:
        # rounded_delta = round(float(target_delta_abs), 2)
        dte = (datetime.strptime(expiry, '%Y%m%d').date() - datetime.now().date()).days
        offset = self._calculate_moneyness_offset_for_delta_and_dte(target_delta_abs, dte)

        if option_type == 'C':
            guessed_strike = float(spot_price) * (1.0 + offset)
        else:
            guessed_strike = float(spot_price) * (1.0 - offset)

        if candidate_strikes:
            return min(candidate_strikes, key=lambda strike: abs(float(strike) - guessed_strike))

        idx = self._nearest_strike_index(chain_strikes, guessed_strike)
        return float(chain_strikes[idx])

    def _fetch_single_option_market_data(
        self,
        symbol: str,
        expiry: str,
        strike: float,
        option_type: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            logger.info(f"Fetching market data for {symbol} {expiry} {strike} {option_type}")

            option = Option(symbol, expiry, float(strike), option_type, 'SMART')
            contract = None 
            qualified = self.ib.qualifyContracts(option)
            self.ib.sleep(0.05)

            if not qualified:
                logger.info(f"No qualified contract found for {symbol} {expiry} {strike} {option_type}")
                return None

            # if not contract: 
            #     logger.info(f"No contract returned after qualification for {symbol} {expiry} {strike} {option_type}")
            #     return None

            contract = qualified[0]
            ticker = self.ib.reqTickers(contract)[0]
            self.ib.sleep(0.05)

            iv_bid = ticker.bidGreeks.impliedVol if ticker.bidGreeks else np.nan
            iv_ask = ticker.askGreeks.impliedVol if ticker.askGreeks else np.nan
            delta = ticker.bidGreeks.delta if ticker.bidGreeks else np.nan
            gamma = ticker.bidGreeks.gamma if ticker.bidGreeks else np.nan
            vega = ticker.bidGreeks.vega if ticker.bidGreeks else np.nan
            theta = ticker.bidGreeks.theta if ticker.bidGreeks else np.nan

            dte = (pd.to_datetime(contract.lastTradeDateOrContractMonth) - pd.Timestamp.today()).days + 1
            spot_for_moneyness = ticker.last if ticker.last > 0 else ticker.close
            moneyness = contract.strike / spot_for_moneyness if spot_for_moneyness and spot_for_moneyness > 0 else np.nan

            return {
                'contract': contract,
                'bid': ticker.bid if ticker.bid > 0 else None,
                'ask': ticker.ask if ticker.ask > 0 else None,
                'last': ticker.last if ticker.last > 0 else None,
                'volume': ticker.volume if ticker.volume > 0 else 0,
                'open_interest': getattr(ticker, 'openInterest', 0),
                'iv_bid': iv_bid,
                'iv_ask': iv_ask,
                'delta': delta,
                'gamma': gamma,
                'vega': vega,
                'theta': theta,
                'dte': dte,
                'moneyness': moneyness,
                'is_atm': abs(moneyness - 1) < 0.01 if not pd.isna(moneyness) else False,
            }
        except Exception as e:
            logger.warning(
                f"Error getting market data for {symbol} {expiry} {strike} {option_type}: {e}"
            )
            return None

    def _get_cached_or_fetch_option_market_data(
        self,
        symbol: str,
        expiry: str,
        strike: float,
        option_type: str,
        market_data_cache: Dict[Tuple[str, float, str], Optional[Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:

        if market_data_cache:
            logger.info(f"Retrieving market data from cache for {symbol} {expiry} {strike} {option_type}")
            cache_key = self._market_cache_key(expiry, strike, option_type)
            if cache_key in market_data_cache:
                return market_data_cache[cache_key]

        fetched = self._fetch_single_option_market_data(
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
        )

        if market_data_cache is not None:
            logger.info(f"Caching market data for {symbol} {expiry} {strike} {option_type}")
            market_data_cache[cache_key] = fetched

        return fetched

    def _select_best_by_strike_traversal(
        self,
        symbol: str,
        expiry: str,
        option_type: str,
        target_delta_signed: float,
        spot_price: float,
        chain_strikes: List[float],
        starting_strike: float,
        market_data_cache: Dict[Tuple[str, float, str], Optional[Dict[str, Any]]],
        max_hops: int,
        no_improvement_patience: int,
    ) -> Optional[Tuple[Dict[str, Any], float]]:
        """
            traverses the chain in the direction of improvement
            Where improvement is based on difference in delta between the candidate and the target delta 
            Stop traversing when 
                1) we have traversed a maximum number of strikes away from the starting strike, or 
                2) we have gone a certain number of strikes without finding an improvement in delta difference.
        """

        if not chain_strikes:
            return None

        start_idx = self._nearest_strike_index(chain_strikes, starting_strike)
        max_hops = max(0, int(max_hops))
        no_improvement_patience = max(1, int(no_improvement_patience))
        max_radius = min(max_hops, max(start_idx, len(chain_strikes) - start_idx - 1))

        logger.info(f"Starting strike traversal for {symbol} {expiry} {option_type} with target delta {target_delta_signed}, starting strike {starting_strike}, max hops {max_hops}, no improvement patience {no_improvement_patience}")

        best_candidate = None
        best_score = None
        no_improvement_count = 0
        visited_indices = set()
        direction = 0


        # make sure starting strike is valid 
        starting_candidate = self._get_cached_or_fetch_option_market_data(
            symbol=symbol,
            expiry=expiry,
            strike=float(chain_strikes[start_idx]),
            option_type=option_type,
            market_data_cache=market_data_cache,
        )
        visited_indices.add(start_idx)

        if not starting_candidate:
            logger.info(f"Starting strike {chain_strikes[start_idx]} is invalid, searching for nearest valid strike")
            candidate_above_starting_strike = self._get_cached_or_fetch_option_market_data(
                symbol=symbol,
                expiry=expiry,
                strike=float(chain_strikes[min(start_idx + 1, len(chain_strikes) - 1)]),
                option_type=option_type,
                market_data_cache=market_data_cache,
            )
            visited_indices.add(min(start_idx + 1, len(chain_strikes) - 1))
            completeness_flag = 0 if self._is_complete_market_reading(candidate_above_starting_strike) else 1
            strike_gap = abs(float(chain_strikes[min(start_idx + 1, len(chain_strikes) - 1)]) - float(spot_price))
            candidate_score_above_starting_strike = (abs(float(candidate_above_starting_strike.get('delta', np.nan)) - float(target_delta_signed)) if candidate_above_starting_strike else None, completeness_flag, strike_gap)

            candidate_below_starting_strike = self._get_cached_or_fetch_option_market_data(
                symbol=symbol,
                expiry=expiry,
                strike=float(chain_strikes[max(start_idx - 1, 0)]),
                option_type=option_type,
                market_data_cache=market_data_cache,
            )
            visited_indices.add(max(start_idx - 1, 0))
            completeness_flag = 0 if self._is_complete_market_reading(candidate_below_starting_strike) else 1
            strike_gap = abs(float(chain_strikes[max(start_idx - 1, 0)]) - float(spot_price))
            candidate_score_below_starting_strike = (abs(float(candidate_below_starting_strike.get('delta', np.nan)) - float(target_delta_signed)) if candidate_below_starting_strike else None, completeness_flag, strike_gap)

            if candidate_score_above_starting_strike is not None and (candidate_score_below_starting_strike is None or candidate_score_above_starting_strike < candidate_score_below_starting_strike):
                logger.info(f"Starting strike {chain_strikes[start_idx]} is invalid, but candidate above starting strike {chain_strikes[min(start_idx + 1, len(chain_strikes) - 1)]} has better score ({candidate_score_above_starting_strike}) than candidate below starting strike {chain_strikes[max(start_idx - 1, 0)]} ({candidate_score_below_starting_strike}), setting direction to up")
                direction = 1
                best_candidate = candidate_above_starting_strike
                best_score = candidate_score_above_starting_strike
                starting_strike = float(chain_strikes[min(start_idx + 1, len(chain_strikes) - 1)])
                start_idx = min(start_idx + 1, len(chain_strikes) - 1)

            elif candidate_score_below_starting_strike is not None and (candidate_score_above_starting_strike is None or candidate_score_below_starting_strike < candidate_score_above_starting_strike):
                logger.info(f"Starting strike {chain_strikes[start_idx]} is invalid, but candidate below starting strike {chain_strikes[max(start_idx - 1, 0)]} has better score ({candidate_score_below_starting_strike}) than candidate above starting strike {chain_strikes[min(start_idx + 1, len(chain_strikes) - 1)]} ({candidate_score_above_starting_strike}), setting direction to down")
                direction = -1
                best_candidate = candidate_below_starting_strike
                best_score = candidate_score_below_starting_strike
                starting_strike = float(chain_strikes[max(start_idx - 1, 0)])
                start_idx = max(start_idx - 1, 0)

            logger.info(f"Starting strike set to {chain_strikes[start_idx]} with score {best_score} and direction {direction}")

        else: 
            completeness_flag = 0 if self._is_complete_market_reading(starting_candidate) else 1
            strike_gap = abs(float(chain_strikes[start_idx]) - float(spot_price))
            starting_candidate_score = abs(float(starting_candidate.get('delta', np.nan)) - float(target_delta_signed))
            if starting_candidate_score is not None:
                best_candidate = starting_candidate
                best_score = (starting_candidate_score, completeness_flag, strike_gap)
                logger.info(f"Starting strike {chain_strikes[start_idx]} is valid with score {starting_candidate_score}, setting as initial best candidate")

        for radius in range(max_radius + 1):
            indices = []
            if radius == 0:
                indices.append(start_idx)
            else:
                if direction == -1:
                    indices.append(start_idx - radius)
                elif direction == 1:
                    indices.append(start_idx + radius)
                else:
                    # direction is 0 so we need to check both sides 
                    indices.append(start_idx - radius)
                    indices.append(start_idx + radius)

                # left_idx = start_idx - radius
                # right_idx = start_idx + radius
                # if left_idx >= 0:
                #     indices.append(left_idx)
                # if right_idx < len(chain_strikes) and right_idx != left_idx:
                #     indices.append(right_idx)

            if not indices:
                break


            improved_this_round = False
            repeat_visit = False
            for idx in indices:
                if idx in visited_indices:
                    repeat_visit = True 
                    continue

                # since ib provies strikes for ALL AVAILABLE expiries, 
                # when looking at further out expiries, there are cases where 
                # strikes get more spaced out the further away from atm we go 
                # for this reason we want to keep traversing strikes in the case where the candidate
                # returns as none 
                while True: 
                    visited_indices.add(idx)
                    strike = float(chain_strikes[idx])
                    candidate = self._get_cached_or_fetch_option_market_data(
                        symbol=symbol,
                        expiry=expiry,
                        strike=strike,
                        option_type=option_type,
                        market_data_cache=market_data_cache,
                    )

                    if candidate is not None:
                        break
                    sleep(0.2)  # brief pause before retrying
                    idx += direction if direction != 0 else (1 if strike < starting_strike else -1)

                # print(strike)
                # exit() 
                if not candidate:
                    continue

                candidate_delta = candidate.get('delta')
                if candidate_delta is None or pd.isna(candidate_delta):
                    continue

                delta_gap = abs(float(candidate_delta) - float(target_delta_signed))
                completeness_flag = 0 if self._is_complete_market_reading(candidate) else 1
                strike_gap = abs(strike - float(spot_price))
                score = (delta_gap, completeness_flag, strike_gap)

                if best_score is None or score < best_score:
                    best_score = score
                    best_candidate = (candidate, delta_gap)
                    improved_this_round = True

                    # sets the direction of traversal for the next iteration
                    direction = -1 if strike < starting_strike else (1 if strike > starting_strike else 0)

            if radius > 0:
                if improved_this_round:
                    logger.info(f"Found improvement at radius {radius} for strike {chain_strikes[idx]} with candidate delta {candidate_delta} and target delta {target_delta_signed}, resetting no improvement count")
                    no_improvement_count = 0
                elif repeat_visit:
                    logger.info(f"Repeated visit at radius {radius} for strike {chain_strikes[idx]}, no improvement count maintained at {no_improvement_count}")
                else:
                    logger.info(f"No improvement found at radius {radius} for strike {chain_strikes[idx]} with candidate delta {candidate_delta} and target delta {target_delta_signed}")
                    no_improvement_count += 1

                if no_improvement_count >= no_improvement_patience:
                    logger.info(f"Stopping traversal after {no_improvement_count} consecutive non-improving strikes at radius {radius} for option {symbol} {expiry} {option_type}")
                    break

        return best_candidate, delta_gap

    @staticmethod
    def _is_complete_market_reading(data: Dict) -> bool:
        """Check required fields for storing tenor-delta snapshots."""
        required_fields = ('bid', 'ask', 'iv_bid', 'iv_ask', 'delta', 'gamma', 'theta', 'vega')
        for field in required_fields:
            value = data.get(field)
            if value is None or pd.isna(value):
                return False

        if data.get('bid') <= 0 or data.get('ask') <= 0:
            return False

        return True

    @staticmethod
    def _calculate_iv_mid(iv_bid: float, iv_ask: float) -> float:
        if pd.isna(iv_bid) or pd.isna(iv_ask):
            return np.nan
        return (iv_bid + iv_ask) / 2.0

    def _select_best_delta_candidate(
        self,
        candidates: List[Dict],
        target_delta_signed: float,
        spot_price: float,
    ) -> Optional[Tuple[Dict, float]]:
        """Select the closest delta match with deterministic tie breakers."""
        if not candidates:
            return None

        scored = []
        for candidate in candidates:
            candidate_delta = candidate.get('delta')
            if candidate_delta is None or pd.isna(candidate_delta):
                continue

            strike_gap = abs(float(candidate['contract'].strike) - float(spot_price))
            delta_gap = abs(float(candidate_delta) - float(target_delta_signed))
            completeness_flag = 0 if self._is_complete_market_reading(candidate) else 1
            scored.append((delta_gap, completeness_flag, strike_gap, candidate))

        if not scored:
            return None

        scored.sort(key=lambda row: (row[0], row[1], row[2]))
        best = scored[0]
        return best[3], best[0]
    
    def get_current_underlying_price(self, symbol: str) -> Optional[Dict]:
        """Get current underlying price and market data"""
        try:
            if symbol == 'SPXW':
                symbol = 'SPX'
            if symbol in cfg.INDEX_LIST:
                contract = Index(symbol, cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART'), 'USD')
            else:
                contract = Stock(symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(contract)
            
            # Request market data
            market_data = self.ib.reqMktData(contract, '', False, False)
            self.ib.sleep(2)  # Wait for data
            
            # if market_data.last and market_data.last > 0:
                # return {
                #     'price': market_data.last,
                #     'bid': market_data.bid if market_data.bid > 0 else None,
                #     'ask': market_data.ask if market_data.ask > 0 else None,
                #     'volume': market_data.volume if market_data.volume > 0 else 0
                # }
                # return market_data

            # logger.warning(f"No valid last price for {symbol}, falling back to historical data")
            # Fallback to historical data if live data not available
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr='1 D',
                barSizeSetting='5 mins',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            
            if bars:
                latest_bar = bars[-1]
                return {
                    'price': latest_bar.close,
                    'bid': None,
                    'ask': None,
                    'volume': latest_bar.volume
                }
                # return bars 
                
        except Exception as e:
            logger.error(f"Error getting underlying price for {symbol}: {e}")
        
        return None
    
    def get_option_strikes_around_spot(self, symbol: str, spot_price: float, 
                                     num_strikes: int = 10) -> List[float]:
        """Get strikes around the current spot price"""
        try:
            if symbol in cfg.INDEX_LIST:
                stock = Index(symbol, cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART'), 'USD')
            else:
                stock = Stock(symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(stock)
            chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
            
            if not chains:
                raise ValueError(f"No option chains found for {symbol}")
                
            chain = next(c for c in chains)
            strikes = sorted([strike for strike in chain.strikes if strike % 1 == 0])
            
            # Find strikes around spot price
            closest_idx = min(range(len(strikes)), 
                            key=lambda i: abs(strikes[i] - spot_price))
            
            start_idx = max(0, closest_idx - num_strikes // 2)
            end_idx = min(len(strikes), closest_idx + num_strikes // 2 + 1)
            
            return strikes[start_idx:end_idx]
            
        except Exception as e:
            logger.error(f"Error getting strikes for {symbol}: {e}")
            return []
    
    def get_valid_expirations(self, symbol: str, num_expiries: int = 5) -> List[str]:
        """Get valid expiration dates"""
        try:
            if symbol in cfg.INDEX_LIST:
                stock = Index(symbol, cfg.EXCHANGE_MAPPINGS_INDEX.get(symbol, 'SMART'), 'USD')
            else:
                stock = Stock(symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(stock)
            chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
            
            if not chains:
                raise ValueError(f"No option chains found for {symbol}")
                
            chain = next(c for c in chains)
            
            # Filter expirations that are in the future
            valid_expirations = []
            today = datetime.now().date()
            
            for exp in chain.expirations:
                exp_date = datetime.strptime(exp, '%Y%m%d').date()
                if exp_date > today:
                    valid_expirations.append(exp)
            
            valid_expirations.sort()
            return valid_expirations[:num_expiries]
            
        except Exception as e:
            logger.error(f"Error getting expirations for {symbol}: {e}")
            return []
    
    # def create_option_contracts(self, symbol: str, strikes: List[float], 
    #                           expirations: List[str]) -> List[Contract]:
    #     """Create and qualify option contracts"""
    #     contracts = []
    #     for expiry in expirations:
    #         for strike in strikes:
    #             try:
    #                 call = Option(symbol, expiry, strike, 'C', 'SMART')
    #                 put = Option(symbol, expiry, strike, 'P', 'SMART')
    #                 contracts.extend([call, put])
    #             except Exception as e:
    #                 logger.error(f"Error creating contract: {e}")
        
    #     try:
    #         qualified_contracts = self.ib.qualifyContracts(*contracts)
    #         return qualified_contracts
    #     except Exception as e:
    #         logger.error(f"Error qualifying contracts: {e}")
    #         return []
    def create_option_contracts(self, symbol, candidates: Dict[str, List[float]]) -> List[Contract]:
        """Create and qualify option contracts based on candidate strikes for each expiry."""
        contracts = []
        for expiry, strikes in candidates.items():
            strikeindex = 1
            strikes = sorted(strikes)
            for strike in strikes:
                try:
                    ## if strike is in the lower half of strikes then its a put otherwise its a call
                    option_type = 'P' if strikeindex <= len(strikes) / 2 else 'C'
                    strikeindex += 1
                    opt = Option(symbol, expiry, strike, option_type, 'SMART')
                    contracts.append(opt)

                    # call = Option(symbol, expiry, strike, 'C', 'SMART')
                    # put = Option(symbol, expiry, strike, 'P', 'SMART')
                    # contracts.extend([call, put])
                except Exception as e:
                    logger.error(f"Error creating contract for {symbol} {expiry} {strike}{option_type}: {e}")

        try:
            qualified_contracts = self.ib.qualifyContracts(*contracts)
            return qualified_contracts
        except Exception as e:
            logger.error(f"Error qualifying contracts: {e}")
            return []
    
    def get_option_market_data(self, contracts: List[Contract]) -> Dict[int, Dict]:
        """Get market data for option contracts"""
        market_data = {}
        
        for contract in contracts:
            try:
                # ticker = self.ib.reqMktData(contract, '', False, False)
                ticker = self.ib.reqTickers(contract)[0]
                self.ib.sleep(0.1)  # Small delay between requests
                logger.info(f"getting market data for {ticker.contract}")
                
                # Use 'bid' or 'ask' for IV and Greeks from tickOptionComputation
                iv_bid = ticker.bidGreeks.impliedVol if ticker.bidGreeks else np.nan
                iv_ask = ticker.askGreeks.impliedVol if ticker.askGreeks else np.nan
                
                # The Greeks are also available on the ticker object
                delta = ticker.bidGreeks.delta if ticker.bidGreeks else np.nan
                gamma = ticker.bidGreeks.gamma if ticker.bidGreeks else np.nan
                vega = ticker.bidGreeks.vega if ticker.bidGreeks else np.nan
                theta = ticker.bidGreeks.theta if ticker.bidGreeks else np.nan
                
                # Calculate days to expiration
                dte = (pd.to_datetime(contract.lastTradeDateOrContractMonth) - pd.Timestamp.today()).days + 1

                # Calculate moneyness as Strike / Spot Price
                spot_price = ticker.last if ticker.last > 0 else ticker.close
                moneyness = contract.strike / spot_price if spot_price > 0 else np.nan
                
                market_data[contract.conId] = {
                    'contract': contract,
                    'bid': ticker.bid if ticker.bid > 0 else None,
                    'ask': ticker.ask if ticker.ask > 0 else None,
                    'last': ticker.last if ticker.last > 0 else None,
                    'volume': ticker.volume if ticker.volume > 0 else 0,
                    'open_interest': getattr(ticker, 'openInterest', 0),
                    'iv_bid': iv_bid,
                    'iv_ask': iv_ask,
                    'delta': delta,
                    'gamma': gamma, 
                    'vega': vega,
                    'theta': theta,
                    'dte': dte,
                    'moneyness': moneyness, 
                    'is_atm': abs(moneyness - 1) < 0.01 if not pd.isna(moneyness) else False
                }
                
            except Exception as e:
                logger.warning(f"Error getting market data for {contract.localSymbol}: {e}")
        
        # Wait for data to populate
        self.ib.sleep(2)
        
        # Cancel market data subscriptions
        for contract in contracts:
            try:
                self.ib.cancelMktData(contract)
            except:
                pass
        
        return market_data
    
    def process_and_store_options_data(self, symbol: str, snapshot_time: datetime,
                                     underlying_data: Dict, market_data: Dict[int, Dict],
                                     batch_id: str) -> int:
        """Process market data and store options with IV and Greeks"""
        stored_count = 0
        
        with self.db_manager.get_session() as session:
            for con_id, data in market_data.items():
                try:
                    contract = data['contract']
                    
                    # Calculate all metrics
                    expiry_date = datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d')
                    
                    # metrics = self.calculator.calculate_all_metrics(
                    #     bid=data['bid'],
                    #     ask=data['ask'],
                    #     last=data['last'],
                    #     S=underlying_data['price'],
                    #     K=contract.strike,
                    #     expiry_date=expiry_date,
                    #     option_type=contract.right
                    # )
                    
                    # Create options data record
                    option_data = OptionsData(
                        symbol=symbol,
                        strike=contract.strike,
                        expiry=expiry_date,
                        option_type=contract.right,
                        snapshot_time=snapshot_time,
                        bid=data['bid'],
                        ask=data['ask'],
                        last=data['last'],
                        volume=data['volume'],
                        open_interest=data['open_interest'],
                        underlying_price=underlying_data['price'],
                        iv_bid=data['iv_bid'],
                        iv_ask=data['iv_ask'],
                        iv_mid=data['iv_bid'] + data['iv_ask'] / 2 if not (pd.isna(data['iv_bid']) or pd.isna(data['iv_ask'])) else np.nan,
                        delta=data['delta'],
                        gamma=data['gamma'],
                        theta=data['theta'],
                        vega=data['vega'],
                        dte=data['dte'],
                        moneyness=data['moneyness'],
                        is_atm=data['is_atm'],
                        collection_batch=batch_id
                    )
                    
                    session.add(option_data)
                    stored_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing option data for {contract.localSymbol}: {e}")
                    continue
            
            # Store underlying price data
            try:
                underlying_price_data = UnderlyingPriceData(
                    symbol=symbol,
                    snapshot_time=snapshot_time,
                    price=underlying_data['price'],
                    bid=underlying_data.get('bid'),
                    ask=underlying_data.get('ask'),
                    volume=underlying_data.get('volume', 0),
                    collection_batch=batch_id
                )
                session.add(underlying_price_data)
            except Exception as e:
                logger.error(f"Error storing underlying price data: {e}")
            
            try:
                session.commit()
                logger.info(f"Successfully stored {stored_count} options records")
            except Exception as e:
                logger.error(f"Error committing to database: {e}")
                session.rollback()
                stored_count = 0
        
        return stored_count
    
    def collect_options_snapshot(self, symbols: List[str], num_strikes: int = 10,
                               num_expiries: int = 5, force: bool = False) -> Dict[str, bool]:
        """
        Collect options snapshot for given symbols
        
        Args:
            symbols (List[str]): List of ticker symbols
            num_strikes (int): Number of strikes around spot price
            num_expiries (int): Number of expiration dates
            force (bool): Force collection regardless of time constraints
            
        Returns:
            Dict[str, bool]: Success status for each symbol
        """
        results = {}
        current_time = datetime.now()
        
        # Round to the top of the hour for consistency
        snapshot_time = current_time
        
        for symbol in symbols:
            try:
                logger.info(f"Processing {symbol} at {snapshot_time}")
                
                # Check if we should collect data
                if not force:
                    if not self._should_collect_data(current_time):
                        logger.info(f"Skipping {symbol} - outside collection window")
                        results[symbol] = False
                        continue
                    
                    if not self._check_minimum_interval(symbol, snapshot_time):
                        logger.info(f"Skipping {symbol} - minimum interval not met")
                        results[symbol] = False
                        continue
                
                # Generate batch ID
                batch_id = self._generate_batch_id(symbol, snapshot_time)
                
                # Initialize collection progress
                with self.db_manager.get_session() as session:
                    progress = CollectionProgress(
                        batch_id=batch_id,
                        symbol=symbol,
                        snapshot_time=snapshot_time,
                        status='STARTED'
                    )
                    session.add(progress)
                    session.commit()
                
                # Get underlying price
                underlying_data = self.get_current_underlying_price(symbol)
                if not underlying_data:
                    logger.error(f"Failed to get underlying price for {symbol}")
                    results[symbol] = False
                    continue
                
                logger.info(f"Underlying price for {symbol}: ${underlying_data['price']:.2f}")
                
                # Get strikes and expirations
                strikes = self.get_option_strikes_around_spot(
                    symbol, underlying_data['price'], num_strikes
                )
                expirations = self.get_valid_expirations(symbol, num_expiries)
                
                if not strikes or not expirations:
                    logger.error(f"Failed to get strikes or expirations for {symbol}")
                    results[symbol] = False
                    continue
                
                logger.info(f"Found {len(strikes)} strikes and {len(expirations)} expirations")
                
                # Create contracts
                contracts = self.create_option_contracts(symbol, strikes, expirations)
                if not contracts:
                    logger.error(f"Failed to create contracts for {symbol}")
                    results[symbol] = False
                    continue
                
                logger.info(f"Created {len(contracts)} contracts")
                
                # Update progress
                with self.db_manager.get_session() as session:
                    session.query(CollectionProgress).filter(
                        CollectionProgress.batch_id == batch_id
                    ).update({
                        'status': 'IN_PROGRESS',
                        'num_contracts_total': len(contracts)
                    })
                    session.commit()
                
                # Get market data
                logger.info("Collecting market data...")
                market_data = self.get_option_market_data(contracts)
                
                # Process and store data
                stored_count = self.process_and_store_options_data(
                    symbol, snapshot_time, underlying_data, market_data, batch_id
                )
                
                # Update progress
                with self.db_manager.get_session() as session:
                    session.query(CollectionProgress).filter(
                        CollectionProgress.batch_id == batch_id
                    ).update({
                        'status': 'COMPLETED' if stored_count > 0 else 'FAILED',
                        'num_contracts_processed': stored_count,
                        'error_message': None if stored_count > 0 else 'No data stored'
                    })
                    session.commit()
                
                results[symbol] = stored_count > 0
                logger.info(f"Completed {symbol}: {stored_count} records stored")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                
                # Update progress as failed
                try:
                    batch_id = self._generate_batch_id(symbol, snapshot_time)
                    with self.db_manager.get_session() as session:
                        session.query(CollectionProgress).filter(
                            CollectionProgress.batch_id == batch_id
                        ).update({
                            'status': 'FAILED',
                            'error_message': str(e)[:500]
                        })
                        session.commit()
                except:
                    pass
                
                results[symbol] = False
        
        return results

    def collect_skew_data(
        self,
        symbols: List[str],
        force: bool = False,
        min_interval_hours: int = 1,
        # strike_wing: int = 1,
    ) -> Dict[str, bool]:
        """
        Collect point-in-time option snapshots for target tenors and deltas to be used for skew research/monitoring. 
        """
        results: Dict[str, bool] = {}
        current_time = datetime.now()
        snapshot_time = current_time

        for symbol in symbols:
            symbol = symbol.upper().strip()
            batch_id = self._generate_batch_id(f"{symbol}_tenor_delta", snapshot_time)

            target_cfg = cfg.SKEW_DATA_SYMBOL_CONFIG.get(symbol, {})
            tenors_dte = target_cfg.get('tenors_dte', cfg.TENOR_DELTA_DEFAULT_TENORS_DTE)
            deltas_abs = target_cfg.get('deltas_abs', cfg.TENOR_DELTA_DEFAULT_DELTAS_ABS)
            tenors_dte = sorted({int(t) for t in tenors_dte if int(t) > 0})
            deltas_abs = sorted({float(d) for d in deltas_abs if float(d) > 0})

            expected_targets = len(tenors_dte) * len(deltas_abs) * 2

            try:
                logger.info(f"Processing tenor-delta snapshot for {symbol} at {snapshot_time}")

                if not force:
                    if not self._should_collect_data(current_time):
                        logger.info(f"Skipping tenor-delta {symbol} - outside collection window")
                        results[symbol] = False
                        continue

                    if not self._check_tenor_delta_minimum_interval(symbol, snapshot_time, min_interval_hours):
                        logger.info(f"Skipping tenor-delta {symbol} - minimum interval not met")
                        results[symbol] = False
                        continue

                if expected_targets == 0:
                    logger.warning(f"No tenor-delta targets configured for {symbol}")
                    results[symbol] = False
                    continue

                with self.db_manager.get_session() as session:
                    progress = TenorDeltaCollectionProgress(
                        batch_id=batch_id,
                        symbol=symbol,
                        snapshot_time=snapshot_time,
                        status='STARTED',
                        expected_targets=expected_targets,
                    )
                    session.add(progress)
                    session.commit()

                underlying_data = self.get_current_underlying_price(symbol)
                logger.info(f"Underlying data for {symbol}: {underlying_data}")
                
                if not underlying_data:
                    raise ValueError(f"Failed to get underlying price for {symbol}")

                spot_price = float(underlying_data['price'])
                chain = self._get_option_chain(symbol)

                chain_strikes = self._get_strikes_from_chain(chain)
                
                if not chain_strikes:
                    raise ValueError(f"No chain strikes found for {symbol}")

                tenor_expiration_map = self.get_expirations_near_tenors(symbol, tenors_dte, chain=chain)
                
                if not tenor_expiration_map:
                    raise ValueError(f"No valid expirations found for {symbol}")
                logger.info(f"Tenor to expiration mapping for {symbol}: {tenor_expiration_map}")
                # expirations = sorted(set(tenor_expiration_map.values()))

                # candidate_strikes = self.get_candidate_strikes_from_deltas(
                #     symbol,
                #     spot_price,
                #     deltas_abs,
                #     strike_wing=strike_wing,
                #     chain=chain,
                # )
                
                # if not candidate_strikes:
                #     raise ValueError(f"No candidate strikes found for {symbol}")

                # candidates = self.get_candidate_strikes(
                #     symbol,
                #     spot_price,
                #     deltas_abs,
                #     expirations, 
                #     strike_wing=strike_wing,
                #     chain=chain,
                # )

                # contracts = self.create_option_contracts(symbol, candidates)
                # if not contracts:
                #     raise ValueError(f"Failed to create contracts for {symbol}")

                ### instead of getting candidates, we need to get starting strikes for each tenor/delta/option_type using the candidate logic. we can then traverse the chain, pinging for market data until delta diff stops improving 

                # with self.db_manager.get_session() as session:
                #     session.query(TenorDeltaCollectionProgress).filter(
                #         TenorDeltaCollectionProgress.batch_id == batch_id
                #     ).update({
                #         'status': 'IN_PROGRESS',
                #     })
                #     session.commit()

                
                # market_data = self.get_option_market_data(contracts)
                ##### looks like the market data cache is just transferring the market_data dict into another dict; we can optimize by directly using market_data and ensuring that the keys are in the format we need for lookup instead of conId
                # market_data_cache: Dict[Tuple[str, float, str], Optional[Dict[str, Any]]] = {}
                # for data in market_data.values():
                #     contract = data.get('contract')
                #     if not contract:
                #         continue

                #     cache_key = self._market_cache_key(
                #         contract.lastTradeDateOrContractMonth,
                #         float(contract.strike),
                #         contract.right,
                #     )
                #     market_data_cache[cache_key] = data

                completed_targets = 0
                missing_targets = 0

                with self.db_manager.get_session() as session:
                    for tenor in tenors_dte:
                        tenor_expiry = tenor_expiration_map.get(tenor)

                        # CASE: no expiry found for tenor - mark all deltas for this tenor as missing with reason and skip
                        if tenor_expiry is None:
                            logger.warning(f"No expiration found for tenor {tenor} DTE for {symbol}, marking all deltas as missing for this tenor")
                            for delta_abs in deltas_abs:
                                for option_type in ('C', 'P'):
                                    session.add(TenorDeltaTargetStatus(
                                        batch_id=batch_id,
                                        symbol=symbol,
                                        snapshot_time=snapshot_time,
                                        option_type=option_type,
                                        target_tenor_dte=int(tenor),
                                        target_delta_abs=float(delta_abs),
                                        status='MISSING_NO_CANDIDATE',
                                        reason='No expiration mapped for tenor',
                                    ))
                                    missing_targets += 1
                            continue

                        expiry_dt = datetime.strptime(tenor_expiry, '%Y%m%d')
                        observed_dte = (expiry_dt.date() - snapshot_time.date()).days

                        for delta_abs in deltas_abs:

                            # for each target delta, 
                            # get the starting strike in cached market data 
                            # select the best strike in cached market data 
                            # save cached market data to db for the found strike/delta combo 
                            # move on to the next delta
                            # when done with deltas, move on to the next tenor/expiry 

                            for option_type in ('C', 'P'):
                                target_delta = float(delta_abs) if option_type == 'C' else -float(delta_abs)

                                # CHANGE: use the candidate logic here to seed a starting strike 
                                starting_strike = self._choose_starting_strike(
                                    candidate_strikes=None,
                                    chain_strikes=chain_strikes,
                                    spot_price=spot_price,
                                    target_delta_abs=float(delta_abs),
                                    option_type=option_type,
                                    expiry = tenor_expiry,
                                )
                                logger.info(f"Starting strike for {symbol} {option_type} {tenor_expiry} delta {delta_abs}: {starting_strike}")

                                # CHANGE: given the starting strike, traverse the LIVE chain, selecting the best strike given target delta  
                                selected = self._select_best_by_strike_traversal(
                                    symbol=symbol,
                                    expiry=tenor_expiry,
                                    option_type=option_type,
                                    target_delta_signed=target_delta,
                                    spot_price=spot_price,
                                    chain_strikes=chain_strikes,
                                    starting_strike=starting_strike,
                                    market_data_cache=None,
                                    max_hops=int(getattr(cfg, 'TENOR_DELTA_TRAVERSAL_MAX_HOPS', 12)),
                                    no_improvement_patience=int(
                                        getattr(cfg, 'TENOR_DELTA_TRAVERSAL_NO_IMPROVEMENT_PATIENCE', 4)
                                    ),
                                )
                                print(selected)
                                exit() 

                                if selected is None:
                                    session.add(TenorDeltaTargetStatus(
                                        batch_id=batch_id,
                                        symbol=symbol,
                                        snapshot_time=snapshot_time,
                                        option_type=option_type,
                                        target_tenor_dte=int(tenor),
                                        target_delta_abs=float(delta_abs),
                                        status='MISSING_NO_CANDIDATE',
                                        reason='No candidates with valid delta after traversal',
                                    ))
                                    missing_targets += 1
                                    continue

                                selected_data, delta_gap = selected
                                contract = selected_data['contract']

                                if not self._is_complete_market_reading(selected_data):
                                    session.add(TenorDeltaTargetStatus(
                                        batch_id=batch_id,
                                        symbol=symbol,
                                        snapshot_time=snapshot_time,
                                        option_type=option_type,
                                        target_tenor_dte=int(tenor),
                                        target_delta_abs=float(delta_abs),
                                        status='MISSING_NULL_FIELDS',
                                        reason='Best delta match had incomplete quote/greeks',
                                        selected_expiry=expiry_dt,
                                        selected_strike=float(contract.strike),
                                        selected_delta=selected_data.get('delta'),
                                        selected_delta_gap_abs=float(delta_gap),
                                    ))
                                    missing_targets += 1
                                    continue

                                snapshot_row = TenorDeltaOptionsSnapshot(
                                    symbol=symbol,
                                    snapshot_time=snapshot_time,
                                    option_type=option_type,
                                    expiry=expiry_dt,
                                    dte=observed_dte,
                                    strike=float(contract.strike),
                                    target_tenor_dte=int(tenor),
                                    target_delta_abs=float(delta_abs),
                                    delta_gap_abs=float(delta_gap),
                                    bid=selected_data.get('bid'),
                                    ask=selected_data.get('ask'),
                                    last=selected_data.get('last'),
                                    volume=selected_data.get('volume', 0),
                                    open_interest=selected_data.get('open_interest', 0),
                                    underlying_price=spot_price,
                                    iv_bid=selected_data.get('iv_bid'),
                                    iv_ask=selected_data.get('iv_ask'),
                                    iv_mid=self._calculate_iv_mid(
                                        selected_data.get('iv_bid'),
                                        selected_data.get('iv_ask'),
                                    ),
                                    delta=selected_data.get('delta'),
                                    gamma=selected_data.get('gamma'),
                                    theta=selected_data.get('theta'),
                                    vega=selected_data.get('vega'),
                                    rho=np.nan,
                                    collection_batch=batch_id,
                                )
                                session.add(snapshot_row)

                                session.add(TenorDeltaTargetStatus(
                                    batch_id=batch_id,
                                    symbol=symbol,
                                    snapshot_time=snapshot_time,
                                    option_type=option_type,
                                    target_tenor_dte=int(tenor),
                                    target_delta_abs=float(delta_abs),
                                    status='SELECTED',
                                    reason='Stored tenor-delta snapshot',
                                    selected_expiry=expiry_dt,
                                    selected_strike=float(contract.strike),
                                    selected_delta=selected_data.get('delta'),
                                    selected_delta_gap_abs=float(delta_gap),
                                ))
                                completed_targets += 1

                    final_status = 'COMPLETED' if completed_targets > 0 else 'FAILED'
                    session.query(TenorDeltaCollectionProgress).filter(
                        TenorDeltaCollectionProgress.batch_id == batch_id
                    ).update({
                        'status': final_status,
                        'completed_targets': completed_targets,
                        'missing_targets': missing_targets,
                        'error_message': None if final_status == 'COMPLETED' else 'No qualifying tenor-delta targets stored',
                    })

                    session.commit()

                logger.info(
                    f"Completed tenor-delta {symbol}: "
                    f"stored={completed_targets}, missing={missing_targets}, expected={expected_targets}"
                )
                results[symbol] = completed_targets > 0

            except Exception as e:
                logger.error(f"Error processing tenor-delta {symbol}: {e}")
                with self.db_manager.get_session() as session:
                    session.query(TenorDeltaCollectionProgress).filter(
                        TenorDeltaCollectionProgress.batch_id == batch_id
                    ).update({
                        'status': 'FAILED',
                        'expected_targets': expected_targets,
                        'error_message': str(e)[:500],
                    })
                    session.commit()
                results[symbol] = False

        return results
    
    def close(self):
        """Close database connection"""
        self.db_manager.close()