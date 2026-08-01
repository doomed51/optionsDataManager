"""
Options Greeks and Implied Volatility Calculator
Uses Black-Scholes model for options pricing and Greeks calculation
"""
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from datetime import datetime, date
from typing import Dict, Optional, Union
import logging

logger = logging.getLogger(__name__)

class OptionsCalculator:
    """
    Calculate implied volatility and Greeks for options
    """
    
    def __init__(self, risk_free_rate: float = 0.05):
        """
        Initialize calculator
        
        Args:
            risk_free_rate (float): Risk-free rate (annualized)
        """
        self.risk_free_rate = risk_free_rate
    
    def _calculate_d1_d2(self, S: float, K: float, T: float, r: float, sigma: float) -> tuple:
        """
        Calculate d1 and d2 for Black-Scholes formula
        
        Args:
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            sigma (float): Volatility
            
        Returns:
            tuple: (d1, d2)
        """
        if T <= 0 or sigma <= 0:
            return 0.0, 0.0
            
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        return d1, d2
    
    def black_scholes_price(self, S: float, K: float, T: float, r: float, 
                           sigma: float, option_type: str = 'C') -> float:
        """
        Calculate Black-Scholes option price
        
        Args:
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            sigma (float): Volatility
            option_type (str): 'C' for call, 'P' for put
            
        Returns:
            float: Option price
        """
        if T <= 0:
            if option_type.upper() == 'C':
                return max(S - K, 0)
            else:
                return max(K - S, 0)
        
        d1, d2 = self._calculate_d1_d2(S, K, T, r, sigma)
        
        if option_type.upper() == 'C':
            price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:
            price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        
        return max(price, 0)
    
    def calculate_implied_volatility(self, market_price: float, S: float, K: float, 
                                   T: float, r: float, option_type: str = 'C') -> Optional[float]:
        """
        Calculate implied volatility using Brent's method
        
        Args:
            market_price (float): Market price of the option
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            option_type (str): 'C' for call, 'P' for put
            
        Returns:
            Optional[float]: Implied volatility or None if calculation fails
        """
        if market_price <= 0 or T <= 0:
            return None
        
        # Define objective function for root finding
        def objective(sigma):
            try:
                return self.black_scholes_price(S, K, T, r, sigma, option_type) - market_price
            except:
                return float('inf')
        
        try:
            # Use Brent's method to find implied volatility
            iv = brentq(objective, 0.001, 5.0, xtol=1e-6, maxiter=100)
            return iv if 0.001 <= iv <= 5.0 else None
        except (ValueError, RuntimeError):
            return None
    
    def calculate_delta(self, S: float, K: float, T: float, r: float, 
                       sigma: float, option_type: str = 'C') -> float:
        """
        Calculate option delta
        
        Args:
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            sigma (float): Volatility
            option_type (str): 'C' for call, 'P' for put
            
        Returns:
            float: Delta
        """
        if T <= 0:
            if option_type.upper() == 'C':
                return 1.0 if S > K else 0.0
            else:
                return -1.0 if S < K else 0.0
        
        d1, _ = self._calculate_d1_d2(S, K, T, r, sigma)
        
        if option_type.upper() == 'C':
            return norm.cdf(d1)
        else:
            return -norm.cdf(-d1)
    
    def calculate_gamma(self, S: float, K: float, T: float, r: float, sigma: float) -> float:
        """
        Calculate option gamma (same for calls and puts)
        
        Args:
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            sigma (float): Volatility
            
        Returns:
            float: Gamma
        """
        if T <= 0 or sigma <= 0:
            return 0.0
        
        d1, _ = self._calculate_d1_d2(S, K, T, r, sigma)
        
        return norm.pdf(d1) / (S * sigma * np.sqrt(T))
    
    def calculate_theta(self, S: float, K: float, T: float, r: float, 
                       sigma: float, option_type: str = 'C') -> float:
        """
        Calculate option theta (time decay)
        
        Args:
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            sigma (float): Volatility
            option_type (str): 'C' for call, 'P' for put
            
        Returns:
            float: Theta (per day)
        """
        if T <= 0:
            return 0.0
        
        d1, d2 = self._calculate_d1_d2(S, K, T, r, sigma)
        
        if option_type.upper() == 'C':
            theta = (-(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) 
                    - r * K * np.exp(-r * T) * norm.cdf(d2))
        else:
            theta = (-(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) 
                    + r * K * np.exp(-r * T) * norm.cdf(-d2))
        
        return theta / 365  # Convert to per day
    
    def calculate_vega(self, S: float, K: float, T: float, r: float, sigma: float) -> float:
        """
        Calculate option vega (sensitivity to volatility)
        
        Args:
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            sigma (float): Volatility
            
        Returns:
            float: Vega
        """
        if T <= 0:
            return 0.0
        
        d1, _ = self._calculate_d1_d2(S, K, T, r, sigma)
        
        return S * norm.pdf(d1) * np.sqrt(T) / 100  # Divide by 100 for 1% volatility change
    
    def calculate_rho(self, S: float, K: float, T: float, r: float, 
                     sigma: float, option_type: str = 'C') -> float:
        """
        Calculate option rho (sensitivity to interest rate)
        
        Args:
            S (float): Current stock price
            K (float): Strike price
            T (float): Time to expiration (in years)
            r (float): Risk-free rate
            sigma (float): Volatility
            option_type (str): 'C' for call, 'P' for put
            
        Returns:
            float: Rho
        """
        if T <= 0:
            return 0.0
        
        _, d2 = self._calculate_d1_d2(S, K, T, r, sigma)
        
        if option_type.upper() == 'C':
            return K * T * np.exp(-r * T) * norm.cdf(d2) / 100
        else:
            return -K * T * np.exp(-r * T) * norm.cdf(-d2) / 100
    
    def calculate_days_to_expiration(self, expiry_date: Union[datetime, date]) -> int:
        """
        Calculate days to expiration
        
        Args:
            expiry_date (Union[datetime, date]): Expiration date
            
        Returns:
            int: Days to expiration
        """
        if isinstance(expiry_date, datetime):
            expiry_date = expiry_date.date()
        
        today = datetime.now().date()
        return max((expiry_date - today).days, 0)
    
    def calculate_time_to_expiration(self, expiry_date: Union[datetime, date]) -> float:
        """
        Calculate time to expiration in years
        
        Args:
            expiry_date (Union[datetime, date]): Expiration date
            
        Returns:
            float: Time to expiration in years
        """
        dte = self.calculate_days_to_expiration(expiry_date)
        return dte / 365.0
    
    def calculate_moneyness(self, strike: float, spot_price: float) -> float:
        """
        Calculate moneyness (strike/spot ratio)
        
        Args:
            strike (float): Strike price
            spot_price (float): Current spot price
            
        Returns:
            float: Moneyness
        """
        return strike / spot_price if spot_price > 0 else 0.0
    
    def is_at_the_money(self, strike: float, spot_price: float, threshold: float = 0.05) -> bool:
        """
        Determine if option is at-the-money
        
        Args:
            strike (float): Strike price
            spot_price (float): Current spot price
            threshold (float): ATM threshold (default 5%)
            
        Returns:
            bool: True if option is at-the-money
        """
        moneyness = self.calculate_moneyness(strike, spot_price)
        return abs(moneyness - 1.0) <= threshold
    
    def calculate_all_metrics(self, bid: Optional[float], ask: Optional[float], 
                            last: Optional[float], S: float, K: float, 
                            expiry_date: Union[datetime, date], r: float = None,
                            option_type: str = 'C') -> Dict:
        """
        Calculate all options metrics (IV, Greeks, DTE, moneyness, ATM)
        
        Args:
            bid (Optional[float]): Bid price
            ask (Optional[float]): Ask price
            last (Optional[float]): Last traded price
            S (float): Current stock price
            K (float): Strike price
            expiry_date (Union[datetime, date]): Expiration date
            r (float): Risk-free rate (uses default if None)
            option_type (str): 'C' for call, 'P' for put
            
        Returns:
            Dict: Dictionary containing all calculated metrics
        """
        if r is None:
            r = self.risk_free_rate
        
        T = self.calculate_time_to_expiration(expiry_date)
        dte = self.calculate_days_to_expiration(expiry_date)
        moneyness = self.calculate_moneyness(K, S)
        is_atm = self.is_at_the_money(K, S)
        
        # Calculate mid price
        mid_price = None
        if bid is not None and ask is not None:
            mid_price = (bid + ask) / 2
        
        # Calculate implied volatilities
        iv_bid = self.calculate_implied_volatility(bid, S, K, T, r, option_type) if bid else None
        iv_ask = self.calculate_implied_volatility(ask, S, K, T, r, option_type) if ask else None
        iv_mid = self.calculate_implied_volatility(mid_price, S, K, T, r, option_type) if mid_price else None
        
        # Use mid IV for Greeks calculation, fallback to last, bid, or ask
        iv_for_greeks = iv_mid or iv_bid or iv_ask
        
        # Calculate Greeks if we have IV
        delta = gamma = theta = vega = rho = None
        if iv_for_greeks and T > 0:
            try:
                delta = self.calculate_delta(S, K, T, r, iv_for_greeks, option_type)
                gamma = self.calculate_gamma(S, K, T, r, iv_for_greeks)
                theta = self.calculate_theta(S, K, T, r, iv_for_greeks, option_type)
                vega = self.calculate_vega(S, K, T, r, iv_for_greeks)
                rho = self.calculate_rho(S, K, T, r, iv_for_greeks, option_type)
            except Exception as e:
                logger.warning(f"Error calculating Greeks: {e}")
        
        return {
            'dte': dte,
            'moneyness': moneyness,
            'is_atm': is_atm,
            'iv_bid': iv_bid,
            'iv_ask': iv_ask,
            'iv_mid': iv_mid,
            'delta': delta,
            'gamma': gamma,
            'theta': theta,
            'vega': vega,
            'rho': rho
        }