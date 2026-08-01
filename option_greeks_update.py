"""
Options Data Manager with IV and Greeks Collection
Main module for collecting options data with implied volatility and Greeks from IBKR to PostgreSQL

Usage:
    python option_greeks_update.py --symbols SPY QQQ IWM --force
    python option_greeks_update.py --config-check
    python option_greeks_update.py --schedule
"""
import argparse
import logging
import sys
import schedule
import time
from datetime import datetime, timedelta
from typing import List
import os
from pathlib import Path

from ib_insync import IB
from dotenv import load_dotenv

import config as cfg
from skew_data_collector import OptionsSkewDataCollector
from database import DatabaseManager

# Load environment variables
load_dotenv()

def setup_logging(log_level: str = 'INFO') -> None:
    """Setup logging configuration"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('options_data_collection.log')
        ]
    )

def connect_to_ibkr() -> IB:
    """Connect to Interactive Brokers TWS/Gateway"""
    ib = IB()
    
    host = os.getenv('TWS_HOST', '127.0.0.1')
    port = int(os.getenv('TWS_PORT', '7496'))
    client_id = int(os.getenv('CLIENT_ID', '10'))
    
    try:
        ib.connect(host, port, clientId=client_id, timeout=10)
        logging.info(f"Connected to IBKR at {host}:{port} with client ID {client_id}")
        return ib
    except Exception as e:
        logging.error(f"Failed to connect to IBKR: {e}")
        raise

def check_database_connection() -> bool:
    """Check PostgreSQL database connection"""
    try:
        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            # Simple query to test connection
            result = session.execute("SELECT 1").fetchone()
            if result:
                logging.info("Database connection successful")
                return True
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return False
    finally:
        try:
            db_manager.close()
        except:
            pass
    
    return False

def validate_symbols(symbols: List[str]) -> List[str]:
    """Validate and clean symbol list"""
    valid_symbols = []
    for symbol in symbols:
        symbol = symbol.upper().strip()
        if symbol and len(symbol) <= 10:  # Basic validation
            valid_symbols.append(symbol)
        else:
            logging.warning(f"Invalid symbol ignored: {symbol}")
    
    return valid_symbols


def _get_tenor_delta_symbols_from_env_or_config() -> List[str]:
    """Resolve tenor-delta symbols from env override, then config defaults."""
    env_symbols = os.getenv('TENOR_DELTA_SYMBOLS', '').strip()
    if env_symbols:
        symbols = [s.strip() for s in env_symbols.split(',') if s.strip()]
    else:
        symbols = list(cfg.SKEW_DATA_SYMBOLS)
    return validate_symbols(symbols)

def collect_options_data(symbols: List[str], num_strikes: int = 10, 
                        num_expiries: int = 5, force: bool = False) -> bool:
    """
    Main function to collect options data
    
    Args:
        symbols (List[str]): List of ticker symbols
        num_strikes (int): Number of strikes around spot price
        num_expiries (int): Number of expiration dates
        force (bool): Force collection regardless of time constraints
        
    Returns:
        bool: True if any symbol was successfully processed
    """
    ib = None
    collector = None
    
    try:
        # Validate inputs
        symbols = validate_symbols(symbols)
        if not symbols:
            logging.error("No valid symbols provided")
            return False
        
        # Connect to IBKR
        ib = connect_to_ibkr()
        
        # Initialize collector
        risk_free_rate = float(os.getenv('RISK_FREE_RATE', '0.05'))
        collector = OptionsSkewDataCollector(ib, risk_free_rate)
        
        # Collect data
        logging.info(f"Starting data collection for symbols: {', '.join(symbols)}")
        results = collector.collect_options_snapshot(
            symbols=symbols,
            num_strikes=num_strikes,
            num_expiries=num_expiries,
            force=force
        )
        
        # Log results
        successful_symbols = [symbol for symbol, success in results.items() if success]
        failed_symbols = [symbol for symbol, success in results.items() if not success]
        
        if successful_symbols:
            logging.info(f"Successfully collected data for: {', '.join(successful_symbols)}")
        
        if failed_symbols:
            logging.warning(f"Failed to collect data for: {', '.join(failed_symbols)}")
        
        return len(successful_symbols) > 0
        
    except Exception as e:
        logging.error(f"Error in data collection: {e}")
        return False
    
    finally:
        # Cleanup
        if collector:
            try:
                collector.close()
            except:
                pass
        
        if ib and ib.isConnected():
            try:
                ib.disconnect()
            except:
                pass


def collect_tenor_delta_data(symbols: List[str], force: bool = False) -> bool:
    """
    Collect tenor-delta point-in-time snapshots for configured symbols.
    """
    ib = None
    collector = None

    try:
        symbols = validate_symbols(symbols)
        if not symbols:
            logging.error("No valid tenor-delta symbols provided")
            return False

        ib = connect_to_ibkr()

        risk_free_rate = float(os.getenv('RISK_FREE_RATE', '0.05'))
        collector = OptionsSkewDataCollector(ib, risk_free_rate)

        logging.info(f"Starting tenor-delta collection for symbols: {', '.join(symbols)}")
        results = collector.collect_skew_data(
            symbols=symbols,
            force=force,
            min_interval_hours=int(os.getenv('TENOR_DELTA_MIN_INTERVAL_HOURS', '1')),
            strike_wing=int(os.getenv('TENOR_DELTA_STRIKE_WING', '1')),
        )

        successful_symbols = [symbol for symbol, success in results.items() if success]
        failed_symbols = [symbol for symbol, success in results.items() if not success]

        if successful_symbols:
            logging.info(f"Successfully collected tenor-delta data for: {', '.join(successful_symbols)}")

        if failed_symbols:
            logging.warning(f"Failed tenor-delta collection for: {', '.join(failed_symbols)}")

        return len(successful_symbols) > 0

    except Exception as e:
        logging.error(f"Error in tenor-delta collection: {e}")
        return False

    finally:
        if collector:
            try:
                collector.close()
            except:
                pass

        if ib and ib.isConnected():
            try:
                ib.disconnect()
            except:
                pass

def scheduled_collection():
    """Function to run scheduled data collection"""
    symbols = ['GLD']  # Default symbols
    
    # Get symbols from environment if available
    env_symbols = os.getenv('COLLECTION_SYMBOLS', '').strip()
    if env_symbols:
        symbols = [s.strip() for s in env_symbols.split(',') if s.strip()]
    
    num_strikes = int(os.getenv('DEFAULT_NUM_STRIKES', '10'))
    num_expiries = int(os.getenv('DEFAULT_NUM_EXPIRIES', '5'))
    
    logging.info("Running scheduled options data collection")
    success = collect_options_data(symbols, num_strikes, num_expiries, force=False)
    
    if success:
        logging.info("Scheduled collection completed successfully")
    else:
        logging.error("Scheduled collection failed")


def scheduled_tenor_delta_collection():
    """Run scheduled tenor-delta data collection."""
    symbols = _get_tenor_delta_symbols_from_env_or_config()
    if not symbols:
        logging.info("No tenor-delta symbols configured; skipping scheduled tenor-delta run")
        return

    logging.info("Running scheduled tenor-delta options data collection")
    success = collect_tenor_delta_data(symbols=symbols, force=False)

    if success:
        logging.info("Scheduled tenor-delta collection completed successfully")
    else:
        logging.error("Scheduled tenor-delta collection failed")

def run_scheduler():
    """Run the scheduler for hourly data collection"""
    logging.info("Starting options data collection scheduler")
    
    # Schedule collection at the top of each hour during market hours
    market_hours = ['09:30', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00']
    tenor_delta_hours = list(cfg.TENOR_DELTA_RUN_TIMES)
    
    for hour in market_hours:
        schedule.every().monday.at(hour).do(scheduled_collection)
        schedule.every().tuesday.at(hour).do(scheduled_collection)
        schedule.every().wednesday.at(hour).do(scheduled_collection)
        schedule.every().thursday.at(hour).do(scheduled_collection)
        schedule.every().friday.at(hour).do(scheduled_collection)

    for hour in tenor_delta_hours:
        schedule.every().monday.at(hour).do(scheduled_tenor_delta_collection)
        schedule.every().tuesday.at(hour).do(scheduled_tenor_delta_collection)
        schedule.every().wednesday.at(hour).do(scheduled_tenor_delta_collection)
        schedule.every().thursday.at(hour).do(scheduled_tenor_delta_collection)
        schedule.every().friday.at(hour).do(scheduled_tenor_delta_collection)
    
    logging.info(f"Scheduled standard collection at: {', '.join(market_hours)}")
    logging.info(f"Scheduled tenor-delta collection at: {', '.join(tenor_delta_hours)}")
    
    # Run scheduler
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logging.info("Scheduler stopped by user")
            break
        except Exception as e:
            logging.error(f"Error in scheduler: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying

def configuration_check() -> bool:
    """Check system configuration and dependencies"""
    logging.info("Running configuration check...")
    
    issues = []
    
    # Check environment variables
    required_env_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    for var in required_env_vars:
        if not os.getenv(var):
            issues.append(f"Missing environment variable: {var}")
    
    # Check database connection
    if not check_database_connection():
        issues.append("Database connection failed")
    
    # Check IBKR connection
    try:
        ib = connect_to_ibkr()
        if ib.isConnected():
            logging.info("IBKR connection successful")
            ib.disconnect()
        else:
            issues.append("IBKR connection failed")
    except Exception as e:
        issues.append(f"IBKR connection error: {e}")
    
    # Check required directories
    required_dirs = ['logs', 'data']
    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        if not dir_path.exists():
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                logging.info(f"Created directory: {dir_path}")
            except Exception as e:
                issues.append(f"Failed to create directory {dir_path}: {e}")
    
    # Report results
    if issues:
        logging.error("Configuration issues found:")
        for issue in issues:
            logging.error(f"  - {issue}")
        return False
    else:
        logging.info("Configuration check passed!")
        return True

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Options Data Manager with IV and Greeks Collection',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Load defaults from environment (set via .env)
    env_symbols = os.getenv('COLLECTION_SYMBOLS') or os.getenv('DEFAULT_SYMBOLS') or 'UVXY,GLD,TLT'
    default_symbols = [s.strip() for s in env_symbols.split(',') if s.strip()]

    default_strikes = int(os.getenv('DEFAULT_NUM_STRIKES', '10'))
    default_expiries = int(os.getenv('DEFAULT_NUM_EXPIRIES', '5'))

    parser.add_argument(
        '--symbols', '-s',
        nargs='+',
        default=default_symbols,
        help=f'List of ticker symbols to collect (default from env: {", ".join(default_symbols)})'
    )

    parser.add_argument(
        '--strikes', '-k',
        type=int,
        default=default_strikes,
        help=f'Number of strikes around spot price (default from env: {default_strikes})'
    )

    parser.add_argument(
        '--expiries', '-e',
        type=int,
        default=default_expiries,
        help=f'Number of expiration dates to collect (default from env: {default_expiries})'
    )
    
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Force collection regardless of time constraints'
    )
    
    parser.add_argument(
        '--schedule',
        action='store_true',
        help='Run in scheduler mode for automatic hourly collection'
    )

    parser.add_argument(
        '--tenor-delta',
        action='store_true',
        help='Run tenor-delta snapshot collection mode'
    )

    parser.add_argument(
        '--tenor-delta-symbols',
        nargs='+',
        default=None,
        help='Override tenor-delta symbols (defaults to TENOR_DELTA_SYMBOLS env/config)'
    )
    
    parser.add_argument(
        '--config-check',
        action='store_true',
        help='Check system configuration and exit'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    try:
        # Configuration check
        if args.config_check:
            success = configuration_check()
            sys.exit(0 if success else 1)
        
        # Scheduler mode
        if args.schedule:
            if not configuration_check():
                logging.error("Configuration check failed. Fix issues before running scheduler.")
                sys.exit(1)
            run_scheduler()
            return

        if args.tenor_delta:
            td_symbols = args.tenor_delta_symbols or _get_tenor_delta_symbols_from_env_or_config()
            success = collect_tenor_delta_data(
                symbols=td_symbols,
                force=args.force,
            )

            if success:
                logging.info("Tenor-delta data collection completed successfully")
                sys.exit(0)
            else:
                logging.error("Tenor-delta data collection failed")
                sys.exit(1)
        
        # One-time collection
        success = collect_options_data(
            symbols=args.symbols,
            num_strikes=args.strikes,
            num_expiries=args.expiries,
            force=args.force
        )
        
        if success:
            logging.info("Data collection completed successfully")
            sys.exit(0)
        else:
            logging.error("Data collection failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logging.info("Program interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
