# Options Data Manager with IV and Greeks

A comprehensive system for collecting options data with implied volatility, Greeks, and market metrics from Interactive Brokers (IBKR) and storing them in PostgreSQL.

## Features

- **Real-time Options Data Collection**: Collects bid/ask/last prices, volume, and open interest
- **Implied Volatility Calculation**: Calculates IV for bid, ask, and mid prices using Black-Scholes model
- **Greeks Calculation**: Computes Delta, Gamma, Theta, Vega, and Rho
- **Enhanced Metrics**: 
  - Days to Expiration (DTE)
  - Moneyness (strike/spot ratio)
  - At-the-money (ATM) identification
- **Scheduled Collection**: Automated hourly snapshots during market hours
- **Tenor-Delta Snapshots**: Low-call-volume snapshots for target DTEs and delta buckets
- **PostgreSQL Storage**: Robust database with proper indexing and constraints
- **Configurable Parameters**: Customizable number of strikes and expiration dates

## Requirements

- Python 3.8+
- Interactive Brokers TWS or Gateway
- PostgreSQL 12+
- Required Python packages (see requirements.txt)

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd optionsDataManager
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up PostgreSQL database**:
   - Install PostgreSQL and ensure it's running
   - Create a database user with appropriate permissions

4. **Configure environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your database and IBKR connection details
   ```

5. **Initialize the database**:
   ```bash
   python setup_database.py
   ```

## Configuration

Edit the `.env` file with your settings:

```env
# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=options_data
DB_USER=postgres
DB_PASSWORD=your_password_here

# IBKR Configuration
TWS_HOST=127.0.0.1
TWS_PORT=7496
CLIENT_ID=10

# Data Collection Settings
COLLECTION_INTERVAL_HOURS=1
DEFAULT_NUM_STRIKES=10
DEFAULT_NUM_EXPIRIES=5
COLLECTION_SYMBOLS=SPY,QQQ,IWM,DIA,TLT
RISK_FREE_RATE=0.05

# Optional tenor-delta settings
TENOR_DELTA_SYMBOLS=SPY,QQQ
TENOR_DELTA_MIN_INTERVAL_HOURS=1
TENOR_DELTA_STRIKE_WING=1
```

Edit [config.py](config.py) for per-symbol tenor and delta targets:

```python
TENOR_DELTA_SYMBOLS = ["SPY", "QQQ"]
TENOR_DELTA_TARGETS = {
   "SPY": {
      "tenors_dte": [7, 14, 30, 60],
      "deltas_abs": [0.01, 0.05, 0.10, 0.20],
   },
}
TENOR_DELTA_RUN_TIMES = ['10:00', '12:00', '13:00', '15:00']
```

## Usage

### Configuration Check
```bash
python option_greeks_update.py --config-check
```

### One-time Data Collection
```bash
# Collect data for SPY with default parameters
python option_greeks_update.py --symbols SPY

# Collect data for multiple symbols with custom parameters
python option_greeks_update.py --symbols SPY QQQ IWM --strikes 15 --expiries 3

# Force collection regardless of time constraints
python option_greeks_update.py --symbols SPY --force
```

### Scheduled Collection
```bash
# Run in scheduler mode for automatic hourly collection
python option_greeks_update.py --schedule
```

### Tenor-Delta Collection
```bash
# Run tenor-delta snapshots using symbols from config/env
python option_greeks_update.py --tenor-delta

# Run tenor-delta snapshots with explicit symbols
python option_greeks_update.py --tenor-delta --tenor-delta-symbols SPY QQQ --force
```

### Command Line Options

- `--symbols, -s`: List of ticker symbols (default: SPY)
- `--strikes, -k`: Number of strikes around spot price (default: 10)
- `--expiries, -e`: Number of expiration dates (default: 5)
- `--force, -f`: Force collection regardless of time constraints
- `--schedule`: Run in scheduler mode
- `--tenor-delta`: Run tenor-delta snapshot mode
- `--tenor-delta-symbols`: Override tenor-delta symbol list
- `--config-check`: Check system configuration
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR)

## Database Schema

### options_data
Main table storing options data with IV and Greeks:
- `symbol`: Ticker symbol
- `strike`: Strike price
- `expiry`: Expiration date
- `option_type`: 'C' for call, 'P' for put
- `snapshot_time`: Data collection timestamp
- `bid`, `ask`, `last`: Market prices
- `volume`, `open_interest`: Volume and OI
- `underlying_price`: Spot price at collection time
- `iv_bid`, `iv_ask`, `iv_mid`: Implied volatilities
- `delta`, `gamma`, `theta`, `vega`, `rho`: Greeks
- `dte`: Days to expiration
- `moneyness`: Strike/spot ratio
- `is_atm`: At-the-money flag

### underlying_price_data
Underlying asset price snapshots:
- `symbol`: Ticker symbol
- `snapshot_time`: Timestamp
- `price`, `bid`, `ask`: Market data
- `volume`: Trading volume

### collection_progress
Tracks data collection batches and status:
- `batch_id`: Unique batch identifier
- `symbol`: Ticker symbol
- `snapshot_time`: Collection time
- `status`: STARTED, IN_PROGRESS, COMPLETED, FAILED
- Collection metrics and error tracking

### tenor_delta_options_snapshot
Dedicated point-in-time records for tenor/delta targets:
- `symbol`, `snapshot_time`
- `target_tenor_dte`, `target_delta_abs`
- `option_type`, `expiry`, `dte`, `strike`
- `bid`, `ask`, `last`, `iv_bid`, `iv_ask`, `iv_mid`
- `delta`, `gamma`, `theta`, `vega`
- `delta_gap_abs`, `collection_batch`

### tenor_delta_collection_progress
Dedicated progress tracker for tenor-delta batches:
- `batch_id`, `symbol`, `snapshot_time`, `status`
- `expected_targets`, `completed_targets`, `missing_targets`
- `error_message`

### tenor_delta_target_status
Per-target outcome tracking for selection attempts:
- `target_tenor_dte`, `target_delta_abs`, `option_type`
- `status` (SELECTED, MISSING_NO_CANDIDATE, MISSING_NULL_FIELDS)
- Selected contract metadata and reason text

## Key Features

### Automatic Scheduling
- Collects data at the top of each hour during market hours (9:30 AM - 4:00 PM ET)
- Minimum 1-hour interval between collections
- Weekdays only (Monday-Friday)

### Tenor-Delta Scheduling
- Runs at configured times (default: 10:00, 12:00, 13:00, 15:00 ET)
- Uses symbols configured in `config.py` (or `TENOR_DELTA_SYMBOLS` env override)
- Skips null/incomplete best-match readings and records the miss in dedicated target status

### At-the-Money Identification
Options are flagged as ATM when their moneyness (strike/spot) is within 5% of 1.0.

### Implied Volatility Calculation
Uses Newton-Raphson method with Black-Scholes model to calculate IV from market prices.

### Greeks Calculation
All Greeks are calculated using the Black-Scholes model:
- **Delta**: Price sensitivity to underlying price changes
- **Gamma**: Delta sensitivity to underlying price changes  
- **Theta**: Time decay (per day)
- **Vega**: Volatility sensitivity (per 1% vol change)
- **Rho**: Interest rate sensitivity (per 1% rate change)

## Error Handling

The system includes comprehensive error handling:
- Connection failures to IBKR or database
- Invalid symbols or contract creation failures
- Data collection timeouts
- Database constraint violations

All errors are logged with appropriate detail levels.

## Performance Considerations

- Uses connection pooling for database efficiency
- Implements rate limiting for IBKR API calls
- Batch processing for database operations
- Automatic cleanup of market data subscriptions

## Monitoring

- Detailed logging to console and file
- Collection progress tracking in database
- Status reporting for each symbol and batch
- Error message storage for troubleshooting

## Dependencies

Key libraries used:
- `ib_insync`: Interactive Brokers API client
- `sqlalchemy`: Database ORM and connection management
- `psycopg2-binary`: PostgreSQL adapter
- `numpy`, `scipy`: Scientific computing for IV and Greeks
- `schedule`: Task scheduling
- `python-dotenv`: Environment variable management

## License

MIT License - see LICENSE file for details. 
