"""
Database models for options data with PostgreSQL.
"""
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Date,
    Boolean,
    Index,
    UniqueConstraint,
    Text,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from os import getenv
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class OptionsData(Base):
    """
    Enhanced options data table with IV, Greeks, DTE, and moneyness
    """
    __tablename__ = 'options_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    strike = Column(Float, nullable=False)
    expiry = Column(DateTime, nullable=False, index=True)
    option_type = Column(String(1), nullable=False)  # 'C' or 'P'
    snapshot_time = Column(DateTime, nullable=False, index=True)
    
    # Market data
    bid = Column(Float)
    ask = Column(Float)
    last = Column(Float)
    volume = Column(Integer, default=0)
    open_interest = Column(Integer, default=0)
    
    # Underlying data
    underlying_price = Column(Float, nullable=False)
    
    # Implied Volatility
    iv_bid = Column(Float)
    iv_ask = Column(Float) 
    iv_mid = Column(Float)
    
    # Greeks
    delta = Column(Float)
    gamma = Column(Float)
    theta = Column(Float)
    vega = Column(Float)
    rho = Column(Float)
    
    # Additional metrics
    dte = Column(Integer, nullable=False)  # Days to expiration
    moneyness = Column(Float, nullable=False)  # Strike/Spot ratio
    is_atm = Column(Boolean, default=False, index=True)  # At-the-money flag
    
    # Metadata
    collection_batch = Column(String(64), index=True)
    created_at = Column(DateTime, default=func.now())
    
    # Unique constraint to prevent duplicates
    __table_args__ = (
        UniqueConstraint('symbol', 'strike', 'expiry', 'option_type', 'snapshot_time', 
                        name='unique_option_snapshot'),
        Index('idx_symbol_snapshot_time', 'symbol', 'snapshot_time'),
        Index('idx_expiry_dte', 'expiry', 'dte'),
        Index('idx_moneyness_atm', 'moneyness', 'is_atm'),
    )

class UnderlyingPriceData(Base):
    """
    Underlying asset price snapshots
    """
    __tablename__ = 'underlying_price_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    snapshot_time = Column(DateTime, nullable=False, index=True)
    price = Column(Float, nullable=False)
    bid = Column(Float)
    ask = Column(Float)
    volume = Column(Integer, default=0)
    collection_batch = Column(String(64), index=True)
    created_at = Column(DateTime, default=func.now())
    
    __table_args__ = (
        UniqueConstraint('symbol', 'snapshot_time', name='unique_underlying_snapshot'),
    )

class CollectionProgress(Base):
    """
    Track data collection progress and scheduling
    """
    __tablename__ = 'collection_progress'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(64), unique=True, nullable=False, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    snapshot_time = Column(DateTime, nullable=False, index=True)
    status = Column(String(20), nullable=False, default='STARTED')  # STARTED, IN_PROGRESS, COMPLETED, FAILED
    num_contracts_processed = Column(Integer, default=0)
    num_contracts_total = Column(Integer, default=0)
    error_message = Column(String(500))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class TenorDeltaOptionsSnapshot(Base):
    """
    Point-in-time tenor-delta option snapshots.
    """
    __tablename__ = 'tenor_delta_options_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    snapshot_time = Column(DateTime, nullable=False, index=True)
    option_type = Column(String(1), nullable=False)  # 'C' or 'P'

    expiry = Column(DateTime, nullable=False, index=True)
    dte = Column(Integer, nullable=False, index=True)
    strike = Column(Float, nullable=False)

    target_tenor_dte = Column(Integer, nullable=False, index=True)
    target_delta_abs = Column(Float, nullable=False, index=True)
    delta_gap_abs = Column(Float)

    bid = Column(Float)
    ask = Column(Float)
    last = Column(Float)
    volume = Column(Integer, default=0)
    open_interest = Column(Integer, default=0)

    underlying_price = Column(Float, nullable=False)

    iv_bid = Column(Float)
    iv_ask = Column(Float)
    iv_mid = Column(Float)

    delta = Column(Float)
    gamma = Column(Float)
    theta = Column(Float)
    vega = Column(Float)
    rho = Column(Float)

    collection_batch = Column(String(64), nullable=False, index=True)
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint(
            'symbol',
            'snapshot_time',
            'expiry',
            'strike',
            'option_type',
            'target_tenor_dte',
            'target_delta_abs',
            name='unique_tenor_delta_snapshot_target',
        ),
        Index('idx_tenor_delta_symbol_snapshot', 'symbol', 'snapshot_time'),
        Index('idx_tenor_delta_targets', 'symbol', 'target_tenor_dte', 'target_delta_abs'),
    )


class TenorDeltaCollectionProgress(Base):
    """
    Progress tracker dedicated to tenor-delta collections.
    """
    __tablename__ = 'tenor_delta_collection_progress'

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(64), unique=True, nullable=False, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    snapshot_time = Column(DateTime, nullable=False, index=True)
    status = Column(String(20), nullable=False, default='STARTED')  # STARTED, IN_PROGRESS, COMPLETED, FAILED
    expected_targets = Column(Integer, nullable=False, default=0)
    completed_targets = Column(Integer, nullable=False, default=0)
    missing_targets = Column(Integer, nullable=False, default=0)
    error_message = Column(String(500))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class TenorDeltaTargetStatus(Base):
    """
    Per-target status for tenor-delta selection attempts.
    """
    __tablename__ = 'tenor_delta_target_status'

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    snapshot_time = Column(DateTime, nullable=False, index=True)
    option_type = Column(String(1), nullable=False)  # 'C' or 'P'
    target_tenor_dte = Column(Integer, nullable=False)
    target_delta_abs = Column(Float, nullable=False)

    status = Column(String(32), nullable=False, index=True)  # SELECTED, MISSING_NO_CANDIDATE, MISSING_NULL_FIELDS, ERROR
    reason = Column(String(500))

    selected_expiry = Column(DateTime)
    selected_strike = Column(Float)
    selected_delta = Column(Float)
    selected_delta_gap_abs = Column(Float)
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint(
            'batch_id',
            'symbol',
            'option_type',
            'target_tenor_dte',
            'target_delta_abs',
            name='unique_tenor_delta_target_status',
        ),
        Index('idx_tenor_delta_status_lookup', 'symbol', 'snapshot_time', 'status'),
    )


class OptionsHistoricalData(Base):
    """
    Historical options OHLCV data keyed by contract and bar timestamp.
    """
    __tablename__ = 'options_historical_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    strike = Column(Float, nullable=False)
    expiry = Column(Date, nullable=False, index=True)
    right = Column(String(1), nullable=False)
    what_to_show = Column(String(32), nullable=False, default='TRADES')
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    underlying_price = Column(Float)
    collection_batch = Column(String(64), index=True)
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint(
            'symbol',
            'strike',
            'expiry',
            'right',
            'what_to_show',
            'date',
            name='unique_option_history_bar',
        ),
        Index('idx_options_history_contract', 'symbol', 'expiry', 'strike', 'right', 'what_to_show'),
    )


class UnderlyingPriceHistory(Base):
    """
    Historical underlying daily bars used to derive strike universe per date.
    """
    __tablename__ = 'underlying_price_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    price = Column(Float)
    collection_batch = Column(String(64), index=True)
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint('symbol', 'date', name='unique_underlying_history_date'),
    )


class ContractCheckpoint(Base):
    """
    Per-contract checkpoint status for completeness-first historical collection.
    """
    __tablename__ = 'contract_checkpoints'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Composite identity fields for a target contract/date request.
    batch_id = Column(String(64), nullable=False, index=True)
    symbol = Column(String(10), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    expiry = Column(Date, nullable=False, index=True)
    strike = Column(Float, nullable=False)
    right = Column(String(1), nullable=False)
    bar_size = Column(String(20), nullable=False)
    what_to_show = Column(String(32), nullable=False, default='TRADES')
    use_rth = Column(Boolean, nullable=False, default=True)

    # Execution state and retry bookkeeping.
    status = Column(String(40), nullable=False, default='PENDING', index=True)
    attempts = Column(Integer, nullable=False, default=0)
    retry_budget = Column(Integer, nullable=False, default=3)
    backoff_until = Column(DateTime)
    last_error = Column(Text)
    unavailable_reason = Column(String(120))
    probe_evidence = Column(Text)
    last_attempt_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint(
            'batch_id',
            'symbol',
            'trade_date',
            'expiry',
            'strike',
            'right',
            'bar_size',
            'what_to_show',
            'use_rth',
            name='unique_contract_checkpoint_target',
        ),
        Index('idx_checkpoint_queue', 'batch_id', 'status', 'expiry', 'strike'),
        Index('idx_checkpoint_trade_day', 'symbol', 'trade_date', 'status'),
    )

class DatabaseManager:
    """
    Database connection and session management
    """
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection"""
        print("Initializing database connection...")
        db_host = getenv('DB_HOST', 'localhost')
        db_port = getenv('DB_PORT', '5432')
        db_name = getenv('DB_NAME', 'options_data')
        db_user = getenv('DB_USER', 'postgres')
        db_password = getenv('DB_PASSWORD', '')
        
        try: 
            connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=3600
            )
            
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise

    def create_tables(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)
    
    def get_session(self):
        """Get database session"""
        return self.SessionLocal()
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()