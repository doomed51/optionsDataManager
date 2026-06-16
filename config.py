IB_CLIENT_ID = 2
IB_TWS_PORT = 7496

WHATTOSHOW_MAPPING = {
    "AVGO": "BID_ASK",
}


# symbol, strikes above the high and below the ask price of the underlying, number of expiries forward from the current date
COLLECTION_SYMBOLS_METADATA = {
    "AVGO": {"strikes": 10, "expiries": 5},
}

DEFAULT_NUM_STRIKES = 10
DEFAULT_NUM_EXPIRIES = 5




#_____________________________ skew data collection configs _____________________________
SKEW_DATA_SYMBOLS = [
    "SPX",
]

SKEW_DATA_SYMBOL_CONFIG = {
    "SPX": {
        "tenors_dte": [180, 210, 240],              # 6, 7, 8 months 
        "deltas_abs": [0.01, 0.05, 0.10, 0.20],
    },
}

TENOR_DELTA_DEFAULT_TENORS_DTE = [7, 14, 30, 60]
TENOR_DELTA_DEFAULT_DELTAS_ABS = [0.01, 0.05, 0.10, 0.20]
TENOR_DELTA_RUN_TIMES = ['10:00', '12:00', '13:00', '15:00']

# Strike traversal controls when searching for target deltas 
TENOR_DELTA_TRAVERSAL_MAX_HOPS = 12
TENOR_DELTA_TRAVERSAL_NO_IMPROVEMENT_PATIENCE = 2

# candidate strike selection configs 
DELTA_TO_MONEYNESS_OFFSET = {
        0.40: 0.012,
        0.30: 0.021,
        0.20: 0.031,
        0.10: 0.046,
        0.05: 0.06,
        0.01: 0.098,
    }
TENOR_DELTA_DTE_REF_DAYS = 30
TENOR_DELTA_DTE_OFFSET_ALPHA = 0.4


#_________________________________________________ Global security related lookups
INDEX_LIST = ['SPX', 'SPXW']

EXCHANGE_MAPPINGS_INDEX = {
    'DXJ': 'ARCA',
    'GE': 'SMART',
    'GVZ': 'CBOE',
    'INDU': 'CME',
    'NDX': 'NASDAQ',
    'SPX': 'CBOE',
    'SPXW': 'CBOE',
    'TSX': 'TSE',
    'VIX': 'CFE',
    'VIX1D': 'CBOE',
    'VIX3M': 'CBOE',
    'VVIX': 'CBOE',
}