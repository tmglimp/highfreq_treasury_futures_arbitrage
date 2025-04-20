# Configuration settings for bond trading application
import pandas as pd

#IBKR Client Portal Web API
IBKR_BASE_URL = "https://localhost:5000"
IBKR_ACCT_ID = "DUK699024"  # populate with IBKR Acct ID. Leave empty for security.

# Logging Settings
LOG_FORMAT = "'%(asctime)s - %(name)s - %(levelname)s - %(message)s'"
LOG_LEVEL = "INFO"
LOG_FILE = "application.log"

### SCALARS ###
PERCENT_PROFIT = 1 # scales net profit after fees
PRICE_EXP = 1 # expands instantaneous spread as scalar
VS = 8 #denominator coefficient for logarithmic volume scalar penalty fn (larger no. = less weight to vol var in RENTD).
SPREAD_TRACK_WINDOW_MINUTES = 10 # of minutes from which eligible bid/ask values may be drawn
SPREAD_POP = 1500 # of accumulated rows to pass for determining spread width
PEND_CLEAR = 100 # max/s to let position sit in queue before DEL operation.

###m MARGIN ###
MARGIN_CUSHION = .05 # Margin cushion (volatility control measure)
UNDER = 1 - MARGIN_CUSHION #KPIs2Orders Reciprocal % of acct value to use determining nominal size
INIT_MARG_THRESH = pd.DataFrame()
INITIAL_MARGIN = pd.DataFrame()

### DICTIONARY OBJECTS ###
FUT_SYMBOLS = "ZT,ZF,ZN,TN,Z3N"
USTs = pd.DataFrame()  # DF to be populated during runtime
FUTURES = pd.DataFrame()  # DF to be populated during runtime
FUTURES_historical = pd.DataFrame()  # DF to be populated during runtime
USTs_historical = pd.DataFrame()  # DF to be populated during runtime
HEDGES = pd.DataFrame()
X = 0

### ORDERS ###
ACTIVE_ORDERS_LIMIT = 5  # Limit for number of active orders per time
VOLUME = 1
ORDERS = pd.DataFrame()  # to be populated during runtime
updated_ORDERS = pd.DataFrame() # to be populated during runtime
SUPPRESSED_IDS = "o163,o451,o354,o383"  # A csv of applicable message ids to suppress

### HISTORICAL DATA FOR RISK METRICS ###
FUTURES_VARIANCE = pd.DataFrame()


