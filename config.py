# Configuration settings for bond trading application
import pandas as pd

# IBKR Client Portal Web API
IBKR_BASE_URL = "https://localhost:5000"
IBKR_ACCT_ID = ""  # populate with IBKR Acct ID. Leave empty for security.

# Logging Settings
LOG_FORMAT = "'%(asctime)s - %(name)s - %(levelname)s - %(message)s'"
LOG_LEVEL = "INFO"
LOG_FILE = "application.log"

PERCENT_PROFIT = .3

# Applicable Objects
FUT_SYMBOLS = "ZT,ZF,ZN,TN,Z3N"
USTs = pd.DataFrame()  # DF to be populated during runtime
FUTURES = pd.DataFrame()  # DF to be populated during runtime
FUTURES_historical = pd.DataFrame()  # DF to be populated during runtime
USTs_historical = pd.DataFrame()  # DF to be populated during runtime
HEDGES = pd.DataFrame()
X = 0
ACTIVE_ORDERS_LIMIT = 5  # Limit for number of active orders per time
VOLUME = 1
ORDERS = pd.DataFrame()  # to be populated during runtime
updated_ORDERS = pd.DataFrame()
ORDERS2 = pd.DataFrame()  # for multi-threading
ORDERS3 = pd.DataFrame()  # for multi-threading
SUPPRESSED_IDS = "o163,o451,o354,o383"  # A csv of applicable message ids to suppress

INITIAL_MARGIN = pd.DataFrame()
RTH_INITIAL_MARGIN = pd.DataFrame()
