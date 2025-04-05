import threading
import time

import Future_index
import config
from business_logic import business_logic_function
from orders import suppress_order_warning
from scraper import run_scraper

if __name__ == "__main__":
    print("🚀 Running initial UST scraper...")
    run_scraper()  # This would populate config.USTs
    Future_index.main()  # Run Futures discovery and secdef. This would populate config.FUTURES

    # Suppress applicable message ids in config at start of script
    suppress_order_warning(config.SUPPRESSED_IDS.split(','))

    logic_thread = threading.Thread(target=business_logic_function, daemon=True)
    logic_thread.start()

    while True:
        time.sleep(.0000000000001)
