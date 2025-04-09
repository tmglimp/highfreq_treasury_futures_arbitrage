import logging
import sys

import requests
import urllib3
import time
import config
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

# Ignore insecure error messages
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class OrdersLeakyBucket:
    def __init__(self, poll_interval=1.0):
        """
        Initialize the leaky bucket.
        :param poll_interval: Time (seconds) between polling the API for active order count.
        """
        self.api_base_url = config.IBKR_BASE_URL
        self.max_orders = config.ACTIVE_ORDERS_LIMIT
        self.poll_interval = poll_interval
        self.session = requests.Session()  # Reuse connection for efficiency

    def _get_active_orders_count(self):
        """
        Ping the IBKR API to get the current number of active orders.
        Returns the count of orders with status like 'Submitted' or 'PreSubmitted'.
        """
        try:

            # Wait until an API request token is available (rate-limiting control)
            leaky_bucket.wait_for_token()

            url = f"{self.api_base_url}/v1/api/iserver/account/orders"
            response = self.session.get(url, verify=False)  # Disable SSL verification for local dev
            response.raise_for_status()

            orders = response.json().get("orders", [])
            active_statuses = {"Submitted", "PreSubmitted", "PendingSubmit"}
            active_count = sum(1 for order in orders if order.get("status") in active_statuses)
            return active_count
        except requests.RequestException as e:
            logging.info(f"OrdersLeakyBucket: Error fetching active orders: {e}")
            return None

    def wait_for_slot(self):
        """
        Block until there's an available slot for a new order.
        Polls the API periodically to check active order count.
        """
        while True:
            active_count = self._get_active_orders_count()
            if active_count is None:
                time.sleep(self.poll_interval)
                continue
            if active_count < self.max_orders:
                logging.info(f"OrdersLeakyBucket: Order slot available: {self.max_orders - active_count} remaining")
                return
            logging.info(f"OrdersLeakyBucket: Active orders ({active_count}) at limit ({self.max_orders}). Waiting...")
            time.sleep(self.poll_interval)
