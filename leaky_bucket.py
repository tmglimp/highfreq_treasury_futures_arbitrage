import logging
import sys
import time
import threading

import config

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])


class LeakyBucket:
    def __init__(self, capacity, leak_rate):
        self.capacity = capacity  # Maximum number of requests allowed in {leak_rate} second(s)
        self.leak_rate = leak_rate  # Base time rate for leak
        self.tokens = capacity  # Initially, the bucket is set to full capacity
        self.last_checked = time.time()  # Track the last time we checked for leaks
        self.lock = threading.Lock()  # To make the tracker thread-safe

    def _leak(self):
        """Leak tokens from the bucket based on time elapsed"""
        current_time = time.time()
        time_elapsed = current_time - self.last_checked

        if time_elapsed > self.leak_rate:
            # Re-fill bucket with tokens up to intended capacity
            self.tokens = min(self.capacity, self.tokens + int(time_elapsed * self.leak_rate))
            self.last_checked = current_time

    def acquire(self):
        """Acquire a token from the bucket (to make an API request)"""
        with self.lock:
            self._leak()

            if self.tokens > 0:
                self.tokens -= 1
                return True

            logging.warning(
                f"LeakyBucket: No tokens to leak at the moment. {self.capacity} requests per {self.leak_rate} second("
                f"s) reached. Check back shortly.")
            return False

    def wait_for_token(self):
        """Wait until a token is available"""
        while not self.acquire():
            time.sleep(0.1)  # Sleep briefly before checking again


# Initialize the global LeakyBucket with a capacity of 49 requests per second
leaky_bucket = LeakyBucket(capacity=49, leak_rate=1)
