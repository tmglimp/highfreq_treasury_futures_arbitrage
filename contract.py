import logging
import sys

import requests
import urllib3

import config
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

# Disable SSL Warnings (Against Client Web API)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Contract:

    @staticmethod
    def get_security_definition(contracts, batch_size=50):
        """
        Fetch contract details for corresponding array list [] of contracts.
        """

        # Request contract details for all contracts collected
        contract_details = []
        logging.info('Fetching contract details...')
        for i in range(0, len(contracts), batch_size):
            batch_contracts = contracts[i:i + batch_size]
            csv_con_ids = ",".join(str(item.get("con_id", item.get("conid"))) for item in batch_contracts)

            url = config.IBKR_BASE_URL + f"/v1/api/trsrv/secdef?conids={csv_con_ids}"

            # Block until a token is available
            leaky_bucket.wait_for_token()

            logging.info(f'Requesting from {url}')

            response = requests.get(url=url, verify=False)

            if response.status_code != 200:
                logging.error(f'Response from {url}: {response.status_code} : Unable to proceed '
                              f'with contract details retrieval.')
                continue  # Skip to the next batch on error

            logging.info(f'Response from {url}: {response.status_code} : Successfully retrieved contract details')

            contract_details.extend(response.json()["secdef"])  # Add details to the main list

        logging.info('Done fetching contract details...')

        return contract_details

