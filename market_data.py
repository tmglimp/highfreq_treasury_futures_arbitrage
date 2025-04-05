import logging
import math
import sys

import pandas as pd
import requests
import urllib3

import config
from enums.MarketDataField import MarketDataField
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

# Disable SSL Warnings (Against Client Web API)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MarketData:

    @staticmethod
    def get_index_market_data():
        """
        This method retrieves market data for config DataFrames (USTs and FUTURES) using the `get_market_data` method
        and updates the USTs and FUTURES DataFrames by extracting relevant market data fields and converting
        prices to decimals.
        """
        # Run market data only after global variables config.USTs and config.FUTURES have been populated.
        if config.USTs is not None and config.FUTURES is not None and not config.USTs.empty and not config.FUTURES.empty:
            market_data = MarketData.get_market_data(config.USTs, config.FUTURES)

            # Map the market data information to local USTs, FUTURES without tampering with the
            # global config.USTs & config.FUTURES
            USTs, FUTURES = MarketData.extract_market_data_fields(config.USTs, config.FUTURES, market_data)
            FUTURES = MarketData.convert_price_to_decimal(FUTURES)
            FUTURES = MarketData.update_empty_price(FUTURES)
            USTs = MarketData.update_ust_price(USTs)

            return USTs, FUTURES

    @staticmethod
    def get_market_data(UST_index, FUT_index, batch_size=500):
        """
        Fetches market data for a list of contracts (conids) from two dataframes in batches.

        Args:
            UST_index: A pandas DataFrame containing a 'conid' column.
            FUT_index: A pandas DataFrame containing a 'conid' column.
            batch_size: The number of contracts to process in each API call (default 500).

        Returns:
            An array containing market data for all contracts.
        """

        # Extract and merge conids from both dataframes
        con_ids = UST_index['conid'].tolist() + FUT_index['conid'].tolist()

        market_data = []  # Array to store all market data

        # Get the market data fields we are interested in
        fields = [field.value for field in MarketDataField]

        logging.info('Fetching market data...')
        for i in range(0, len(con_ids), batch_size):
            batch_conids = con_ids[i:i + batch_size]
            csv_con_ids = ",".join(map(str, batch_conids))
            csv_fields = ",".join(map(str, fields))

            url = config.IBKR_BASE_URL + (f"/v1/api/iserver/marketdata/snapshot?"
                                          f"conids={csv_con_ids}&fields={csv_fields}&live=true")

            # Block until a token is available
            leaky_bucket.wait_for_token()

            logging.info(f'Requesting from {url}')

            response = requests.get(url=url, verify=False)

            if response.status_code != 200:
                logging.error(f'Response from {url}: {response.status_code} : Unable to fetch market data.')
                continue  # Skip to the next batch on error

            logging.info(f'Response from {url}: {response.status_code} : Successfully scanned market data.')
            # Added print statement to output the raw JSON response from the server
            print(f"Batch starting at index {i} response from server:")
            print(response.json())

            # Update the market_data array based on response
            market_data.extend(response.json())

        logging.info('Retrieved market data...')

        return market_data
        print(f'market_data printing as', market_data)

    @staticmethod
    def extract_market_data_fields(UST_index, FUT_index, market_data):
        """
        Extracts and merges market data fields into UST_index and FUT_index DataFrames
        based on matching 'conid' values.

        Parameters:
        - UST_index (pd.DataFrame): DataFrame with a 'conid' column.
        - FUT_index (pd.DataFrame): DataFrame with a 'conid' column.
        - market_data (list): A list of market data dictionaries.

        Returns:
        - tuple: (updated UST_index, updated FUT_index)
        """
        logging.info("Extracting market data fields...")

        # Convert market_data list to DataFrame
        market_data_df = pd.DataFrame(market_data)

        # Added print statement to show the head of the market_data DataFrame
        print("Raw market_data DataFrame from server:")
        print(market_data_df.head())

        if market_data_df.empty or 'conid' not in market_data_df.columns:
            logging.warning("Market data is empty or missing 'conid'. Skipping merge.")
            return UST_index, FUT_index

        # Filter columns returned by IBKR
        available_fields = [str(field.value) for field in MarketDataField if str(field.value) in market_data_df.columns]
        columns_to_use = ['conid'] + available_fields
        market_data_df = market_data_df[columns_to_use]

        # Rename numeric IBKR field identifiers to human-readable column names
        rename_map = {str(field.value): field.name for field in MarketDataField if
                      str(field.value) in market_data_df.columns}
        market_data_df.rename(columns=rename_map, inplace=True)

        # Ensure 'conid' is consistent type
        market_data_df['conid'] = pd.to_numeric(market_data_df['conid'], errors='coerce').astype('Int64')
        UST_index['conid'] = pd.to_numeric(UST_index['conid'], errors='coerce').astype('Int64')
        FUT_index['conid'] = pd.to_numeric(FUT_index['conid'], errors='coerce').astype('Int64')

        # Separate shared and new columns (excluding 'conid')
        existing_columns = set(UST_index.columns).intersection(market_data_df.columns) - {'conid'}
        new_columns = set(market_data_df.columns) - set(UST_index.columns) - {'conid'}

        # Update existing market data columns (if applicable) based on 'conid'
        print(f"UST_index length => {len(UST_index)}")
        if existing_columns:
            UST_index.set_index('conid', inplace=True)
            FUT_index.set_index('conid', inplace=True)
            market_data_df.set_index('conid', inplace=True)

            UST_index.update(market_data_df[list(existing_columns)])
            FUT_index.update(market_data_df[list(existing_columns)])

            UST_index.reset_index(inplace=True)
            FUT_index.reset_index(inplace=True)
            market_data_df.reset_index(inplace=True)
            print(f"Updated UST_index length => {len(UST_index)}")

        # Merge new market data columns into UST_index and FUT_index
        if new_columns:
            new_col_list = ['conid'] + list(new_columns)
            UST_index = pd.merge(UST_index, market_data_df[new_col_list], how='left', on='conid')
            FUT_index = pd.merge(FUT_index, market_data_df[new_col_list], how='left', on='conid')
            print(f"Merged UST_index length => {len(UST_index)}")
            print(f"New columns => {new_columns}")

        logging.info("Updated UST_index and FUT_index with market data details.")
        print(f"Final UST_index length => {len(UST_index)}", UST_index, FUT_index)
        return UST_index, FUT_index


    @staticmethod
    def convert_futures_price(price: str, fut_type: str) -> float:
        """
        Convert a futures price (e.g., "134'16.5") to its decimal representation.
        If the price starts with a "C", return None.
        """
        try:
            p = str(price).strip()
            # If the price starts with "C", don't convert it.
            if p.startswith("C"):
                return None

            # If there is no apostrophe, try converting directly to float.
            if "'" not in p:
                return float(p)

            # Split the price into whole and fraction parts
            whole, fraction = p.split("'")
            whole = int(whole)
            denominators = {
                'whole': 1,
                'half': 2,
                'quarter': 4,
                'eighth': 8,
                'sixteenth': 16
            }
            denominator = denominators.get(fut_type, 32)
            # Process the fractional part
            if '.' in fraction:
                frac_32nd, frac_part = map(int, fraction.split('.'))
                decimal_fraction = frac_32nd / 32 + frac_part / (32 * denominator)
            else:
                decimal_fraction = int(fraction) / 32
            return whole + decimal_fraction
        except ValueError:
            raise ValueError("Invalid futures price format. Expected format: 134'16.5")


    @staticmethod
    def convert_price_to_decimal(df):
        """
        Converts futures price quotes (ask, bid, and last prices) to their decimal representation.
        """
        fut_type = {
            '0.001953125': 'sixteenth',
            '0.00390625': 'eighth',
            '0.0078125': 'quarter',
            '0.015625': 'half',
            '0.03125': 'whole',
        }
        # Process ask_price on a per-row basis
        if "ask_price" in df.columns and "increment" in df.columns:
            df["ask_price_decimal"] = df.apply(
                lambda row: MarketData.convert_futures_price(
                    row["ask_price"] if row["ask_price"] not in [None, ""] else "0",
                    fut_type.get(str(row["increment"]))
                ),
                axis=1
            )

        # Process bid_price on a per-row basis
        if "bid_price" in df.columns and "increment" in df.columns:
            df["bid_price_decimal"] = df.apply(
                lambda row: MarketData.convert_futures_price(
                    row["bid_price"] if row["bid_price"] not in [None, ""] else "0",
                    fut_type.get(str(row["increment"]))
                ),
                axis=1
            )

        # Process last_price on a per-row basis
        if "last_price" in df.columns and "increment" in df.columns:
            df["last_price_decimal"] = df.apply(
                lambda row: MarketData.convert_futures_price(
                    row["last_price"] if row["last_price"] not in [None, ""] else "0",
                    fut_type.get(str(row["increment"]))
                ),
                axis=1
            )
        return df


    @staticmethod
    def update_empty_price(df):
        """
        Fill price-related columns as follows:
          - ask_price: use ask_price_decimal if available; otherwise, use last_price_decimal.
          - bid_price: use bid_price_decimal if available; otherwise, use last_price_decimal.
          - last_price: use last_price_decimal.
          - price: set to bid_price_decimal if available; otherwise, use last_price_decimal.
        """
        if "last_price_decimal" in df.columns:
            # Fill ask_price using ask_price_decimal if available, else last_price_decimal.
            if "ask_price_decimal" in df.columns:
                df["ask_price"] = df["ask_price_decimal"].combine_first(df["last_price_decimal"])
            else:
                df["ask_price"] = df["last_price_decimal"]

            # Fill bid_price using bid_price_decimal if available, else last_price_decimal.
            if "bid_price_decimal" in df.columns:
                df["bid_price"] = df["bid_price_decimal"].combine_first(df["last_price_decimal"])
            else:
                df["bid_price"] = df["last_price_decimal"]

            # Set last_price as last_price_decimal.
            df["last_price"] = df["last_price_decimal"]

            # For 'price', prefer bid_price_decimal if available; otherwise use last_price_decimal.
            if "bid_price_decimal" in df.columns:
                df["price"] = df["bid_price_decimal"].combine_first(df["last_price_decimal"])
            else:
                df["price"] = df["last_price_decimal"]

        return df


    @staticmethod
    def update_ust_price(df):
        """
        For USTs: ensure that bid_price and last_price are numeric,
        and set 'price' from bid_price when available, otherwise last_price.
        """
        if 'bid_price' in df.columns and 'last_price' in df.columns:
            df['bid_price'] = pd.to_numeric(df['bid_price'], errors='coerce')
            df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
            df['price'] = df['bid_price'].combine_first(df['last_price'])
        elif 'last_price' in df.columns:
            df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
            df['price'] = df['last_price']

        return df
