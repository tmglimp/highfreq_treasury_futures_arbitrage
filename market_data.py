import logging
import sys
import pandas as pd
import numpy as np
import requests
import urllib3
import config
from enums.MarketDataField import MarketDataField
from leaky_bucket import leaky_bucket
from ir_data.market_data import 

# Configure logging to both file and stdout
logging.basicConfig(
    level=config.LOG_LEVEL,
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# Disable SSL Warnings (for Client Web API)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MarketData:
    @staticmethod
    def get_index_market_data() -> tuple:
        """
        Retrieves market data and updates the global USTs and FUTURES DataFrames.
        Steps:
          1. Fetch market data in batches using the conid values.
          2. Merge and extract market data fields.
          3. Mark rows where any price column (ask, bid, or last) starts with "C" (or "c") as closed and remove them.
          4. Convert FUTURES prices to their decimal representations.
          5. Update price fields for futures and USTs.
          6. Write the updated DataFrames to CSV.
        """
        if (config.USTs is None or config.FUTURES is None or
                config.USTs.empty or config.FUTURES.empty):
            logging.warning("USTs or FUTURES not populated; skipping market data retrieval.")
            return None, None

        # Fetch market data in batches
        market_data = MarketData.get_market_data(config.USTs, config.FUTURES, config.ZEROES)

        # Merge market data fields into config DataFrames
        config.USTs, config.FUTURES = MarketData.extract_market_data_fields(
            config.USTs, config.FUTURES, config.ZEROES, market_data)

        # Mark rows as "closed" if any price starts with "C"/"c" and remove them
        config.USTs  = MarketData.mark_and_remove_closed_rows(config.USTs)
        config.FUTURES = MarketData.mark_and_remove_closed_rows(config.FUTURES)
        config.ZEROES = MarketData.mark_and_remove_closed_rows(config.ZEROES)

        # For FUTURES, convert futures price quotes to decimals
        config.FUTURES = MarketData.convert_futures_price(config.FUTURES)

        # Update UST pricing values
        config.USTs = MarketData.update_ust_price(config.USTs)
        config.ZEROES = MarketData.update_ust_price(config.ZEROES)

        return config.USTs, config.FUTURES, config.ZEROES

    @staticmethod
    def convert_futures_price(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert futures price columns like 'bid_price', 'ask_price', and 'last_price' to their decimal representations.
        Prices are in the format 'whole'fraction, and they are converted as per the defined rules.
        """
        def convert_price(price: str) -> float:
            """
            Converts the price (e.g., '113'221') to a decimal. The fractional part is split and processed.
            """
            # Ensure the price is a string and check if it starts with 'C' or 'c' (to exclude it)
            price = str(price).strip()

            # Exclude prices starting with 'C' or 'c'
            if price.startswith(('C', 'c')):
                return None

            try:
                # Split the price into whole and fractional parts
                whole, fraction = price.split("'")
                whole = int(whole)

                # Process the fractional part depending on its length
                if len(fraction) == 3:
                    A = int(fraction[:2])  # First two digits (A)
                    B = int(fraction[2])   # Last digit (B)
                else:
                    A = int(fraction)  # Handle shorter fractions (e.g., 17 in 108'17)
                    B = 0

                # If A is 0, just return the whole part without adding anything for A
                if A == 0:
                    return whole

                # Convert A and B as per the described rules
                A_fraction = A / 32  # A is a fraction of 32
                B_fraction = 0  # Default value for B

                if B == 1:
                    B_fraction = 0.125 / 32
                elif B == 2:
                    B_fraction = 0.25 / 32
                elif B == 3:
                    B_fraction = 0.375 / 32
                elif B == 4:
                    B_fraction = 0.375 / 32
                elif B == 5:
                    B_fraction = 0.5 / 32
                elif B == 6:
                    B_fraction = 0.625 / 32
                elif B == 7:
                    B_fraction = 0.75 / 32
                elif B == 8:
                    B_fraction = 0.875 / 32

                # Combine A and B and return as a float
                decimal_price = whole + A_fraction + B_fraction
                return decimal_price

            except ValueError:
                print(f"Error converting futures price: {price}.")
                return None

        # Apply conversion for all relevant price columns in the FUTURES DataFrame
        for price_column in ['bid_price', 'ask_price', 'last_price']:
            df[f'{price_column}_decimal'] = df[price_column].apply(convert_price)

        return df

    @staticmethod
    def get_market_data(UST_index: pd.DataFrame, FUT_index: pd.DataFrame, batch_size: int = 500) -> list:
        """
        Retrieve market data from the IBKR API in batches for all conids combined
        from UST_index and FUT_index.
        """
        con_ids = UST_index['conid'].tolist() + FUT_index['conid'].tolist() + UST_index['corpuscusip_conid'].tolist()
        market_data = []

        # Get the market data fields from enum
        fields = [field.value for field in MarketDataField]
        logging.info("Fetching market data...")

        # Process batches of conids
        for i in range(0, len(con_ids), batch_size):
            batch_conids = con_ids[i:i + batch_size]
            csv_con_ids = ",".join(map(str, batch_conids))
            csv_fields = ",".join(map(str, fields))
            url = f"{config.IBKR_BASE_URL}/v1/api/iserver/marketdata/snapshot?conids={csv_con_ids}&fields={csv_fields}&live=true"

            leaky_bucket.wait_for_token()
            logging.info(f"Requesting from {url}")

            response = requests.get(url=url, verify=False)
            if response.status_code != 200:
                logging.error(f"Response from {url}: {response.status_code} - Unable to fetch market data.")
                continue

            logging.info(f"Response from {url}: {response.status_code} - Successfully scanned market data.")
            print(f"Batch starting at index {i} response:", response.json())

            md = market_data.extend(response.json())
            md = pd.DataFrame(md)
            md.to_csv("md.csv")

        logging.info("Retrieved market data.")
        return market_data

    @staticmethod
    def extract_market_data_fields(UST_index: pd.DataFrame, FUT_index: pd.DataFrame, market_data: list) -> tuple:
        """
        Extract and merge market data fields from the market data list into the UST and FUTURES DataFrames.
        """
        logging.info("Extracting market data fields...")
        market_data_df = pd.DataFrame(market_data)
        print("Raw market_data DataFrame from server:")
        print(market_data_df.head())

        if market_data_df.empty or 'conid' not in market_data_df.columns:
            logging.warning("Market data is empty or missing 'conid'. Skipping merge.")
            return UST_index, FUT_index

        # Select only the desired fields plus conid
        available_fields = [str(field.value) for field in MarketDataField if str(field.value) in market_data_df.columns]
        columns_to_use = ['conid'] + available_fields
        market_data_df = market_data_df[columns_to_use]

        # Rename columns from numeric field IDs to human-readable names
        rename_map = {str(field.value): field.name for field in MarketDataField if str(field.value) in market_data_df.columns}
        market_data_df.rename(columns=rename_map, inplace=True)

        # Ensure that conid is numeric in all DataFrames for proper merging
        market_data_df['conid'] = pd.to_numeric(market_data_df['conid'], errors='coerce').astype('Int64')
        UST_index['conid'] = pd.to_numeric(UST_index['conid'], errors='coerce').astype('Int64')
        FUT_index['conid'] = pd.to_numeric(FUT_index['conid'], errors='coerce').astype('Int64')
        UST_index = pd.DataFrame(UST_index)
        FUT_index = pd.DataFrame(FUT_index)

        # Determine columns to update (existing) and columns to merge (new)
        existing_columns = set(UST_index.columns).intersection(market_data_df.columns) - {'conid'}
        new_columns = set(market_data_df.columns) - set(UST_index.columns) - {'conid'}

        print(f"UST_index length => {len(UST_index)}")
        if existing_columns:
            UST_index.set_index('conid', inplace=True)
            FUT_index.set_index('conid', inplace=True)
            UST_index.update(market_data_df[list(existing_columns)])
            FUT_index.update(market_data_df[list(existing_columns)])
            UST_index.reset_index(inplace=True)
            FUT_index.reset_index(inplace=True)
            market_data_df.reset_index(inplace=True)
            print(f"Updated UST_index length => {len(UST_index)}")
            print(f"Updated FUT_index length => {len(FUT_index)}")

        if new_columns:
            new_col_list = ['conid'] + list(new_columns)
            UST_index = pd.merge(UST_index, market_data_df[new_col_list], how='left', on='conid')
            FUT_index = pd.merge(FUT_index, market_data_df[new_col_list], how='left', on='conid')

        logging.info("Updated UST_index and FUT_index with market data details.")
        return UST_index, FUT_index

    @staticmethod
    def mark_and_remove_closed_rows(df: pd.DataFrame) -> pd.DataFrame:
        """
        Mark and remove rows where price columns start with 'C' or 'c' (i.e., closed prices).
        """
        for col in ['ask_price', 'bid_price', 'last_price']:
            if col not in df.columns:
                df[col] = np.nan  # or np.nan if you prefer float dtype

        df['closed'] = (
            df['ask_price'].astype(str).str.startswith(('C', 'c')) |
            df['bid_price'].astype(str).str.startswith(('C', 'c')) |
            df['last_price'].astype(str).str.startswith(('C', 'c'))
        ).map({True: "True", False: "N/A"})
        return df[df['closed'] != "True"]

    @staticmethod
    def update_ust_price(df: pd.DataFrame) -> pd.DataFrame:
        """
        For USTs: Convert bid_price and last_price to numeric values and set the 'price' column,
        preferring bid_price when available.
        """
        if 'bid_price' in df.columns and 'last_price' in df.columns:
            df['bid_price'] = pd.to_numeric(df['bid_price'], errors='coerce')
            df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
            df['price'] = df['bid_price']
        elif 'last_price' in df.columns:
            df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
            df['price'] = df['last_price']
        return df