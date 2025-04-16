import logging
import sys

import pandas as pd
import requests
import urllib3

import config
from enums.MarketDataField import MarketDataField
from leaky_bucket import leaky_bucket

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
        market_data = MarketData.get_market_data(config.USTs, config.FUTURES)

        # Merge market data fields into config DataFrames
        config.USTs, config.FUTURES = MarketData.extract_market_data_fields(
            config.USTs, config.FUTURES, market_data)

        # Mark rows as "closed" if any price starts with "C"/"c" and remove them
        config.USTs = MarketData.mark_and_remove_closed_rows(config.USTs)
        config.FUTURES = MarketData.mark_and_remove_closed_rows(config.FUTURES)

        # For FUTURES, convert futures price quotes to decimals using the increment
        config.FUTURES = MarketData.convert_price_to_decimal(config.FUTURES)
        config.FUTURES = MarketData.update_empty_price(config.FUTURES)

        # Update UST pricing values
        config.USTs = MarketData.update_ust_price(config.USTs)

        # Save the updated DataFrames to CSV
        config.USTs.to_csv("config.USTs_43.csv", index=False)
        config.FUTURES.to_csv("config.FUTURES_41.csv", index=False)

        return config.USTs, config.FUTURES

    @staticmethod
    def mark_and_remove_closed_rows(df: pd.DataFrame) -> pd.DataFrame:
        """
        Create a 'closed' column that marks rows as "True" if any of the
        price columns (ask, bid, last) start with "C" or "c", and then remove them.
        """
        df['closed'] = (
            df['ask_price'].astype(str).str.startswith(('C', 'c')) |
            df['bid_price'].astype(str).str.startswith(('C', 'c')) |
            df['last_price'].astype(str).str.startswith(('C', 'c'))
        ).map({True: "True", False: "N/A"})
        return df[df['closed'] != "True"]

    @staticmethod
    def get_market_data(UST_index: pd.DataFrame, FUT_index: pd.DataFrame, batch_size: int = 500) -> list:
        """
        Retrieve market data from the IBKR API in batches for all conids combined
        from UST_index and FUT_index.
        """
        con_ids = UST_index['conid'].tolist() + FUT_index['conid'].tolist()
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

            market_data.extend(response.json())

        logging.info("Retrieved market data.")
        return market_data

    @staticmethod
    def extract_market_data_fields(UST_index: pd.DataFrame,
                                   FUT_index: pd.DataFrame,
                                   market_data: list) -> tuple:
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

        # Determine columns to update (existing) and columns to merge (new)
        existing_columns = set(UST_index.columns).intersection(market_data_df.columns) - {'conid'}
        new_columns = set(market_data_df.columns) - set(UST_index.columns) - {'conid'}

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

        if new_columns:
            new_col_list = ['conid'] + list(new_columns)
            UST_index = pd.merge(UST_index, market_data_df[new_col_list], how='left', on='conid')
            FUT_index = pd.merge(FUT_index, market_data_df[new_col_list], how='left', on='conid')
            print(f"Merged UST_index length => {len(UST_index)}")
            print(f"New columns => {new_columns}")

        logging.info("Updated UST_index and FUT_index with market data details.")
        print("Final UST_index length =>", len(UST_index))
        return UST_index, FUT_index

    @staticmethod
    def convert_futures_price(price: str, fut_type: str) -> float:
        """
        Convert a futures price (e.g., "134'16.5") to its decimal representation.
        If the price starts with "C", return None.
        """
        try:
            p = str(price).strip()
            if p.startswith("C"):
                return None
            if "'" not in p:
                return float(p)

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

            if '.' in fraction:
                frac_32nd, frac_part = map(int, fraction.split('.'))
                decimal_fraction = frac_32nd / 32 + frac_part / (32 * denominator)
            else:
                decimal_fraction = int(fraction) / 32
            return whole + decimal_fraction
        except ValueError:
            raise ValueError("Invalid futures price format. Expected format: 134'16.5")

    @staticmethod
    def convert_price_to_decimal(df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts futures price quotes (ask, bid, and last) to their decimal representation using the 'increment' value.
        """
        fut_type = {
            '0.001953125': 'sixteenth',
            '0.00390625': 'eighth',
            '0.0078125': 'quarter',
            '0.015625': 'half',
            '0.03125': 'whole',
        }
        for price_col in ["ask_price", "bid_price", "last_price"]:
            if price_col in df.columns and "increment" in df.columns:
                new_col = price_col + "_decimal"
                df[new_col] = df.apply(
                    lambda row: MarketData.convert_futures_price(
                        row[price_col] if row[price_col] not in [None, ""] else "0",
                        fut_type.get(str(row["increment"]))
                    ),
                    axis=1
                )
        return df

    @staticmethod
    def update_empty_price(df: pd.DataFrame) -> pd.DataFrame:
        """
        Updates price-related columns:
          - Sets 'ask_price' from ask_price_decimal, defaulting to last_price_decimal if needed.
          - Sets 'bid_price' similarly.
          - Replaces 'last_price' with last_price_decimal.
          - Sets 'price' to bid_price_decimal if present, otherwise last_price_decimal.
        """
        if "last_price_decimal" in df.columns:
            df["ask_price"] = df.get("ask_price_decimal").combine_first(df["last_price_decimal"])
            df["bid_price"] = df.get("bid_price_decimal").combine_first(df["last_price_decimal"])
            df["last_price"] = df["last_price_decimal"]
            df["price"] = df.get("bid_price_decimal").combine_first(df["last_price_decimal"])
        return df

    @staticmethod
    def update_ust_price(df: pd.DataFrame) -> pd.DataFrame:
        """
        For USTs: Convert bid_price and last_price to numeric values and set the 'price' column,
        preferring bid_price when available.
        """
        if 'bid_price' in df.columns and 'last_price' in df.columns:
            df['bid_price'] = pd.to_numeric(df['bid_price'], errors='coerce')
            df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
            df['price'] = df['bid_price'].combine_first(df['last_price'])
        elif 'last_price' in df.columns:
            df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
            df['price'] = df['last_price']
        return df
