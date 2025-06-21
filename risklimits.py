import logging
import sys
import os
from idlelib.configdialog import changes

import numpy as np
import pandas as pd
import requests
import urllib3
from scipy.stats import norm
import datetime
from datetime import datetime, timedelta

import config
from config import ORDERS
from fixed_income_calc import approximate_convexity, approximate_duration
from leaky_bucket import leaky_bucket

# Global accumulator for overlayA
duration_register = pd.DataFrame(columns=['NET_OVERLAY'])

# Configure logging to both file and stdout
logging.basicConfig(
    level=config.LOG_LEVEL,
    format=config.LOG_FORMAT,
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# Ignore insecure error messages
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_all_historical():
    logging.info("Checking historical data files...")

    futures_path = 'FUTURES_historical.csv'
    usts_path = 'USTs_historical.csv'

    futures_exists = os.path.exists(futures_path)
    usts_exists = os.path.exists(usts_path)

    if not futures_exists:
        logging.info("FUTURES historical data missing. Fetching new data.")
        futures_data = fetch__historical(config.FUTURES.iterrows(), 'FUTURES')
        futures_rows = []
        for conid, records in futures_data.items():
            futures_rows.extend(records)
        df = pd.DataFrame(futures_rows)
        df.set_index("conid", inplace=True)
        df.to_csv(futures_path, index=False)
        config.FUTURES_historical = df
    else:
        logging.info("Loading existing FUTURES historical data.")
        config.FUTURES_historical = pd.read_csv(futures_path)

    if not usts_exists:
        logging.info("USTs historical data missing. Fetching new data.")
        usts_data = fetch__historical(config.USTs.iterrows(), 'USTs')
        usts_rows = []
        for conid, records in usts_data.items():
            usts_rows.extend(records)
        df = pd.DataFrame(usts_rows)
        df.set_index("conid", inplace=True)
        df.to_csv(usts_path, index=False)
        config.USTs_historical = df
    else:
        logging.info("Loading existing USTs historical data.")
        config.USTs_historical = pd.read_csv(usts_path)

    logging.info("Finished processing historical market data.")

def fetch__historical(contracts, type):
    historical_data = {}
    meta_columns = [
        "serverId", "symbol", "text", "priceFactor", "startTime", "high", "low",
        "timePeriod", "barLength", "mdAvailability", "mktDataDelay", "outsideRth",
        "volumeFactor", "priceDisplayRule", "priceDisplayValue", "negativeCapable", "messageVersion"
    ]
    for idx, row in contracts:
        conid = row["conid"]
        logging.info(f"Fetching {type} historical data for conid {conid}: {idx + 1}...")
        url = f"{config.IBKR_BASE_URL}/v1/api/iserver/marketdata/history"
        params = {
            "conid": conid,
            "period": "1mo",
            "bar": "1d",
            "outsideRth": False
        }
        leaky_bucket.wait_for_token()
        response = requests.get(url, params=params, verify=False)
        if response.status_code == 200:
            json_obj = response.json()
            metadata = {key: json_obj.get(key) for key in meta_columns}
            records = []
            today_date = datetime.today()
            for seq, point in enumerate(json_obj.get("data", []), start=1):
                observation_date = today_date - timedelta(days=seq)
                combined_row = {**metadata, **point}
                if "t" in combined_row:
                    combined_row["t"] = pd.to_datetime(combined_row["t"], unit="ms").strftime("%m/%d/%Y")
                combined_row["sequence"] = seq
                combined_row["observation_date"] = observation_date.strftime("%m/%d/%Y")
                combined_row["conid"] = conid
                records.append(combined_row)
            historical_data[conid] = records
            logging.info(f"Retrieved {len(records)} historical points for {type} conid {conid}")
        else:
            logging.info(f"Failed to fetch history for {type} conid {conid}: {idx + 1}")
            historical_data[conid] = []
    return historical_data

def safe_duration(dur):
    return dur if dur is not None else 0.0

def compute_risk_metrics(ORDERS):
    print("Starting risk metrics computation...")

    orders_df = pd.DataFrame(config.ORDERS).copy()
    global duration_register

    for idx, row in orders_df.iterrows():
        # duration + DV01 + overlayA calculations [unchanged for brevity]

        # Assume overlayA is computed as below (context retained from earlier message)
        front_multiplier = row['A_FUT_MULTIPLIER']
        back_multiplier = row['B_FUT_MULTIPLIER']
        front_contract_value = row['A_FUT_TPRICE']
        back_contract_value = row['B_FUT_TPRICE']
        front_ratio = row["A_Q_Value"]
        back_ratio = row["B_Q_Value"]
        front_dv01 = row['A_FUT_DV01']
        back_dv01 = row['B_FUT_DV01']

        net_contract_value = (front_multiplier * front_contract_value * front_ratio +
                              back_multiplier * back_contract_value * back_ratio)

        overlayA = round((front_dv01 * front_ratio * front_multiplier  * front_ratio +
                          back_dv01 * back_ratio * back_multiplier * back_ratio), 8)

        orders_df.at[idx, 'NET_OVERLAY'] = overlayA

        # Append to global accumulator
        duration_register.loc[len(duration_register)] = [overlayA]

        orders_df.at[idx, 'EQUITY_DELTA'] = round((overlayA / net_contract_value), 7)

    # After loop, compute total overlay
    config.TOTAL_OVERLAY = duration_register['NET_OVERLAY'].sum()

    print("Final computed ORDERS:")
    print(orders_df)
    config.updated_ORDERS = orders_df
    return config.updated_ORDERS

if __name__ == "__main__":
    fetch_all_historical()
    compute_risk_metrics(ORDERS)
