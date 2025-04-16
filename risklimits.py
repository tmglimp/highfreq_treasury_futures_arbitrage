import logging
import sys
import os
import numpy as np
import pandas as pd
import requests
import urllib3
from scipy.stats import norm
import datetime

import config
from config import ORDERS
from fixed_income_calc import approximate_convexity, approximate_duration
from leaky_bucket import leaky_bucket

# Configure logging to both file and stdout
logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, handlers=[
    logging.FileHandler(config.LOG_FILE),
    logging.StreamHandler(sys.stdout)
])

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
            for r in records:
                entry = {'conid': conid, **{f'col{i+1}': v for i, v in enumerate(r)}}
                futures_rows.append(entry)
        df = pd.DataFrame(futures_rows)
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
            for r in records:
                entry = {'conid': conid, **{f'col{i+1}': v for i, v in enumerate(r)}}
                usts_rows.append(entry)
        df = pd.DataFrame(usts_rows)
        df.to_csv(usts_path, index=False)
        config.USTs_historical = df
    else:
        logging.info("Loading existing USTs historical data.")
        config.USTs_historical = pd.read_csv(usts_path)

    logging.info("Finished processing historical market data.")


def fetch__historical(contracts, type):
    historical_data = {}
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
            historical_data[conid] = response.json().get("data", [])
            logging.info(f"Retrieved {len(historical_data[conid])} historical points for {type} conid {conid}")
        else:
            logging.info(f"Failed to fetch history for {type} conid {conid}: {idx + 1}")
            historical_data[conid] = []
    return historical_data


def safe_duration(dur):
    """Return the duration if not None; otherwise, return 0.0."""
    return dur if dur is not None else 0.0


def compute_risk_metrics(ORDERS):
    print("Starting risk metrics computation...")

    # Copy the futures and orders data.
    futures_df = config.FUTURES.copy()
    orders_df = pd.DataFrame(config.ORDERS).copy()
    orders_df.to_csv('orders_df_102.csv')

    # Compute volatilities, convexities, and durations for each order.
    volatilities = {'A_Volatility': [], 'B_Volatility': []}
    convexities = {}
    durations = {}
    for idx, row in orders_df.iterrows():
        for leg in ['A', 'B']:
            conid = row[f"{leg}_FUT_CONID"]
            cf = row.get(f"{leg}_CTD_CF", 1.0)
            data = config.FUTURES_historical.get(conid, [])
            closes = [point["c"] for point in data if "c" in point]
            if len(closes) >= 2:
                returns = np.diff(np.log(closes))
                std_dev = np.std(returns)
                vol = std_dev * norm.ppf(0.99)
            else:
                vol = 0.0
            if leg == 'A':
                volatilities['A_Volatility'].append(abs(vol))
            else:
                volatilities['B_Volatility'].append(abs(vol))
            delta_ys = [0.0001, 0.001, -0.0001, -0.001]
            hist_conv = {dy: [] for dy in delta_ys}
            hist_dur = {dy: [] for dy in delta_ys}
            cpn = row.get(f"{leg}_CTD_COUPON_RATE")
            term = row.get(f"{leg}_CTD_YTM")
            ytm = row.get(f"{leg}_CTD_YIELD")
            cf_val = row.get(f"{leg}_CTD_CF", 1.0)
            for price in closes:
                for dy in delta_ys:
                    try:
                        conv = approximate_convexity(cpn, term, ytm, delta_y=dy) / cf_val
                        hist_conv[dy].append(conv)
                        dur = approximate_duration(cpn, term, ytm, delta_y=dy) / cf_val
                        hist_dur[dy].append(dur)
                    except Exception as e:
                        hist_conv[dy].append(0.0)
                        hist_dur[dy].append(0.0)
            convexities[conid] = hist_conv
            durations[conid] = hist_dur

    orders_df['A_Volatility'] = volatilities['A_Volatility']
    orders_df['B_Volatility'] = volatilities['B_Volatility']
    orders_df.to_csv('ORDERS_TEST.csv', index=False)
    print("Orders after volatility computation:")
    print(orders_df)
    orders_df.to_csv('orders_df.csv')

    # Extract non-changing fields for the front (A) and back (B) legs.
    # (Here we assume that the date and coupon fields do not change within the row.)
    for idx, row in orders_df.iterrows():
        # Front leg (A)
        front_cf = row['A_CTD_CF']
        front_cpn = row['A_CTD_COUPON_RATE']
        front_term = row['A_CTD_YTM']
        front_ytm = row['A_IMP_YIELD']
        front_begin = pd.to_datetime(row["A_CTD_PREV_CPN"]).strftime('%Y%m%d')
        front_settle = pd.to_datetime(row["A_CTD_MATURITY_DATE"]).strftime('%Y%m%d')
        front_nextcpn = pd.to_datetime(row["A_CTD_NCPDT"]).strftime('%Y%m%d')
        # Back leg (B)
        back_cf = row['B_CTD_CF']
        back_cpn = row['B_CTD_COUPON_RATE']
        back_term = row['B_CTD_YTM']
        back_ytm = row['B_IMP_YIELD']
        back_begin = pd.to_datetime(row["B_CTD_PREV_CPN"]).strftime('%Y%m%d')
        back_settle = pd.to_datetime(row["B_CTD_MATURITY_DATE"]).strftime('%Y%m%d')
        back_nextcpn = pd.to_datetime(row["B_CTD_NCPDT"]).strftime('%Y%m%d')

        # Compute durations for various stress levels for front (A)
        f_dur_pos_point5 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                              period=2, begin=front_begin, settle=front_settle,
                                                              next_coupon=front_nextcpn, delta_y=0.5, day_count=1))
        f_dur_neg_point5 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                              period=2, begin=front_begin, settle=front_settle,
                                                              next_coupon=front_nextcpn, delta_y=-0.5, day_count=1))
        f_dur_pos_pointoh5 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                               period=2, begin=front_begin, settle=front_settle,
                                                               next_coupon=front_nextcpn, delta_y=0.05, day_count=1))
        f_dur_neg_pointoh5 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                               period=2, begin=front_begin, settle=front_settle,
                                                               next_coupon=front_nextcpn, delta_y=-0.05, day_count=1))
        f_dur_pos_pointoh1 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                               period=2, begin=front_begin, settle=front_settle,
                                                               next_coupon=front_nextcpn, delta_y=0.01, day_count=1))
        f_dur_neg_pointoh1 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                               period=2, begin=front_begin, settle=front_settle,
                                                               next_coupon=front_nextcpn, delta_y=-0.01, day_count=1))
        f_dur_pos_pointohoh5 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                                  period=2, begin=front_begin, settle=front_settle,
                                                                  next_coupon=front_nextcpn, delta_y=0.005, day_count=1))
        f_dur_neg_pointohoh5 = safe_duration(approximate_duration(cpn=front_cpn, term=front_term, yield_=front_ytm,
                                                                  period=2, begin=front_begin, settle=front_settle,
                                                                  next_coupon=front_nextcpn, delta_y=-0.005, day_count=1))
        # Compute durations for back (B)
        b_dur_pos_point5 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                              period=2, begin=back_begin, settle=back_settle,
                                                              next_coupon=back_nextcpn, delta_y=0.5, day_count=1))
        b_dur_neg_point5 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                              period=2, begin=back_begin, settle=back_settle,
                                                              next_coupon=back_nextcpn, delta_y=-0.5, day_count=1))
        b_dur_pos_pointoh5 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                               period=2, begin=back_begin, settle=back_settle,
                                                               next_coupon=back_nextcpn, delta_y=0.05, day_count=1))
        b_dur_neg_pointoh5 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                               period=2, begin=back_begin, settle=back_settle,
                                                               next_coupon=back_nextcpn, delta_y=-0.05, day_count=1))
        b_dur_pos_pointoh1 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                               period=2, begin=back_begin, settle=back_settle,
                                                               next_coupon=back_nextcpn, delta_y=0.01, day_count=1))
        b_dur_neg_pointoh1 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                               period=2, begin=back_begin, settle=back_settle,
                                                               next_coupon=back_nextcpn, delta_y=-0.01, day_count=1))
        b_dur_pos_pointohoh5 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                                  period=2, begin=back_begin, settle=back_settle,
                                                                  next_coupon=back_nextcpn, delta_y=0.005, day_count=1))
        b_dur_neg_pointohoh5 = safe_duration(approximate_duration(cpn=back_cpn, term=back_term, yield_=back_ytm,
                                                                  period=2, begin=back_begin, settle=back_settle,
                                                                  next_coupon=back_nextcpn, delta_y=-0.005, day_count=1))

        # Store computed durations into new columns in the DataFrame.
        orders_df.at[idx, 'f_dur_pos_point5'] = f_dur_pos_point5 / front_cf
        orders_df.at[idx, 'f_dur_neg_point5'] = f_dur_neg_point5 / front_cf
        orders_df.at[idx, 'f_dur_pos_pointoh5'] = f_dur_pos_pointoh5 / front_cf
        orders_df.at[idx, 'f_dur_neg_pointoh5'] = f_dur_neg_pointoh5 / front_cf
        orders_df.at[idx, 'f_dur_pos_pointoh1'] = f_dur_pos_pointoh1 / front_cf
        orders_df.at[idx, 'f_dur_neg_pointoh1'] = f_dur_neg_pointoh1 / front_cf
        orders_df.at[idx, 'f_dur_pos_pointohoh5'] = f_dur_pos_pointohoh5 / front_cf
        orders_df.at[idx, 'f_dur_neg_pointohoh5'] = f_dur_neg_pointohoh5 / front_cf

        orders_df.at[idx, 'b_dur_pos_point5'] = b_dur_pos_point5 / back_cf
        orders_df.at[idx, 'b_dur_neg_point5'] = b_dur_neg_point5 / back_cf
        orders_df.at[idx, 'b_dur_pos_pointoh5'] = b_dur_pos_pointoh5 / back_cf
        orders_df.at[idx, 'b_dur_neg_pointoh5'] = b_dur_neg_pointoh5 / back_cf
        orders_df.at[idx, 'b_dur_pos_pointoh1'] = b_dur_pos_pointoh1 / back_cf
        orders_df.at[idx, 'b_dur_neg_pointoh1'] = b_dur_neg_pointoh1 / back_cf
        orders_df.at[idx, 'b_dur_pos_pointohoh5'] = b_dur_pos_pointohoh5 / back_cf
        orders_df.at[idx, 'b_dur_neg_pointohoh5'] = b_dur_neg_pointohoh5 / back_cf

        # Now compute DV01 values for each stress level.
        orders_df.at[idx, 'f_Dv01_pos_pointohoh5'] = (safe_duration(f_dur_pos_pointohoh5) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_pos_pointohoh5'] = (safe_duration(b_dur_pos_pointohoh5) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'pos_pointohoh5_stress_overlay'] = orders_df.at[idx, 'f_Dv01_pos_pointohoh5'] + orders_df.at[idx, 'b_Dv01_pos_pointohoh5']

        orders_df.at[idx, 'f_Dv01_neg_pointohoh5'] = (safe_duration(f_dur_neg_pointohoh5) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_neg_pointohoh5'] = (safe_duration(b_dur_neg_pointohoh5) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'neg_pointohoh5_stress_overlay'] = orders_df.at[idx, 'f_Dv01_neg_pointohoh5'] + orders_df.at[idx, 'b_Dv01_neg_pointohoh5']

        orders_df.at[idx, 'f_Dv01_pos_pointoh1'] = (safe_duration(f_dur_pos_pointoh1) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_pos_pointoh1'] = (safe_duration(b_dur_pos_pointoh1) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'pos_pointoh1_stress_overlay'] = orders_df.at[idx, 'f_Dv01_pos_pointoh1'] + orders_df.at[idx, 'b_Dv01_pos_pointoh1']

        orders_df.at[idx, 'f_Dv01_neg_pointoh1'] = (safe_duration(f_dur_neg_pointoh1) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_neg_pointoh1'] = (safe_duration(b_dur_neg_pointoh1) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'neg_pointoh1_stress_overlay'] = orders_df.at[idx, 'f_Dv01_neg_pointoh1'] + orders_df.at[idx, 'b_Dv01_neg_pointoh1']

        orders_df.at[idx, 'f_Dv01_pos_pointoh5'] = (safe_duration(f_dur_pos_pointoh5) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_pos_pointoh5'] = (safe_duration(b_dur_pos_pointoh5) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'pos_pointoh5_stress_overlay'] = orders_df.at[idx, 'f_Dv01_pos_pointoh5'] + orders_df.at[idx, 'b_Dv01_pos_pointoh5']

        orders_df.at[idx, 'f_Dv01_neg_pointoh5'] = (safe_duration(f_dur_neg_pointoh5) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_neg_pointoh5'] = (safe_duration(b_dur_neg_pointoh5) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'neg_pointoh5_stress_overlay'] = orders_df.at[idx, 'f_Dv01_neg_pointoh5'] + orders_df.at[idx, 'b_Dv01_neg_pointoh5']

        orders_df.at[idx, 'f_Dv01_pos_point5'] = (safe_duration(f_dur_pos_point5) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_pos_point5'] = (safe_duration(b_dur_pos_point5) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'pos_point5_stress_overlay'] = orders_df.at[idx, 'f_Dv01_pos_point5'] + orders_df.at[idx, 'b_Dv01_pos_point5']

        orders_df.at[idx, 'f_Dv01_neg_point5'] = (safe_duration(f_dur_neg_point5) * row['A_FUT_TPRICE']) * 0.0001 * row['A_Q_Value']
        orders_df.at[idx, 'b_Dv01_neg_point5'] = (safe_duration(b_dur_neg_point5) * row['B_FUT_TPRICE']) * 0.0001 * row['B_Q_Value']
        orders_df.at[idx, 'neg_point5_stress_overlay'] = orders_df.at[idx, 'f_Dv01_neg_point5'] + orders_df.at[idx, 'b_Dv01_neg_point5']

        # Also compute VAR, Position Risk, NET OVERLAY, and NET VALUE.
        quantity = row["PairsLCM"]
        front_multiplier = row['A_FUT_MULTIPLIER']
        back_multiplier = row['B_FUT_MULTIPLIER']
        front_contract_value = row['A_FUT_TPRICE']
        back_contract_value = row['B_FUT_TPRICE']
        front_ratio = row["A_Q_Value"]
        back_ratio = row["B_Q_Value"]
        front_vol = orders_df.at[idx, "A_Volatility"]
        back_vol = orders_df.at[idx, "B_Volatility"]
        front_dv01 = row['A_FUT_DV01']
        back_dv01 = row['B_FUT_DV01']
        front_sign = row['A_Q_sign']
        back_sign = row['B_Q_sign']

        holding_period_yield_delta = 1
        holding_period_price_delta = 1

        var_front = quantity * front_multiplier * holding_period_yield_delta * front_vol * front_ratio
        var_back = quantity * back_multiplier * holding_period_yield_delta * back_vol * back_ratio
        orders_df.at[idx, 'VAR'] = var_front + var_back

        pos_risk_front = quantity * front_multiplier * front_contract_value * holding_period_price_delta * front_vol * front_ratio
        pos_risk_back = quantity * back_multiplier * back_contract_value * holding_period_price_delta * back_vol * back_ratio
        orders_df.at[idx, 'POS_RISK'] = pos_risk_front + pos_risk_back

        overlayA = (front_dv01 * front_ratio * front_multiplier * quantity * front_sign  +
                    back_dv01 * back_ratio * back_multiplier * quantity * back_sign)
        orders_df.at[idx, 'NET_OVERLAY'] = overlayA

        net_contract_value = (quantity * front_multiplier * front_contract_value * front_ratio +
                              quantity * back_multiplier * back_contract_value * back_ratio)
        orders_df.at[idx, 'NET_VALUE'] = net_contract_value

    # Save the fully computed DataFrame.
    print("Final computed ORDERS:")
    print(orders_df)
    updated_ORDERS = orders_df
    updated_ORDERS.to_csv('updated_ORDERS.csv', index=False)
    config.updated_ORDERS = updated_ORDERS
    return orders_df, updated_ORDERS, config.updated_ORDERS

if __name__ == "__main__":
    # First, ensure historical data is available.
    fetch_all_historical()

    # Compute risk metrics and update ORDERS (no filtering is done).
    compute_risk_metrics(ORDERS)