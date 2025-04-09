"""
KPIs2_Orders
"""
import json
import math
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import urllib3
from pandas.core.interchange.dataframe_protocol import DataFrame
from scipy import stats
from math import lcm

import config
import fees
from leaky_bucket import leaky_bucket
import risklimits  # <-- Import risklimits module

# Disable SSL Warnings (for external API requests)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# SIA-Standardized Utility Functions  ## (Amend as needed to work with correct dataframe columns)
def accrued_interest(coupon, mat_date, today):
    last_coupon = pd.Timestamp(year=today.year, month=mat_date.month, day=mat_date.day)
    if last_coupon > today:
        last_coupon = last_coupon - pd.DateOffset(months=6)
    days_accrued = (today - last_coupon).days
    return (coupon / 2) * (days_accrued / 182.5)

def sia_implied_repo(fut_price, dirty_price, cf, days):
    adj_fut = fut_price * cf
    return ((adj_fut - dirty_price) / dirty_price) * (365 / days)

def sia_gross_basis(fut_price, cf, dirty_price):
    return fut_price * cf - dirty_price

def sia_convexity_yield(dirty_price, coupon, days):
    return ((coupon / dirty_price) * (days / 365)) * (365 / days)

def sia_carry(gross_basis, implied_repo, dirty_price, days):
    financing_cost = dirty_price * implied_repo * days / 365
    return gross_basis - financing_cost

def sia_net_basis(gross_basis, carry):
    return gross_basis + carry

def get_acct_dets():
    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/pnl/partitioned"
    leaky_bucket.wait_for_token()
    print(f'Requesting from {url}')
    pnl_res = requests.get(url=url, verify=False)
    print(f'Response from {url}: {pnl_res.status_code}')
    if pnl_res.headers.get("Content-Type", "").startswith("application/json"):
        print(json.dumps(pnl_res.json(), indent=2))
    else:
        print(pnl_res.text)
    pnl_json = pnl_res.json()
    acct_key = f"{config.IBKR_ACCT_ID}.Core"
    return pnl_json.get("upnl", {}).get(acct_key, {}).get("nl")

def calculate_quantities_with_sma(HEDGES_Combos):
    current_date = pd.to_datetime(datetime.now())

    for leg in ['A', 'B']:
        coupons = HEDGES_Combos[f'{leg}_CTD_COUPON_RATE']
        mat_dates = pd.to_datetime(HEDGES_Combos[f'{leg}_CTD_MATURITY_DATE'])
        fut_prices = HEDGES_Combos[f'{leg}_FUT_PRICE']
        cfs = HEDGES_Combos[f'{leg}_CTD_CF']
        HEDGES_Combos[f'{leg}_AccruedInterest'] = [
            accrued_interest(c, m, current_date) for c, m in zip(coupons, mat_dates)
        ]
        HEDGES_Combos[f'{leg}_DirtyPrice'] = HEDGES_Combos[f'{leg}_CTD_PRICE'] + HEDGES_Combos[f'{leg}_AccruedInterest']
        HEDGES_Combos[f'{leg}_Days'] = (mat_dates - current_date).dt.days
        HEDGES_Combos[f'{leg}_GrossBasis'] = [
            sia_gross_basis(fp, cf, dp)
            for fp, cf, dp in zip(fut_prices, cfs, HEDGES_Combos[f'{leg}_DirtyPrice'])
        ]
        HEDGES_Combos[f'{leg}_ImpliedRepo'] = [
            sia_implied_repo(fp, dp, cf, d)
            for fp, dp, cf, d in zip(fut_prices, HEDGES_Combos[f'{leg}_DirtyPrice'], cfs, HEDGES_Combos[f'{leg}_Days'])
        ]
        HEDGES_Combos[f'{leg}_ConvexityYield'] = [
            sia_convexity_yield(dp, cpn, d)
            for dp, cpn, d in zip(HEDGES_Combos[f'{leg}_DirtyPrice'], coupons, HEDGES_Combos[f'{leg}_Days'])
        ]
        HEDGES_Combos[f'{leg}_Carry'] = [
            sia_carry(gb, repo, dp, d)
            for gb, repo, dp, d in zip(
                HEDGES_Combos[f'{leg}_GrossBasis'],
                HEDGES_Combos[f'{leg}_ImpliedRepo'],
                HEDGES_Combos[f'{leg}_DirtyPrice'],
                HEDGES_Combos[f'{leg}_Days']
            )
        ]
        HEDGES_Combos[f'{leg}_NetBasis'] = [
            sia_net_basis(gb, carry)
            for gb, carry in zip(HEDGES_Combos[f'{leg}_GrossBasis'], HEDGES_Combos[f'{leg}_Carry'])
        ]

    SMA = get_acct_dets() * 4
    print(f'SMA => {SMA}')
    return calculate_quantities(HEDGES_Combos, SMA)
    # (Optionally, you can call run_risk_checks(ORDERS) here.)

def calculate_quantities(HEDGES_Combos, SMA):
    # Convert key columns to numeric.
    front_multiplier = pd.to_numeric(HEDGES_Combos['A_FUT_MULTIPLIER'], errors='coerce')
    back_multiplier = pd.to_numeric(HEDGES_Combos['B_FUT_MULTIPLIER'], errors='coerce')
    A_fut_price = pd.to_numeric(HEDGES_Combos['A_FUT_PRICE'], errors='coerce')
    B_fut_price = pd.to_numeric(HEDGES_Combos['B_FUT_PRICE'], errors='coerce')
    A_fut_dv01 = pd.to_numeric(HEDGES_Combos['A_FUT_DV01'], errors='coerce')
    B_fut_dv01 = pd.to_numeric(HEDGES_Combos['B_FUT_DV01'], errors='coerce')

    # Compute per-row whole-unit costs.
    HEDGES_Combos['cost_A'] = front_multiplier * A_fut_price
    HEDGES_Combos['cost_B'] = back_multiplier * B_fut_price
    # Compute the DV01 ratio per row.
    HEDGES_Combos['ratio'] = A_fut_dv01 / B_fut_dv01

    # Define the notional limit per row (global limit used in each row).
    limit = 0.99 * SMA
    print(f"Using notional limit (0.99 * SMA) = {limit}")

    # For each row, compute the optimal integer quantities.
    qtys = HEDGES_Combos.apply(lambda row: optimize_quantities_for_row(row, limit), axis=1)
    HEDGES_Combos = pd.concat([HEDGES_Combos, qtys], axis=1)

    # Compute the row-level notional.
    HEDGES_Combos['Row_Notional'] = (
        HEDGES_Combos['A_Q_Value'] * HEDGES_Combos['cost_A'] +
        HEDGES_Combos['B_Q_Value'] * HEDGES_Combos['cost_B']
    )
    total_notional = HEDGES_Combos['Row_Notional'].sum()
    print(f"Total notional computed from row values = {total_notional}")

    # Set a placeholder for PairsLCM (if needed).
    HEDGES_Combos['PairsLCM'] = HEDGES_Combos.apply(
        lambda row: math.gcd(int(row['A_Q_Value']), int(row['B_Q_Value']))
        if pd.notnull(row['A_Q_Value']) and pd.notnull(row['B_Q_Value'])
        else np.nan, axis=1
    )

    # Normalize quantities by dividing by the common factor.
    HEDGES_Combos['A_Q_Value'] = HEDGES_Combos.apply(
        lambda row: int(row['A_Q_Value']) // row['PairsLCM']
        if row['PairsLCM'] not in [0, np.nan] else row['A_Q_Value'], axis=1
    )
    HEDGES_Combos['B_Q_Value'] = HEDGES_Combos.apply(
        lambda row: int(row['B_Q_Value']) // row['PairsLCM']
        if row['PairsLCM'] not in [0, np.nan] else row['B_Q_Value'], axis=1
    )

    # Compute the adjusted net basis.
    HEDGES_Combos['PairsAdjNetBasis'] = (
        (HEDGES_Combos['A_NetBasis'] * HEDGES_Combos['A_Q_Value'] * front_multiplier * HEDGES_Combos['PairsLCM']) -
        (HEDGES_Combos['B_NetBasis'] * HEDGES_Combos['B_Q_Value'] * back_multiplier * HEDGES_Combos['PairsLCM'])
    )
    HEDGES_Combos['PairsAdjNetBasis'] = pd.to_numeric(HEDGES_Combos['PairsAdjNetBasis'], errors='coerce')

    # --- NEW: Determine roll direction and assign quantity signs ---
    # If A_ImpliedRepo < B_ImpliedRepo, then the front is rich (forward roll: short A, long B).
    # Otherwise, it's a reverse roll (short B, long A).
    HEDGES_Combos['A_Q_sign'] = np.where(
        HEDGES_Combos['A_ImpliedRepo'] < HEDGES_Combos['B_ImpliedRepo'],
        -1,  # short A for forward roll
         1   # long A for reverse roll
    )
    HEDGES_Combos['B_Q_sign'] = np.where(
        HEDGES_Combos['A_ImpliedRepo'] < HEDGES_Combos['B_ImpliedRepo'],
         1,  # long B for forward roll
        -1   # short B for reverse roll
    )
    # --- END NEW: roll direction logic ---

    # Convert volume columns to numeric.
    HEDGES_Combos['A_FUT_VOLUME'] = pd.to_numeric(HEDGES_Combos['A_FUT_VOLUME'], errors='coerce')
    HEDGES_Combos['B_FUT_VOLUME'] = pd.to_numeric(HEDGES_Combos['B_FUT_VOLUME'], errors='coerce')
    HEDGES_Combos = HEDGES_Combos.dropna(subset=['A_FUT_VOLUME', 'B_FUT_VOLUME'])
    HEDGES_Combos = HEDGES_Combos[
        (HEDGES_Combos['A_FUT_VOLUME'] > 0) & (HEDGES_Combos['B_FUT_VOLUME'] > 0)
    ]

    HEDGES_Combos['ln_A_FUT'] = np.log(HEDGES_Combos['A_FUT_VOLUME'])
    HEDGES_Combos['ln_B_FUT'] = np.log(HEDGES_Combos['B_FUT_VOLUME'])
    HEDGES_Combos['Base_Ln_A'] = (1 + HEDGES_Combos['ln_A_FUT']/25) # int 25 should be converted to dynamic optimization parameter for later testing
    print(f'printing base ln A as', HEDGES_Combos['Base_Ln_A'])
    HEDGES_Combos['Base_Ln_B'] = (1 + HEDGES_Combos['ln_B_FUT']/25)
    print(f'printing base ln B as', HEDGES_Combos['Base_Ln_B'])
    HEDGES_Combos['Z_Ln_WeightedVol'] = ((HEDGES_Combos['Base_Ln_A'] + HEDGES_Combos['Base_Ln_A'])/2)
    print(f'printing avg weighted ln param as', HEDGES_Combos['Z_Ln_WeightedVol'])

    # Compute RENTD metric.
    HEDGES_Combos['RENTD'] = HEDGES_Combos['PairsAdjNetBasis'] * HEDGES_Combos['Z_Ln_WeightedVol']
    HEDGES_Combos = HEDGES_Combos.sort_values(by='RENTD', ascending=False)
    HEDGES_Combos.to_csv('HEDGES_Combos.csv')

    unique_rows = HEDGES_Combos.drop_duplicates(keep='first')
    if len(unique_rows) >= 3:
        A = unique_rows.iloc[0]
        B = unique_rows.iloc[1]
        C = unique_rows.iloc[2]

    ORDERS = []
    ORDERS = pd.DataFrame([A, B, C], columns=HEDGES_Combos.columns)

    ORDERS.to_csv('ORDERS.csv')
    config.ORDERS = ORDERS
    return HEDGES_Combos, ORDERS

def optimize_quantities_for_row(row, limit):
    """
    For a given row (i.e. for one hedge pair), find the integer quantities Q_A and Q_B
    (with Q_A >= 1 and Q_B >= 1) such that the total cost:
       cost = Q_A * (A_FUT_MULTIPLIER * A_FUT_PRICE) + Q_B * (B_FUT_MULTIPLIER * B_FUT_PRICE)
    is maximized while remaining <= limit,
    and such that Q_A/Q_B is as close as possible to the DV01 ratio, r.
    """
    cost_A = row['A_FUT_MULTIPLIER'] * row['A_FUT_PRICE']
    cost_B = row['B_FUT_MULTIPLIER'] * row['B_FUT_PRICE']
    r = 1.0 if row['B_FUT_DV01'] == 0 else row['A_FUT_DV01'] / row['B_FUT_DV01']

    best_q_a = None
    best_q_b = None
    best_cost = -1
    best_error = float('inf')
    max_q_b = int(limit // cost_B) if cost_B > 0 else 1
    for q_b in range(1, max_q_b + 1):
        q_a_candidate = int(round(r * q_b))
        if q_a_candidate < 1:
            q_a_candidate = 1
        cost_candidate = q_a_candidate * cost_A + q_b * cost_B
        if cost_candidate <= limit:
            error_candidate = abs((q_a_candidate / q_b) - r)
            if cost_candidate > best_cost:
                best_cost = cost_candidate
                best_q_a = q_a_candidate
                best_q_b = q_b
                best_error = error_candidate
            elif cost_candidate == best_cost and error_candidate < best_error:
                best_q_a = q_a_candidate
                best_q_b = q_b
                best_error = error_candidate

    if best_q_a is None or best_q_b is None:
        best_q_a, best_q_b = 1, 1
    return pd.Series({'A_Q_Value': best_q_a, 'B_Q_Value': best_q_b})

def calculate_total_fees(top_row, if2risky, if2riskyagain):
    # Note: Ensure that the fee functions from the 'fees' module return appropriate values.
    top_fee_data = fees.calculate_total_fees(
        config.VOLUME,
        top_row['A_FUT_LISTING_EXCHANGE'],
        top_row['B_FUT_LISTING_EXCHANGE'],
        top_row['A_FUT_TICKER'],
        top_row['B_FUT_TICKER']
    )
    # (Assuming fees.calculate_total_fees returns a tuple or dictionary with fee values.)
    # For demonstration, we'll assume the returned fees for top_row are stored in variables A_fees and B_fees.
    A_fees, B_fees = top_fee_data  # Adjust according to your actual fee function's return structure.
    top_total_fees = A_fees + B_fees  # Example combination

    mid_fee_data = fees.calculate_total_fees(
        config.VOLUME,
        if2risky['A_FUT_LISTING_EXCHANGE'],
        if2risky['B_FUT_LISTING_EXCHANGE'],
        if2risky['A_FUT_TICKER'],
        if2risky['B_FUT_TICKER']
    )
    mid_fee_A, mid_fee_B = mid_fee_data
    mid_total_fees = mid_fee_A + mid_fee_B

    bot_fee_data = fees.calculate_total_fees(
        config.VOLUME,
        if2riskyagain['A_FUT_LISTING_EXCHANGE'],
        if2riskyagain['B_FUT_LISTING_EXCHANGE'],
        if2riskyagain['A_FUT_TICKER'],
        if2riskyagain['B_FUT_TICKER']
    )
    bot_fee_A, bot_fee_B = bot_fee_data
    bot_total_fees = bot_fee_A + bot_fee_B

    # Here we return a dictionary or similar object containing fee values.
    return fee_dict, top_total_fees, mid_total_fees, bot_total_fees

def build_orders(fee_dict):
    # Use the fee_dict returned from calculate_total_fees.
    # Build orders using the quantity sign fields we set earlier.
    # (Make sure that top_row, if2risky, and if2riskyagain are available in your current scope.)
    ORDERS = config.ORDERS  # This DataFrame was set earlier in calculate_quantities.
    top_row = ORDERS.iloc[0]
    if2risky = ORDERS.iloc[1]
    if2riskyagain = ORDERS.iloc[2]

    config.ORDERS = [
       {
           "front_conId": top_row['A_FUT_CONID'],
           "front_ratio": top_row['B_Q_Value'] * top_row['A_Q_sign'],
           "back_conId": top_row['B_FUT_CONID'],
           "back_ratio": top_row['A_Q_Value'] * top_row['B_Q_sign'],
           "quantity": top_row['PairsLCM'],
           "price": -0.8 * (top_row['PairsAdjNetBasis'] - fee_dict['top_total_fees'])
       },
       {
           "front_conId": if2risky['A_FUT_CONID'],
           "front_ratio": if2risky['B_Q_Value'] * if2risky['A_Q_sign'],
           "back_conId": if2risky['B_FUT_CONID'],
           "back_ratio": if2risky['A_Q_Value'] * if2risky['B_Q_sign'],
           "quantity": if2risky['PairsLCM'],
           "price": -0.8 * (if2risky['PairsAdjNetBasis'] - fee_dict['mid_total_fees'])
       },
       {
           "front_conId": if2riskyagain['A_FUT_CONID'],
           "front_ratio": if2riskyagain['B_Q_Value'] * if2riskyagain['A_Q_sign'],
           "back_conId": if2riskyagain['B_FUT_CONID'],
           "back_ratio": if2riskyagain['B_Q_Value'] * if2riskyagain['B_Q_sign'],
           "quantity": if2riskyagain['PairsLCM'],
           "price": -0.8 * (if2riskyagain['PairsAdjNetBasis'] - fee_dict['bot_total_fees'])
       }
    ]
    pd.DataFrame(config.ORDERS).to_csv('config.ORDERS.csv')
    return config.ORDERS

## Optionally, include the run_risk_checks function as needed.
#def run_risk_checks(orders):
#    for order in orders:
#        result = risklimits.compute_risk_metrics(order)
#        if result:
#            config.SELECTED_ORDER = order
#            print("Selected Order Passed Risk Check:", result)
#            break
