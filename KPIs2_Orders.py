"""
KPIs2_Orders
"""

import json
import logging
import math
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import urllib3

import config
from config import updated_ORDERS
from leaky_bucket import leaky_bucket
from risklimits import compute_risk_metrics

# Disable SSL Warnings (for external API requests)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# SIA-Standardized Utility Functions  ## (Amend as needed to work with correct dataframe columns)
def accrued_interest(coupon, mat_date, today):
    try:
        # Try direct substitution of month/day with today's year
        last_coupon = mat_date.replace(year=today.year)
    except ValueError:
        # If invalid day (e.g., Feb 30), fallback to the last day of that month
        last_day = pd.Timestamp(today.year, mat_date.month, 1) + pd.offsets.MonthEnd(0)
        last_coupon = last_day

    if last_coupon > today:
        last_coupon -= pd.DateOffset(months=6)

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
    nl_value = pnl_json.get("upnl", {}).get(acct_key, {}).get("nl")
    return nl_value

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
                HEDGES_Combos[f'{leg}_Days'])]
        HEDGES_Combos[f'{leg}_NetBasis'] = [
            sia_net_basis(gb, carry)
            for gb, carry in zip(HEDGES_Combos[f'{leg}_GrossBasis'], HEDGES_Combos[f'{leg}_Carry'])
        ]

    nl_value = get_acct_dets()
    nl_value = float(nl_value)
    SMA = nl_value * 4  # approximate reg-t margin limit
    config.SMA = SMA
    print(f'SMA => {SMA}')
    return config.SMA

def calculate_quantities(HEDGES_Combos, SMA):
    SMA = calculate_quantities_with_sma(HEDGES_Combos)
    if SMA > 25000:
        HEDGES_Combos['A_Q_Value'], HEDGES_Combos['B_Q_Value'] = 1, 1
        # If A_ImpliedRepo < B_ImpliedRepo, front is rich (forward roll: short A, long B).
        # Otherwise, reverse roll (short B, long A).
        HEDGES_Combos['A_Q_Value'] = np.where(
            HEDGES_Combos['A_ImpliedRepo'] < HEDGES_Combos['B_ImpliedRepo'],
            1,  # short A for forward roll
            -1  # long A for reverse roll
        )
        HEDGES_Combos['B_Q_Value'] = np.where(
            HEDGES_Combos['A_ImpliedRepo'] < HEDGES_Combos['B_ImpliedRepo'],
            -1,  # long B for forward roll
            1  # short B for reverse roll
        )

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

    # Compute the row-level notional value of each pair.
    HEDGES_Combos['Row_Notional'] = (HEDGES_Combos['A_Q_Value']*HEDGES_Combos['cost_A'] + HEDGES_Combos['B_Q_Value']*HEDGES_Combos['cost_B'])

    # Compute the adjusted net basis for each leg at the prescribed hedge ratio.
    HEDGES_Combos['PairsAdjNetBasis'] = ((
        (HEDGES_Combos['A_NetBasis'] * HEDGES_Combos['A_Q_Value']) -
        (HEDGES_Combos['B_NetBasis'] * HEDGES_Combos['B_Q_Value'])
    ))

    # Convert volume columns to numeric.
    HEDGES_Combos['A_FUT_VOLUME'] = pd.to_numeric(HEDGES_Combos['A_FUT_VOLUME'], errors='coerce')
    HEDGES_Combos['B_FUT_VOLUME'] = pd.to_numeric(HEDGES_Combos['B_FUT_VOLUME'], errors='coerce')
    HEDGES_Combos = HEDGES_Combos.dropna(subset=['A_FUT_VOLUME', 'B_FUT_VOLUME'])
    HEDGES_Combos = HEDGES_Combos[
        (HEDGES_Combos['A_FUT_VOLUME'] > 0) & (HEDGES_Combos['B_FUT_VOLUME'] > 0)
    ]

    HEDGES_Combos['ln_A_FUT'] = np.log(HEDGES_Combos['A_FUT_VOLUME'])
    HEDGES_Combos['ln_B_FUT'] = np.log(HEDGES_Combos['B_FUT_VOLUME'])
    HEDGES_Combos['Base_Ln_A'] = (1 + HEDGES_Combos['ln_A_FUT']/config.VS)
    HEDGES_Combos['Base_Ln_B'] = (1 + HEDGES_Combos['ln_B_FUT']/config.VS)
    HEDGES_Combos['Z_Ln_WeightedVol'] = ((HEDGES_Combos['Base_Ln_A'] + HEDGES_Combos['Base_Ln_B'])/2)
    print(f'printing avg weighted ln param as', HEDGES_Combos['Z_Ln_WeightedVol'])

    # Compute RENTD metric.
    HEDGES_Combos['RENTD'] = HEDGES_Combos['PairsAdjNetBasis'] * HEDGES_Combos['Z_Ln_WeightedVol']
    HEDGES_Combos = HEDGES_Combos.sort_values(by='RENTD', ascending=False)
    HEDGES_Combos.to_csv('HEDGES_Combos.csv')

    unique_rows = (HEDGES_Combos
        .drop_duplicates(subset=['A_FUT_CONID', 'B_FUT_CONID'], keep='first')
        .copy())
    unique_rows = (HEDGES_Combos
        .drop_duplicates(subset=['A_FUT_CONID', 'B_FUT_CONID'], keep='first')
        .copy())

    if len(unique_rows) >= 5:
        A = unique_rows.iloc[0]
        B = unique_rows.iloc[1]
        C = unique_rows.iloc[2]
        D = unique_rows.iloc[3]
        E = unique_rows.iloc[4]

    else:
        # Assign defaults or replicate the last row enough times to have 3 rows.
        default = unique_rows.iloc[0] if len(unique_rows) > 0 else pd.Series(
            {col: None for col in HEDGES_Combos.columns})
        A = unique_rows.iloc[0] if len(unique_rows) > 0 else default
        B = unique_rows.iloc[1] if len(unique_rows) > 1 else default
        C = unique_rows.iloc[2] if len(unique_rows) > 2 else default
        D = unique_rows.iloc[3] if len(unique_rows) > 3 else default
        E = unique_rows.iloc[4] if len(unique_rows) > 4 else default

    config.ORDERS = pd.DataFrame([A, B, C, D, E], columns=HEDGES_Combos.columns)

    # call risklimits here
    config.updated_ORDERS = compute_risk_metrics(config.ORDERS)
    config.updated_ORDERS.to_csv('updated_ORDERS.csv')
    print('printing updated_ORDERS as', config.updated_ORDERS)
    return config.updated_ORDERS

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
