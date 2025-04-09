#!/usr/bin/env python
"""
fixed_income_calc.py

This module computes key fixed‑income metrics for U.S. Treasury bonds including:
  - Theoretical bond price (BPrice)
  - Yield‑to‑maturity (calculate_ytm and P2Y)
  - Accrued interest (AInt)
  - Modified duration (MDur)
  - Macaulay duration (MacDur)
  - DV01
  - Convexity (Cvx)
  - Approximate duration and approximate convexity

It also includes helper functions for date calculations and rounding of years‑to‑maturity.
"""

from datetime import datetime, timedelta
from math import pow
import pandas as pd
from scipy.optimize import minimize_scalar


# ---------------- Helper: Round Years-to-Maturity ----------------
def round_ytm(ytm):
    """
    Round the years‑to‑maturity to the nearest 0.5 increment.
    """
    if pd.isnull(ytm):
        return None
    return round(ytm * 2) / 2.0


# ---------------- Date & Term Functions ----------------
def calculate_term(settlement_date_str, maturity_date_str, day_count_convention=365.25):
    """
    Calculate the time to maturity (term) in years based on a day count convention.
    """
    settlement_date = datetime.strptime(settlement_date_str, '%Y%m%d')
    maturity_date = datetime.strptime(maturity_date_str, '%Y%m%d')
    days_to_maturity = (maturity_date - settlement_date).days
    term_in_years = days_to_maturity / day_count_convention
    return term_in_years


def compute_settlement_date(trade_date, t_plus=1):
    """
    Calculate the settlement date (T+1 by default) taking weekends into account.
    trade_date can be a string (YYYYMMDD) or a datetime object.
    """
    if isinstance(trade_date, str):
        trade_date = datetime.strptime(trade_date, '%Y%m%d')
    settlement_date = trade_date
    business_days_added = 0
    while business_days_added < t_plus:
        settlement_date += timedelta(days=1)
        if settlement_date.weekday() < 5:  # Monday=0, ..., Friday=4
            business_days_added += 1
    return settlement_date.strftime('%Y%m%d')


# ---------------- Yield and Price Functions ----------------
def calculate_ytm(market_price, face_value, coupon_rate, time_to_maturity, periods_per_year=2, n_digits=2):
    """
    Calculate yield‑to‑maturity using an iterative method.

    market_price: percentage price relative to face value (e.g. 98.5 means 98.5% of face)
    face_value: par value (typically 100)
    coupon_rate: annual coupon rate in percent (e.g., 5.0 for 5%)
    time_to_maturity: in years
    """
    # Convert market_price from percent to actual price in currency terms.
    market_price = market_price / 100.0 * face_value
    coupon_rate = coupon_rate / 100.0
    coupon_payment = face_value * coupon_rate / periods_per_year

    def bond_price(ytm):
        pv = 0
        T = int(time_to_maturity * periods_per_year)
        for t in range(1, T + 1):
            pv += coupon_payment / (1 + ytm / periods_per_year) ** t
        pv += face_value / (1 + ytm / periods_per_year) ** T
        return pv

    ytm_guess = coupon_rate
    tolerance = 1e-8
    max_iterations = 1000
    ytm = ytm_guess

    for _ in range(max_iterations):
        price_at_ytm = bond_price(ytm)
        delta_ytm = 1e-5
        price_up = bond_price(ytm + delta_ytm)
        price_down = bond_price(ytm - delta_ytm)
        price_derivative = (price_up - price_down) / (2 * delta_ytm)

        if abs(price_derivative) < 1e-12:
            return round(ytm, n_digits)

        ytm_new = ytm - (price_at_ytm - market_price) / price_derivative
        if abs(ytm_new - ytm) < tolerance:
            return round(ytm_new, n_digits)
        ytm = ytm_new
    print("YTM calculation did not converge within the maximum number of iterations.")
    return ytm


def accrual_period(begin, settle, next_coupon, day_count=1):
    """
    Computes the accrual period.

    For day_count==1, uses Actual/Actual.
    Otherwise, assumes a 30/360 convention.
    """
    if day_count == 1:
        L = datetime.strptime(str(begin), '%Y%m%d')
        S = datetime.strptime(str(settle), '%Y%m%d')
        N = datetime.strptime(str(next_coupon if next_coupon is not None else settle), '%Y%m%d')
        return (S - L).days / (N - L).days
    else:
        # 30/360 convention
        L = [int(begin[:4]), int(begin[4:6]), int(begin[6:8])]
        S = [int(settle[:4]), int(settle[4:6]), int(settle[6:8])]
        return (360 * (S[0] - L[0]) + 30 * (S[1] - L[1]) + S[2] - L[2]) / 180


def AInt(cpn, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Computes the accrued interest.
    """
    v = accrual_period(begin, settle, next_coupon, day_count)
    return cpn / period * v


def BPrice(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Calculates the theoretical bond price (clean price). Rounds the term using round_ytm
    and computes the total coupon periods as an integer.
    If begin, settle, and next_coupon are provided, adjusts for accrued interest.
    """
    if term is None or yield_ is None:
        return None

    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None

    T = int(rounded_term * period)  # Total coupon periods
    C = cpn / period
    Y = yield_ / period

    try:
        price = C * (1 - pow(1 + Y, -T)) / Y + 100 / pow(1 + Y, T)
    except ZeroDivisionError:
        price = None

    if begin and settle and next_coupon:
        v = accrual_period(begin, settle, next_coupon, day_count)
        price = pow(1 + Y, v) * price - v * C

    return price


# ---------------- Duration, Convexity and DV01 Functions ----------------
def MDur(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Computes the modified duration.
    """
    if term is None or yield_ is None:
        return None

    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None

    T = int(rounded_term * period)
    C = cpn / period
    Y = yield_ / period
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or P == 0:
        return None

    if begin and settle and next_coupon:
        v = accrual_period(begin, settle, next_coupon, day_count)
        P = pow(1 + Y, v) * P
        mdur = (
                -v * pow(1 + Y, v - 1) * C / Y * (1 - pow(1 + Y, -T))
                + pow(1 + Y, v) * (
                        C / pow(Y, 2) * (1 - pow(1 + Y, -T))
                        - T * C / (Y * pow(1 + Y, T + 1))
                        + (T - v) * 100 / pow(1 + Y, T + 1)
                )
        )
    else:
        mdur = (C / pow(Y, 2) * (1 - pow(1 + Y, -T))) + (T * (100 - C / Y) / pow(1 + Y, T + 1))
    return mdur / (period * P)


def MacDur(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Computes the Macaulay duration.
    """
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if mdur is None:
        return None
    return mdur * (1 + yield_ / period)


def DV01(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Calculates DV01: the change in bond price for a 1 basis point change in yield.
    DV01 = Modified Duration * Price * 0.0001
    """
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    mdur = MDur(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or mdur is None:
        return None
    return round(mdur * P * 0.0001, 5)


def Cvx(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1):
    """
    Calculates convexity.
    """
    if term is None or yield_ is None:
        return None

    rounded_term = round_ytm(term)
    if rounded_term is None:
        return None

    T = int(rounded_term * period)
    C = cpn / period
    Y = yield_ / period
    P = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    if P is None or P == 0:
        return None

    v = accrual_period(begin, settle, next_coupon, day_count) if (begin and settle and next_coupon) else 0

    dcv = (
            -v * (v - 1) * pow(1 + Y, v - 2) * C / Y * (1 - pow(1 + Y, -T))
            - 2 * v * pow(1 + Y, v - 1) * (C / pow(Y, 2) * (1 - pow(1 + Y, -T)) - T * C / (Y * pow(1 + Y, T + 1)))
            - pow(1 + Y, v) * (
                    -C / pow(Y, 3) * (1 - pow(1 + Y, -T)) +
                    2 * T * C / (pow(Y, 2) * pow(1 + Y, T + 1)) +
                    T * (T + 1) * C / (Y * pow(1 + Y, T + 2))
            )
            + (T - v) * (T + 1) * 100 / pow(1 + Y, T + 2 - v)
    )
    return dcv / (P * period ** 2)


def P2Y(price, cpn, term, period=2, begin=None, settle=None, next_coupon=None):
    """
    Convert bond price to yield‑to‑maturity by minimizing the squared error between
    the given price and the theoretical price computed by BPrice.
    """

    def objective(yield_):
        bp = BPrice(cpn, term, yield_, period, begin, settle, next_coupon)
        if bp is None:
            return float('inf')
        return (price - bp) ** 2

    result = minimize_scalar(objective, bounds=(-0.5, 1), method='bounded')
    return result.x


def approximate_duration(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,
                         delta_y=0.0001):
    """
    Compute approximate duration using finite differences.
    """
    if yield_ is None:
        return None
    price = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    price_up = BPrice(cpn, term, (yield_ + delta_y), period, begin, settle, next_coupon, day_count)
    price_down = BPrice(cpn, term, (yield_ - delta_y), period, begin, settle, next_coupon, day_count)
    if price is None or price == 0:
        return None
    return (price_down - price_up) / (2 * price * delta_y)


def approximate_convexity(cpn, term, yield_, period=2, begin=None, settle=None, next_coupon=None, day_count=1,
                          delta_y=0.0001):
    """
    Compute approximate convexity using finite differences.
    """
    if yield_ is None:
        return None
    price = BPrice(cpn, term, yield_, period, begin, settle, next_coupon, day_count)
    price_up = BPrice(cpn, term, (yield_ + delta_y), period, begin, settle, next_coupon, day_count)
    price_down = BPrice(cpn, term, (yield_ - delta_y), period, begin, settle, next_coupon, day_count)
    if price is None or price == 0:
        return None
    return (price_down + price_up - 2 * price) / (price * delta_y ** 2)


# ---------------- Bond Metrics Calculation ----------------
def calculate_bond_metrics(face_value, market_price, issue_date_str, maturity_date_str, coupon_rate,
                           periods_per_year, day_count, coupon_prev_date_str=None, coupon_next_date_str=None,
                           trade_settle_date_str=None, market_yield=None):
    """
    Compute key bond metrics.
    """
    begin = coupon_prev_date_str if coupon_prev_date_str is not None else issue_date_str
    settle = trade_settle_date_str if trade_settle_date_str is not None else maturity_date_str
    effective_date = trade_settle_date_str if trade_settle_date_str is not None else begin

    time_to_maturity = calculate_term(effective_date, maturity_date_str)
    ytm = calculate_ytm(market_price, face_value, coupon_rate, time_to_maturity, periods_per_year, n_digits=5)
    yield_to_maturity = market_yield if market_yield is not None else ytm
    bond_price = BPrice(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                        settle, coupon_next_date_str, day_count)
    accrued_interest = AInt(coupon_rate, periods_per_year, begin, settle, coupon_next_date_str, day_count)
    modified_duration = MDur(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                             settle, coupon_next_date_str, day_count)
    macaulay_duration = MacDur(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                               settle, coupon_next_date_str, day_count)
    dv01 = DV01(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                settle, coupon_next_date_str, day_count)
    convexity = Cvx(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                    settle, coupon_next_date_str, day_count)
    approx_duration = approximate_duration(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                                           settle, coupon_next_date_str, day_count)
    approx_convexity = approximate_convexity(coupon_rate, time_to_maturity, yield_to_maturity, periods_per_year, begin,
                                             settle, coupon_next_date_str, day_count)
    return {
        "time_to_maturity": time_to_maturity,
        "accrued_interest": accrued_interest,
        "yield_to_maturity": ytm,
        "clean_price": bond_price,
        "dirty_price": bond_price,
        "macaulay_duration": macaulay_duration,
        "modified_duration": modified_duration,
        "convexity": convexity,
        "dv01": dv01,
        "approx_duration": approx_duration,
        "approx_convexity": approx_convexity,
    }


def compute_ust_kpis(item):
    """
    Computes key performance indicators (KPIs) for a U.S. Treasury bond based on provided attributes.

    Expected keys:
      issue_date, maturity_date, coupon_rate, coupon_prev_date, coupon_ncpdt,
      principal_value, ask_price, bid_price, last_price, and optionally "price".

    If "price" is provided and nonempty, that value is used as the market price.
    Otherwise, the average of ask, bid, and last prices is used.
    """
    issue_date = item['issue_date']
    maturity_date = item['maturity_date']
    coupon_rate = item['coupon_rate']
    coupon_prev_date = item['coupon_prev_date']
    coupon_next_date = item['coupon_ncpdt']
    face_value = item['principal_value']
    ask_price = item['ask_price']
    bid_price = item['bid_price']
    last_price = item['last_price']

    is_valid = (
            pd.notna(ask_price) and ask_price != "" and
            pd.notna(bid_price) and bid_price != "" and
            pd.notna(issue_date) and issue_date != "" and
            pd.notna(maturity_date) and maturity_date != "" and
            pd.notna(coupon_rate) and coupon_rate != "" and
            pd.notna(coupon_prev_date) and coupon_prev_date != "" and
            pd.notna(coupon_next_date) and coupon_next_date != "" and
            pd.notna(face_value) and face_value != ""
    )

    if is_valid:
        coupon_rate = float(coupon_rate)
        issue_date = str(int(issue_date))
        maturity_date = str(int(maturity_date))
        coupon_prev_date = str(int(coupon_prev_date))
        coupon_next_date = str(int(coupon_next_date))
        periods_per_year = 2
        day_count = 1

        if 'price' in item and pd.notna(item['price']) and item['price'] != "":
            current_market_price = float(item['price'])
        else:
            current_market_price = (float(ask_price) + float(bid_price) + float(last_price)) / 3

        trade_date = compute_settlement_date(datetime.today().strftime('%Y%m%d'))

        bond_metrics = calculate_bond_metrics(
            face_value,
            current_market_price,
            issue_date,
            maturity_date,
            coupon_rate,
            periods_per_year,
            day_count,
            coupon_prev_date,
            coupon_next_date,
            trade_date,
            market_yield=None
        )

        return {
            "time_to_maturity": bond_metrics.get('time_to_maturity'),
            "mols_bond_price": bond_metrics.get('clean_price'),
            "mols_yield_to_maturity": bond_metrics.get('yield_to_maturity'),
            "mols_accrued_interest": bond_metrics.get('accrued_interest'),
            "mols_modified_duration": bond_metrics.get('modified_duration'),
            "mols_macaulay_duration": bond_metrics.get('macaulay_duration'),
            "mols_dv_01": bond_metrics.get('dv01'),
            "mols_convexity_measure": bond_metrics.get('convexity'),
            "effective_duration": bond_metrics.get('approx_duration'),
            "effective_convexity": bond_metrics.get('approx_convexity'),
        }
    else:
        return None


# ---------------- Main Test Block ----------------
if __name__ == '__main__':
    # Example test dictionary; adjust with real values as needed.
    test_item = {
        'issue_date': '20220101',
        'maturity_date': '20300101',
        'coupon_rate': '4.5',
        'coupon_prev_date': '20220701',
        'coupon_ncpdt': '20230101',
        'principal_value': '100',
        'ask_price': '101.0',
        'bid_price': '100.5',
        'last_price': '100.75',
        # Optionally, provide a 'price' field to be used for yield calculation:
        'price': '100.2'
    }
    kpis = compute_ust_kpis(test_item)
    print("Computed KPIs:")
    print(kpis)
