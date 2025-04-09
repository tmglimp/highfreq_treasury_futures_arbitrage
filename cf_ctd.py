import logging
import os
from datetime import datetime
import re

import pandas as pd

import config
from fixed_income_calc import BPrice, P2Y  # BPrice and P2Y functions
from market_data import MarketData

# ---------------- Helper Function ----------------
def normalize_date(date_val):
    """Convert a date value to an 8-digit string (YYYYMMDD)."""
    if pd.isnull(date_val):
        return None
    date_str = str(date_val).strip()
    match = re.search(r"(\d{8})", date_str)
    if match:
        return match.group(1)
    # Fallback: remove common non-digit characters
    return date_str.replace('-', '').replace(' ', '')

# ---------------- Logging Setup ----------------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

# ---------------- Preliminary Validation ----------------
def validate_alignment():
    """
    Check that the global FUTURES and USTs (from config) have the expected columns.
    Then force all column names to lowercase.
    """
    fut_required = {'conidex', 'ticker', 'expiry', 'bid_price', 'ask_price', 'last_price'}
    ust_required = {'cusip', 'maturity_date', 'cf1', 'cf2', 'bid_price', 'last_price'}
    fut_cols = set(config.FUTURES.columns.str.lower())
    ust_cols = set(config.USTs.columns.str.lower())
    missing_fut = fut_required - fut_cols
    missing_ust = ust_required - ust_cols
    if missing_fut:
        logging.error("FUTURES missing columns: %s", missing_fut)
    if missing_ust:
        logging.error("USTs missing columns: %s", missing_ust)

# ---------------- CSV Save Function ----------------
#def save_with_timestamp(df, base_name, folder="fin_eng_wrk_prod"):
#    """
#    Save a dataframe to CSV with a timestamp appended to the filename.
#    """
#    os.makedirs(folder, exist_ok=True)
#    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#    filename = f"{base_name}_{timestamp}.csv"
#    path = os.path.join(folder, filename)
#    df.to_csv(path, index=False)
#    logging.info("Saved file: %s", path)
#   return path

# ---------------- Market Data Import ----------------
def refresh_data():
    """
    Import FUTURES and USTs from config as deep copies (so that originals remain unchanged),
    force lowercase column names, and update them with fresh market data.
    """
    md = MarketData()
    ust_updated, fut_updated = md.get_index_market_data()

    ust_updated.columns = ust_updated.columns.str.lower()
    fut_updated.columns = fut_updated.columns.str.lower()

    # Ensure 'maturity_date' is datetime.
    if 'maturity_date' in ust_updated.columns:
        ust_updated['maturity_date'] = pd.to_datetime(ust_updated['maturity_date'], errors='coerce')
    else:
        logging.error("USTs does not contain 'maturity_date'")
        return fut_updated, ust_updated

    logging.info("Market data updated for FUTURES and USTs (local copies).")
    return fut_updated, ust_updated

# ---------------- FUTURES -> HEDGES Transformation ----------------
def transform_futures_hedges(futures_df):
    """
    Transform FUTURES into HEDGES:
      - Exclude rows where last_price indicates a closed contract (string starting with 'c' or 'C').
      - Use the bid_price_decimal, ask_price_decimal, and last_price_decimal (which MarketData should have populated)
        to set the 'price' field.
      - If both bid and ask are available, split into two rows (src = 'bid' and 'ask').
      - Otherwise, if only bid or only ask is available, use that value and set src appropriately.
      - Otherwise, if only last_price is available, use that and set src to 'last'.
      - Finally, rename all columns by adding the prefix "fut_" and insert a new column "fut_index"
        as the concatenation of fut_conidex and fut_src.
    """
    new_rows = []
    # Removed the call to save_with_timestamp or to_csv() for futures_df here.
    for idx, row in futures_df.iterrows():
        # Exclude rows where last_price indicates a closed contract.
        raw_last = row.get('last_price')
        if isinstance(raw_last, str) and raw_last.lower().startswith("c"):
            continue

        bid_dec = row.get('bid_price_decimal')
        ask_dec = row.get('ask_price_decimal')
        last_dec = row.get('last_price_decimal')

        bid_valid = (bid_dec is not None) and (not pd.isna(bid_dec)) and (bid_dec != 0)
        ask_valid = (ask_dec is not None) and (not pd.isna(ask_dec)) and (ask_dec != 0)
        last_valid = (last_dec is not None) and (not pd.isna(last_dec))

        if bid_valid and ask_valid:
            row_bid = row.copy()
            row_bid['price'] = bid_dec
            row_bid['src'] = 'bid'
            new_rows.append(row_bid)
            row_ask = row.copy()
            row_ask['price'] = ask_dec
            row_ask['src'] = 'ask'
            new_rows.append(row_ask)
        elif last_valid:
            row_single = row.copy()
            row_single['price'] = last_dec
            row_single['src'] = 'last'
            new_rows.append(row_single)
        else:
            continue

    hedges_df = pd.DataFrame(new_rows)
    hedges_df = modulate_volume(hedges_df)  # Reassign HEDGES = df inside modulate_volume
    # Removed the call to save_with_timestamp (or to_csv) for hedges_df here.
    hedges_df.columns = hedges_df.columns.astype(str).str.lower().str.strip()
    hedges_df = hedges_df.add_prefix("fut_")
    logging.info("Transformed FUTURES into HEDGES with %d rows.", len(hedges_df))
    return hedges_df

# ---------------- Modified modulate_volume Function ----------------
def modulate_volume(hedges_df):
    """
    Process the 'volume' column by converting values with 'K' and 'M' suffixes,
    rounding them, and then reassign the processed DataFrame to HEDGES before returning.
    """
    df = hedges_df
    logging.info("Processing volume data...")
    df['volume'] = df['volume'].astype(str)

    def process_value(val):
        if isinstance(val, str) and val.endswith('K'):
            try:
                return float(val[:-1]) * 1000
            except ValueError:
                return float('nan')
        if isinstance(val, str) and val.endswith('M'):
            try:
                return float(val[:-1]) * 1000000
            except ValueError:
                return float('nan')
        try:
            return float(val)
        except ValueError:
            return float('nan')
    df['volume'] = df['volume'].apply(process_value)
    df.to_csv('df.csv', index=False)
    HEDGES = df  # reassign the variable name HEDGES to the processed df
    return HEDGES

# ---------------- CTD Pairing Processing ----------------
def process_futures_ctd(HEDGES, usts):
    """
    For each row in HEDGES, determine the eligible CTD UST.
    For each hedge row:
      - Determine the maturity bin using fut_expiry (converted to datetime) and fut_ticker.
      - Use symbol-specific offsets (in years) to compute the bin boundaries.
      - Filter eligible USTs that have a maturity_date within the bin and a non-null conversion factor (cf1).
      - Compute an IRR for each candidate using a no-arbitrage formula.
      - Select the UST with the maximum IRR as the CTD.
      - If the candidate UST lacks a valid 'yield' field, compute market yield via P2Y.
      - Compute CTD_BPrice via BPrice.
      - Compute the futures TPrice (fut_tprice) as BPrice divided by the futures conversion factor (fut_cf1).
      - Append the CTD details (prefixed with ctd_) and fut_tprice to the hedge row.
    """
    new_ctd_cols = ['ctd_cusip', 'ctd_conidex', 'ctd_conid', 'ctd_price', 'ctd_yield',
                    'ctd_coupon_rate', 'ctd_prev_cpn', 'ctd_ncpdt', 'ctd_maturity_date',
                    'ctd_cf', 'ctd_ytm', 'ctd_irr', 'ctd_bprice', 'imp_yield']

    for col in new_ctd_cols:
        HEDGES[col] = None

    offsets = {
        "ZT": (1.74, 2.01),
        "Z3N": (2.74, 3.01),
        "ZF": (4.14, 5.26),
        "ZN": (6.49, 10.01),
        "TN": (9.49, 10.01)
    }
    trade_date = pd.Timestamp.today()

    for idx, hedge in HEDGES.iterrows():
        symbol = str(hedge.get('fut_ticker')).upper()
        # Assume fut_expiry is already in a format convertible to a date string (YYYYMMDD)
        f_expiry_date = hedge.get('fut_expiry')
        f_expiry_date = normalize_date(f_expiry_date)
        try:
            f_expiry = pd.to_datetime(f_expiry_date, format='%Y%m%d', errors='coerce')
        except Exception as e:
            logging.error("Error converting fut_expiry for row %s: %s", idx, e)
            continue

        if symbol not in offsets:
            logging.warning("Symbol %s not in offsets; skipping CTD pairing for row %s.", symbol, idx)
            continue

        # Compute maturity bin by adding the symbol-specific offset to f_expiry.
        min_off, max_off = offsets[symbol]
        min_maturity_date = f_expiry + pd.DateOffset(days=int(min_off * 365.25))
        max_maturity_date = f_expiry + pd.DateOffset(days=int(max_off * 365.25))
       # logging.info("Maturity bin for %s contract expiring %s: %s to %s",
       #              symbol, f_expiry.date(), min_maturity_date.date(), max_maturity_date.date())

        # Filter eligible USTs based on the computed bin.
        eligible = usts[
            (usts['maturity_date'] >= min_maturity_date) &
            (usts['maturity_date'] <= max_maturity_date) &
            (usts['cf1'].notna())
        ].copy(deep=True)
        if eligible.empty:
            continue

        F = hedge['fut_price']
        print(f"Futures price F as", F)
        days_to_expiry = (f_expiry - trade_date).days
        T_days = max(days_to_expiry, 1)
        eligible['irr'] = (((F * eligible['cf1']) - eligible['price']) / eligible['price']) * (365 / T_days)
        print(f'IRR selection as', eligible['irr'])
        selected = eligible.loc[eligible['irr'].idxmax()]
        print(f'selected as', selected)

        HEDGES.at[idx, 'ctd_cusip'] = selected.get('cusip')
        HEDGES.at[idx, 'ctd_conid'] = selected.get('conid')
        HEDGES.at[idx, 'ctd_price'] = selected.get('price')

        raw_yld = selected.get('yield')
        computed_yld = float(raw_yld.strip('%')) if raw_yld else P2Y(
            selected.get('price'),
            selected.get('coupon'),
            selected.get('yrstomat'),
            period=2
        )
        HEDGES.at[idx, 'ctd_yield'] = computed_yld
        HEDGES.at[idx, 'ctd_coupon_rate'] = selected.get('coupon')
        HEDGES.at[idx, 'ctd_maturity_date'] = selected.get('maturity_date')
        HEDGES.at[idx, 'ctd_cf'] = selected.get('cf1')
        HEDGES.at[idx, 'ctd_prev_cpn'] = selected.get('prvcpn')
        HEDGES.at[idx, 'ctd_ncpdt'] = selected.get('nxtcpn')
        HEDGES.at[idx, 'ctd_ytm'] = selected.get('yrstomat')

        # Normalize dates for the BPrice calculation.
        begin_date = normalize_date(selected.get('prvcpn'))
        settle_date = normalize_date(selected.get('maturity_date'))
        next_coupon_date = normalize_date(selected.get('nxtcpn'))


    # Rename CTD columns to uppercase for downstream code.
    ctd_cols = {col: col.upper() for col in HEDGES.columns if col.startswith("ctd_")}
    HEDGES.rename(columns=ctd_cols, inplace=True)
    return HEDGES

# ---------------- Main Processing Function ----------------
def cf_ctd_main():
    logging.info("Starting cf_ctd processing script.")

    # 0. Validate input column alignment.
    # validate_alignment()

    # 1. Get market data (local copies from config).
    fut_local, ust_local = refresh_data()

    # 2. Transform FUTURES into HEDGES.
    HEDGES = transform_futures_hedges(fut_local)

    # 3. Process CTD pairing (append CTD details and fut_tprice).
    HEDGES = process_futures_ctd(HEDGES, ust_local)

    # Save the final HEDGES globally before further processing.
    config.HEDGES = HEDGES
    return HEDGES

if __name__ == "__main__":
    cf_ctd_main()
