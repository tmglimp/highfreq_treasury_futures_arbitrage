import logging
import os
from datetime import datetime

import pandas as pd

import config
from fixed_income_calc import BPrice, P2Y  # BPrice and P2Y functions
from market_data import MarketData

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
def save_with_timestamp(df, base_name, folder="fin_eng_wrk_prod"):
    """
    Save a dataframe to CSV with a timestamp appended to the filename.
    """
    os.makedirs(folder, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{base_name}_{timestamp}.csv"
    path = os.path.join(folder, filename)
    df.to_csv(path, index=False)
    logging.info("Saved file: %s", path)
    return path

# ---------------- Market Data Import ----------------
def refresh_data():
    """
    Import FUTURES and USTs from config as deep copies (so that originals remain unchanged), force lowercase column names, and update them with fresh market data.
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
    futures_df.to_csv("futures_df.csv")
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
    modulate_volume(hedges_df)
    hedges_df.to_csv("hedges_df.csv")

    hedges_df.columns = hedges_df.columns.astype(str).str.lower().str.strip()
    hedges_df = hedges_df.add_prefix("fut_")
    logging.info("Transformed FUTURES into HEDGES with %d rows.", len(hedges_df))
    return hedges_df

def modulate_volume(hedges_df):
    # Filter out rows where the 'volume' is either 0 or an empty string.
    # Adjust the condition as needed if you have other representations of "empty" values.
    df = hedges_df
    print(f'printing df pre-vol as', df)
    df['volume'] = df['volume'].astype(str)
    df['volume'] = round(df['volume'], 2)
    val = df['volume']
    # Define a function to process each value in 'volume'
    def process_value(val):
        if isinstance(val, str) and val.endswith('K'):
            # Remove 'K', convert to float, and multiply by 1,000
            try:
                return float(val[:-1]) * 1000
            except ValueError:
                # Return NaN if conversion fails
                return float('nan')
        if isinstance(val, str) and val.endswith('M'):
            # Remove 'K', convert to float, and multiply by 1,000
            try:
                return float(val[:-1]) * 1000000
            except ValueError:
                # Return NaN if conversion fails
                return float('nan')
        try:
            return float(val)
        except ValueError:
            return float('nan')
    # Apply the conversion function to the 'volume' column
    df['volume'] = df['volume'].apply(process_value)
    print(df)
    df.to_csv('df.csv')
    return df

# ---------------- Helper: Round Years-to-Maturity ----------------
def round_ytm(ytm):
    """
    Round the years-to-maturity to the nearest 0.5 increment.
    """
    if pd.isnull(ytm):
        return None
    return round(ytm * 2) / 2.0


# ---------------- CTD Pairing Processing ----------------
def process_futures_ctd(hedges_df, usts):
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
        hedges_df[col] = None
    hedges_df['fut_tprice'] = None

    offsets = {
        "ZT": (1.73, 2.02),
        "Z3N": (2.73, 3.02),
        "ZF": (4.11, 5.27),
        "ZN": (6.47, 10.02),
        "TN": (9.4, 10.02)
    }
    trade_date = pd.Timestamp.today()

    for idx, hedge in hedges_df.iterrows():
        symbol = hedge.get('fut_ticker')
        if symbol is None:
            logging.warning("Row %s missing futures ticker; skipping CTD pairing.", idx)
            continue
        symbol = str(symbol).upper()
        try:
            f_expiry = pd.to_datetime(hedge['fut_expiry'])
        except Exception as e:
            logging.error("Error converting fut_expiry for row %s: %s", idx, e)
            continue

        if symbol not in offsets:
            logging.warning("Symbol %s not in offsets; skipping CTD pairing for row %s.", symbol, idx)
            continue

        min_off, max_off = offsets[symbol]
        min_maturity_date = f_expiry + pd.DateOffset(days=int(min_off * 365.25))
        max_maturity_date = f_expiry + pd.DateOffset(days=int(max_off * 365.25))
        logging.info("Maturity bin for %s contract expiring %s: %s to %s",
                     symbol, f_expiry.date(), min_maturity_date.date(), max_maturity_date.date())

        debug_dir = "fin_eng_wrk_prod_outputs"
        os.makedirs(debug_dir, exist_ok=True)
        future_filename = os.path.join(debug_dir, f"future_{symbol}_{f_expiry.date()}.csv")
        pd.DataFrame([hedge]).to_csv(future_filename, index=False)
        logging.info("Future data saved to %s", future_filename)

        # For CTD calculations, use CF1 for the first contract and CF2 for subsequent ones. (to be fixed)
        # CF_col = 'cf1' if i == 0 else 'cf2'
        CF_col = 'cf1'

        eligible = usts[
            (usts['maturity_date'] >= min_maturity_date) &
            (usts['maturity_date'] <= max_maturity_date) &
            (usts[CF_col].notna())
            ].copy(deep=True)
        for col in ['cf1', 'cf2', 'price']:
            if col in eligible.columns:
                eligible[col] = pd.to_numeric(eligible[col], errors='coerce')
        eligible_filename = os.path.join(debug_dir, f"eligible_{symbol}_{f_expiry.date()}.csv")
        eligible.to_csv(eligible_filename, index=False)
        logging.info("Eligible data saved to %s", eligible_filename)

        if eligible.empty:
            continue

        F = hedge['fut_price']
        if pd.isna(F):
            continue
        T_days = (f_expiry - trade_date).days
        if T_days <= 0:
            T_days = 1

        if 'accrued_interest_purchase' not in eligible.columns:
            eligible['accrued_interest_purchase'] = 0
        if 'accrued_interest_delivery' not in eligible.columns:
            eligible['accrued_interest_delivery'] = 0

        eligible['irr'] = (((F * eligible['cf1'] + eligible['accrued_interest_delivery']) -
                            (eligible['price'] + eligible['accrued_interest_purchase'])) /
                           (eligible['price'] + eligible['accrued_interest_purchase'])) * (365 / T_days)

        try:
            selected = eligible.loc[eligible['irr'].idxmax()]
        except Exception as e:
            logging.error("Error selecting CTD for symbol %s with expiry %s: %s", symbol, f_expiry, e)
            continue

        hedges_df.at[idx, 'ctd_cusip'] = selected.get('cusip')
        hedges_df.at[idx, 'ctd_conidex'] = selected.get('conid')
        hedges_df.at[idx, 'ctd_conid'] = selected.get('conid')
        hedges_df.at[idx, 'ctd_price'] = selected.get('price')

        raw_yld = float(selected.get('yield').strip().rstrip("%")) if selected.get('yield') is not None else None
        if raw_yld is None or pd.isna(raw_yld):
            computed_yld = P2Y(selected.get('price'), selected.get('coupon'),
                               round_ytm(selected.get('yrstomat')), period=2,
                               begin=None, settle=None, next_coupon=None) \
                if selected.get('price') is not None else None
        else:
            computed_yld = raw_yld
        hedges_df.at[idx, 'ctd_yield'] = computed_yld

        hedges_df.at[idx, 'ctd_coupon_rate'] = selected.get('coupon')
        hedges_df.at[idx, 'ctd_prev_cpn'] = selected.get('prvcpn')
        hedges_df.at[idx, 'ctd_ncpdt'] = selected.get('nxtcpn')
        hedges_df.at[idx, 'ctd_maturity_date'] = selected.get('maturity_date')
        hedges_df.at[idx, 'ctd_cf'] = selected.get('cf1')

        ytm_val = selected.get('yrstomat')
        hedges_df.at[idx, 'ctd_ytm'] = round_ytm(ytm_val)
        hedges_df.at[idx, 'ctd_irr'] = selected.get('irr')

        try:
            bprice = BPrice(cpn=selected.get('coupon'),
                            term=round_ytm(ytm_val),
                            yield_=computed_yld,
                            period=2,
                            begin=None,
                            settle=None,
                            next_coupon=selected.get('nxtcpn'),
                            day_count=365)
        except Exception as e:
            logging.error("Error computing BPrice for CTD in symbol %s: %s", symbol, e)
            bprice = None
        hedges_df.at[idx, 'ctd_bprice'] = bprice

        if bprice is not None:
            imp_yld = P2Y(bprice, selected.get('coupon'),
                          round_ytm(ytm_val), period=2, begin=None, settle=None, next_coupon=selected.get('ncpdt'))
        else:
            imp_yld = None
        hedges_df.at[idx, 'imp_yield'] = imp_yld

        fut_cf = hedge.get('ctd_cf1')
        if pd.notna(fut_cf) and fut_cf != 0 and bprice is not None:
            fut_tprice = bprice / fut_cf
        else:
            fut_tprice = None
        hedges_df.at[idx, 'fut_tprice'] = fut_tprice

    # Rename CTD columns to uppercase for downstream code
    ctd_cols = {col: col.upper() for col in hedges_df.columns if col.startswith("ctd_")}
    hedges_df.rename(columns=ctd_cols, inplace=True)
    return hedges_df


# ---------------- Main Processing Function ----------------
def cf_ctd_main():
    logging.info("Starting cf_ctd processing script.")

    # 0. Validate input column alignment.
    # validate_alignment()

    # 1. Get market data (local copies from config).
    fut_local, ust_local = refresh_data()
    save_with_timestamp(fut_local, "futures_updated")
    save_with_timestamp(ust_local, "usts_updated")

    # 2. Transform FUTURES into HEDGES.
    HEDGES = transform_futures_hedges(fut_local)
    save_with_timestamp(HEDGES, "hedges_transformed")

    # 3. Process CTD pairing (append CTD details and fut_tprice).
    HEDGES = process_futures_ctd(HEDGES, ust_local)
    save_with_timestamp(HEDGES, "hedges_with_ctd")

    # Save the final HEDGES dataframe locally for validation before further processing.
    final_csv = save_with_timestamp(HEDGES, "hedges_final")
    logging.info("Final HEDGES saved for inspection at: %s", final_csv)
    logging.info("cf_ctd processing completed.")

    return HEDGES


if __name__ == "__main__":
    cf_ctd_main()
