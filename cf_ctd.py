import logging
import re
import pandas as pd
from datetime import datetime, timedelta
import config
from config import SPREAD_POP, SPREAD_TRACK_WINDOW_MINUTES
from fixed_income_calc import P2Y
from market_data import MarketData

row_pool = pd.DataFrame()

# ---------------- Helper Function ----------------
def normalize_date(date_val):
    if pd.isnull(date_val):
        return None
    date_str = str(date_val).strip()
    match = re.search(r"(\d{8})", date_str)
    if match:
        return match.group(1)
    return date_str.replace('-', '').replace(' ', '')

# ---------------- Logging Setup ----------------
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

# ---------------- Market Data Import ----------------
def refresh_data():
    md = MarketData()
    ust_updated, fut_updated = md.get_index_market_data()
    ust_updated.columns = ust_updated.columns.str.lower()
    fut_updated.columns = fut_updated.columns.str.lower()
    if 'maturity_date' in ust_updated.columns:
        ust_updated['maturity_date'] = pd.to_datetime(ust_updated['maturity_date'], errors='coerce')
    else:
        logging.error("USTs does not contain 'maturity_date")
        return ust_updated, fut_updated
    logging.info("Market data updated for FUTURES and USTs (local copies).")
    return ust_updated, fut_updated

# ---------------- Volume Modulation ----------------
def modulate_volume(fut_updated):
    """Convert the api response object of mixed type"""
    df = fut_updated
    logging.info("Processing volume data...")
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
    return df

# ---------------- Spread Tracking Pool ----------------
def add_to_row_pool(row):
    global row_pool
    timestamp = datetime.now()
    bid = row.get('bid_price_decimal')
    ask = row.get('ask_price_decimal')
    conid = row.get('conid')
    if pd.notna(bid) and pd.notna(ask) and conid is not None:
        new_entry = pd.DataFrame.from_records([{
            'timestamp': timestamp,
            'conid': conid,
            'bid': bid,
            'ask': ask,
            'spread': ask - bid
        }])
        row_pool = pd.concat([row_pool, new_entry], ignore_index=True)
        cutoff = timestamp - timedelta(minutes=SPREAD_TRACK_WINDOW_MINUTES)
        row_pool = row_pool[row_pool['timestamp'] >= cutoff]

        #if row_pool['conid'].value_counts().ge(10).any():
        #    row_pool.to_csv("row_pool_all.csv", index=False)

# ---------------- FUTURES -> HEDGES Transformation ----------------
def transform_futures_hedges(fut_updated, SPREAD_POP):
    row_pool_local = pd.DataFrame()
    for idx, row in fut_updated.iterrows():
        raw_last = row.get('last_price')
        if isinstance(raw_last, str) and raw_last.lower().startswith("c"):
            continue
        bid_dec = row.get('bid_price_decimal')
        ask_dec = row.get('ask_price_decimal')
        last_dec = row.get('last_price_decimal')
        bid_valid = pd.notna(bid_dec) and bid_dec != 0
        ask_valid = pd.notna(ask_dec) and ask_dec != 0
        last_valid = pd.notna(last_dec)
        generated_rows = []
        conid = row.get('conid')
        recent = row_pool[row_pool['conid'] == conid] if 'conid' in row_pool.columns else pd.DataFrame()
        if bid_valid and ask_valid:
            row_bid = row.copy()
            row_ask = row.copy()
            if not recent.empty:
                row_bid['price'] = recent['bid'].min()
                row_ask['price'] = recent['ask'].max()
                row_bid['spread_std'] = recent['spread'].std()
                row_ask['spread_std'] = recent['spread'].std()
                row_bid['spread_mean'] = recent['spread'].mean()
                row_ask['spread_mean'] = recent['spread'].mean()
            else:
                row_bid['price'] = bid_dec
                row_ask['price'] = ask_dec
                row_bid['spread_std'] = None
                row_ask['spread_std'] = None
                row_bid['spread_mean'] = None
                row_ask['spread_mean'] = None
            row_bid['src'] = 'bid'
            row_ask['src'] = 'ask'
            generated_rows.extend([row_bid, row_ask])
        elif last_valid:
            row_single = row.copy()
            row_single['price'] = last_dec
            row_single['src'] = 'last'
            row_single['spread_std'] = None
            row_single['spread_mean'] = None
            generated_rows.append(row_single)
        if generated_rows:
            row_pool_local = pd.concat([row_pool_local, pd.DataFrame(generated_rows)], ignore_index=True)
            if len(row_pool_local) > SPREAD_POP:
                row_pool_local = row_pool_local.iloc[-SPREAD_POP:]
            for r in generated_rows:
                add_to_row_pool(r)
    fut_updated = row_pool_local.reset_index(drop=True)
    fut_updated = modulate_volume(fut_updated)
    fut_updated.columns = fut_updated.columns.astype(str).str.lower().str.strip()
    fut_updated = fut_updated.add_prefix("fut_")
    logging.info("Transformed FUTURES into HEDGES with %d rows (limited to most recent %d).",
                 len(fut_updated), SPREAD_POP)
    return fut_updated

# ---------------- CTD Pairing Processing ----------------
def process_futures_ctd(HEDGES, usts):
    new_ctd_cols = ['ctd_cusip', 'ctd_conidex', 'ctd_conid', 'ctd_price', 'ctd_yield',
                    'ctd_coupon_rate', 'ctd_prev_cpn', 'ctd_ncpdt', 'ctd_maturity_date',
                    'ctd_cf', 'ctd_ytm']
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
        min_off, max_off = offsets[symbol]
        min_maturity_date = f_expiry + pd.DateOffset(days=int(min_off * 365.25))
        max_maturity_date = f_expiry + pd.DateOffset(days=int(max_off * 365.25))
        logging.info("Maturity bin for %s contract expiring %s: %s to %s",
                     symbol, f_expiry.date(), min_maturity_date.date(), max_maturity_date.date())
        eligible = usts[
            (usts['maturity_date'] >= min_maturity_date) &
            (usts['maturity_date'] <= max_maturity_date) &
            (usts['cf1'].notna())
        ].copy(deep=True)
        if eligible.empty:
            continue
        F = hedge['fut_price']
        # print(f"Futures price F as", F)
        days_to_expiry = (f_expiry - trade_date).days
        T_days = max(days_to_expiry, 1)
        eligible['irr'] = (((F * eligible['cf1']) - eligible['price']) / eligible['price']) * (365 / T_days)
        # print(f'IRR selection as', eligible['irr'])
        selected = eligible.loc[eligible['irr'].idxmax()]
        # print(f'selected as', selected)
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
    ctd_cols = {col: col.upper() for col in HEDGES.columns if col.startswith("ctd_")}
    HEDGES.rename(columns=ctd_cols, inplace=True)
    return HEDGES

# ---------------- Main ----------------
def cf_ctd_main():
    logging.info("Starting cf_ctd processing script.")
    ust_updated, fut_updated = refresh_data()
    HEDGES = transform_futures_hedges(fut_updated, SPREAD_POP)
    HEDGES = process_futures_ctd(HEDGES, ust_updated)
    config.HEDGES = HEDGES
    return HEDGES

if __name__ == "__main__":
    cf_ctd_main()
