import os
import requests
import pandas as pd
from datetime import datetime
import config

# ────────────────────────────────────────────────────────
# configuration
# ────────────────────────────────────────────────────
client_id = "82c5ee299b404f62af1177b109377ea6"
client_secret = "87391cd4aDF34645AE5D8687350C441A"
tcf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "TCF.xlsx")
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "UST.index.csv")

# ────────────────────────────────────────────────────
# utilities
# ────────────────────────────────────────────────────

def convert_date_format(iso_date):
    try:
        return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%m/%d/%Y")
    except Exception:
        return iso_date

def parse_date(date_str):
    if isinstance(date_str, pd.Timestamp):
        return date_str.to_pydatetime()
    return datetime.strptime(date_str, "%m/%d/%y")

def add_months(dt, months):
    from calendar import monthrange
    month = dt.month - 1 + months
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)

def compute_cf(coupon_rate, prev_cpn_str, next_cpn_str, mat_date_str, yield_rate=0.06):
    prev_cpn = parse_date(prev_cpn_str)
    next_cpn = parse_date(next_cpn_str)
    mat_date = parse_date(mat_date_str)
    delivery = prev_cpn + (next_cpn - prev_cpn) / 2
    frac = (next_cpn - delivery).days / (next_cpn - prev_cpn).days
    coupon_payment = coupon_rate / 2.0
    coupon_dates = []
    current_cpn = next_cpn
    while current_cpn <= mat_date:
        coupon_dates.append(current_cpn)
        current_cpn = add_months(current_cpn, 6)
    last_full_cpn = coupon_dates[-1] if coupon_dates else next_cpn
    if mat_date > last_full_cpn:
        next_cpn_after_last = add_months(last_full_cpn, 6)
        delta = (mat_date - last_full_cpn).days / (next_cpn_after_last - last_full_cpn).days
        N = len(coupon_dates) + 1
    else:
        delta = 1.0
        N = len(coupon_dates)
    r = yield_rate / 2.0
    pv = coupon_payment / ((1 + r) ** frac)
    for j in range(1, N - 1):
        t = frac + j
        pv += coupon_payment / ((1 + r) ** t)
    t_last = frac + (N - 1)
    final_cash = 100 + coupon_payment * delta
    pv += final_cash / ((1 + r) ** t_last)
    return pv / 100.0

def get_coupon_bounds(issue_date, maturity_date):
    if pd.isna(issue_date) or pd.isna(maturity_date):
        return None, None
    first_coupon = add_months(issue_date, 6)
    coupons = []

    # Backward from first coupon
    current = first_coupon
    while current > issue_date - pd.DateOffset(years=11):
        current = add_months(current, -6)
        if current >= issue_date:
            continue
        coupons.insert(0, current)

    # Forward to maturity
    current = first_coupon
    while current <= maturity_date:
        coupons.append(current)
        current = add_months(current, 6)

    today = pd.Timestamp.today()
    prev_coupon = max([d for d in coupons if d <= today], default=None)
    next_coupon = min([d for d in coupons if d > today], default=None)
    return prev_coupon, next_coupon

def query_security_detail(cusip, issue_date):
    url = f"https://api.fiscal.treasury.gov/ap/exp/v1/marketable-securities/securities/{cusip}/{issue_date}"
    headers = {
        "client_id": client_id,
        "client_secret": client_secret,
        "accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# ────────────────────────────────────────────────────
# main function
# ────────────────────────────────────────────────────


def fetch_treasury_data():
    df = pd.read_excel(tcf_path, sheet_name="Security Database", header=2)
    cols_to_keep = [
        "OTR Issue",
        "Original Maturity",
        "Coupon",
        "Issue\nDate",
        "Maturity\nDate",
        "CUSIP",
        "Adjusted\nIssuance\n(Billions)",
        "Original Issuance (Billions)"
    ]
    df = df[cols_to_keep].copy()
    df.columns = [
        "otr_issue",
        "original_maturity",
        "coupon",
        "issue_date_raw",
        "maturity_date",
        "cusip",
        "adjusted_issuance_billions",
        "original_issuance_billions"
    ]

    df = df.dropna(subset=["cusip", "maturity_date"]).copy()
    df["issue_date_raw"] = pd.to_datetime(df["issue_date_raw"], errors="coerce")
    df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")
    df["issue_date"] = df["issue_date_raw"].dt.strftime("%Y-%m-%d")
    df["years_to_maturity"] = (df["maturity_date"] - pd.Timestamp.today()).dt.days / 365.25
    df = df[df["years_to_maturity"] <= 10.25].copy()
    df["cusip"] = df["cusip"].astype(str).str.strip()

    df["prev_coupon"], df["next_coupon"] = zip(*df.apply(
        lambda row: get_coupon_bounds(row["issue_date_raw"], row["maturity_date"]),
        axis=1
    ))

    df["prev_coupon_str"] = df["prev_coupon"].dt.strftime("%m/%d/%y")
    df["next_coupon_str"] = df["next_coupon"].dt.strftime("%m/%d/%y")
    df["maturity_str"] = df["maturity_date"].dt.strftime("%m/%d/%y")

    results = []
    for _, row in df.iterrows():
        cusip = row["cusip"]
        issue_date_raw = row["issue_date"]
        issue_date = convert_date_format(issue_date_raw)
        try:
            data = query_security_detail(cusip, issue_date)
            if data:
                result = data[0] if isinstance(data, list) else data
                result["cusip"] = cusip
                result["issue_date"] = issue_date_raw
                results.append(result)
        except Exception as e:
            print(f"Failed for {cusip}/{issue_date}: {e}")

    df_results = pd.DataFrame(results)
    df_combined = df.merge(df_results, on="cusip", how="left")
    df_combined["conversion_factor"] = df_combined.apply(lambda row: round(
            compute_cf(
                row["coupon"],
                row["prev_coupon"],
                row["next_coupon"],
                row["maturity_date"]
            ), 6
        ) if pd.notna(row["prev_coupon"]) and pd.notna(row["next_coupon"]) else None,
        axis=1
    )
    df_combined.to_csv(output_path, index=False)
    config.ZEROES = df_combined
    print(f"Enriched file written to: {output_path}, saved to config.ZEROES")

# ────────────────────────────────────────────────────
# entry point
# ────────────────────────────────────────────────────

if __name__ == "__main__":
    fetch_treasury_data()
