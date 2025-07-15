import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from scan import pop_zeroes

# ─── Configuration ───
client_id = ""
client_secret = ""
tcf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "TCF.xlsx")
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "UST.index.csv")

# ─── Utilities ───
def convert_date_format(iso_date):
    try:
        return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%m/%d/%Y")
    except Exception:
        return iso_date

def parse_date(date_str):
    if isinstance(date_str, pd.Timestamp):
        return date_str.to_pydatetime()
    if isinstance(date_str, datetime):
        return date_str
    if isinstance(date_str, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%y", "%m/%d/%Y"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
    raise ValueError(f"Unrecognized date format: {date_str}")

def add_months(dt, months):
    from calendar import monthrange
    month = dt.month - 1 + months
    year = dt.year + month // 12
    month = month % 12 + 1
    day = min(dt.day, monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)

def compute_cf(coupon_rate, prev_cpn, next_cpn, mat_date, yield_rate=0.06):
    prev_cpn = parse_date(prev_cpn)
    next_cpn = parse_date(next_cpn)
    mat_date = parse_date(mat_date)
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

def get_coupon_bounds(issueDate, years_to_maturity, original_maturity):
    if pd.isna(issueDate) or pd.isna(years_to_maturity) or pd.isna(original_maturity):
        return None, None

    try:
        anchor = parse_date(issueDate)
    except:
        return None, None

    elapsed_years = original_maturity - years_to_maturity
    periods_elapsed = elapsed_years / 0.5  # semiannual periods

    prev_offset = int(np.floor(periods_elapsed)) * 6
    next_offset = int(np.ceil(periods_elapsed)) * 6

    prev_coupon = add_months(anchor, prev_offset)
    next_coupon = add_months(anchor, next_offset)

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

# ─── Main Logic ───
def fetch_treasury_data():
    df = pd.read_excel(tcf_path, sheet_name="Security Database", header=2)
    cols_to_keep = ["OTR Issue", "Original Maturity", "Coupon", "Issue\nDate", "Maturity\nDate", "CUSIP",
        "Adjusted\nIssuance\n(Billions)", "Original Issuance (Billions)"]
    df = df[cols_to_keep].copy()
    df.columns = ["otr_issue", "original_maturity", "coupon", "issue_date_raw", "maturity_date", "cusip", "adjusted_issuance_billions", "original_issuance_billions"]
    df = df.dropna(subset=["cusip", "maturity_date"]).copy()
    df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")
    df["issue_date"] = df["issue_date_raw"].dt.strftime("%Y-%m-%d")
    df["years_to_maturity"] = (df["maturity_date"] - pd.Timestamp.today()).dt.days / 365.25
    df = df[df["years_to_maturity"] <= 11].copy()
    df["cusip"] = df["cusip"].astype(str).str.strip()

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
    df_parse = df.merge(df_results, on="cusip", how="left")
    df_parse = df_parse.loc[:, ~df_parse.columns.duplicated()]

    df_parse["issueDate"] = pd.to_datetime(df_parse["issueDate"], errors="coerce")
    df_parse["issueDate"] = df_parse["issueDate"].dt.strftime("%Y-%m-%d")
    df_parse["maturity_date"] = pd.to_datetime(df_parse["maturity_date"], errors="coerce")
    df_parse["maturity_date"] = df_parse["maturity_date"].dt.strftime("%Y-%m-%d")
    df_parse[["prev_coupon", "next_coupon"]] = df_parse.apply(lambda r: pd.Series(get_coupon_bounds(r["issueDate"], r["years_to_maturity"], r["original_maturity"])),axis=1)
    df_parse["conversion_factor"] = df_parse.apply(lambda row: round(compute_cf(row["coupon"], row["prev_coupon"], row["next_coupon"], row["maturity_date"]), 6)

        if pd.notna(row["prev_coupon"]) and pd.notna(row["next_coupon"]) else None,axis=1)

    cols_to_drop = [
        "issue_date_raw", "issue_date_x", "prev_coupon_str", "next_coupon_str", "maturity_str", "maturityDate", "interestRate",
        "refCpiOnIssueDate", "refCpiOnDatedDate", "announcementDate", "auctionDateYear", "datedDate", "accruedInterestPer1000", "accruedInterestPer100",
        "adjustedAccruedInterestPer1000", "adjustedPrice", "allocationPercentageDecimals", "announcedCusip", "auctionFormat", "averageMedianDiscountRate",
        "averageMedianInvestmentRate", "averageMedianPrice", "averageMedianDiscountMargin", "callDate", "calledDate", "cashManagementBillCMB",
        "closingTimeCompetitive", "closingTimeNoncompetitive", "competitiveAccepted", "competitiveBidDecimals", "competitiveTendered",
        "competitiveTendersAccepted", "cpiBaseReferencePeriod", "currentlyOutstanding", "directBidderAccepted", "directBidderTendered",
        "estimatedAmountOfPubliclyHeldMaturingSecuritiesByType", "fimaIncluded", "fimaNoncompetitiveAccepted", "fimaNoncompetitiveTendered",
        "firstInterestPeriod", "frnIndexDeterminationDate", "frnIndexDeterminationRate", "highDiscountRate", "highInvestmentRate",
        "highPrice", "highDiscountMargin", "highYield", "indexRatioOnIssueDate", "interestPaymentFrequency", "lowDiscountRate",
        "lowInvestmentRate", "lowPrice", "lowDiscountMargin", "lowYield", "minimumBidAmount", "minimumStripAmount", "minimumToIssue", "multiplesToBid",
        "multiplesToIssue", "nlpExclusionAmount", "nlpReportingThreshold", "originalCusip", "originalDatedDate", "originalIssueDate", "pdfFilenameAnnouncement",
        "pdfFilenameCompetitiveResults", "pdfFilenameNoncompetitiveResults", "pdfFilenameSpecialAnnouncement", "pricePer100",
        "reopening", "securityTermDayMonth", "securityTermWeekYear", "spread", "standardInterestPaymentPer1000", "strippable", "term", "tiinConversionFactorPer1000",
        "tips", "type", "unadjustedAccruedInterestPer1000", "unadjustedPrice", "updatedTimestamp", "xmlFilenameAnnouncement", "xmlFilenameCompetitiveResults",
        "xmlFilenameSpecialAnnouncement", "tintCusip2", "tintCusip1DueDate", "tintCusip2DueDate", "issue_date_y", 'maturityDate'
    ]

    df_parse.drop(columns=cols_to_drop, inplace=True, errors="ignore")
    df_parse = df_parse[df_parse['corpusCusip'].notna() & (df_parse['corpusCusip'] != '')]
    df_parse.to_csv(output_path, index=False)


    print(f"Enriched file written to: {output_path}")

if __name__ == "__main__":
    fetch_treasury_data()