import os
import re
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from pandas.tseries.offsets import BDay
from pandas.tseries.holiday import USFederalHolidayCalendar

import config


def is_logged_in(session):
    try:
        r = session.get(f"{config.IBKR_BASE_URL}/v1/api/iserver/auth/status", verify=False)
        return r.status_code == 200 and r.json().get("authenticated", False)
    except Exception as e:
        print("⚠️ Login check failed:", e)
        return False

def run_scraper():
    print("🚀 Starting UST Index Generator...")

    session = requests.Session()
    if not is_logged_in(session):
        print("❌ Not authenticated in IBKR. Please login at https://localhost:5000 in your browser.")
        exit(1)

    def download_tcf_file():
        print("\U0001F4E5 [Step 1] Connecting to CME Group to retrieve TCF.xlsx metadata...")

        base_url = "https://www.cmegroup.com/trading/interest-rates/treasury-conversion-factors.html"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36"
        }

        try:
            response = requests.get(base_url, headers=headers, timeout=10)
            response.raise_for_status()
            html_text = response.text
        except requests.exceptions.Timeout:
            raise Exception("❌ Connection to CME timed out. Check your internet or firewall.")
        except Exception as e:
            raise Exception(f"❌ Failed to fetch CME Group page: {e}")

        print("🔍 [Step 2] Searching for update date in page content...")
        match = re.search(r"Updated U\.S\. Treasury Conversion Factors\s*-\s*(\d{1,2} \w+ \d{4})", html_text)
        if not match:
            raise Exception("❌ Could not find update date on CME page.")

        raw_date = match.group(1)
        print(f"📅 Found update date: {raw_date}")

        try:
            parsed_date = datetime.strptime(raw_date, "%d %B %Y")
            formatted_date = parsed_date.strftime("%Y-%m-%d")
        except Exception as e:
            raise Exception(f"❌ Failed to parse date '{raw_date}': {e}")

        download_url = f"https://www.cmegroup.com/trading/interest-rates/files/TCF.xlsx?lastUpdated-{formatted_date}"
        print(f"⬇️  [Step 3] Downloading TCF.xlsx from: {download_url}")

        try:
            output_path = os.path.join(os.getcwd(), "TCF.xlsx")
            file_response = requests.get(download_url, headers=headers, timeout=10)
            file_response.raise_for_status()

            with open(output_path, "wb") as f:
                f.write(file_response.content)

            print(f"✅ [Step 4] TCF.xlsx downloaded and saved to: {output_path}")
        except requests.exceptions.Timeout:
            raise Exception("❌ Download timed out. Check your internet connection.")
        except Exception as e:
            raise Exception(f"❌ Failed to download or save TCF.xlsx: {e}")

        return output_path

    tcf_path = download_tcf_file()

    print("📊 Loading and filtering CUSIP data...")
    try:
        df = pd.read_excel(tcf_path, sheet_name=0, usecols="C:Q")
        print("✅ Loaded TCF.xlsx from:", tcf_path)
    except Exception as e:
        print(f"❌ Failed to load TCF.xlsx from {tcf_path}: {e}")
        return

    df.columns = ['coupon', 'issue_date', 'maturity_date', 'cusip', 'issuance', 'cf_1', 'cf_2', 'cf_3', 'cf_4', 'cf_5', 'cf_6', 'cf_7', 'cf_8', 'cf_9', 'cf_10']
    print("🧹 Cleaning up early non-data rows...")
    df = df.dropna(subset=['maturity_date', 'cusip']).copy()
    df['cusip'] = df['cusip'].astype(str).str.strip()
    valid_format = df['cusip'].str.fullmatch(r'^[0-9A-Z]{9}$', na=False)
    invalid_cusips = df.loc[~valid_format, 'cusip']
    if not invalid_cusips.empty:
        print("⚠️ Ignoring invalid CUSIPs:\n", invalid_cusips.to_list())
    df = df[valid_format].copy()

    df['maturity_date'] = pd.to_datetime(df['maturity_date'], errors='coerce')
    df['issue_date'] = pd.to_datetime(df['issue_date'], errors='coerce')
    df['yrstomat'] = (df['maturity_date'] - datetime.today()).dt.days / 365.25
    df_filtered = df[df['yrstomat'] <= 10.5].copy()

    cf_cols = ['cf_1', 'cf_2', 'cf_3', 'cf_4', 'cf_5', 'cf_6', 'cf_7', 'cf_8', 'cf_9', 'cf_10']
    def extract_cf(row):
        # Extract strings that are non-empty and represent a number
        cfs = [str(row[col]).strip()
               for col in cf_cols
               if pd.notna(row[col])
               and str(row[col]).strip() != ''
               and str(row[col]).strip().replace('.', '', 1).isdigit()]
        # Convert the first two valid cash flow values to float; if not available, return NaN.
        cf1 = float(cfs[0]) if len(cfs) > 0 else np.nan
        cf2 = float(cfs[1]) if len(cfs) > 1 else np.nan
        return pd.Series([cf1, cf2])

    df_filtered[['CF1', 'CF2']] = df_filtered.apply(extract_cf, axis=1)

    print("🧪 Sample cleaned CUSIP data:")
    print(df_filtered[['cusip', 'maturity_date', 'yrstomat', 'issuance', 'CF1', 'CF2']])

    target_cusips = set(df_filtered['cusip'])
    print(f"🎯 Targeting {len(target_cusips)} valid CUSIPs.")

    if len(target_cusips) == 0:
        print("❌ No valid CUSIPs found after filtering. Exiting.")
        return

    print("📡 Starting secdef search for each CUSIP...")

    matched = []
    matched_cusips = set()

    search_url = f"{config.IBKR_BASE_URL}/v1/api/iserver/secdef/search"
    conid_to_meta = {}

    for idx, cusip in enumerate(target_cusips, start=1):
        print(f"🔎 Searching for CUSIP {cusip} ({idx}/{len(target_cusips)})...")

        try:
            response = session.post(search_url, json={"symbol": cusip}, verify=False)
            response.raise_for_status()
            results = response.json()
        except requests.RequestException as e:
            print(f"❌ Request failed for {cusip}: {e}")
            continue

        if isinstance(results, list):
            for item in results:
                result_conId = item.get('conid')
                if result_conId and cusip not in matched_cusips:
                    conid_to_meta[result_conId] = cusip
                    bond_info = df_filtered[df_filtered['cusip'] == cusip].iloc[0]
                    matched.append({
                        'cusip': cusip,
                        'conid': result_conId,
                        'coupon': bond_info['coupon'],
                        'issue_date': bond_info['issue_date'],
                        'maturity_date': bond_info['maturity_date'],
                        'yrstomat': bond_info['yrstomat'],
                        'issuance': bond_info['issuance'],
                        'CF1': bond_info.get('CF1', ''),
                        'CF2': bond_info.get('CF2', '')
                    })
                    matched_cusips.add(cusip)
                    print(f"✅ Matched: {cusip} ({len(matched_cusips)}/{len(target_cusips)})")
        else:
            print(f"⚠️ No results returned for {cusip}")

    print(f"✅ Finished matching loop. Matched {len(matched_cusips)} of {len(target_cusips)} CUSIPs.")

    print("📅 Imputing coupon dates based on maturity and 6-month intervals...")
    today = datetime.today().date()
    calendar = USFederalHolidayCalendar()
    holidays = calendar.holidays(start=today - timedelta(days=365*10), end=today + timedelta(days=365*10)).to_pydatetime()

    def calculate_coupon_dates(row):
        maturity = row['maturity_date'].date()
        prev = None
        next_ = None

        coupon_dates = []
        current = maturity
        while current > row['issue_date'].date():
            coupon_dates.append(current)
            current = current - timedelta(days=182)

        coupon_dates = sorted(coupon_dates)
        for date in coupon_dates:
            if date <= today:
                prev = date
            elif date > today and next_ is None:
                next_ = date

        def adjust(date):
            date += timedelta(days=1)
            while date.weekday() >= 5 or date in holidays:
                date += timedelta(days=1)
            return date

        return pd.Series([adjust(prev) if prev else '', adjust(next_) if next_ else ''])

    matched_df = pd.DataFrame(matched)
    matched_df[['prvcpn', 'nxtcpn']] = matched_df.apply(calculate_coupon_dates, axis=1)

    print("💾 Saving results to 'UST.index'...")
    matched_df.to_csv("UST.index", index=False)
    print(f"🎉 Done! {len(matched_df)} bonds saved to 'UST.index'.")

    print("📌 Updating global variable USTs from UST.index...")
    try:
        config.USTs = pd.read_csv("UST.index")
        print("✅ Global variable `USTs` has been updated.")
        print("🖨️ Final UST.index preview:")
        print(config.USTs)
    except Exception as e:
        print(f"❌ Failed to update config.USTs: {e}")

if __name__ == "__main__":
    run_scraper()
