"""
scraper.py
"""

import os
import re
import time
import requests
import pandas as pd
from datetime import datetime
from zeroes import fetch_treasury_data
import config


# ────────────────────────────────────────────────────────────────────────────────
# IBKR gateway helper
# ────────────────────────────────────────────────────────────────────────────────
def is_logged_in(sess: requests.Session) -> bool:
    try:
        r = sess.get(f"{config.IBKR_BASE_URL}/v1/api/iserver/auth/status",
                     verify=False, timeout=5)
        return r.status_code == 200 and r.json().get("authenticated", False)
    except Exception as exc:
        print("Login-status check failed:", exc)
        return False


# ────────────────────────────────────────────────────────────────────────────────
# Fetch CTD Basket List from CME Group
# ────────────────────────────────────────────────────────────────────────────────
def download_tcf_file() -> str:
    print("Connecting to CME for TCF.xlsx metadata …")
    base_url = "https://www.cmegroup.com/trading/interest-rates/treasury-conversion-factors.html"
    headers   = {"User-Agent": "Mozilla/5.0"}
    html = requests.get(base_url, headers=headers, timeout=10).text

    m = re.search(r"Updated U\.S\. Treasury Conversion Factors\s*-\s*(\d{1,2} \w+ \d{4})", html)
    if not m:
        raise RuntimeError("Could not locate update date on CME page.")

    raw_date = m.group(1)
    date_obj = datetime.strptime(raw_date, "%d %B %Y")
    date_str = date_obj.strftime("%Y-%m-%d")
    tcf_url  = f"https://www.cmegroup.com/trading/interest-rates/files/TCF.xlsx?lastUpdated-{date_str}"

    out_path = os.path.join(os.getcwd(), "TCF.xlsx")
    print(f"Downloading TCF.xlsx ({date_str}) …")
    r = requests.get(tcf_url, headers=headers, timeout=15)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)
    print("Saved:", out_path)
    return out_path


# ────────────────────────────────────────────────────────────────────────────────
# Ping Treasury for Detailed SecDef. Derive Dirty CF-Client. Ping IBKR for Contract #
# ────────────────────────────────────────────────────────────────────────────────
def run_scraper() -> None:
    print("Starting UST Index Generator")

    sess = requests.Session()
    if not is_logged_in(sess):
        print("Not authenticated in IBKR.  Visit https://localhost:5000 and login, then retry.")
        return

    # 1) CME download  →  dataframe
    tcf_file = download_tcf_file()

    # 2) Create / update USTs.index.csv via your helper
    print("Running fetch_treasury_data() …")
    fetch_treasury_data()             # writes USTs.index.csv

    # 3) Load USTs.index.csv
    csv_name = "UST.index.csv"
    if not os.path.exists(csv_name):
        raise FileNotFoundError(f"Expected {csv_name} produced by fetch_treasury_data()")

    ust_df = pd.read_csv(csv_name, dtype=str)
    if {"cusip", "corpusCusip"} - set(ust_df.columns):
        raise RuntimeError("CSV missing required 'cusip' or 'corpusCusip' columns")

    # prep output cols
    ust_df["cusip_conid"]        = pd.NA
    ust_df["corpusCusip_conid"]  = pd.NA

    search_url = f"{config.IBKR_BASE_URL}/v1/api/iserver/secdef/search"

    def lookup_conid(code: str) -> str | None:
        if not code or code.lower() == "nan":
            return None
        try:
            r = sess.post(search_url, json={"symbol": code.strip()}, verify=False, timeout=10)
            r.raise_for_status()
            res = r.json() or []
            if isinstance(res, list) and res:
                return res[0].get("conid")
        except requests.RequestException as exc:
            print(f"{code} lookup error:", exc)
        return None

    # 4) Iterate rows once, do both look-ups
    print(f"Querying IBKR for {len(ust_df)} rows …")
    for i, row in ust_df.iterrows():
        cusip  = str(row["cusip"]).strip()        if pd.notna(row["cusip"])       else ""
        corpus = str(row["corpusCusip"]).strip()  if pd.notna(row["corpusCusip"]) else ""

        if cusip:
            conid = lookup_conid(cusip)
            if conid:
                ust_df.at[i, "conid"] = conid

        if corpus:
            conid = lookup_conid(corpus)
            if conid:
                ust_df.at[i, "corpusCusip_conid"] = conid

        time.sleep(0.009)        # gentle pacing

    # 5) Write out enriched index
    ust_df.to_csv("UST.index.csv", index=False)
    print(f"Wrote enriched file UST.index  ({len(ust_df)} rows)")

    # 6) Push into config
    try:
        config.USTs = ust_df.copy()
        print("config.USTs updated.")
    except Exception as exc:
        print("Could not assign to config.USTs:", exc)

    print(ust_df.head())


# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_scraper()
