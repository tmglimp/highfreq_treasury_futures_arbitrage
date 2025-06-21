import threading
import time
import os
from datetime import datetime, timedelta
import pandas as pd
import Future_index
import config
from business_logic import business_logic_function
from orders import suppress_order_warning
from risklimits import fetch__historical
from scraper import run_scraper

def search_for_updates(file):
    """Placeholder for your update/search logic."""
    print(f"Searching for updates on '{file}'...")

def file_has_valid_header(path):
    """
    Check if the CSV file at 'path' contains expected headers.
    We expect to see at least one of these keys: "serverId", "symbol", or "conid".
    """
    try:
        df = pd.read_csv(path, nrows=1)
        expected_keys = {"serverId", "symbol", "conid"}
        # Return True if any expected key is found in the file header.
        return not expected_keys.isdisjoint(set(df.columns))
    except Exception as e:
        print(f"Failed to read header from {path}: {e}")
        return False

def normalize_conid_column(df: pd.DataFrame, file_label: str) -> pd.DataFrame:
    """Ensure DataFrame has a lowercase 'conid' column, renaming if necessary."""
    df.columns = df.columns.str.strip()
    cols_lower = [c.lower() for c in df.columns]
    if 'conid' not in cols_lower:
        raise KeyError(f"Expected 'conid' column in {file_label}, got {df.columns.tolist()}")
    for orig in df.columns:
        if orig.lower() == 'conid':
            df.rename(columns={orig: 'conid'}, inplace=True)
            break
    return df

def check_files(file_list, update_config=False):
    statuses = {}
    cwd = os.getcwd()
    now = datetime.now()
    if now.hour >= 17:
        recent_reset = now.replace(hour=17, minute=0, second=0, microsecond=0)
    else:
        yesterday = now - timedelta(days=1)
        recent_reset = yesterday.replace(hour=17, minute=0, second=0, microsecond=0)

    print(f"Scanning directory: {cwd}")
    print(f"Most recent reset: {recent_reset}")

    for file in file_list:
        path = os.path.join(cwd, file)
        if os.path.exists(path):
            # For historical CSV files, check the header.
            if file in ("FUTURES_historical.csv", "USTs_historical.csv"):
                if not file_has_valid_header(path):
                    print(f"{file} has an invalid header.")
                    statuses[file] = False
                    search_for_updates(file)
                    continue

            size = os.path.getsize(path)
            mod_time = datetime.fromtimestamp(os.path.getmtime(path))
            up_to_date = size > 0 and (mod_time >= recent_reset)

            if size == 0:
                print(f"{file} is empty (modified {mod_time}).")
                statuses[file] = False
                search_for_updates(file)
            elif not up_to_date:
                print(f"{file} is outdated (modified {mod_time}).")
                statuses[file] = False
                search_for_updates(file)
            else:
                print(f"{file} is up to date (modified {mod_time}).")
                statuses[file] = True

                if update_config and file.endswith(('.csv', '.index')):
                    try:
                        df = pd.read_csv(path)
                        if file in ("FUTURES_historical.csv", "USTs_historical.csv", "UST.index.csv", "FUTURES.index"):
                            df = normalize_conid_column(df, file)
                        if file == "FUTURES_historical.csv":
                            config.FUTURES_historical = df
                            print("Loaded FUTURES_historical.csv >>> config.FUTURES_historical")
                        elif file == "USTs_historical.csv":
                            config.USTs_historical = df
                            print("Loaded USTs_historical.csv >>> config.USTs_historical")
                        elif file == "UST.index":
                            config.USTs = df
                            print("Loaded UST.index >>> config.USTs")
                        elif file == "FUTURES.index":
                            config.FUTURES = df
                            print("Loaded FUTURES.index >>> config.FUTURES")
                    except Exception as e:
                        print(f"Error loading {file}: {e}")
        else:
            print(f"{file} does not exist.")
            statuses[file] = False

    return statuses

if __name__ == "__main__":
    files_to_check = [
        "UST.index.csv",
        "USTs_historical.csv",
        "FUTURES.index",
        "FUTURES_historical.csv",
        "TCF_enriched.csv"
    ]
    statuses = check_files(files_to_check, update_config=True)

    # Ensure index files
    if not statuses.get("UST.index.csv", False):
        print("UST.index missing/stale: running scraper()")
        run_scraper()
        try:
            df = pd.read_csv("UST.index.csv")
            df = normalize_conid_column(df, "UST.index.csv")
            config.USTs = df
            expected_ust = len(df)
        except Exception as e:
            print(f"Couldn’t read UST.index after scrape: {e}")
            expected_ust = 0
    else:
        print("Using cached UST.index from config.")
        df = pd.read_csv("UST.index.csv")
        config.USTs = df
        expected_ust = len(config.USTs) if hasattr(config, 'USTs') and 'conid' in config.USTs.columns else 0

    if not statuses.get("FUTURES.index", False):
        print("FUTURES.index missing/stale: running Future_index.main()")
        Future_index.main()
        try:
            df = pd.read_csv("FUTURES.index")
            df = normalize_conid_column(df, "FUTURES.index")
            config.FUTURES = df
            expected_futures = len(df)
        except Exception as e:
            print(f"Couldn’t read FUTURES.index after discovery: {e}")
            expected_futures = 0
    else:
        print("Using cached FUTURES.index from config.")
        df = pd.read_csv("FUTURES.index")
        config.USTs = df
        expected_ust = len(config.FUTURES) if hasattr(config, 'FUTURES') and 'conid' in config.FUTURES.columns else 0
        expected_futures = len(config.FUTURES) if hasattr(config, 'FUTURES') and 'conid' in config.FUTURES.columns else 0

    print(f"Expected UST rows: {expected_ust}")
    print(f"Expected FUTURES rows: {expected_futures}")

    # Initialize empty historical DataFrames if missing
    if not statuses.get("USTs_historical.csv", False):
        config.USTs_historical = pd.DataFrame(columns=['conid'])
    if not statuses.get("FUTURES_historical.csv", False):
        config.FUTURES_historical = pd.DataFrame(columns=['conid'])

    def is_data_empty(row: pd.Series) -> bool:
        return row.drop(labels=["conid"], errors='ignore').isna().all()

    def ensure_full_coverage_csv(expected_ids, historical_df: pd.DataFrame, label: str):
        existing_ids = set(historical_df["conid"].astype(str))
        missing_or_empty = [cid for cid in expected_ids
                            if cid not in existing_ids
                            or is_data_empty(historical_df.loc[historical_df["conid"].astype(str) == cid].iloc[0])]
        attempt = 1
        rows = historical_df.to_dict(orient="records")

        while missing_or_empty:
            print(f"Retry {attempt} for {label}: missing or empty {len(missing_or_empty)} of {len(expected_ids)} ids")
            new_data = fetch__historical(((i, {"conid": cid}) for i, cid in enumerate(missing_or_empty)), label)

            # Check if first item in new_data is empty AND year_to_maturity < 0
            first_key = next(iter(new_data), None)
            if first_key is not None:
                first_record = new_data[first_key]
                if isinstance(first_record, list) and first_record:
                    ytm = first_record[0].get("year_to_maturity", 1)
                    if ytm < 0 and is_data_empty(first_record[0]):
                        print(f"  Stopping retries for {label}: year_to_maturity < 0 and first result is empty.")
                        break

            for cid, record in new_data.items():
                if isinstance(record, list):
                    rows.extend(record)
                else:
                    rows.append(record)

            df = pd.DataFrame(rows)
            df.set_index("conid", inplace=True)
            csv_path = f"{label}_historical.csv"
            df.to_csv(csv_path, index=True)
            historical_df = df.reset_index()
            existing_ids = set(historical_df["conid"].astype(str))
            missing_or_empty = [cid for cid in expected_ids
                                if cid not in existing_ids
                                or is_data_empty(historical_df.loc[historical_df["conid"].astype(str) == cid].iloc[0])]
            attempt += 1

        print(f"Saved {label}_historical.csv ({len(historical_df)} rows)")


    suppress_order_warning(config.SUPPRESSED_IDS.split(','))
    logic_thread = threading.Thread(target=business_logic_function, daemon=True)
    logic_thread.start()

    while True:
        time.sleep(.00000001)