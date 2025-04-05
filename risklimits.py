import config
import pandas as pd
import numpy as np
import requests
import datetime
from scipy.stats import norm
from fixed_income_calc import DV01, approximate_convexity, approximate_duration

IBKR_API_BASE = "https://localhost:5000/v1/api"

def fetch_all_historical():
    futures_df = config.FUTURES.copy()
    historical_data = {}

    for idx, row in futures_df.iterrows():
        conid = row["conid"]
        print(f"Fetching historical data for conid {conid}...")
        url = f"{IBKR_API_BASE}/iserver/marketdata/history"
        params = {
            "conid": conid,
            "period": "1mo",
            "bar": "1d",
            "outsideRth": False
        }
        response = requests.get(url, params=params, verify=False)
        if response.status_code == 200:
            historical_data[conid] = response.json().get("data", [])
            print(f"Retrieved {len(historical_data[conid])} historical points for conid {conid}.")
        else:
            print(f"Failed to fetch history for conid {conid}")
            historical_data[conid] = []

    config.FUTURES_historical = historical_data

def compute_risk_metrics():
    print("Starting risk metrics computation...")
    fetch_all_historical()

    futures_df = config.FUTURES.copy()
    volatilities = []
    convexities = {}
    durations = {}

    for idx, row in futures_df.iterrows():
        conid = row["conid"]
        cf = row.get("CF", 1.0)
        data = config.FUTURES_historical.get(conid, [])
        closes = [point["c"] for point in data if "c" in point]
        if len(closes) >= 2:
            returns = np.diff(np.log(closes))
            std_dev = np.std(returns)
            vol = std_dev * norm.ppf(0.99)
        else:
            vol = 0.0
        print(f"Volatility for conid {conid}: {vol:.6f}")
        volatilities.append(vol)

        delta_ys = [0.0001, 0.001, -0.0001, -0.001]
        historical_convexities = {dy: [] for dy in delta_ys}
        historical_durations = {dy: [] for dy in delta_ys}

        cpn = row.get("coupon", 0.03)
        term = row.get("year_to_maturity", 2)
        ytm = row.get("ytm", 0.03)

        for price in closes:
            for delta_y in delta_ys:
                try:
                    conv = approximate_convexity(cpn, term, ytm, delta_y=delta_y) / cf
                    historical_convexities[delta_y].append(conv)
                    dur = approximate_duration(cpn, term, ytm, delta_y=delta_y) / cf
                    historical_durations[delta_y].append(dur)
                except:
                    historical_convexities[delta_y].append(0.0)
                    historical_durations[delta_y].append(0.0)

        convexities[conid] = historical_convexities
        durations[conid] = historical_durations

    futures_df["Volatility"] = volatilities
    config.FUTURES_historical_df = futures_df

    orders = config.ORDERS.copy()
    results = []

    for idx, order in enumerate(orders):
        if idx > 0:
            previous_result = results[idx - 1]
            if not (previous_result["OverlayTestFlag"] or previous_result["ConvexityRisk"] or previous_result["DurationRisk"] or previous_result["StressTestFlag"]):
                print(f"Skipping pair {idx+1} because previous passed all tests.")
                continue

        front_conid = order["front_conId"]
        back_conid = order["back_conId"]
        front_ratio = order["front_ratio"]
        back_ratio = order["back_ratio"]
        quantity = order["quantity"]

        front = futures_df[futures_df["conid"] == front_conid].iloc[0]
        back = futures_df[futures_df["conid"] == back_conid].iloc[0]

        front_multiplier = front.get("multiplier", 1000)
        front_contract_value = front.get("price", 100.0)
        front_vol = front["Volatility"]
        front_cf = front.get("CF", 1.0)
        front_cpn = front.get("coupon", 0.03)
        front_term = front.get("year_to_maturity", 2)
        front_ytm = front.get("ytm", 0.03)

        back_multiplier = back.get("multiplier", 1000)
        back_contract_value = back.get("price", 100.0)
        back_vol = back["Volatility"]
        back_cf = back.get("CF", 1.0)
        back_cpn = back.get("coupon", 0.03)
        back_term = back.get("year_to_maturity", 2)
        back_ytm = back.get("ytm", 0.03)

        holding_period_yield_delta = 1
        holding_period_price_delta = 1

        var_front = quantity * front_multiplier * holding_period_yield_delta * front_vol * front_ratio
        var_back = quantity * back_multiplier * holding_period_yield_delta * back_vol * back_ratio
        var = var_front + var_back
        print(f"VaR for pair {idx+1}: {var:.4f}")

        pos_risk_front = quantity * front_multiplier * front_contract_value * holding_period_price_delta * front_vol * front_ratio
        pos_risk_back = quantity * back_multiplier * back_contract_value * holding_period_price_delta * back_vol * back_ratio
        pos_risk = pos_risk_front + pos_risk_back
        print(f"Position Risk for pair {idx+1}: {pos_risk:.4f}")

        overlay = (DV01(front_cpn, front_term, front_ytm) / front_cf) * front_ratio + \
                  (DV01(back_cpn, back_term, back_ytm) / back_cf) * back_ratio

        net_contract_value = (quantity * front_multiplier * front_contract_value * front_ratio) + \
                             (quantity * back_multiplier * back_contract_value * back_ratio)

        overlay_test = abs(overlay) > 0.1 * abs(net_contract_value)
        print(f"Overlay for pair {idx+1}: {overlay:.4f} | Test Passed: {not overlay_test}")

        convexity_risk_flag = False
        duration_risk_flag = False
        for delta_y in [0.0001, 0.001, -0.0001, -0.001]:
            front_hist_conv = np.array(convexities.get(front_conid, {}).get(delta_y, [0.0]))
            back_hist_conv = np.array(convexities.get(back_conid, {}).get(delta_y, [0.0]))

            front_new_conv = approximate_convexity(front_cpn, front_term, front_ytm, delta_y=delta_y) / front_cf
            back_new_conv = approximate_convexity(back_cpn, back_term, back_ytm, delta_y=delta_y) / back_cf

            front_conv_thresh = np.percentile(front_hist_conv, 99)
            back_conv_thresh = np.percentile(back_hist_conv, 99)

            if front_new_conv > front_conv_thresh or back_new_conv > back_conv_thresh:
                convexity_risk_flag = True

            front_hist_dur = np.array(durations.get(front_conid, {}).get(delta_y, [0.0]))
            back_hist_dur = np.array(durations.get(back_conid, {}).get(delta_y, [0.0]))

            front_new_dur = approximate_duration(front_cpn, front_term, front_ytm, delta_y=delta_y) / front_cf
            back_new_dur = approximate_duration(back_cpn, back_term, back_ytm, delta_y=delta_y) / back_cf

            front_dur_thresh = np.percentile(front_hist_dur, 99)
            back_dur_thresh = np.percentile(back_hist_dur, 99)

            if front_new_dur > front_dur_thresh or back_new_dur > back_dur_thresh:
                duration_risk_flag = True

        print(f"Convexity risk flag for pair {idx+1}: {convexity_risk_flag}")
        print(f"Duration risk flag for pair {idx+1}: {duration_risk_flag}")

        stress_dv01s = []
        stress_test_flag = False
        for stress_dy in [0.005, -0.005, 0.05, -0.05, 0.5, -0.5]:
            f_dur = approximate_duration(front_cpn, front_term, front_ytm, delta_y=stress_dy)
            b_dur = approximate_duration(back_cpn, back_term, back_ytm, delta_y=stress_dy)
            f_dv01 = (f_dur * front_contract_value) * 0.0001 * front_ratio
            b_dv01 = (b_dur * back_contract_value) * 0.0001 * back_ratio
            stress_overlay = f_dv01 + b_dv01
            stress_dv01s.append((stress_dy, stress_overlay))

            if abs(stress_overlay) > 0.1 * abs(net_contract_value):
                stress_test_flag = True

        print(f"Stress test flag for pair {idx+1}: {stress_test_flag}")

        results.append({
            "pair": (front_conid, back_conid),
            "VaR": var,
            "PosRisk": pos_risk,
            "Overlay_1bp": overlay,
            "OverlayTestFlag": overlay_test,
            "ConvexityRisk": convexity_risk_flag,
            "DurationRisk": duration_risk_flag,
            "StressTest": stress_dv01s,
            "StressTestFlag": stress_test_flag
        })

    risk_df = pd.DataFrame(results)
    print(risk_df)
    return risk_df

if __name__ == "__main__":
    compute_risk_metrics()
