'''
Orders
'''
import json
import pandas as pd
import requests
import urllib3
import time
from cf_ctd import cf_ctd_main
from ctd_fut_kpis import run_fixed_income_calculation
from KPIs2_Orders import calculate_quantities

from fees import calculate_total_fees
import config
from config import risk_reducer
from leaky_bucket import leaky_bucket

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

cleared_trades = []
_last_empty_time = None
placed_orders_runtime = pd.DataFrame()

initial_margins = {"ZT": 1200, "ZF": 1250, "ZN": 2200, "Z3N": 1800, "TN": 2550}

def get_current_timestamp():
    from datetime import datetime
    import pytz
    cst = pytz.timezone('US/Central')
    return datetime.now(cst).isoformat(timespec='microseconds').replace("-06:00", "").replace("-05:00", "")

def format_value(value, A_symbol, B_symbol, A_incr, B_incr):
    chosen_symbol = A_symbol if A_incr >= B_incr else B_symbol
    min_fluc = 0.015625 if chosen_symbol in ['Z3N', 'ZT', 'ZF', 'ZN', 'TN'] else None
    if min_fluc is None:
        raise ValueError(f"Unsupported symbol: {chosen_symbol}")
    exact_price = (value // min_fluc) * min_fluc
    exact_price = round(exact_price, 7)
    exact_price_str = f"{exact_price:.7f}"
    int_part, dec_part = exact_price_str.split('.')
    dec_part_five = dec_part[:-1] if len(dec_part) > 6 else dec_part
    final_price = float(f"{int_part}.{dec_part_five}")
    print(f"Exact price with Futures contact-specific incremental rounding is being printed as {final_price}")
    return final_price

def suppress_order_warning(message_ids):
    print(f"Suppressing messageIds => {message_ids}")
    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/questions/suppress"
    data = {"messageIds": message_ids}
    leaky_bucket.wait_for_token()
    response = requests.post(url, json=data, verify=False)
    print("Suppression response:", response.text)
    try:
        return response.json()
    except Exception:
        print("Could not decode JSON from suppression response, returning raw text.")
        return {"response_text": response.text}

def fetch_pending_orders():
    orders_url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/orders?DU3297612=Account"
    response = requests.get(orders_url, verify=False)
    leaky_bucket.wait_for_token()
    response.raise_for_status()
    data = response.json()
    raw_orders = data.get("orders", [])
    fields = ["acct", "exchange", "conidex", "conid", "account", "orderId", "cashCcy", "sizeAndFills",
              "orderDesc", "description1", "description2", "ticker", "secType", "remainingQuantity",
              "filledQuantity", "totalSize", "companyName", "status", "order_ccp_status", "origOrderType",
              "supportsTaxOpt", "lastExecutionTime", "orderType", "bgColor", "fgColor", "isEventTrading",
              "price", "timeInForce", "lastExecutionTime_r", "side"]

    orders = []
    for raw_order in raw_orders:
        order = {field: raw_order.get(field) for field in fields}
        order["orderId"] = str(order.get("orderId", ""))
        order["order_id"] = order["orderId"]
        order["quantity"] = order.get("totalSize", 0)
        order["client_timestamp"] = get_current_timestamp()
        orders.append(order)
    return pd.DataFrame(orders)

def cancel_order(orderId):
    global placed_orders_runtime
    cancel_url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/{config.IBKR_ACCT_ID}/order/{orderId}"
    response = requests.delete(cancel_url, verify=False)
    print(f"Cancel response: {response.status_code} - {response.text}")
    leaky_bucket.wait_for_token()
    placed_orders_runtime = placed_orders_runtime[placed_orders_runtime["order_id"] != str(orderId)]
    return response

def _clean_orders(df):
    cleared_statuses = {"Clear", "Cleared"}
    rem = df["status"].isin(cleared_statuses) | (df["remainingQuantity"] == 0)
    df_cleared = df[rem]
    df_remaining = df[~rem]
    return df_remaining, df_cleared

def extract_margin_from_conidex(df_placed):
    try:
        total_margin = 0
        for _, row in df_placed.iterrows():
            try:
                ticker_A = row.get('A_FUT_TICKER') or row.get('A_FUT_SYM')
                ticker_B = row.get('B_FUT_TICKER') or row.get('B_FUT_SYM')
                qty_A = abs(int(row.get('A_Q_Value', 0)))
                qty_B = abs(int(row.get('B_Q_Value', 0)))

                if ticker_A in initial_margins:
                    total_margin += initial_margins[ticker_A] * qty_A
                else:
                    print(f"Ticker '{ticker_A}' not found in initial_margins.")

                if ticker_B in initial_margins:
                    total_margin += initial_margins[ticker_B] * qty_B
                else:
                    print(f"Ticker '{ticker_B}' not found in initial_margins.")

            except Exception as row_err:
                print(f"Error processing row for margin calculation: {row_err}")

        config.INITIAL_MARGIN = pd.DataFrame([{
            "pending_orders": len(df_placed),
            "initial_margin_total": float(total_margin)
        }])

        return float(total_margin)
    except Exception as e:
        print(f"General error calculating margin from df_placed: {e}")
        return 0.0

def check_and_cancel_orders():
    global cleared_trades, _last_empty_time
    pending_statuses = {"Submitted", "PreSubmitted"}

    try:
        df_placed = config.placed_orders_runtime
        df_placed["order_id"] = df_placed["order_id"].astype(str)
    except Exception as e:
        print("⚠️ Could not load placed_orders_runtime.csv. Assuming it's empty.")
        df_placed = pd.DataFrame(columns=["order_id", "timestamp"])

    try:
        df_orders = fetch_pending_orders()
    except Exception as e:
        print("Error fetching pending orders:", e)
        return

    df_orders["order_id"] = df_orders["order_id"].astype(str)
    print("\nFetched orders from endpoint:")
    print(df_orders)

    try:
        if not df_placed.empty and "remainingQuantity" in df_orders.columns:
            filled_orders = df_orders[df_orders["remainingQuantity"] == 0.0]["order_id"].astype(str)
            if not filled_orders.empty:
                initial_len = len(df_placed)
                df_placed = df_placed[~df_placed["order_id"].isin(filled_orders)]
                removed_count = initial_len - len(df_placed)
                if removed_count > 0:
                    print(f"Removed {removed_count} filled orders from placed_orders_runtime")
    except Exception as e:
        print(f"⚠️ Error removing filled orders from placed_orders: {e}")

    required_columns = {"status", "remainingQuantity", "orderId"}
    if df_orders.empty or not required_columns.issubset(df_orders.columns):
        print("⚠️ Warning: No valid orders returned or required fields missing from API response.")
        config.NO_PENDING_ORDERS = True
        return

    df_remaining, df_cleared = _clean_orders(df_orders)
    cleared_trades.extend(df_cleared["order_id"].tolist())

    df_pending = df_remaining[df_remaining["status"].isin(pending_statuses)].copy()
    print("\nOrders considered pending (status in Submitted/PreSubmitted):")
    print(df_pending)

    try:
        df_pending.to_csv("pending_orders.csv", index=False)
        print("Saved pending orders to pending_orders.csv")
    except Exception as e:
        print("Error saving pending orders to CSV:", e)

    if df_pending.empty:
        if _last_empty_time is None:
            _last_empty_time = time.time()
        elif time.time() - _last_empty_time > config.PEND_CLEAR:
            config.NO_PENDING_ORDERS = True
            print("\n⚠️  No pending orders for more than 5 minutes — setting config.NO_PENDING_ORDERS = True")
    else:
        _last_empty_time = None
        config.NO_PENDING_ORDERS = False

    if df_placed.empty:
        print("⚠️ placed_orders.csv is empty. Cancelling all pending orders.")
        for order_id in df_pending["order_id"]:
            try:
                resp = cancel_order(order_id)
                print(f"Cancelled order {order_id}. Status: {resp.status_code}")
            except Exception as e:
                print(f"Error cancelling order {order_id}: {e}")
        return

    if df_pending.shape[0] < 4:
        print("Pending orders less than 4. No cancellation needed.")
        return

    df_placed_ids = set(df_placed["order_id"])
    print(f'printing placed as', df_placed_ids)
    df_pending_ids = set(df_pending["order_id"])
    print(f'printing pending as', df_pending_ids)
    orphan_ids = df_pending_ids - df_placed_ids
    orphan_orders = df_placed[df_placed["order_id"].isin(orphan_ids)]

    if not orphan_orders.empty and df_pending.shape[0] >= 4:
        print(f"\nFound {len(orphan_orders)} orphan orders:")
        orphan_orders = orphan_orders.sort_values("timestamp")
        print(f'timestamp list of pending orders as', orphan_orders)
        orphan_to_cancel = orphan_orders.head(1)
        for order_id in orphan_to_cancel["order_id"]:
            try:
                resp = cancel_order(order_id)
                print(f"Cancelled orphan order {order_id}. Status: {resp.status_code}")
                df_pending = df_pending[df_pending["order_id"] != order_id]
            except Exception as e:
                print(f"Error cancelling orphan order {order_id}: {e}")

    matched_pending = df_pending[df_pending["order_id"].isin(df_placed["order_id"])]

    if matched_pending.shape[0] > 4:
        print(f"\n⚠️ There are {len(matched_pending)} matched pending orders. Cancelling oldest to maintain max of 5...")

        matched_pending = matched_pending.merge(
            df_placed[["order_id", "timestamp"]],
            how="left", left_on="order_id", right_on="order_id"
        )
        matched_pending["timestamp"] = pd.to_datetime(matched_pending["timestamp"], errors="coerce")
        matched_pending = matched_pending.dropna(subset=["timestamp"]).sort_values("timestamp")

        oldest_order_id = matched_pending.iloc[0]["order_id"]
        try:
            resp = cancel_order(oldest_order_id)
            print(f"✅ Cancelled oldest matched order {oldest_order_id}. Status: {resp.status_code}")
        except Exception as e:
            print(f"⚠️ Error cancelling order {oldest_order_id}: {e}")


...

def orderRequest(updated_ORDERS: pd.DataFrame | None = None) -> None:
    """Place **one** spread order from *updated_ORDERS* (or *config.updated_ORDERS*)
    then reconcile the open‑orders queue.
    """
    global placed_orders_runtime

    timestamp = get_current_timestamp()
    updated_ORDERS = updated_ORDERS if updated_ORDERS is not None else config.updated_ORDERS

    if updated_ORDERS is None or updated_ORDERS.empty:
        print("No orders available in config.updated_ORDERS to place.")
        return

    print("\nupdated_ORDERS:\n", updated_ORDERS)

    # -----------------------------------------------------------------
    # Work with the *first* row by **position**, regardless of its index
    # label.  This avoids KeyError when the frame's index is not 0.
    # -----------------------------------------------------------------
    first_row = updated_ORDERS.iloc[0]

    value = first_row["PairsAdjNetBasis"]
    print("value 265 as", value)

    value_after_fees = value - calculate_total_fees(
        first_row["A_FUT_UNDERLYING_EXCHANGE"],
        first_row["A_FUT_TICKER"],
        first_row["B_FUT_UNDERLYING_EXCHANGE"],
        first_row["B_FUT_TICKER"],
    )
    reduced_value = value_after_fees * risk_reducer

    if reduced_value <= 0:
        print(
            f"Skipping trade because reduced_value is {reduced_value}. "
            "Requesting fresh data to retry..."
        )
        HEDGES = cf_ctd_main()
        HEDGES_Combos = run_fixed_income_calculation(HEDGES)
        config.updated_ORDERS = calculate_quantities(HEDGES_Combos, config.SMA)
        config.updated_ORDERS = config.get_updated_orders()
        return

    exact_price = format_value(
        reduced_value,
        first_row["A_FUT_TICKER"],
        first_row["B_FUT_TICKER"],
        first_row["A_FUT_INCREMENT"],
        first_row["B_FUT_INCREMENT"],
    )
    print("exact_price as", exact_price)

    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/{config.IBKR_ACCT_ID}/orders"

    front_conid = int(first_row["A_FUT_CONID"])
    back_conid = int(first_row["B_FUT_CONID"])
    front_ratio = int(first_row["A_Q_Value"])
    back_ratio = int(first_row["B_Q_Value"])
    quantity = 1
    price = float(back_ratio * exact_price)

    json_body = {
        "orders": [
            {
                "exchange": f"CBOT;;;{front_conid},{back_conid}",
                "conidex": f"28812380;;;{front_conid}/{front_ratio},{back_conid}/{back_ratio}",
                "orderType": "LMT",
                "price": price,
                "side": "BUY",
                "tif": "DAY",
                "quantity": quantity,
                "secType": "FUT",
                "outsideRth": True,
            }
        ]
    }

    leaky_bucket.wait_for_token()
    print("\nPlacing order:", url)
    print(json.dumps(json_body, indent=2))
    order_req = requests.post(url, verify=False, json=json_body)
    print("status code ⇒", order_req.status_code)
    print("response     ⇒", order_req.text)

    if order_req.status_code != 200:
        print("⚠️ Failed to place order. Skipping this attempt.")
        return

    try:
        order_response = order_req.json()
        order_id = order_response[0].get("order_id")
        order_status = order_response[0].get("order_status")
        encrypt_message = order_response[0].get("encrypt_message")
    except Exception as exc:
        print("⚠️ Error parsing order response:", exc)
        return

    placed_orders = pd.DataFrame(
        [
            {
                "order_id": order_id,
                "order_status": order_status,
                "encrypt_message": encrypt_message,
                "orderType": "LMT",
                "price": price,
                "side": "BUY",
                "tif": "DAY",
                "quantity": quantity,
                "secType": "FUT",
                "outsideRth": True,
                "pairs_net_basis": value,
                "RENTD": first_row["RENTD"],
                "timestamp": timestamp,
            }
        ]
    )

    placed_orders_runtime = pd.concat([placed_orders_runtime, placed_orders], ignore_index=True)
    config.placed_orders_runtime = placed_orders_runtime
    print("Order recorded in config.placed_orders_runtime")

    # Always reconcile the open‑orders queue at the end of each attempt
    check_and_cancel_orders()


if __name__ == "__main__":
    orderRequest()

