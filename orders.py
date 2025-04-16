"""
Orders
"""
import json
import pandas as pd
import requests
import urllib3
from fees import calculate_total_fees
import config
from config import PERCENT_PROFIT
from leaky_bucket import leaky_bucket

# Ignore insecure error messages (for self-signed certificates)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global list to track orderIds that have cleared.
cleared_trades = []

# ---------------- Utility Functions ----------------

def format_value(value, A_symbol, B_symbol, A_incr, B_incr):
    """
    Returns:
        float: The rounded price based on the minimum price fluctuation for the chosen symbol.
    """
    chosen_symbol = A_symbol if A_incr >= B_incr else B_symbol

    if chosen_symbol in ['Z3N', 'ZT']:
        min_fluc = 0.015625
    elif chosen_symbol in ['ZF']:
        min_fluc = 0.015625
    elif chosen_symbol in ['ZN', 'TN']:
        min_fluc = 0.015625
    else:
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
    """
    Suppresses given message IDs.

    Args:
      message_ids (list): List of message IDs to suppress (e.g., ["o163"]).

    Returns:
      dict: The parsed JSON response from the suppression endpoint,
            or, if JSON decoding fails, a dictionary containing the raw response text.
    """
    print(f"Suppressing messageIds => {message_ids}")
    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/questions/suppress"
    data = {"messageIds": message_ids}
    leaky_bucket.wait_for_token()
    response = requests.post(url, json=data, verify=False)
    print("Suppression response:", response.text)
    try:
        return response.json()
    except Exception as e:
        print("Could not decode JSON from suppression response, returning raw text.")
        return {"response_text": response.text}

def get_current_timestamp():
    """
    Returns the current time in ISO format using the US/Central timezone.
    """
    import pytz
    from datetime import datetime
    cst = pytz.timezone('US/Central')
    return datetime.now(cst).isoformat()

# ---------------- Orders Endpoint Functions ----------------

def fetch_pending_orders():
    """
    Fetches orders from the IBKR orders endpoint, unpacks required fields,
    and adds a client-side timestamp ("client_timestamp") for each order.
    """
    orders_url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/orders?DU3297612=Account"
    response = requests.get(orders_url, verify=False)
    leaky_bucket.wait_for_token()
    response.raise_for_status()
    data = response.json()
    raw_orders = data.get("orders", [])

    fields = [
        "acct", "exchange", "conidex", "conid", "account", "orderId", "cashCcy", "sizeAndFills",
        "orderDesc", "description1", "description2", "ticker", "secType", "remainingQuantity",
        "filledQuantity", "totalSize", "companyName", "status", "order_ccp_status", "origOrderType",
        "supportsTaxOpt", "lastExecutionTime", "orderType", "bgColor", "fgColor", "isEventTrading",
        "price", "timeInForce", "lastExecutionTime_r", "side"
    ]

    orders = []
    for raw_order in raw_orders:
        order = {field: raw_order.get(field) for field in fields}
        # Convert orderId to string for consistency.
        order["orderId"] = str(order.get("orderId", ""))
        order["quantity"] = order.get("totalSize", 0)
        order["client_timestamp"] = get_current_timestamp()
        orders.append(order)
    return orders

def cancel_order(orderId):
    """
    Cancels an order using the DELETE HTTP method.

    Constructs the cancellation URL using the global account (config.IBKR_ACCT_ID).
    """
    cancel_url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/{config.IBKR_ACCT_ID}/order/{orderId}"
    response = requests.delete(cancel_url, verify=False)
    print(f"Cancel response: {response.status_code} - {response.text}")
    leaky_bucket.wait_for_token()
    return response

# ---------------- Pending Orders Check and Cleanup ----------------

def _clean_orders(df):
    """
    Removes orders that have transitioned to "Clear" or "Cleared" or whose remainingQuantity equals 0.
    Returns a tuple (df_remaining, df_cleared).
    """
    cleared_statuses = {"Clear", "Cleared"}
    mask = df["status"].isin(cleared_statuses) | (df["remainingQuantity"] == 0)
    df_cleared = df[mask]
    df_remaining = df[~mask]
    return df_remaining, df_cleared

def check_and_cancel_orders():
    """
    Fetches orders from the IBKR endpoint, then:
      - Removes orders that are cleared (status "Clear" or "Cleared" or remainingQuantity == 0),
        adding those orderIds to the global cleared_trades list.
      - Filters the remaining orders to only include pending orders (status in {"Submitted", "PreSubmitted"}).
      - If the number of pending orders is 5 or more, cancels the oldest pending order (using client_timestamp)
        repeatedly until fewer than 5 pending orders remain.
      - Finally, updates config.INITIAL_MARGIN with the full list of fetched orders.
    """
    global cleared_trades
    pending_statuses = {"Submitted", "PreSubmitted"}

    try:
        orders = fetch_pending_orders()
    except Exception as e:
        print("Error fetching pending orders:", e)
        return

    df_orders = pd.DataFrame(orders)
    print("\nFetched orders from endpoint:")
    print(df_orders)

    df_remaining, df_cleared = _clean_orders(df_orders)
    cleared_trades.extend(df_cleared["orderId"].tolist())

    df_pending = df_remaining[df_remaining["status"].isin(pending_statuses)]
    print("\nOrders considered pending (status in Submitted/PreSubmitted):")
    print(df_pending)

    # Cancel the oldest pending orders if count is 5 or more.
    while df_pending.shape[0] >= 4:   # Modified condition to include exactly 5 pending orders
        df_sorted = df_pending.sort_values("client_timestamp")
        oldest_order = df_sorted.iloc[0]
        orderId_to_cancel = oldest_order["orderId"]
        print(f"\nPending orders count ({df_pending.shape[0]}) is >= 4. Cancelling oldest pending order (orderId: {orderId_to_cancel}).")
        try:
            resp = cancel_order(orderId_to_cancel)
            if resp.status_code in [200, 204]:
                print(f"Successfully cancelled order {orderId_to_cancel}.")
                # Remove the cancelled order from the local DataFrame.
                df_remaining = df_remaining[df_remaining["orderId"] != orderId_to_cancel]
            else:
                print(f"Failed to cancel order {orderId_to_cancel}. HTTP status: {resp.status_code}")
                break
        except Exception as e:
            print(f"Exception while cancelling order {orderId_to_cancel}: {e}")
            break
        df_pending = df_remaining[df_remaining["status"].isin(pending_statuses)]

    # Update global INITIAL_MARGIN.
    if hasattr(config, "INITIAL_MARGIN") and not config.INITIAL_MARGIN.empty:
        config.INITIAL_MARGIN = pd.concat([config.INITIAL_MARGIN, df_remaining], ignore_index=True)
    else:
        config.INITIAL_MARGIN = df_remaining.copy()
    print("\nUpdated INITIAL_MARGIN:")
    print(config.INITIAL_MARGIN)

# ---------------- Order Request Function ----------------

def orderRequest(updated_ORDERS=None):
    """
    Invokes check_and_cancel_orders() to ensure that pending orders (status "Submitted" or "PreSubmitted")
    are fewer than 5, then submits the next order from config.updated_ORDERS (or updated_ORDERS if provided)
    whose reduced_value > 0.
    If no such order is found, no order is placed.
    """
    # Use global config.updated_ORDERS if no parameter is provided.
    if updated_ORDERS is None:
        updated_ORDERS = config.updated_ORDERS

    # First, clean up pending orders.
    check_and_cancel_orders()

    if updated_ORDERS.empty:
        print("No orders available in config.updated_ORDERS to place.")
        return

    print(f"\nupdated_ORDERS:\n{updated_ORDERS}")

    acceptable_order_found = False
    selected_order = None

    # Iterate over orders until one with reduced_value > 0 is found
    for idx, row in updated_ORDERS.iterrows():
        try:
            front_conId = int(row["A_FUT_CONID"])
            front_ratio = int(row["A_Q_Value"] * row["A_Q_sign"])
            back_conId = int(row["B_FUT_CONID"])
            back_ratio = int(row["B_Q_Value"] * row["B_Q_sign"])
            quantity = int(row["PairsLCM"])
            A_symbol = row["A_FUT_TICKER"]
            B_symbol = row["B_FUT_TICKER"]
            A_exch = row["A_FUT_UNDERLYING_EXCHANGE"]
            B_exch = row["B_FUT_UNDERLYING_EXCHANGE"]
            A_incr = float(row["A_FUT_INCREMENT"])
            B_incr = float(row["B_FUT_INCREMENT"])
            value = row["PairsAdjNetBasis"]
            print("Printing order value as:", value)
            # calculate_total_fees is imported from fees
            value_after_fees = float(value - calculate_total_fees(A_exch, A_symbol, B_exch, B_symbol))
            print("Printing order value after fees as:", value_after_fees)
            reduced_value = value_after_fees * PERCENT_PROFIT
            print(f"Order index {idx} reduced_value: {reduced_value}")
        except KeyError as e:
            print(f"Missing column: {e}")
            continue

        if reduced_value <= 0:
            print(f"Order at index {idx} has reduced_value {reduced_value} <= 0, skipping trade.")
            continue

        acceptable_order_found = True
        selected_order = row
        break

    if not acceptable_order_found:
        print("No orders with reduced_value > 0 available for placement.")
        return

    print(f"\nSelected order (first acceptable):\n{selected_order}")

    try:
        front_conId = int(selected_order["A_FUT_CONID"])
        front_ratio = int(selected_order["A_Q_Value"] * selected_order["A_Q_sign"])
        back_conId = int(selected_order["B_FUT_CONID"])
        back_ratio = int(selected_order["B_Q_Value"] * selected_order["B_Q_sign"])
        quantity = int(selected_order["PairsLCM"])
        A_symbol = selected_order["A_FUT_TICKER"]
        B_symbol = selected_order["B_FUT_TICKER"]
        A_exch = selected_order["A_FUT_UNDERLYING_EXCHANGE"]
        B_exch = selected_order["B_FUT_UNDERLYING_EXCHANGE"]
        A_incr = float(selected_order["A_FUT_INCREMENT"])
        B_incr = float(selected_order["B_FUT_INCREMENT"])
        value = selected_order["PairsAdjNetBasis"]
        print("Printing order value as:", value)
        value_after_fees = float(value - calculate_total_fees(A_exch, A_symbol, B_exch, B_symbol))
        print("Printing order value after fees as:", value_after_fees)
        reduced_value = value_after_fees * PERCENT_PROFIT
        exact_price = format_value(reduced_value, A_symbol, B_symbol, A_incr, B_incr)
    except KeyError as e:
        print(f"Missing column: {e}")
        raise

    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/{config.IBKR_ACCT_ID}/orders"
    json_body = {
        "orders": [
            {
                "exchange": f"SMART;;;{front_conId},{back_conId}",
                "conidex": f"28812380;;;{front_conId}/{front_ratio},{back_conId}/{back_ratio}",
                "orderType": "LMT",
                "price": float(1 * exact_price),
                "side": "BUY",
                "tif": "GTC",
                "quantity": int(quantity),
                "secType": "FUT",
                "outsideRth": True
            }
        ]
    }

    leaky_bucket.wait_for_token()
    print(f"\nPlacing order: {url}")
    print(json.dumps(json_body, indent=2))
    order_req = requests.post(url=url, verify=False, json=json_body)
    print(order_req.status_code)
    print(order_req.text)

# ---------------- Main ----------------

if __name__ == "__main__":
    orderRequest()  # Now orderRequest defaults to using config.updated_ORDERS if no parameter is passed.
