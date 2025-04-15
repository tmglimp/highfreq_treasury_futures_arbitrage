"""
Orders
"""
import json
import pandas as pd
import requests
import urllib3
from fees import calculate_total_fees
import config
from leaky_bucket import leaky_bucket

# Ignore insecure error messages
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def format_value(value, A_symbol, B_symbol, A_incr, B_incr):
    """
    Returns:
        float: The rounded price (exact_price) based on the minimum price fluctuation for the chosen symbol.
    """
    # Step 1: Determine which dyad has the larger increment
    if A_incr >= B_incr:
        chosen_symbol = A_symbol
    else:
        chosen_symbol = B_symbol

    # Step 2: Determine the minimum price fluctuation based on the chosen symbol
    if chosen_symbol in ['Z3N', 'ZT', 'ZF']:
        min_fluc = 7.8125
    elif chosen_symbol in ['ZN', 'TN']:
        min_fluc = 15.625
    else:
        raise ValueError(f"Unsupported symbol: {chosen_symbol}")

    # Step 3: Round the value to the nearest incremental interval (min_fluc)
    exact_price = (value*.6 // min_fluc)
    exact_price = (exact_price * min_fluc)

    # Step 4: Round the result to 6 decimal places
    exact_price = round(exact_price, 6)

    # Step 5: Format the number as a string with exactly 6 decimal places
    exact_price = str(exact_price)

    # Step 6: Split the string at the decimal
    int_part, dec_part = exact_price.split('.')

    # Step 7: Truncate the last character off the string
    if len(dec_part) > 5:
        dec_part_five = dec_part[:-1]
    else:
        dec_part_five = dec_part

    # Step 8: Convert to float
    exact_price = str(int_part + "." + dec_part_five)
    exact_price = float(exact_price)

    return exact_price
    print(f'Exact price with Futures contact-specific incremental rounding is being printed as', exact_price)

def suppress_order_warning(message_ids):
    """
    Args:
      message_ids (list): List of message IDs to suppress (e.g., ["o163"])
    Returns:
      dict: Response from the suppression endpoint.
    """
    print(f'Suppressing messageIds => {message_ids}')
    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/questions/suppress"
    data = {"messageIds": message_ids}
    leaky_bucket.wait_for_token()
    response = requests.post(url, json=data, verify=False)
    print("Suppression response:", response.text)
    return response.json()

def orderRequest(updated_ORDERS):

    updated_ORDERS = config.updated_ORDERS
    print(f'updated_ORDERS as', updated_ORDERS)
    first_row = updated_ORDERS.iloc[0]
    print(f'first_row as', first_row)

    try:
        front_conId = int(first_row["A_FUT_CONID"])
        front_ratio = int(first_row["A_Q_Value"] * first_row["A_Q_sign"])
        back_conId = int(first_row["B_FUT_CONID"])
        back_ratio = int(first_row["B_Q_Value"] * first_row["B_Q_sign"])
        quantity = int(first_row["PairsLCM"])
        A_symbol = (first_row["A_FUT_TICKER"])
        B_symbol = (first_row["B_FUT_TICKER"])
        A_exch = (first_row["A_FUT_UNDERLYING_EXCHANGE"])
        B_exch = (first_row["B_FUT_UNDERLYING_EXCHANGE"])
        A_incr = float(first_row["A_FUT_INCREMENT"])
        B_incr = float(first_row["A_FUT_INCREMENT"])
        value = (first_row["PairsAdjNetBasis"])/1000
        print('printing order value as', value)
        value_after_fees = float(value - calculate_total_fees(A_exch, A_symbol, B_exch, B_symbol))
        print('printing order value after fees as', value_after_fees)
        reduced_value = value_after_fees * .3
        exact_price_i = format_value(reduced_value, A_symbol, B_symbol, A_incr, B_incr)
        exact_price = float(exact_price_i)

    except KeyError as e:
        print(f"Missing column: {e}")
        raise

    url = f"{config.IBKR_BASE_URL}/v1/api/iserver/account/{config.IBKR_ACCT_ID}/orders"
    json_body = {
        "orders": [
            {
                "exchange":  f"SMART;;;{front_conId},{back_conId}",
                "conidex": f"28812380;;;{front_conId}/{front_ratio},{back_conId}/{back_ratio}",
                "orderType": "LMT",
                "price": float(1*exact_price),
                "side": "BUY",
                "tif": "DAY",
                "quantity": int(quantity),
                "secType": 'FUT',
                "outsideRth": "True"
            }
        ]
    }

    leaky_bucket.wait_for_token()

    print(f"Placing order: {url}")
    print(json.dumps(json_body, indent=2))
    order_req = requests.post(url=url, verify=False, json=json_body)
    print(order_req.status_code)
    print(order_req.text)

if __name__ == "__main__":
    orderRequest(config.updated_ORDERS)
