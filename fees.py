import config
from config import VOLUME

TIERED_COMMISSIONS = [
    (0, 1000, 0.85),
    (1001, 10000, 0.65),
    (10001, 20000, 0.45),
    (20001, float('inf'), 0.25)
]

# Static exchange + regulatory + give-up fees
EXCHANGE_FEES = {
    "CBOT": {
        "ZT": {"exchange": 0.65, "reg": 0.02, "giveup": 0.06},
        "ZF": {"exchange": 0.65, "reg": 0.02, "giveup": 0.06},
        "Z3N": {"exchange": 0.65, "reg": 0.02, "giveup": 0.06},
        "ZN": {"exchange": 0.80, "reg": 0.02, "giveup": 0.06},
        "TN": {"exchange": 0.80, "reg": 0.02, "giveup": 0.06},
    },
    "QBAlgo": {
        "ZT": {"exchange": 0.75, "reg": 0.00, "giveup": 0.00},
        "ZF": {"exchange": 0.75, "reg": 0.00, "giveup": 0.00},
        "Z3N": {"exchange": 0.75, "reg": 0.00, "giveup": 0.00},
        "ZN": {"exchange": 0.75, "reg": 0.00, "giveup": 0.00},
        "TN": {"exchange": 0.75, "reg": 0.00, "giveup": 0.00},
    },
    "SmallExch": {
        "ZT": {"exchange": 0.15, "reg": 0.02, "giveup": 0.02},
        "ZF": {"exchange": 0.15, "reg": 0.02, "giveup": 0.02},
        "Z3N": {"exchange": 0.15, "reg": 0.02, "giveup": 0.02},
        "ZN": {"exchange": 0.15, "reg": 0.02, "giveup": 0.02},
        "TN": {"exchange": 0.15, "reg": 0.02, "giveup": 0.02},
    },
}

def get_commission_rate(VOLUME):
    for low, high, rate in TIERED_COMMISSIONS:
        if low < VOLUME <= high:
            return rate
    return 0.0

def get_symbol_fees(symbol, preferred_exchange=None):
    """Get fees for a symbol using a preferred exchange if defined, else fallback to default.
    If the symbol is None or empty, it defaults to 'QBALGO'.
    """
    DEFAULT_EXCHANGE = 'QBAlgo'
    SMALL_EXCHANGE = 'SmallExch'
    CBOT = 'CBOT'
    exchanges_to_try = [preferred_exchange, CBOT, DEFAULT_EXCHANGE, SMALL_EXCHANGE] if preferred_exchange else [DEFAULT_EXCHANGE]

    for exchange in exchanges_to_try:
        if not exchange:
            continue
        symbol_fees = EXCHANGE_FEES.get(exchange, {}).get(symbol)
        if symbol_fees:
            return symbol_fees

    raise ValueError(f"Fees not found for symbol '{symbol}' in exchanges: {exchanges_to_try}")

def calculate_total_fees(A_exchange, A_symbol, B_exchange, B_symbol):
    if config.updated_ORDERS.empty:
        first_row = updated_ORDERS.iloc[0]
    else:
        first_row = config.updated_ORDERS.iloc[0]

    commission = get_commission_rate(config.VOLUME)

    fees_A = get_symbol_fees(A_symbol, A_exchange)
    fees_B = get_symbol_fees(B_symbol, B_exchange)

    total_A = (commission + fees_A["exchange"] + fees_A["reg"] + fees_A["giveup"]) * first_row["A_Q_Value"]
    total_B = (commission + fees_B["exchange"] + fees_B["reg"] + fees_B["giveup"]) * first_row["B_Q_Value"]

    return float(total_A + total_B)
