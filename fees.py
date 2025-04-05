import config
# Static fee schedule by tier and exchange/symbol
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

def get_commission_rate(volume):
    if not isinstance(TIERED_COMMISSIONS, list):
        raise TypeError(f"TIERED_COMMISSIONS must be a list, got {type(TIERED_COMMISSIONS)}")
    for low, high, rate in TIERED_COMMISSIONS:
        if low < volume <= high:
            return rate
    return 0.0

def calculate_total_fees(A_exchange, A_symbol, B_exchange, B_symbol):
    commission = get_commission_rate(config.VOLUME)

    def get_symbol_fees(exchange, symbol):
        symbol_fees = EXCHANGE_FEES.get(exchange, {}).get(symbol)
        if not symbol_fees:
            raise ValueError(f"Fees not found for symbol '{symbol}' on exchange '{exchange}'.")
        return symbol_fees

    fees_A = get_symbol_fees(A_exchange, A_symbol)
    fees_B = get_symbol_fees(B_exchange, B_symbol)

    total_A = commission + fees_A["exchange"] + fees_A["reg"] + fees_A["giveup"]
    total_B = commission + fees_B["exchange"] + fees_B["reg"] + fees_B["giveup"]

    return {
        "A_symbol": {
            "exchange": A_exchange,
            "commission": commission,
            "exchange_fee": fees_A["exchange"],
            "reg_fee": fees_A["reg"],
            "giveup_fee": fees_A["giveup"],
            "total_A": total_A
        },
        "B_symbol": {
            "exchange": B_exchange,
            "commission": commission,
            "exchange_fee": fees_B["exchange"],
            "reg_fee": fees_B["reg"],
            "giveup_fee": fees_B["giveup"],
            "total_B": total_B
        },
        "A_fees": total_A,
        "B_fees": total_B
    }

