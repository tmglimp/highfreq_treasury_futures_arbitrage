import time

from KPIs2_Orders import calculate_quantities_with_sma, build_orders, calculate_total_fees
import config
from leaky_bucket_orders import OrdersLeakyBucket
from orders import orderRequest
from cf_ctd import cf_ctd_main
from ctd_fut_kpis import run_fixed_income_calculation

# Initialize the leaky bucket for orders
leaky_bucket_orders = OrdersLeakyBucket()


def business_logic_function():
    """
    Continuously executes the business logic as a separate process all the way to the order placement.
    This function runs in a loop and executes every 3 seconds.
    """
    while True:
        # Ensure that the config.USTs and config.FUTURES DataFrames exist and are neither None nor empty.
        # config.USTs and config.FUTURES are being populated continuously from another thread.
        # IF any other script modifies the config.* objects, business_logic.py will always have the latest version.
        if config.USTs is not None and config.FUTURES is not None and not config.USTs.empty and not config.FUTURES.empty:
            print("****Business logic started, leveraging other integrated scripts***")

            leaky_bucket_orders.wait_for_slot()  # Wait until there's an available slot for orders

            print("Populated USTs:")
            print(config.USTs)

            print("Populated FUTURES:")
            print(config.FUTURES)

            HEDGES = cf_ctd_main()  # From cf_ctd.py. Takes the immutable global variables and deep copies them
            print("Populated HEDGES:")
            print(HEDGES)  # results object
            HEDGES.to_csv("HEDGES.csv")

            HEDGES_Combos = run_fixed_income_calculation(HEDGES)  # From ctd_fut_kpis.py
            print("Updated HEDGES:")
            print(HEDGES)

            print("Populated HEDGES_Combos:")
            print(HEDGES_Combos)  # results object

            ORDERS = calculate_quantities_with_sma(HEDGES_Combos)  # From KPIs2_Orders.py
            print("Updated HEDGES_Combos:")
            print(ORDERS)

            orderRequest([config.ORDERS])  # From orders.py

        time.sleep(.000001)  # Wait .000001 seconds before the next iteration
