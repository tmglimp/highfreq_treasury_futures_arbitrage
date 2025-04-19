import time
import config
from orders import orderRequest
from cf_ctd import cf_ctd_main
from ctd_fut_kpis import run_fixed_income_calculation
from KPIs2_Orders import calculate_quantities_with_sma
from leaky_bucket import leaky_bucket

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

            leaky_bucket.wait_for_token()  # Wait until there's an available slot for orders

            print("Populated USTs:")
            print(config.USTs)

            print("Populated FUTURES:")
            print(config.FUTURES)

            HEDGES = cf_ctd_main()  # From cf_ctd.py. Takes the immutable global variables and deep copies them
            HEDGES.to_csv("HEDGES.csv")

            HEDGES_Combos = run_fixed_income_calculation(HEDGES)  # From ctd_fut_kpis.py
            print("Populated HEDGES_Combos:")
            print(HEDGES_Combos)  # results object

            config.updated_ORDERS = calculate_quantities_with_sma(HEDGES_Combos)  # From KPIs2_Orders.py
            print("Updated ORDERS:")
            print(config.updated_ORDERS)

            # TODO: Only trigger this when we have an pair that passes our risk management check
            #orderRequest()  # From orders.py

        time.sleep(.000001)  # Wait .000001 seconds before the next iteration