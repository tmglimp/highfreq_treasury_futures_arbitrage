"""
CTD and FUT KPIs
"""

import itertools
import pandas as pd
from config import HEDGES
from fixed_income_calc import (BPrice, MDur, MacDur, DV01,
                               approximate_duration,
                               approximate_convexity)

# Generate the CTD DataFrame (HEDGES) using the function

def display_hedges_info():
    print("Displaying first 5 rows of HEDGES dataframe:")
    print(HEDGES.head())

def run_fixed_income_calculation(HEDGES):
    # Ensure the HEDGES dataframe has uppercase column names.
    HEDGES.columns = HEDGES.columns.str.upper()

    # Define constant parameters
    period = 2
    day_count = 1

    # Compute CTD KPIs using the uppercase column names.
    HEDGES['CTD_BPRICE'] = HEDGES.apply(
        lambda row: BPrice(cpn=row['CTD_COUPON_RATE'],
                           term=row['CTD_YTM'],
                           yield_=row['CTD_YIELD'],
                           period=period,
                           begin=row['CTD_PREV_CPN'],
                           next_coupon=row['CTD_NCPDT'],
                           day_count=day_count), axis=1)

    HEDGES['CTD_MDUR'] = HEDGES.apply(
        lambda row: MDur(cpn=row['CTD_COUPON_RATE'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_CPN'],
                         next_coupon=row['CTD_NCPDT'],
                         day_count=day_count), axis=1)

    HEDGES['CTD_MACDUR'] = HEDGES.apply(
        lambda row: MacDur(cpn=row['CTD_COUPON_RATE'],
                           term=row['CTD_YTM'],
                           yield_=row['CTD_YIELD'],
                           period=period,
                           begin=row['CTD_PREV_CPN'],
                           next_coupon=row['CTD_NCPDT'],
                           day_count=day_count), axis=1)

    HEDGES['CTD_DV01'] = HEDGES.apply(
        lambda row: DV01(cpn=row['CTD_COUPON_RATE'],
                         term=row['CTD_YTM'],
                         yield_=row['CTD_YIELD'],
                         period=period,
                         begin=row['CTD_PREV_CPN'],
                         next_coupon=row['CTD_NCPDT'],
                         day_count=day_count), axis=1)

    HEDGES['CTD_APRXDUR'] = HEDGES.apply(
        lambda row: approximate_duration(cpn=row['CTD_COUPON_RATE'],
                                         term=row['CTD_YTM'],
                                         yield_=row['CTD_YIELD'],
                                         period=period,
                                         begin=row['CTD_PREV_CPN'],
                                         next_coupon=row['CTD_NCPDT'],
                                         day_count=day_count), axis=1)

    HEDGES['CTD_APRXCVX'] = HEDGES.apply(
        lambda row: approximate_convexity(cpn=row['CTD_COUPON_RATE'],
                                          term=row['CTD_YTM'],
                                          yield_=row['CTD_YIELD'],
                                          period=period,
                                          begin=row['CTD_PREV_CPN'],
                                          next_coupon=row['CTD_NCPDT'],
                                          day_count=day_count), axis=1)

    # Now compute the FUT KPIs by dividing the CTD metrics by the conversion factor.
    HEDGES['FUT_TPRICE'] = HEDGES['CTD_BPRICE'] / HEDGES['CTD_CF']
    HEDGES['FUT_MDUR'] = HEDGES['CTD_MDUR'] / HEDGES['CTD_CF']
    HEDGES['FUT_MACDUR'] = HEDGES['CTD_MACDUR'] / HEDGES['CTD_CF']
    HEDGES['FUT_DV01'] = HEDGES['CTD_DV01'] / HEDGES['CTD_CF']
    HEDGES['FUT_APRXDUR'] = HEDGES['CTD_APRXDUR'] / HEDGES['CTD_CF']
    HEDGES['FUT_APRXCVX'] = HEDGES['CTD_APRXCVX'] / HEDGES['CTD_CF']
    HEDGES.to_csv('HEDGES.csv')

    # Generate all combinations of distinct HEDGES rows (based on CTD_CONID).
    combinations = [(row1, row2) for row1, row2 in itertools.product(HEDGES.iterrows(), repeat=2)
                    if row1[1]['CTD_CONID'] != row2[1]['CTD_CONID']]

    combos_data = []
    for combo in combinations:
        row1, row2 = combo
        # Prefix the headers of row1 with 'A_' and row2 with 'B_'
        row1_data = {f'A_{key}': value for key, value in row1[1].to_dict().items()}
        row2_data = {f'B_{key}': value for key, value in row2[1].to_dict().items()}
        combined_row = {**row1_data, **row2_data}
        combos_data.append(combined_row)

    HEDGES_Combos = pd.DataFrame(combos_data)
    HEDGES_Combos.to_csv('HEDGES_Combos.csv')

    return HEDGES_Combos

if __name__ == "__main__":
    display_hedges_info()
    combos = run_fixed_income_calculation(HEDGES)
    print("Fixed income calculation completed. CTD-FUT combinations shape:", combos.shape)
