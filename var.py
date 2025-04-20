import pandas as pd
import numpy as np

# Load the file
df = pd.read_csv("FUTURES_historical.csv")

# Filter and clean
df = df[['conid', 'sequence', 'c', 'v']].dropna()
df['sequence'] = pd.to_numeric(df['sequence'], errors='coerce')
df['c'] = pd.to_numeric(df['c'], errors='coerce')
df['v'] = pd.to_numeric(df['v'], errors='coerce')
df = df.dropna()

# Filter for sequence between 1 and 29
df_filtered = df[(df['sequence'] >= 1) & (df['sequence'] <= 29)]

# Compute upper/lower bounds
results = pd.DataFrame()
for conid, group in df_filtered.groupby('conid'):
    c_vals = group['c'].values
    v_vals = group['v'].values

    if len(c_vals) >= 2 and len(v_vals) >= 2:
        c_std = np.std(c_vals)
        c_mean = np.mean(c_vals)
        v_std = np.std(v_vals)
        v_mean = np.mean(v_vals)

        results.concat({
            'conid': conid,
            'price_var_mu': c_mean,
            'price_var_upper': c_mean + 2.326 * c_std,
            'price_var_lower': c_mean - 2.326 * c_std,
            'imp_vol_mu': v_mean,
            'imp_vol_upper': v_mean + 2.326 * v_std,
            'imp_vol_lower': v_mean - 2.326 * v_std
        })
        results.to_csv('results.csv')

# Save or display
bounds_df = pd.DataFrame(results)
print(bounds_df.head())
config.FUTURES_VARIANCE
