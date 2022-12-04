# %%
import pandas as pd

df = pd.read_csv('rides.csv')
df
# %%
# Find out all the rows that have bad values
# - Missing values are not allowed
# df.iloc[2]["plate"]
# df['plate'] = df['plate'].str.strip()
# df.iloc[2]['plate']

# import numpy as np
# df.loc[df['plate'] == '', 'plate'] = np.nan
# df
null_mask = df.isnull().any(axis=1)
df[null_mask]
# %%
# - A plate must be a combination of at least 3 upper case letters or digits
plate_mask = df['plate'].str.match(r'^[0-9A-Za-z]{3,}', na=False)
df[~plate_mask]
# %%
# - Distance much be bigger than 0
dist_mask = df['distance'] <= 0
df[dist_mask]
# %%
df[null_mask | ~plate_mask | dist_mask]
# %%
