# %%
import pandas as pd
import numpy as np

df = pd.read_csv('workshops.csv')
df
# %%
"""
Fix the data frame. At the end, row should have the following columns:
- [x] start: pd.Timestemap
- [x] end: pd.Timestamp
- [x] name: str
- [x] topic: str (python or go)
- [x] earnings: np.float64
"""
# %%
# fill in the year and month, using fillna
df['Year'].fillna(method='ffill', inplace=True)
df['Month'].fillna(method='ffill', inplace=True)
df['Start'].fillna(method='bfill', inplace=True)
df['End'].fillna(method='bfill', inplace=True)
# %%
# drop the non-data rows
df = df.loc[2:, :]

# %%
# month to int
m = {
    'June': 6,
    'July': 7,
}
df['Month'] = df['Month'].map(m)
# %%
# year to int
df['Year'] = df['Year'].astype(int)

# %%
# start and end to int
df['Start'] = df['Start'].astype(int)
df['End'] = df['End'].astype(int)

# %%
df['Start'] = pd.to_datetime((df['Year']*10000+df['Month']*100+df['Start']).apply(str), format='%Y%m%d')
# %%
df['End'] = pd.to_datetime((df['Year']*10000+df['Month']*100+df['End']).apply(str), format='%Y%m%d')
# %%
df['Name'].mode()[0]
# %%
df['Name'].fillna(df['Name'].mode()[0], inplace=True)
# %%
def topic(row):
  if "python" in row['Name'].lower():
    return "python"
  elif "go" in row['Name'].lower():
    return "go"
  else:
    return "not sure"
df['topic'] = df.apply(topic, axis=1)

# %%
def replace(s):
    try:
      s = s.replace("$", "")
      s = s.replace(",", "")
    except:
      pass
    return s


df['Earnings'] = df['Earnings'].apply(replace)

# %%
df['Earnings'] = df['Earnings'].astype(float)
earnings = df.groupby('Name')['Earnings'].transform(np.mean)
# %%
df['Earnings'].fillna(earnings, inplace=True)
# %%
df.dtypes
# %%
df
# %%
