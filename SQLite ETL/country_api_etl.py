# Second ETL project: extract from api, filter/clean, load to file

import requests
import pandas as pd

### EXTRACT
# Simple API, no auth, no pagination, no rate limits
data = requests.get('https://restcountries.com/v3.1/all?fields=name,capital,region,population').json()
df = pd.DataFrame(data)

# This contains country name, capital, region, population

### TRANSFORM
# The name column is a dict - extract the common name
df['name'] = df['name'].apply(lambda x: x['common'])
# The capital column is a list (of one usually) - extract the first
df['capital'] = df['capital'].apply(lambda x: x[0] if len(x) > 0 else None)
# Print columns
print(df.columns)

# Get the

# print top row 
print(df.head(1)['name'])
print(df.head(1)['capital'])
print(df.head(1)['region'])
print(df.head(1)['population'])
