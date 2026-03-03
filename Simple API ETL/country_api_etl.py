# Second ETL project: extract from api, filter/clean, load to file

import requests
import pandas as pd

### EXTRACT
# Simple API, no auth, no pagination, no rate limits
data = requests.get('https://restcountries.com/v3.1/all?fields=name,capital,region,population').json()
df = pd.DataFrame(data)

print(df.head())

### LOAD (parquet?)
