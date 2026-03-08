# Second ETL project: extract from api, filter/clean, load to file

import requests
import pandas as pd

### EXTRACT
# Simple API, no auth, no pagination, no rate limits
data = requests.get('https://restcountries.com/v3.1/all?fields=name,capital,region,population').json()
df = pd.DataFrame(data)

# This contains country name, capital, region, population

### CLEAN
# The name column is a dict - extract the common name
df['name'] = df['name'].apply(lambda x: x['common'])
# The capital column is a list (of one usually) - extract the first
df['capital'] = df['capital'].apply(lambda x: x[0] if len(x) > 0 else None)


### TRANSFORM
# Output total population by region in millions
df = df.groupby('region').agg(
    #population = ('population','sum')
    #second element in agg definitions has to be function, but can use lambdas
    population = ('population',lambda x: round(sum(x)/1000000,2))
)

### LOAD
# Save to file
df.to_csv('population_by_region.csv')


# print top row 
print(df.head)
