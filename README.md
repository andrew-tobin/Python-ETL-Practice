# Python-ETL-Practice
Simple projects to develop understanding of ETL processes in Python, using combinations of the below ETL variants.

| Extracting from  | Transformations | Loading into |
| - | - | - |
| APIs  | (Using Pandas)  | Databases (SQLite)
| Databases (SQLite)  | Filtering, joining, aggregation  | Files/Cloud |
| Files/Cloud | Renaming, cleaning | |

# Projects

**SQLite ETL**: I generated and seeded a mock two-table financial dataset into SQLite, extracted it as a pandas dataframe, joined the tables and aggregated into a summary table, then loaded back into the same SQLite database.

**Simple API ETL**: I called the REST Countries to get national population data in a pandas dataframe. I did a simple transformation (aggregating and changing units) then saved the output as a CSV file. Also used three lambda functions to familiarise myself with Python lambda notation.
