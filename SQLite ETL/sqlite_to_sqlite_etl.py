# This ETL project extracts mock data from SQLite, transforms it, then loads back to SQLite

### LIBRARIES
# Remember to run pip install -r library_requirements.txt
import sqlite3
import pandas as pd
import random as rn

### SEEDING
# Create some mock data to extract
# Two tables: a customer table and an orders table

def generate_data(customer_rows=1000,order_rows=5000):
    customers = pd.DataFrame({
        'id': [n for n in range(customer_rows)],
        'country': [rn.choice(['UK','UK','UK','France','France','Germany','Spain']) for n in range(customer_rows)] # Repetition to imbalance distribution
    })

    orders = pd.DataFrame({
        'id':[n for n in range(order_rows)],
        'customer_id':[rn.choice(customers['id']) for n in range(order_rows)],
        'value':[rn.randint(1,300) for n in range(order_rows)],
        'category':[rn.choice(['Products','Products','Products','Services','Other']) for n in range(order_rows)]
    })
    return customers, orders

def seed_data(conn):
    print("Seeding data...")
    customers, orders = generate_data()
    customers.to_sql('customers', conn, if_exists='replace', index=False)
    orders.to_sql('orders', conn, if_exists='replace', index=False)
    

def extract(conn):
    print("Extracting data...")
    customers = pd.read_sql_query("SELECT * FROM customers", conn)
    orders = pd.read_sql_query("SELECT * FROM orders", conn)
    return customers, orders

def transform(customers,orders):
    print("Transforming data...")
    joined = customers.merge(orders, left_on='id', right_on='customer_id', how='inner')
    # aggregate total/avg order value by country and category
    aggregated = joined.groupby(['country','category']).agg(
        total_value=('value','sum'),
        avg_value=('value','mean')
    ).reset_index()
    return aggregated

def load(conn,df):
    print("Loading data...")
    # load back into SQLite as a new table
    df.to_sql('summary_table', conn, if_exists='replace', index=False)   

def pipeline():
    # Putting the connection here so it doesn't need to be remade in each function
    conn = sqlite3.connect('customer_orders.db') # Create a SQLite database connection (stores db as a file in repo)
    seed_data(conn) # Seed the database with mock data
    customer,orders = extract(conn) # Extract the data into pandas dataframes
    summary_table = transform(customer,orders) # Transform the data into a summary table
    load(conn,summary_table) # Load the summary table back into SQLite
    
    # Check the results
    df = pd.read_sql_query("SELECT * FROM summary_table", conn)
    print(df)
    
    conn.close()

    

    
if __name__ == "__main__": # Using this so I can import seeding into other scripts without running it
    pipeline()
