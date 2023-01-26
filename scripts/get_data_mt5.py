""" Extracting data directly from MetaTrader5"""

# Import all the necessary libraries!
import pandas as pd
import psycopg2
import pytz
from datetime import datetime
import MetaTrader5 as mt5 
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
pd.set_option('display.max_columns', 500) # number of columns to be displayed
pd.set_option('display.width', 1500)      # max table width to display


# # Disable autocommit
# conn.set_session(autocommit=True)
engine = create_engine("postgresql://postgres:postgres@localhost:5432")
conn = engine.connect()
conn.execute("commit")
conn.execute("create database xauusd_daily_4")

# Create a DataFrame
df = pd.read_csv('../data/XAUUSD_mt5.csv')
#print(df.head())
postgres_engine = create_engine('postgresql://postgres:postgres@localhost:5432/xauusd_daily_4')

postgres_engine.execute('''
CREATE TABLE IF NOT EXISTS daily_price (
    date date,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    tick_volume numeric,
    spread numeric,
    real_volume numeric
);
''')
# Save the DataFrame to a table in the PostgreSQL database
df.to_sql('daily_price', postgres_engine, if_exists='append', index=False)


conn.close()
# import psycopg2

# # Connect to the default database
# conn = psycopg2.connect(
#     host="localhost",
#     user="postgres",
#     password="postgres"
# )
# # Disable autocommit
# conn.set_session(autocommit=True)
# # Create a cursor object
# cur = conn.cursor()

# # Create the database
# cur.execute("CREATE DATABASE XAUUSD_Price_GOld")

# # Close the cursor and the connection
# cur.close()
# conn.close()










