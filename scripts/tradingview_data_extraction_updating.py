from tvDatafeed import TvDatafeed, Interval

from typing import Dict, Optional, Any
import pandas as pd
import numpy as np
import argparse
import logging
from sqlalchemy import Table, Column, Integer, Date, String, Float
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from datetime import datetime
from sqlalchemy import inspect
from sqlalchemy import insert
from sqlalchemy import text
# local files
from session import *
from datetime import datetime, date 
from credential import postgresql as settings
from credential import tradingview as tv_settings


# Configure logging for the program
logging.basicConfig(filename='tradingview_data_extraction.log', level=logging.DEBUG)
logging.info("Program started at {}".format(datetime.now()))

def Sessions():
    """
    Function to create the SQLAlchemy engine and session
    Returns:
        session object
    """
    engine = get_engine_from_settings()
    DynamicBase.metadata.create_all(bind=engine)
    session = get_session()
    return session

# datetime	symbol	open	high	low	close	volume
def create_table(table_name, Base):
    """
    Function to create table structure using SQLAlchemy ORM
    Args:
        table_name : name of the table to be created
        Base       : SQLAlchemy Base object
        engine     : SQLAlchemy engine object
    Returns:
        User       : SQLAlchemy ORM Class for the table
    """
    
    engine = get_engine_from_settings()
    #DynamicBase = declarative_base(class_registry=dict())
    class User(DynamicBase):
            __tablename__ = table_name
            id = Column(Integer, primary_key=True, autoincrement=True)
            datetime = Column(Date) #nullable=False
            open = Column(Float)
            high = Column(Float)
            low = Column(Float)
            close = Column(Float)
            volume = Column(Float)
        
    inspector = inspect(engine)
    #if engine.has_table(table_name):
    if inspector.has_table(table_name):
        # if table exists, overwrite it
        User.__table__.drop(engine)
        User.__table__.create(engine)
    else:
        # if table does not exist, create it
        Base.metadata.create_all(engine)
        
    return User


def get_historical_data(tv: Any, symbol_exchange_dict: Dict[str, str], interval: str, n_bars: int) -> Dict[str, Optional[pd.DataFrame]]:

    """
        Fetches historical market data for a set of symbols from a given exchange, over a specified interval 
        and number of bars. Adjusts the number of bars for specific symbols if needed.

        Parameters:
        - tv (Any): An object/interface to access historical market data. Assumed to have a method get_hist().
                    The type is specified as Any since the exact type depends on the implementation of the
                    historical data retrieval interface.
        - symbol_exchange_dict (Dict[str, str]): A dictionary mapping symbols to their respective exchanges. 
                                                Example: {'AAPL': 'NASDAQ', 'BTCUSD': 'BINANCE'}
        - interval (str): The time interval for the data points. For example, '1d' for daily data.
        - n_bars (int): The number of data points/bars to retrieve. This is adjusted for certain symbols.

        Returns:
        Dict[str, Optional[pd.DataFrame]]: A dictionary with symbols as keys and their corresponding historical data as 
                                            values. If no data is returned for a symbol, the value will be None. 

        The function handles a special case for the symbol 'USCCPI', where it overrides the default number
        of bars to 500, assuming this symbol requires a longer historical data span.

        Note: This function requires the 'logging' module for logging missing data information.
    """

    # Initialize an empty dictionary to store the result
    result = {}
    
    # Iterate over the symbol_exchange_dict to fetch data for each symbol
    for symbol, exchange in symbol_exchange_dict.items():
        # Adjust the n_bars for the 'USCCPI' symbol, as it requires more historical data
        effective_n_bars = n_bars if symbol != 'USCCPI' else 500
        
        # Fetch historical data using the provided interface and the adjusted number of bars
        data = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=effective_n_bars)
        
        # Check if data is returned
        if data is not None:
            # Reset index for cleaner DataFrame format
            data.reset_index(inplace=True)
            # Add the data to the result dictionary
            result[symbol] = data
        else:
            # Log the absence of data for a specific symbol and exchange
            logging.info(f"No data returned for {symbol} on {exchange}")
            # Handle missing data by setting the result to None
            result[symbol] = None
    
    # Return the populated result dictionary
    return result


def extract_load_data_to_postgres_db(Base,currency_symbol,historical_data):

    name = currency_symbol.lower()+'_'+'data'
    table_name = name
    # Create SQLAlchemy Base object and User class using the create_table function
    User = create_table(table_name, Base)
    # Create a SQLAlchemy session
    session = Sessions()

    # Log the start of data insertion into the database
    logging.info("Start inserting data into {}".format(table_name))
    # Insert the data into the database using the bulk_insert_mappings method
    session.bulk_insert_mappings(User,historical_data.to_dict(orient='records'))
    # Commit the transaction to save the changes to the database
    session.commit()
    # Log the completion of data insertion and the successful completion of the program
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")

def get_latest_date(session, table_name):
    # Directly inserting the table name into the query. Ensure table_name is safe!
    sql_query = f"SELECT max(datetime) FROM {table_name} LIMIT 5"
    # Using text() for any user-provided values in the rest of the query
    result = session.execute(text(sql_query)).fetchone()
    return result[0] if result else None

def table_exists(session, table_name):
    sql = text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table_name)")
    result = session.execute(sql, {'table_name': table_name}).scalar()
    return result

def load_data_to_postgres_db(Base,currency_symbol,historical_data, session):
    
    name = currency_symbol.lower()+'_'+'data'
    table_name = name
    # Create SQLAlchemy Base object and User class using the create_table function
    User = create_table(table_name, Base)
    # Log the start of data insertion into the database
    logging.info("Start inserting data into {}".format(table_name))
    # Insert the data into the database using the bulk_insert_mappings method
    session.bulk_insert_mappings(User,historical_data.to_dict(orient='records'))
    # Commit the transaction to save the changes to the database
    session.commit()
    # Log the completion of data insertion and the successful completion of the program
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")

def process_historical_data(tv, symbol_exchange_dict, settings):
    """
    Process historical data for multiple symbols and store it in a PostgreSQL database.

    Args:
        tv: TradingView object or module for fetching data.
        symbol_exchange_dict (Dict[str, str]): A dictionary mapping symbols to exchanges.
        settings (dict): A dictionary containing PostgreSQL database connection settings.

    Returns:
        None
    """
    session = Sessions()
    # initializing the dictionary
    historical_data = {}
    new_historical_data = {}
    # Iterate over the dictionary items
    for symbol in symbol_exchange_dict.keys():
        symbol_name = symbol
        # creating table name
        table_name = symbol_name.lower() + '_data'
        # checking if table exists or not!
        if not table_exists(session, table_name):
            historical_data = get_historical_data(tv, symbol_exchange_dict, interval=Interval.in_daily, n_bars=10000)
        else:
            # if table exists, then we extract the last updated data and extract the date!
            latest_date = get_latest_date(session, table_name)
            # converting the date to the correct format!
            latest_date = pd.to_datetime(latest_date)
            # getting the last 100 day data!
            new_historical_data = get_historical_data(tv, symbol_exchange_dict, interval=Interval.in_daily, n_bars=100)

    if len(historical_data) > 0:
        for symbol, data in historical_data.items():
            symbol_name, symbol_data = symbol, data
            # loading the data to the postgres database!
            load_data_to_postgres_db(DynamicBase ,symbol_name,symbol_data, session)
            logging.info(f"Loaded historical data for {symbol_name} into the database.")
    else:
        for symbol, data in new_historical_data.items():
            symbol_name, symbol_data = symbol, data
            table_name = symbol_name.lower() + '_data'
            data = symbol_data.loc[symbol_data['datetime'].dt.date > latest_date.date()]
            if 'index' in data.columns:
                data = data.drop(columns=['index'])
            data.loc[:, 'datetime'] = pd.to_datetime(data['datetime'].dt.date)
            data = data.drop(columns=['symbol'])
            # updating the table with new data!
            data.to_sql(table_name, con= get_engine(settings['pguser'], 
                            settings['pgpass'], 
                            settings['host'], 
                            settings['port'], 
                            settings['pgdb']), if_exists='append', index=False)
            logging.info(f"Appended new historical data for {symbol_name} into the database.")

if __name__ == '__main__':
    DynamicBase = declarative_base()

    username = tv_settings['username']
    password = tv_settings['password']
    tv = TvDatafeed(username, password)


    # Define symbol and exchange dictionary
    symbol_exchange_dict = {
        'XAUUSD': 'OANDA',
        'DXY': 'TVC',
        'USOIL': 'TVC',
        'USINTR': 'ECONOMICS',
        'USCCPI': 'ECONOMICS',
        'SPX500USD': 'OANDA'
    }

    process_historical_data(tv, symbol_exchange_dict, settings)