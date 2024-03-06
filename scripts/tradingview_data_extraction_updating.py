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

from sqlalchemy.orm import Session

# local files
from session import *
from datetime import datetime, date 
from credential import postgresql as settings
from credential import tradingview as tv_settings

from sqlalchemy.orm import Session
from sqlalchemy import text


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

def extract_load_data_to_postgres_db(Base: declarative_base, currency_symbol: str, historical_data: pd.DataFrame) -> None:
    """
    Extracts historical market data and loads it into a PostgreSQL database table. The table name is derived from
    the currency symbol, and the data is inserted using SQLAlchemy's bulk insert functionality.

    Parameters:
    - Base (declarative_base): SQLAlchemy declarative base object used for defining the table schema.
    - currency_symbol (str): The currency symbol that identifies the data set and is used to name the database table.
    - historical_data (pd.DataFrame): A DataFrame containing the historical market data to be inserted into the database.

    Returns:
    None: This function does not return a value. It logs the process of data insertion.

    The function dynamically creates a new table based on the currency symbol, inserts the data from the DataFrame
    into this table, commits the transaction, and logs the start and completion of this process.
    """
    # Derive the table name from the currency symbol
    name = currency_symbol.lower() + '_data'
    table_name = name

    # Dynamically create a table class using the provided Base and table name
    User = create_table(table_name, Base)

    # Create a new SQLAlchemy session for database operations
    session = Sessions()

    # Log the start of data insertion
    logging.info(f"Start inserting data into {table_name}")

    # Convert DataFrame to a list of dictionaries for bulk insert
    data_to_insert = historical_data.to_dict(orient='records')
    
    # Bulk insert the data into the created table
    session.bulk_insert_mappings(User, data_to_insert)

    # Commit the changes to the database
    session.commit()

    # Log the completion of data insertion
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Data insertion completed at {current_time}")
    logging.info("Program completed successfully.")

def get_latest_date(session: Session, table_name: str) -> Any:
    """
    Retrieves the latest date (maximum datetime value) from a specified table in the database.

    This function constructs a SQL query dynamically using the table name provided. It's crucial to ensure
    that the table_name argument is safe to include directly in the query to prevent SQL injection.

    Parameters:
    - session (Session): An instance of SQLAlchemy Session for database connection and operations.
    - table_name (str): The name of the table from which to retrieve the latest date.

    Returns:
    - Any: The latest date (maximum datetime) found in the specified table. The return type is 'Any'
        because the datatype of the datetime column is not explicitly known here. Returns None
        if the query result is empty.

    Note:
    The function uses the SQLAlchemy `text` function for executing a raw SQL query, ensuring that
    the table_name parameter is interpolated safely is the responsibility of the caller.
    """
    # Construct SQL query string, incorporating the table name directly
    # It's important to ensure that table_name is safe to prevent SQL injection
    sql_query = f"SELECT max(datetime) FROM {table_name} LIMIT 1"

    # Execute the SQL query and fetch the first (and only) result
    result = session.execute(text(sql_query)).fetchone()

    # Return the first element of the result if it exists, else return None
    return result[0] if result else None

def table_exists(session: Session, table_name: str) -> bool:
    """
    Checks if a table exists in the database.

    This function queries the information_schema.tables to determine if the specified table exists within
    the database. It uses a parameterized SQL query to safely include user-provided table names and prevent
    SQL injection.

    Parameters:
    - session (Session): An instance of SQLAlchemy Session for database connection and operations.
    - table_name (str): The name of the table to check for existence.

    Returns:
    - bool: True if the table exists, False otherwise.

    The function prepares a parameterized query that checks for the existence of the specified table
    by name within the database's information schema. This approach is database-agnostic but assumes
    access to the information_schema.tables view or table.
    """
    # Prepare a parameterized SQL query to check table existence in the information schema
    sql = text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table_name)")

    # Execute the query with the provided table_name as a parameter
    result = session.execute(sql, {'table_name': table_name}).scalar()

    # The result is a boolean value indicating the existence of the table
    return result


# def load_data_to_postgres_db(Base,currency_symbol,historical_data, session):
    
#     name = currency_symbol.lower()+'_'+'data'
#     table_name = name
#     # Create SQLAlchemy Base object and User class using the create_table function
#     User = create_table(table_name, Base)
#     # Log the start of data insertion into the database
#     logging.info("Start inserting data into {}".format(table_name))
#     # Insert the data into the database using the bulk_insert_mappings method
#     session.bulk_insert_mappings(User,historical_data.to_dict(orient='records'))
#     # Commit the transaction to save the changes to the database
#     session.commit()
#     # Log the completion of data insertion and the successful completion of the program
#     logging.info("Data insertion completed at {}".format(datetime.now()))
#     logging.info("Program completed successfully.")

def load_data_to_postgres_db(Base: declarative_base, currency_symbol: str, historical_data: pd.DataFrame, session: Session) -> None:
    """
    Loads historical market data into a PostgreSQL database for a specific currency symbol. 
    It dynamically creates a new database table based on the currency symbol, then inserts the data.

    Parameters:
    - Base (declarative_base): The SQLAlchemy declarative base, used to generate ORM models.
    - currency_symbol (str): The currency symbol, which influences the table name (`<symbol>_data`).
    - historical_data (pd.DataFrame): The historical data to load into the database. 
    The data should be in a DataFrame format.
    - session (Session): An instance of SQLAlchemy Session for executing database transactions.

    Returns:
    None: The function does not return a value. It logs the progress of data insertion.

    This function first constructs a table name from the currency symbol, then uses a custom function
    `create_table` to dynamically create a new table model. It logs the start of the data insertion process,
    uses the `bulk_insert_mappings` method to insert data efficiently, commits the transaction, and logs
    the successful completion of data insertion.
    """
    # Construct table name from currency symbol
    name = currency_symbol.lower() + '_data'
    table_name = name

    # Dynamically create a table model using the provided Base and table name
    User = create_table(table_name, Base)

    # Log the initiation of data insertion
    logging.info(f"Start inserting data into {table_name}")

    # Convert DataFrame into a list of dictionaries for bulk insertion
    data_to_insert = historical_data.to_dict(orient='records')

    # Perform bulk insertion of data into the database
    session.bulk_insert_mappings(User, data_to_insert)

    # Commit the transaction to finalize data insertion
    session.commit()

    # Log the completion of data insertion and the program's success
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Data insertion completed at {current_time}")
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
            if symbol_data is not None:
                try:
                    data = symbol_data.loc[symbol_data['datetime'].dt.date > latest_date.date()]
                except AttributeError as e:
                    logging.error(f"Unexpected error occurred while filtering symbol_data: {e}")
            else:
                logging.warning("symbol_data is None, cannot proceed with filtering or further processing.")

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