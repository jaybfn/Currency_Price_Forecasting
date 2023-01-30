import pandas as pd
import numpy as np
import logging
import MetaTrader5 as mt5
from sqlalchemy import Column, Integer,Date, Float
from sqlalchemy.ext.declarative import declarative_base
from session import *
from get_price import *

# Configure logging for the program
logging.basicConfig(filename='mt5_data_extraction.log', level=logging.DEBUG)
logging.info("Program started at {}".format(datetime.now()))

def Sessions():
    """
    Function to create the SQLAlchemy engine and session
    Returns:
        session object
    """
    engine = get_engine_from_settings()
    Base.metadata.create_all(bind=engine)
    session = get_session()
    return session

def create_table(table_name, Base):
    """
    Function to create table structure using SQLAlchemy ORM
    Args:
        table_name : name of the table to be created
        Base       : SQLAlchemy Base object
    Returns:
        User       : SQLAlchemy ORM Class for the table
    """
    class User(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        date = Column(Date, nullable=False)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        tick_volume = Column(Float)
        spread = Column(Float)
        real_volume = Column(Float)
    
    return User

if __name__ == '__main__':
    """
    Main program that retrieves data from the MT5 platform and inserts it into the database.
    """
    table_name = 'goldtest2'
    currency_symbol = "XAUUSD"
    timeframe_val= mt5.TIMEFRAME_D1
    fromdate = '01-01-2002'
    todate = '31-12-2020'

    # Create SQLAlchemy Base object and User class using the create_table function
    Base = declarative_base()
    User = create_table(table_name, Base)

    # Create a SQLAlchemy session
    session = Sessions()

    # Get data from the MT5 platform using the get_mt5_data function
    df = get_mt5_data(currency_symbol,timeframe_val, fromdate, todate)

    # Log the start of data insertion into the database
    logging.info("Start inserting data into {}".format(table_name))

    # Insert the data into the database using the bulk_insert_mappings method
    session.bulk_insert_mappings(User,df.to_dict(orient='records'))

    # Commit the transaction to save the changes to the database
    session.commit()

    # Log the completion of data insertion and the successful completion of the program
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")
