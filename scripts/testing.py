
import pandas as pd
import numpy as np
import logging
import MetaTrader5 as mt5
from sqlalchemy import Column, Integer,Date, Float
from sqlalchemy.ext.declarative import declarative_base
from session import *
from get_price import *

logging.basicConfig(filename='mt5_data_extraction.log', level=logging.DEBUG)
logging.info("Program started at {}".format(datetime.now()))

def Sessions():
    engine = get_engine_from_settings()
    Base.metadata.create_all(bind=engine)
    session = get_session()
    return session

def create_table(table_name, Base):
    
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

    table_name = 'goldtest2'
    currency_symbol = "XAUUSD"
    timeframe_val= mt5.TIMEFRAME_D1
    fromdate = '01-01-2002'
    todate = '31-12-2020'

    Base = declarative_base()
    User = create_table(table_name, Base)
    session = Sessions()

    df = get_mt5_data(currency_symbol,timeframe_val, fromdate, todate)

    logging.info("Start inserting data into {}".format(table_name))
    session.bulk_insert_mappings(User,df.to_dict(orient='records'))

    session.commit()
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")











# session = Sessions()

# df = get_mt5_data(currency_symbol,timeframe_val, fromdate, todate)
# #df = df.reset_index()
# logging.info("Start inserting data into {}".format(table_name))

# # if you want to add data row by row the use the below code instead of bulk_insert_mapping
# # for i, r in df.iterrows():
# #     price = User(date = r['date'],
# #                 open = r['open'],
# #                 high = r['high'],
# #                 close = r['close'],
# #                 tick_volume = r['tick_volume'],
# #                 spread = r['spread'],
# #                 real_volume = r['real_volume'] )
# #     session.add(price)
# session.commit()
# logging.info("Data insertion completed at {}".format(datetime.now()))
# logging.info("Program completed successfully.")