import enum
import pandas as pd
import numpy as np

from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from session import *

# Import all the necessary libraries!
import os
import logging
import pytz
from datetime import datetime
import MetaTrader5 as mt5
logging.basicConfig(filename='mt5_data_extraction.log', level=logging.DEBUG)
from get_price import *

currency_symbol = "XAUUSD"
timeframe_val= mt5.TIMEFRAME_D1
fromdate = '01-01-2002'
todate = '31-12-2020'


Base = declarative_base()

table_name = 'goldtest2'
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


engine = get_engine_from_settings()
Base.metadata.create_all(bind=engine)
session = get_session()


df = get_mt5_data(currency_symbol,timeframe_val, fromdate, todate)
#df = df.reset_index()
session.bulk_insert_mappings(User,df.to_dict(orient='records'))
# if you want to add data row by row the use the below code instead of bulk_insert_mapping
# for i, r in df.iterrows():
#     price = User(date = r['date'],
#                 open = r['open'],
#                 high = r['high'],
#                 close = r['close'],
#                 tick_volume = r['tick_volume'],
#                 spread = r['spread'],
#                 real_volume = r['real_volume'] )
#     session.add(price)
session.commit()


