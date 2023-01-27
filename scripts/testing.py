import enum
import pandas as pd
import numpy as np

from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from session import *

Base = declarative_base()

class User(Base):
    __tablename__ = 'newprice2'
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

df = pd.read_csv('../data/XAUUSD_mt5.csv')

for i, r in df.iterrows():
    price = User(id = r['id'],
                date = r['date'],
                open = r['open'],
                high = r['high'],
                close = r['close'],
                tick_volume = r['tick_volume'],
                spread = r['spread'],
                real_volume = r['real_volume'] )
    session.add(price)
session.commit()



