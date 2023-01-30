""" Extracting data directly from MetaTrader5"""

# Import all the necessary libraries!
import os
import logging
import pandas as pd
import pytz
from datetime import datetime
import MetaTrader5 as mt5
logging.basicConfig(filename='mt5_data_extraction.log', level=logging.DEBUG)
# Set pandas options for display
pd.set_option('display.max_columns', 500) # number of columns to be displayed
pd.set_option('display.width', 1500)      # max table width to display

def create_dir(path):
    """
    Creates a directory with the specified path
    """
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path, exist_ok = False)
        logging.info("New directory is created")

def initialize_mt5():
    """
    Initialize connection to the MetaTrader 5 terminal
    """
    if not mt5.initialize():
        logging.error("initialize() failed, error code = %d", mt5.last_error())
        quit()

def save_to_dataframe(rates, currency_symbol):
    """
    Converts rates data from MetaTrader 5 to a pandas DataFrame
    and saves it to a csv file
    """
    # dumping data from mt5 to pandas dataframe
    rates_frame = pd.DataFrame(rates)
    # convert time in seconds into the datetime format
    rates_frame['time']=pd.to_datetime(rates_frame['time'], unit='s')
    # rename colum 
    rates_frame.rename(columns = {'time':'date'}, inplace = True)
    #set the path to create a folder for saving the data into csv format
    path = f'../data'
    #create a directory with the specified path
    create_dir(path)
    # save to csv file format
    rates_frame.to_csv(f'{path}/{currency_symbol}_mt5.csv')
    logging.info(f"Data Saved to the directory:: {path}")
    return rates_frame

def strip_date(date_string):
    """
    Converts a string date to a tuple of integers (day, month, year)
    input: '01-09-1990'
    """
    str_to_date = datetime.strptime(date_string, "%d-%m-%Y")
    return str_to_date.day, str_to_date.month, str_to_date.year

def set_daterange(from_date, to_date):
    """
    Converts string dates to datetime objects in UTC time zone
    """
    timezone = pytz.timezone("Etc/UTC")
    # extrating day, month and year from date string
    start_day, start_month, start_year = strip_date(from_date)
    end_day, end_month, end_year = strip_date(to_date)
    # setting the time to UTC timezone.
    utc_from = datetime(start_year,start_month,start_day, tzinfo=timezone)  
    utc_to = datetime(end_year,end_month,end_day, hour = 13, tzinfo=timezone)

    return utc_from, utc_to

def get_mt5_data(currency_symbol = "XAUUSD", timeframe_val= mt5.TIMEFRAME_M1, fromdate = '01-01-2002', todate = '31-12-2020'):

    """ This function extracts stock or currency data from mt5 terminal and saves it to a csv file:
    the function needs 2 inputs:
    1. currency_symbol: eg: "XAUUSD" "USDEUR"
    2. timeframe_val: resolution of the data, that could be daily price, 4H(4 hour) price, 1H etc 
                        eg:'mt5.TIMEFRAME_D1' for daily price
                            mt5.TIMEFRAME_H4 for hour 4 price 
                            
    """
    # mt5 initialization
    initialize_mt5()
    # # set time zone to UTC
    utc_from, utc_to = set_daterange(fromdate, todate)
    # getting currency/stock values from mt5 terminal
    rates = mt5.copy_rates_range(currency_symbol, timeframe_val, utc_from, utc_to)
    # once extracted, shutdown mt5 session
    mt5.shutdown()
    # save data to dataframe
    rates_frame = save_to_dataframe(rates, currency_symbol)
    logging.info(f"Display top 10 rows of data for {currency_symbol} with a date range from {fromdate} to {todate}")
    # display data
    print(rates_frame.head(10)) 

    return rates_frame   

if __name__=='__main__':

    currency_symbol = "XAUUSD"
    timeframe_val= mt5.TIMEFRAME_D1
    fromdate = '01-01-2002'
    todate = '31-12-2020'
    data = get_mt5_data(currency_symbol,timeframe_val, fromdate, todate)