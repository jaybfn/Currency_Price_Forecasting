""" Extracting data directly from MetaTrader5"""

# # Import all the necessary libraries!

# import pandas as pd
# import pytz
# from datetime import datetime
# import MetaTrader5 as mt5 # for windows
# #from mt5linux import MetaTrader5 as mt5# for linux
# pd.set_option('display.max_columns', 500) # number of columns to be displayed
# pd.set_option('display.width', 1500)      # max table width to display


# def get_mt5_data(currency_symbol = "XAUUSD", timeframe_val= mt5.TIMEFRAME_M1):

#     """ This function extracts stock or currency data from mt5 terminal and saves it to a csv file:
#     the function needs 2 inputs:
#     1. currency_symbol: eg: "XAUUSD" "USDEUR"
#     2. timeframe_val: resolution of the data, that could be daily price, 4H(4 hour) price, 1H etc 
#                         eg:'mt5.TIMEFRAME_D1' for daily price
#                             mt5.TIMEFRAME_H4 for hour 4 price 
                            
#     """

#     # mt5 initialization
#     if not mt5.initialize():
#         print("initialize() failed, error code =",mt5.last_error())
#         quit()
    
#     # set time zone to UTC
#     timezone = pytz.timezone("Etc/UTC")
#     # create 'datetime' object in UTC time zone to avoid the implementation of a local time zone offset
#     utc_from = datetime(2002, 1, 1, tzinfo=timezone)  
#     utc_to = datetime(2020, 12, 1, hour = 13, tzinfo=timezone)
#     # getting currency/stock values from mt5 terminal
#     rates = mt5.copy_rates_range(currency_symbol, timeframe_val, utc_from, utc_to)
    

#     # once extracted, shutdown mt5 session
#     mt5.shutdown()

#     # dumping data from mt5 to pandas dataframe
#     rates_frame = pd.DataFrame(rates)

#     # convert time in seconds into the datetime format
#     rates_frame['time']=pd.to_datetime(rates_frame['time'], unit='s')
#     rates_frame.rename(columns = {'time':'date'}, inplace = True)
#     rates_frame.to_csv(f'../data/{currency_symbol}_mt5.csv')
#     # display data
#     print("\nDisplay dataframe with data")
#     print(rates_frame.head(10))    
#     print("\n")
#     print(rates_frame.tail(10))
   

# if __name__=='__main__':

#     currency_symbol = "EURUSD"
#     timeframe_val= mt5.TIMEFRAME_M1

#     get_mt5_data(currency_symbol,timeframe_val)


# import the package
from mt5linux import MetaTrader5
# connecto to the server
mt5 = MetaTrader5(
    host = '34.141.21.45',
    port = 18812      
) 
# use as you learned from: https://www.mql5.com/en/docs/integration/python_metatrader5/
mt5.initialize()
mt5.terminal_info()
mt5.copy_rates_from_pos('EURUSD',mt5.TIMEFRAME_M1,0,1000)
# ...
# don't forget to shutdown
mt5.shutdown()