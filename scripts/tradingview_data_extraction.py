import sys
sys.path.append('../scripts/')

from tvDatafeed import TvDatafeed, Interval
from credential import tradingview as settings

def get_historical_data(tv, symbol_exchange_dict, interval, n_bars):
    result = {}
    for symbol, exchange in symbol_exchange_dict.items():
        data = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
        data.reset_index(inplace=True)
        result[symbol] = data
    return result

def main():
    username = settings['username']
    password = settings['password']

    tv = TvDatafeed(username, password)

    # Define symbol and exchange dictionary
    symbol_exchange_dict = {
        'XAUUSD': 'OANDA',
        'DXY': 'TVC',
        'USOIL': 'TVC',
        'USINTR': 'ECONOMICS',
        'SPX500USD': 'OANDA'
    }

    # Get historical data for symbols and exchanges in the dictionary
    historical_data = get_historical_data(tv, symbol_exchange_dict, interval=Interval.in_daily, n_bars=10000)

    # Access individual dataframes
    gold_data = historical_data['XAUUSD']
    dollarIndex_data = historical_data['DXY']
    oil_data = historical_data['USOIL']
    interestrate_data = historical_data['USINTR']
    SP500 = historical_data['SPX500USD']

if __name__ == '__main__':
    main()
