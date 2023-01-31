# MT5 Data Extraction

This script is used to extract data from the MetaTrader 5 (MT5) platform and insert it into a database using SQLAlchemy. 
The data can be extracted for any currency symbol and time frame (e.g. 1 minute, 5 minute, daily, etc.).

## Prerequisites

- You need to have a MetaTrader 5 account.
- You need to install the MetaTrader 5 and SQLAlchemy Python packages.

## Usage

The script can be executed from the command line with the following arguments:

> -s, --symbol: Currency symbol to retrieve data for. (required)
> -t, --timeframe: Timeframe value for data extraction, e.g. mt5.TIMEFRAME_D1. (required)
> -f, --fromdate: From Date (e.g. 01-01-2002) (optional, default: 01-01-2002)
> -o, --todate: To Date (e.g. 31-12-2020) (optional, default: 31-12-2020)

