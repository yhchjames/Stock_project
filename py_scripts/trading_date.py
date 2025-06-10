import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def generate_trading_date():
    #generate trading day by extract info from 2330
    today = datetime.today()
    # yesterday = today - timedelta(days=1)
    # yesterday_str = yesterday.strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')

    # TW2330 = yf.download('2330.TW', start='2023-01-01',end='2024-12-20')
    TW2330 = yf.download('2330.TW', start='2023-01-01', end=today_str) #end is not included
    # TW2330 = yf.download('2330.TW')

    # TW2330['Ticker'] = '2330.TW'
    TW2330 = TW2330.reset_index()
    TW2330.columns = [''.join(col).strip() for col in TW2330.columns.values]

    Tradingdate = TW2330[['Date']].copy()
    Tradingdate.loc[:,'str_date'] = Tradingdate['Date'].dt.strftime('%Y-%m-%d')

    save_dir = '~/Stock_project/TW_stock_data'
    Tradingdate_csv_path = os.path.join(save_dir, 'Tradingdate.csv')
    Tradingdate.to_csv(Tradingdate_csv_path, index = False)

if __name__ == '__main__':
    generate_trading_date()