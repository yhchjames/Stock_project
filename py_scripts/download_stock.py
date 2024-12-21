import os
import yfinance as yf
import pandas as pd


def download_data(save_dir,list_path,isListed):
    list_path = os.path.expanduser(list_path)
    save_dir = os.path.expanduser(save_dir)

    # Create Yahoo Finance tickers by appending '.TW or TWO' to each code
    if isListed:
        df = pd.read_csv(list_path, dtype={"公司代號":"string", "公司簡稱":"string"})
        df = df.loc[ :, ['公司代號','公司簡稱'] ]
        df.rename(columns = {'公司代號':'ticker','公司簡稱':'name'}, inplace=True)
        tickers = df['ticker'] + '.TW'
    else:
        df = pd.read_csv(list_path, dtype={"股票代號":"string", "名稱":"string"})
        df = df.loc[ :, ['股票代號','名稱'] ]
        df.rename(columns = {'股票代號':'ticker','名稱':'name'}, inplace=True)
        tickers = df['ticker'] + '.TWO'
    tickers = tickers.tolist()

    # Set the path for the combined CSV file
    combined_csv_path = os.path.join(save_dir, 'TaiwanStocksHistData.csv')

    # Remove the file if it already exists to prevent duplicate data
    # if os.path.exists(combined_csv_path):
    #     os.remove(combined_csv_path)

    # Download historical data for each ticker and append to the CSV file
    for idx, ticker in enumerate(tickers):
        print(f'Downloading data for {ticker} ({idx+1}/{len(tickers)})')
        try:
            data = yf.download(ticker, period='max', progress=False)
            if not data.empty:
                # Add a column for the ticker symbol
                data['Ticker'] = ticker
                # Reset the index to turn the Date index into a column
                data = data.reset_index()
                # Append data to CSV file
                if not os.path.isfile(combined_csv_path):
                    # If file doesn't exist, write header
                    data.to_csv(combined_csv_path, index=False)
                else:
                    # If file exists, append without writing the header
                    data.to_csv(combined_csv_path, mode='a', index=False, header=False)
            else:
                print(f'No data found for {ticker}')
        except Exception as e:
            print(f'Failed to download data for {ticker}: {e}')

    print(f'Saved all stock data to {combined_csv_path}')


def main():
    save_dir = '~/Stock_project/TW_stock_data'

    #TWSE
    TWSE_path = '~/Stock_project/TW_stock_data/TWSE.csv'
    download_data(save_dir,TWSE_path,True)

    #OTC
    OTC_path = '~/Stock_project/TW_stock_data/OTCs.csv'
    download_data(save_dir,OTC_path,False)

if __name__ == '__main__':
    main()