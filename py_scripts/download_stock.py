import os
import yfinance as yf
import pandas as pd

def clean_directory(save_dir):
    save_dir = os.path.expanduser(save_dir)
    print(f"Expanding save_dir: {save_dir}")

    if os.path.isdir(save_dir):
        contents = os.listdir(save_dir)
        print(f"Contents of {save_dir} before cleaning: {contents}")

        for filename in contents:
            file_path = os.path.join(save_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                    print(f"Deleted file: {file_path}")
                else:
                    print(f"Skipping non-file (maybe directory?): {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Because: {e}")

        # Check remaining contents after attempting to clean
        print(f"Contents of {save_dir} after cleaning: {os.listdir(save_dir)}")

    else:
        print(f"save_dir = {save_dir} is not a dir")



def download_data(save_dir,list_path,isListed):
    list_path = os.path.expanduser(list_path)
    save_dir = os.path.expanduser(save_dir)

    # Create Yahoo Finance tickers by appending '.TW or TWO' to each code
    if isListed:
        df = pd.read_csv(list_path, dtype={"公司代號":"string", "公司簡稱":"string"})
        df = df.loc[ :, ['公司代號','公司簡稱'] ]
        df.rename(columns = {'公司代號':'ticker','公司簡稱':'name'}, inplace=True)
        tickers = df['ticker'].apply(lambda x: x + '.TW').tolist()
    else:
        df = pd.read_csv(list_path, dtype={"股票代號":"string", "名稱":"string"})
        df = df.loc[ :, ['股票代號','名稱'] ]
        df.rename(columns = {'股票代號':'ticker','名稱':'name'}, inplace=True)
        tickers = df['ticker'].apply(lambda x: x + '.TWO').tolist()

    # Remove the file if it already exists to prevent duplicate data
    # if os.path.exists(combined_csv_path):
    #     os.remove(combined_csv_path)


    # Download historical data for each ticker and append to the CSV file
    for idx, ticker in enumerate(tickers):
        print(f'Downloading data for {ticker} ({idx+1}/{len(tickers)})')
        try:
            data = yf.download(ticker, period='max', progress=False)
            if not data.empty:
                #flatten the header
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = [col[0] for col in data.columns.values]
                # Add a column for the ticker symbol
                data['Ticker'] = ticker
                # Reset the index to turn the Date index into a column
                data = data.reset_index()
                file_name = ticker[3] + '-TaiwanStocksHistData.csv'
                combined_csv_path = os.path.join(save_dir, file_name)
                # combined_csv_path = os.path.join(orig_combined_csv_path, ticker[0])

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

    print(f'Saved all stock data to {save_dir}')


def main():
    save_dir = '~/Stock_project/TW_stock_data/AllStockHist'
    clean_directory(save_dir)

    #TWSE
    TWSE_path = '~/Stock_project/TW_stock_data/TWSE.csv'
    download_data(save_dir,TWSE_path,True)

    #OTC
    OTC_path = '~/Stock_project/TW_stock_data/OTCs.csv'
    download_data(save_dir,OTC_path,False)

if __name__ == '__main__':
    main()