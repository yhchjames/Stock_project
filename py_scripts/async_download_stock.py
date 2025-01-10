import os
import asyncio
from functools import partial

import pandas as pd
from twstock import Stock
import math
import random

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

# --------------------------- #
# Async Downloading of Ticker #
# --------------------------- #

async def fetch_data_with_retry(fetch_func, max_retries=5, base_delay=1.0):
    """
    A simple helper for retrying a function (such as stock.fetch_from(...))
    with exponential backoff and jitter.
    
    :param fetch_func:  A no-arg function/coroutine that returns data or raises an exception
    :param max_retries: Max number of retries
    :param base_delay:  Initial backoff delay (seconds)
    """
    for attempt in range(1, max_retries + 1):
        try:
            return await fetch_func()
        except (ConnectionResetError, TimeoutError) as exc:
            # Log or print the error
            print(f"Attempt {attempt} failed with error {exc}. Retrying...")

            if attempt == max_retries:
                # Re-raise the exception if max retries exhausted
                raise

            # Sleep with exponential backoff + jitter
            # e.g. backoff = base_delay * 2^(attempt-1)
            # Add jitter for good measure
            backoff = base_delay * (2 ** (attempt - 1))
            jitter = random.uniform(0, 0.3 * backoff)
            sleep_time = backoff + jitter
            print(f"Sleeping for {sleep_time:.2f} seconds before next retry...")
            await asyncio.sleep(sleep_time)
        except Exception:
            # If it's another exception, just raise it directly
            raise



async def download_single_ticker(ticker: str, is_listed: bool, start_y: int, start_m: int, save_dir: str, sem: asyncio.Semaphore):
    """
    Download historical data for a single ticker asynchronously.
    We wrap the synchronous twstock API with asyncio.to_thread to avoid blocking.
    """
    async with sem:
        print(f'Downloading data for {ticker}...')

        try:
            # twstock.Stock is synchronous. We create Stock object in a threadpool:
            if is_listed:
                stock = await asyncio.to_thread(Stock, ticker[:-3], False)  # initial_fetch=False
            else:
                stock = await asyncio.to_thread(Stock, ticker[:-4], False)  # initial_fetch=False

            async def fetch_func():
                # This function itself needs to be awaitable if we want to call it with 'await'.
                # We'll wrap the synchronous call in asyncio.to_thread:
                data = await asyncio.to_thread(stock.fetch_from, start_y, start_m)
                return data
            
            data = await fetch_data_with_retry(fetch_func, max_retries=5, base_delay=1.0)

            # The actual data fetch is also synchronous, so run in a thread:
            # data = await asyncio.to_thread(stock.fetch_from, start_y, start_m)
            data = pd.DataFrame(data)

            if data.empty:
                print(f'No data found for {ticker}')
                return

            # Add a column for the ticker symbol
            data['Ticker'] = ticker
            # Reset index so Date index becomes a column
            data = data.reset_index()

            # Rename columns
            data.rename(
                columns={
                    'capacity': 'Volume',
                    'open': 'Open',
                    'close': 'Close',
                    'high': 'High',
                    'low': 'Low',
                    'date': 'Date',
                },
                inplace=True
            )

            # Construct a file name. In your original code, you used `ticker[3]` which might be error-prone.
            # We'll just store each ticker's data to a file named <ticker>-TaiwanStocksHistData.csv
            filename = f"{ticker[3]}-TaiwanStocksHistData.csv"
            combined_csv_path = os.path.join(os.path.expanduser(save_dir), filename)

            # Save to CSV. We'll do this on a thread as well to avoid blocking:
            write_mode = 'a' if os.path.isfile(combined_csv_path) else 'w'
            header = not os.path.isfile(combined_csv_path)

            # partial function to call data.to_csv with the correct arguments
            write_func = partial(
                data.to_csv, 
                combined_csv_path, 
                index=False, 
                mode=write_mode, 
                header=header
            )
            await asyncio.to_thread(write_func)

            print(f'Successfully downloaded data for {ticker}')

        except Exception as e:
            print(f'Failed to download data for {ticker}: {e}')

# ------------------------------------------- #
# Async Function to Download a List of Tickers
# ------------------------------------------- #

async def async_download_data(
    save_dir: str, 
    list_path: str, 
    is_listed: bool, 
    start_y: int, 
    start_m: int,
    max_concurrent_tasks: int = 10
):
    """
    Asynchronous version of download_data. 
    1. Reads the CSV file for tickers.
    2. Spawns tasks to download each ticker concurrently (limited by a semaphore).
    """
    list_path = os.path.expanduser(list_path)
    save_dir = os.path.expanduser(save_dir)

    # Create Yahoo Finance tickers by appending '.TW or .TWO' to each code
    if is_listed:
        df = pd.read_csv(list_path, dtype={"公司代號": "string", "公司簡稱": "string"})
        df = df.loc[:, ['公司代號','公司簡稱']]
        df.rename(columns={'公司代號': 'ticker', '公司簡稱': 'name'}, inplace=True)
        tickers = df['ticker'].apply(lambda x: x + '.TW').tolist()
    else:
        df = pd.read_csv(list_path, dtype={"股票代號": "string", "名稱": "string"})
        df = df.loc[:, ['股票代號','名稱']]
        df.rename(columns={'股票代號': 'ticker', '名稱': 'name'}, inplace=True)
        tickers = df['ticker'].apply(lambda x: x + '.TWO').tolist()

    # Create a semaphore to limit the number of simultaneous downloads
    sem = asyncio.Semaphore(max_concurrent_tasks)

    # Prepare tasks for each ticker
    tasks = []
    for ticker in tickers:
        tasks.append(
            asyncio.create_task(
                download_single_ticker(ticker, is_listed, start_y, start_m, save_dir, sem)
            )
        )

    # Run all tasks concurrently
    await asyncio.gather(*tasks)
    print(f'Saved all stock data to {save_dir}')

# ---------- #
# Async Main #
# ---------- #

async def main():
    # Directory to clean and then re-download CSVs into
    save_dir = '~/Stock_project/TW_stock_data/AllStockHist'
    clean_directory(save_dir)

    # TWSE
    TWSE_path = '~/Stock_project/TW_stock_data/TWSE.csv'
    # Download concurrently
    await async_download_data(save_dir, TWSE_path, is_listed=True, start_y=2023, start_m=1, max_concurrent_tasks=10)

    # OTC
    OTC_path = '~/Stock_project/TW_stock_data/OTCs.csv'
    # Download concurrently
    await async_download_data(save_dir, OTC_path, is_listed=False, start_y=2023, start_m=1, max_concurrent_tasks=10)

# ----------------- #
# Entry Point Block #
# ----------------- #

if __name__ == '__main__':
    
    asyncio.run(main())
