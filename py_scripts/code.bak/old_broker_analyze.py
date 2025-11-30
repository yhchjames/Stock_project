import pandas as pd
import gc
import os
from glob import glob

def process_broker_file(broker_file, df_volume, all_trading_days):
    # Load broker data
    df_broker = pd.read_csv(broker_file, 
                            dtype={"Ticker":"string",
                                    "Name":"string",
                                    "buy":int,
                                    "sell":int,
                                    "diff":int,
                                    "Branch":"string",
                                    "Date":"string",
                                    "Branch_Code":"string"})
    # Drop buy/sell early
    df_broker.drop(['buy','sell'], axis=1, inplace=True)
    
    # Convert 'Date' to datetime
    df_broker['Date'] = pd.to_datetime(df_broker['Date'], errors='coerce')
    
    # Merge daily volume
    df_merged = pd.merge(df_broker, df_volume[['Date','Ticker','Volume']], on=['Date','Ticker'], how='left')

    del df_broker
    gc.collect()

    # Reindex to full trading days for each Code in this broker
    # Because we have only one broker here, we can do a groupby on Code only
    # However, if a broker trades many codes, we still need reindexing per code.
    def reindex_single_code(grp):
        grp = grp.set_index('Date').reindex(all_trading_days)
        grp['Ticker'] = grp['Ticker'].ffill().bfill()
        grp['Brocker_id'] = grp['Brocker_id'].ffill().bfill()
        grp['Brockername'] = grp['Brockername'].ffill().bfill()
        grp['Name'] = grp['Name'].ffill().bfill()
        grp['Volume'] = grp['Volume'].fillna(0)
        grp['diff'] = grp['diff'].fillna(0)
        return grp.reset_index()

    df_complete = df_merged.groupby('Ticker', group_keys=False).apply(reindex_single_code)

    del df_merged
    gc.collect()

    # Compute rolling averages
    df_complete = df_complete.sort_values(['Ticker','Date'])
    df_complete['7_day_avg_buy'] = (df_complete.groupby('Ticker')['diff']
                                    .rolling(window=7, min_periods=1)
                                    .mean()
                                    .reset_index(drop=True))

    # Conditions
    condition1 = df_complete['diff'] > (df_complete['Volume'] / 2)
    condition2 = df_complete['diff'] > (3 * df_complete['7_day_avg_buy'])

    df_complete['b_tag'] = condition1 | condition2

    # Filter rows
    df_filtered = df_complete[df_complete['b_tag']]
    return df_filtered


def main():
    # Load volume data and trading days
    df_volume = pd.read_csv('~/Documents/Dev/Cheater_finder/stock_project/TW_stock_data/TW_stock_data_2023_24.csv', 
                            dtype={'Ticker': 'string', 'Volume':'float'})
    df_volume['Ticker'] = df_volume['Ticker'].str.replace(r'\.TWO|\.TW', '', regex=True)
    df_volume['Date'] = pd.to_datetime(df_volume['Date'], errors='coerce')
    all_trading_days = pd.Series(df_volume['Date'].unique()).sort_values()

    # Directory with broker-split files
    broker_dir = '~/Documents/Dev/Cheater_finder/stock_project/Taiwan_stock_data/broker_split_files'
    output_file = '~/Documents/Dev/Cheater_finder/stock_project/Taiwan_stock_data/broker_taged.csv'

    # Process each broker CSV and append results
    broker_files = glob(os.path.join(broker_dir, '*.csv'))

    # We'll append results to one CSV. Initialize it empty or delete if exists.
    # If running multiple times, ensure to remove old file or handle differently.
    if os.path.exists(output_file):
        os.remove(output_file)

    for i, broker_file in enumerate(broker_files, start=1):
        print(f"Processing {broker_file} ({i}/{len(broker_files)})")
        df_filtered = process_broker_file(broker_file, df_volume, all_trading_days)
        if i == 1:
            df_filtered.to_csv(output_file, index=False)
        else:
            df_filtered.to_csv(output_file, index=False, header=False, mode='a')
        del df_filtered
        gc.collect()

    print("All brokers processed.")
