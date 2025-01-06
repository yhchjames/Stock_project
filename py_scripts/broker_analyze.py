import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def list_csv_files(directory):
    """
    return all files in the directory with whole path
    """
    directory = os.path.expanduser(directory)
    return [
        os.path.join(directory, f) 
        for f in os.listdir(directory) 
        if f.endswith('.csv')
    ]

def get_signal_dates_from_price(df_price):
    """
    傳回該股票符合「大漲前 signal」的日期列表，以及對應的 DataFrame 內容
    df_price 欄位需包含 ['Date', 'Close', 'Volume', 'Ticker']，且要有時間排序
    """

    # 確保按照日期排序
    df_price = df_price.sort_values(['Ticker','Date']).reset_index(drop=True)

    # 計算 7 日移動平均 (MA7)
    # 前 7 日平均：shift(1) 之後 rolling(7) 計算
    # （若要包含當天在內，就要調整shift的參數）
    df_price['MA7_past'] = df_price.groupby('Ticker')['Close'].transform(lambda x: x.shift(1).rolling(7).mean())

    # 未來 7 日平均：rolling(7) 對 shift(-1)，亦或 shift(-6) ~ shift(0) 之類做法
    # 這裡示範一個做法：對於當天，取未來7天(含當天~第6天)的平均
    # 也可以改為 shift(-7).rolling(7).mean() 等等看實際需求
    df_price['MA7_future'] = df_price.groupby('Ticker')['Close'].transform(lambda x: x.shift(-6).rolling(7).mean())

    # 建立條件
    # (1) 當日收盤 <= 過去7日MA * 1.03
    cond1 = df_price['Close'] <= df_price['MA7_past'] * 1.03

    # (2) 未來 7 日 MA >= current price
    cond2 = df_price['MA7_future'] >= df_price['Close'] * 1.20

    df_price['is_signal'] = cond1 & cond2

    # Mark rows where the next row's 'is_signal' is True
    df_price['is_signal_next'] = df_price['is_signal'].shift(-1, fill_value=False)

    # 取出符合條件的 row
    df_signals = df_price[df_price['is_signal'] | df_price['is_signal_next']].copy()
    return df_signals

def tag_price_files(price_files, start_date='2023-01-01'):
    all_signals_list = []
    
    for f in price_files:
        df = pd.read_csv(f)
        # 若有需要處理 Ticker 欄位（例如它可能是 "[1110.TW]" 這種格式），可在此處理
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[df['Date'] >= start_date]
        df['Ticker'] = df['Ticker'].str.replace(r'\.TWO|\.TW', '', regex=True)

        df_signals = get_signal_dates_from_price(df)
        all_signals_list.append(df_signals)
    
    # 合併所有股票的 signal
    df_all_signals = pd.concat(all_signals_list, ignore_index=True)
    
    return df_all_signals
    


def build_volume_lookup(price_files, start_date='2023-01-01'):
    """
    build a dict= {(Ticker, Date), value=Volume}
    """
    volume_dict = {}
    
    for f in price_files:
        df_price = pd.read_csv(f)
        df_price['Date'] = pd.to_datetime(df_price['Date'])
        df_price = df_price[df_price['Date'] >= start_date]

        for idx, row in df_price.iterrows():
            # 可能 row['Ticker'] = "[1110.TW]" 等，需要做字串清理
            ticker_str = str(row['Ticker']).replace('.TWO', '').replace('.TW', '')
                # .replace('[', '') \
                # .replace(']', '') \
                # .replace('(', '') \
                # .replace(')', '') \
                # .replace(r'\.TWO|\.TW', '')
            date_str = row['Date'].strftime('%Y-%m-%d')
            volume_dict[(ticker_str, date_str)] = row['Volume'] / 1000

    return volume_dict


def big_buy_calc(broker_files,volume_dict):

    big_buy_result_list = []

    for idx, bf in enumerate(broker_files):
        print(f'--- Deal with {bf[-8:]} , process is {idx+1}/{len(broker_files)} ----')
        df_b = pd.read_csv(bf,
                            dtype={"Ticker":"string",
                                    "Name":"string",
                                    "buy":int,
                                    "sell":int,
                                    "diff":int,
                                    "Branch":"string",
                                    "Date":"string",
                                    "Branch_Code":"string"})

        # 確保 Date 為 datetime
        df_b['Date'] = pd.to_datetime(df_b['Date'])

        # 依據欄位狀況調整；假設檔案欄位是:
        # Ticker,Name,buy,sell,diff,Branch,Date,Branch_Code
        # Ticker 有可能是 "1605"、"00632R" 等，不用再特別清除 .TW。

        # 為了計算過去 7 天 diff 平均，先依 (Branch, Ticker, Date) 排序
        df_b = df_b.sort_values(['Ticker','Date']).reset_index(drop=True)

        # 新增一個「前 7 天 diff 平均」欄位
        df_b['diff_7days_avg'] = df_b.groupby('Ticker')['diff'] \
                                     .transform(lambda x: x.shift(1).rolling(7).mean())

        # 新增一個 volume 欄位 (對每列做 apply)
        def get_volume(row):
            t = row['Ticker']  # e.g. "1605", "00632R"
            d = row['Date'].strftime('%Y-%m-%d')
            return volume_dict.get((t, d), 0)  # 沒找到就用 NaN

        df_b['Volume'] = df_b.apply(get_volume, axis=1)

        # 建立條件
        cond1 = (df_b['diff'] >= 0.18 * df_b['Volume']) & (df_b['Volume']!=0) & (~df_b['Ticker'].str.startswith('0'))
        cond2 = (df_b['diff'] >= 2 * df_b['diff_7days_avg']) & (~df_b['Ticker'].str.startswith('0'))

        df_b['is_big_buy_c1'] = cond1
        df_b['is_big_buy_c2'] = cond2

        # 篩出符合條件的 row
        df_big_buy = df_b[df_b['is_big_buy_c2'] | df_b['is_big_buy_c1']].copy()

        # 如果你的記憶體更小，不想一次收集在 list 中，也可以直接寫檔案：
        # df_big_buy.to_csv('broker_big_buy.csv', mode='a', header=False, index=False)
        # 不過要記得第一次寫入時要含 header；之後再 append 才關掉 header。

        big_buy_result_list.append(df_big_buy)

        # 讀完一個檔案後，就可以把 df_b 釋放
        del df_b

    # return big_buy_result_list
    return pd.concat(big_buy_result_list, ignore_index=True)

def cheating_rate(df_signals, df_big_buy,save_dir,success_threshold):
    df_signals = pd.read_csv(df_signals)
    df_signals['Date'] = pd.to_datetime(df_signals['Date'])
    df_signals['Ticker'] = df_signals['Ticker'].astype(str)

    df_big_buy = pd.read_csv(df_big_buy,
                             thousands=',',
                             dtype={'Ticker':'string',
                                    'Name':'string',
                                    'buy':int,
                                    'sell':int,
                                    'diff':int,
                                    'Branch':'string',
                                    'Date':'string',
                                    'Branch_Code':'string',
                                    'diff_7days_avg':float,
                                    'Volume':float,
                                    'is_big_buy_c1':bool,
                                    'is_big_buy_c2':bool})
    df_big_buy['Date'] = pd.to_datetime(df_big_buy['Date'])
    df_big_buy['Ticker'] = df_big_buy['Ticker'].astype(str)

    # 合併
    df_merged = pd.merge(
        df_big_buy, 
        df_signals[['Ticker','Date']], 
        on=['Ticker','Date'], 
        how='inner'
    )
    df_merged.to_csv(save_dir+'broker_big_buy_success.csv', index=False)

    # 計算成功率
    # - total_count: 該券商(或分點)的「大量買入」次數
    # - success_count: 該券商(或分點)與 signal match 的次數
    df_total = df_big_buy.groupby('Branch_Code').size().reset_index(name='total_count')
    df_success = df_merged.groupby('Branch_Code').size().reset_index(name='success_count')

    df_stat = pd.merge(df_total, df_success, on='Branch_Code', how='left')
    df_stat['success_rate'] = df_stat['success_count'] / df_stat['total_count'] * 100
    df_stat.fillna({'success_count': 0, 'success_rate': 0}, inplace=True)

    df_stat.to_csv(save_dir+'broker_success_rate.csv', index=False)

    #success rate to a every ticker
    df_total_bt = df_big_buy.groupby(['Branch_Code','Ticker']).size().reset_index(name='total_count')
    df_success_bt = df_merged.groupby(['Branch_Code','Ticker']).size().reset_index(name='success_count')
    df_stat_bt = pd.merge(df_total_bt, df_success_bt, on=['Branch_Code','Ticker'], how='left')
    df_stat_bt['success_count'] = df_stat_bt['success_count'].fillna(0).astype(int)
    df_stat_bt['success_rate'] = df_stat_bt['success_count'] / df_stat_bt['total_count']
    df_stat_bt['success_rate_percent'] = df_stat_bt['success_rate'] * 100

    df_stat_bt.to_csv(save_dir+'broker_branch_ticker_success_rate.csv', index=False)

    #calc today big buy branch
    success_rate_file = save_dir+'broker_branch_ticker_success_rate.csv'
    df_success_rate = pd.read_csv(success_rate_file,
                                  dtype={"Branch_Code":"string","Ticker":"string"})
    # 篩選出成功率 > 0.49
    df_high_sr = df_success_rate[df_success_rate['success_rate'] > success_threshold].copy()
    # 先做成 (Branch_Code, Ticker) set 以便快速比對
    high_sr_pairs = set(zip(df_high_sr['Branch_Code'], df_high_sr['Ticker']))
    
    latest_date = df_big_buy['Date'].max()
    df_latest_day = df_big_buy[df_big_buy['Date'] == latest_date].copy()
    if df_latest_day.empty:
        print(f"No branch big buy today {latest_date}")
    else:
        print(f'There are {len(df_latest_day)} big buy data for today {latest_date}')
        df_merge_cheater = pd.merge(
            df_latest_day,
            df_high_sr[['Branch_Code','Ticker','success_rate']],  # 只帶需要的欄位
            on=['Branch_Code','Ticker'],
            how='inner'
        )
        if df_merge_cheater.empty:
            print('No cheater bought previous ticker today ')
        else:
            print('Cheaters bought:')
            print(df_merge_cheater[['Branch','Ticker','Date','diff','Volume','success_rate']].to_string(index=False))
            df_merge_cheater.to_csv(save_dir+'cheater_today_bought.csv', index=False)


if __name__ == '__main__':


    price_folder = '~/Stock_project/TW_stock_data/AllStockHist'
    price_files = list_csv_files(price_folder)

    #Deal with stock hist data, add signal dates in
    df_all_signals = tag_price_files(price_files)
    df_all_signals.to_csv('~/Stock_project/TW_stock_data/calc_result/big_gain_signals.csv', index=False)


    #Deal with brocker trading data, tag big buy date and tickers
    volume_dict = build_volume_lookup(price_files, start_date='2023-01-01')
    # broker_trading_folder = 'small_broker_trading'
    # broker_files = ['~/Stock_project/TW_stock_data/small_broker_trading/9661.csv']
    broker_files = list_csv_files('~/Stock_project/TW_stock_data/small_broker_trading')
    df_broker_big_buy = big_buy_calc(broker_files,volume_dict)  # 用來裝所有檔案計算出的大量買入紀錄
    df_broker_big_buy.to_csv('~/Stock_project/TW_stock_data/calc_result/broker_big_buy.csv', index=False)


    #calculate success rate
    big_gain_signals_path = '~/Stock_project/TW_stock_data/calc_result/big_gain_signals.csv'
    broker_big_buy_path = '~/Stock_project/TW_stock_data/calc_result/broker_big_buy.csv'
    save_dir = '~/Stock_project/TW_stock_data/calc_result/'
    cheating_rate(big_gain_signals_path,broker_big_buy_path, save_dir,0.49)

    print('--Finish broker_analyze--')




