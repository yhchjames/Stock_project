import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import time
import asyncio
import aiohttp
import random


async def fetch_async(session, url):
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            response.raise_for_status()
            return await response.text(errors="replace")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

async def main_async(url_lib):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for date in url_lib['date']:
            url = construct_url(url_lib['brokerHQ_id'], url_lib['broker_id'], 'E', date, date)
            tasks.append(asyncio.create_task(fetch_async(session, url)))
        # Gather results
        results = await asyncio.gather(*tasks)
        return list(zip(results, url_lib['date']))

def construct_url(brokerHQ_id, broker_id, c, start_date, end_date):
    base_url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm'
    params = {
        'a': brokerHQ_id,
        'b': broker_id,
        'c': c,
        'e': transform_date(start_date),
        'f': transform_date(end_date)
    }
    # Build the full URL
    url = base_url + '?' + '&'.join([f"{key}={value}" for key, value in params.items()])
    print(f"++++Fetching data for Broker HQ ID: {brokerHQ_id}, Broker ID: {broker_id}, Date: {start_date}++++")
    return url

def transform_date(date_str):
    """Transforms a date string from 'YYYY-MM-DD' to 'YYYY-M-D'.

    Args:
      date_str: The input date string in 'YYYY-MM-DD' format.

    Returns:
      The transformed date string in 'YYYY-M-D' format.
    """
    year, month, day = date_str.split('-')
    return f"{year}-{int(month)}-{int(day)}"

def new_download_broker_data(brokerHQ_id, broker_id, date):
    url_lib= {'brokerHQ_id':brokerHQ_id,'broker_id':broker_id,'date':date}
    loop = asyncio.get_event_loop()
    htmls_content = loop.run_until_complete(main_async(url_lib))

    # htmls_content = asyncio.run(main_async(url_lib))

    return htmls_content

def extract_code_name(html_fragment):
    # Check if cell_content is a string
    soup = BeautifulSoup(html_fragment, 'html.parser')
    td = soup.find('td', {'id': 'oAddCheckbox'})
    if not td:
        return pd.Series({'Ticker': None, 'Name': None})


    # --- Case A: Look for an <a> tag first ---
    a_tag = td.find('a')
    if a_tag:
        # Example: href="javascript:Link2Stk('00632R');"
        href = a_tag.get('href', '')
        match_link = re.search(r"Link2Stk\('(.*?)'\)", href)
        if match_link:
            code = str(match_link.group(1))  # e.g. "00632R"
            full_text = a_tag.get_text(strip=True)  # e.g. "00632R元大台灣50反1"

            # The name is what's left after removing the code from the beginning
            # Example: full_text = "00632R元大台灣50反1" => name = "元大台灣50反1"
            name = full_text[len(code):]
            return pd.Series({'Ticker': code, 'Name': name})

    # --- Case B: Otherwise, look for a <script> with GenLink2stk(...) ---
    script_tag = td.find('script')
    if script_tag and script_tag.string:
        # Example: GenLink2stk('AS6133','金橋');
        match_genlink = re.search(r"GenLink2stk\('(AS)?(.*?)','(.*?)'\);", script_tag.string)
        if match_genlink:
            # match_genlink.group(1) is either 'AS' or None
            # match_genlink.group(2) is the actual numeric part after AS
            # match_genlink.group(3) is the name
            code = str(match_genlink.group(2))
            name = match_genlink.group(3)
            return pd.Series({'Ticker': code, 'Name': name})


    # If neither case applies, return something default
    return pd.Series({'Ticker': None, 'Name': None})

def extract_name(names):
    df_list = []
    for n in names:
        df_list.append({'stock_name':str(n)})
    name_list = pd.DataFrame(df_list)
    name_list = name_list.stock_name.apply(extract_code_name)
    return name_list

def extract_number(numbers):
    number_list = []
    for n in range(0,len(numbers),3):
        number_list.append({'buy':numbers[n].text,'sell':numbers[n+1].text,'diff':numbers[n+2].text})
    return pd.DataFrame(number_list)


# prompt: Transforms a date string from 'YYYY-M-D' to 'YYYY-MM-DD'.

def transform_date_reverse(date_str):
    """Transforms a date string from 'YYYY-M-D' to 'YYYY-MM-DD'.

    Args:
      date_str: The input date string in 'YYYY-M-D' format.

    Returns:
      The transformed date string in 'YYYY-MM-DD' format.
    """
    year, month, day = date_str.split('-')
    return f"{year}-{int(month):02}-{int(day):02}"


def go_through_dates(Tradingdatefile_path, Brokerlist_path, Saved_path):

    Tradingdatefile_path = os.path.expanduser(Tradingdatefile_path)
    Brokerlist_path = os.path.expanduser(Brokerlist_path)
    Saved_path = os.path.expanduser(Saved_path)

    Ori_Tradingdate = pd.read_csv(Tradingdatefile_path)
    Brockerlist = pd.read_csv(Brokerlist_path)
    last_date = 0
    last_id = 0
    #deal with the case of continuing extract data from web
    #which means the previous process was interupt
    if os.path.isfile(Saved_path):
        # If file doesn't exist, write header
        Brockertradinglist = pd.read_csv(Saved_path)
        last_date = Brockertradinglist.Date.iloc[-1]
        last_id = Brockertradinglist.Branch_Code.iloc[-1]
        # Brockertradinglist = Brockertradinglist[Brocker_id.index(last_id):]
        Brockerlist = Brockerlist.iloc[Brockerlist[Brockerlist.Branch_Code == str(last_id)].index[0]:]

    print('===start from ', last_id, '===')
    print('===start from ', last_date, '===')
    #go through all broker and dates, main loop
    for index, row in Brockerlist.iterrows():
        Broker_Code = row['Broker_Code']
        Branch = row['Branch_Name']
        Branch_Code = row['Branch_Code']

        if last_date:
            Tradingdate = Ori_Tradingdate.iloc[Ori_Tradingdate[Ori_Tradingdate.str_date == transform_date_reverse(last_date)].index[0]+1:]
            last_date = 0
        else:
            Tradingdate = Ori_Tradingdate

        data_tables = []
        str_date_list = Tradingdate['str_date'].tolist()
        for i in range(0, len(str_date_list), 10):
            date_list = str_date_list[i:i+10]
            print(f"==========Processing Broker ID: {Branch_Code}, Date: {date_list[0]} to {date_list[-1]}===============")
            soup_date_list = new_download_broker_data( Broker_Code,Branch_Code, date_list)
            #extract name and numners from soup
            for soup,date in soup_date_list:
                real_soup = BeautifulSoup(soup, 'html.parser')
                numbers = real_soup.find('table').find_all('tr')[5].find('table').find_all('td',class_='t3n1')
                names = real_soup.find('table').find_all('tr')[5].find('table').find_all('td',class_='t4t1')
                data_table = pd.concat([extract_name(names),extract_number(numbers)],axis = 1)

                data_table['Branch'] = Branch
                data_table['Date'] = date
                data_table['Branch_Code'] = Branch_Code
                data_tables.append(data_table)
            if len(data_tables) > 20:
                combine_data_table = pd.concat(data_tables, ignore_index=True)
                if not os.path.isfile(Saved_path):
                    # If file doesn't exist, write header
                    combine_data_table.to_csv(Saved_path, index=False)
                    data_tables = []
                else:
                    # If file exists, append without writing the header
                    combine_data_table.to_csv(Saved_path, mode='a', index=False, header=False)
                    # Clear the list
                    data_tables = []
            time.sleep(0.25)
        if data_tables:
            combine_data_table = pd.concat(data_tables, ignore_index=True)
            combine_data_table.to_csv(Saved_path, mode='a', index=False, header=False)
            data_tables = []
        Tradingdate = Ori_Tradingdate

if __name__ == '__main__':
    USER_AGENTS = [\
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',\
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:106.0) Gecko/20100101 Firefox/106.0',\
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',\
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'\
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763'
               ]

    Tradingdatefile_path = '~/Documents/Dev/Cheater_finder/Stock_project/TW_stock_data/Tradingdate.csv'
    Brokerlist_path = '~/Documents/Dev/Cheater_finder/Stock_project/TW_stock_data/big_branch_list.csv'
    Saved_path = '~/Documents/Dev/Cheater_finder/Stock_project/TW_stock_data/broker_trading_list.csv'

    go_through_dates(Tradingdatefile_path, Brokerlist_path, Saved_path)