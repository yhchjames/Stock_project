import asyncio
import aiohttp
import logging
import random
import os
import time
import re
import pandas as pd
from bs4 import BeautifulSoup

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
# Configure logging: you can adjust the filename, log level, and format as needed.
logging.basicConfig(
    filename='~/logs/broker_data.log',  # or any path you prefer
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Global User Agents List
# -----------------------------------------------------------------------------
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:106.0) Gecko/20100101 '
    'Firefox/106.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763'
]

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def transform_date(date_str):
    """
    Transforms a date string from 'YYYY-MM-DD' to 'YYYY-M-D'.
    
    Args:
      date_str (str): The input date string in 'YYYY-MM-DD' format.

    Returns:
      str: The transformed date string in 'YYYY-M-D' format.
    """
    year, month, day = date_str.split('-')
    return f"{year}-{int(month)}-{int(day)}"


def transform_date_reverse(date_str):
    """
    Transforms a date string from 'YYYY-M-D' to 'YYYY-MM-DD'.
    
    Args:
      date_str (str): The input date string in 'YYYY-M-D' format.

    Returns:
      str: The transformed date string in 'YYYY-MM-DD' format.
    """
    year, month, day = date_str.split('-')
    return f"{year}-{int(month):02d}-{int(day):02d}"


def construct_url(brokerHQ_id, broker_id, c, start_date, end_date):
    """
    Constructs the URL with query parameters given broker IDs and dates.
    
    Args:
      brokerHQ_id (str): HQ broker ID.
      broker_id   (str): Branch broker ID.
      c           (str): A constant query parameter (e.g., 'E').
      start_date  (str): Start date in 'YYYY-MM-DD' format.
      end_date    (str): End date in 'YYYY-MM-DD' format.

    Returns:
      str: Fully constructed URL.
    """
    base_url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm'
    params = {
        'a': brokerHQ_id,
        'b': broker_id,
        'c': c,
        'e': transform_date(start_date),
        'f': transform_date(end_date)
    }
    url = base_url + '?' + '&'.join([f"{key}={value}" for key, value in params.items()])
    logger.info(
        f"++++ Constructing URL - HQ ID: {brokerHQ_id}, Broker ID: {broker_id}, "
        f"Date Range: {start_date} to {end_date} ++++"
    )
    return url

# -----------------------------------------------------------------------------
# Async Fetch
# -----------------------------------------------------------------------------
async def fetch_async(session, url):
    """
    Asynchronously fetches the text content of the provided URL.

    Args:
      session (aiohttp.ClientSession): The aiohttp session to use.
      url (str): The URL to fetch.

    Returns:
      str or None: The HTML text if successful, or None if an error occurred.
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
    }
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            response.raise_for_status()
            return await response.text(errors="replace")
    except Exception as e:
        # Log the error instead of printing
        logger.error(f"Error fetching {url}: {e}", exc_info=True)
        return None

# -----------------------------------------------------------------------------
# Main Async Function
# -----------------------------------------------------------------------------
async def main_async(url_lib):
    """
    Main asynchronous logic to fetch multiple URLs concurrently.

    Args:
      url_lib (dict): Dictionary containing brokerHQ_id, broker_id, and date list.

    Returns:
      list of tuples: Each tuple is (HTML content, date).
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for date in url_lib['date']:
            url = construct_url(
                url_lib['brokerHQ_id'],
                url_lib['broker_id'],
                'E',
                date,
                date
            )
            tasks.append(asyncio.create_task(fetch_async(session, url)))
        results = await asyncio.gather(*tasks)
        return list(zip(results, url_lib['date']))

# -----------------------------------------------------------------------------
# Wrapper Function
# -----------------------------------------------------------------------------
def new_download_broker_data(brokerHQ_id, broker_id, date_list):
    """
    Wrapper for running the async function to download broker data.

    Args:
      brokerHQ_id (str): HQ broker ID.
      broker_id (str): Branch broker ID.
      date_list (list): List of date strings in 'YYYY-MM-DD' format.

    Returns:
      list of tuples: Each tuple is (HTML content, date).
    """
    url_lib = {
        'brokerHQ_id': brokerHQ_id,
        'broker_id': broker_id,
        'date': date_list
    }
    loop = asyncio.get_event_loop()
    # If you are on Python 3.7+, you could use `asyncio.run(main_async(url_lib))` 
    # but for compatibility or if there's a running event loop, do:
    html_contents = loop.run_until_complete(main_async(url_lib))
    return html_contents

# -----------------------------------------------------------------------------
# HTML Parsing Functions
# -----------------------------------------------------------------------------
def extract_code_name(html_fragment):
    """
    Extract Ticker and Name from the given HTML fragment.

    Args:
      html_fragment (str): HTML snippet containing the ticker information.

    Returns:
      pd.Series: A pandas Series with {'Ticker': <str>, 'Name': <str>}.
    """
    if not html_fragment:
        return pd.Series({'Ticker': None, 'Name': None})

    soup = BeautifulSoup(html_fragment, 'html.parser')
    td = soup.find('td', {'id': 'oAddCheckbox'})
    if not td:
        return pd.Series({'Ticker': None, 'Name': None})

    # --- Case A: Look for an <a> tag first ---
    a_tag = td.find('a')
    if a_tag:
        href = a_tag.get('href', '')
        match_link = re.search(r"Link2Stk\('(.*?)'\)", href)
        if match_link:
            code = str(match_link.group(1))
            full_text = a_tag.get_text(strip=True)
            name = full_text[len(code):]  # remove the code prefix from the text
            return pd.Series({'Ticker': code, 'Name': name})

    # --- Case B: Otherwise, look for a <script> with GenLink2stk(...) ---
    script_tag = td.find('script')
    if script_tag and script_tag.string:
        match_genlink = re.search(r"GenLink2stk\('(AS)?(.*?)','(.*?)'\);", script_tag.string)
        if match_genlink:
            code = str(match_genlink.group(2))
            name = match_genlink.group(3)
            return pd.Series({'Ticker': code, 'Name': name})

    # If neither case applies, return something default
    return pd.Series({'Ticker': None, 'Name': None})


def extract_name(names):
    """
    Iterate over a list of <td> elements (class='t4t1') and extract Ticker/Name info.
    
    Args:
      names (list): List of <td> elements containing potential ticker info.

    Returns:
      pd.DataFrame: DataFrame with columns ['Ticker', 'Name'].
    """
    df_list = []
    for n in names:
        # If it's just a string, convert to str explicitly
        fragment = str(n)
        df_list.append({'stock_name': fragment})

    # Apply the extraction to each row
    name_list = pd.DataFrame(df_list)
    name_list = name_list.stock_name.apply(extract_code_name)
    return name_list


def extract_number(numbers):
    """
    Extract buy, sell, and diff from the given list of <td> elements (class='t3n1').
    They come in groups of 3 sequentially (buy, sell, diff).

    Args:
      numbers (list): List of <td> elements for buy, sell, and diff.

    Returns:
      pd.DataFrame: DataFrame with columns ['buy', 'sell', 'diff'].
    """
    number_list = []
    for i in range(0, len(numbers), 3):
        # Safety check to avoid index out-of-bounds
        if i + 2 < len(numbers):
            number_list.append({
                'buy':  numbers[i].get_text(strip=True),
                'sell': numbers[i+1].get_text(strip=True),
                'diff': numbers[i+2].get_text(strip=True)
            })
    return pd.DataFrame(number_list)

# -----------------------------------------------------------------------------
# Main Business Logic
# -----------------------------------------------------------------------------
def go_through_dates(Tradingdatefile_path, Brokerlist_path, Saved_path):
    """
    Main driver function to iterate over brokers and dates, downloading and parsing data.
    Saves or appends to CSV after collecting results.

    Args:
      Tradingdatefile_path (str): Path to CSV containing trading dates.
      Brokerlist_path (str): Path to CSV containing broker list.
      Saved_path (str): Path to CSV where results are saved/appended.
    """
    # Expand user paths ~ => /home/<user> on Linux/Mac, etc.
    Tradingdatefile_path = os.path.expanduser(Tradingdatefile_path)
    Brokerlist_path = os.path.expanduser(Brokerlist_path)
    Saved_path = os.path.expanduser(Saved_path)

    # Read the data
    Ori_Tradingdate = pd.read_csv(Tradingdatefile_path)
    Brockerlist = pd.read_csv(Brokerlist_path)

    # Track last_date and last_id if file already exists (i.e. we are resuming)
    last_date = None
    last_id = None

    if os.path.isfile(Saved_path):
        Brockertradinglist = pd.read_csv(Saved_path)
        if not Brockertradinglist.empty:
            last_date = Brockertradinglist.Date.iloc[-1]
            last_id = Brockertradinglist.Branch_Code.iloc[-1]

            # Filter Brokerlist to start from last known Branch_Code
            # (only if the CSV's last_id indeed exists in that file)
            row_idx = Brockerlist[Brockerlist.Branch_Code == str(last_id)].index
            if not row_idx.empty:
                start_idx = row_idx[0]
                Brockerlist = Brockerlist.iloc[start_idx:]
            else:
                logger.warning(
                    f"Last Branch_Code {last_id} not found in {Brokerlist_path}; starting from beginning."
                )
                last_id = None

    logger.info(f'=== Starting from Branch_Code: {last_id}, Date: {last_date} ===')

    # Main loop over brokers
    for index, row in Brockerlist.iterrows():
        broker_code = row['Broker_Code']
        branch_name = row['Branch_Name']
        branch_code = row['Branch_Code']

        # If we have a last_date to resume from, skip all previous dates
        if last_date:
            # Convert last_date from 'YYYY-M-D' to 'YYYY-MM-DD' if needed
            # or handle it if it is already 'YYYY-MM-DD'.
            reversed_date = last_date
            try:
                # Attempt to parse in case last_date is already 'YYYY-M-D'
                reversed_date = transform_date_reverse(last_date)
            except Exception:
                # We assume it's already 'YYYY-MM-DD'
                pass

            # Find the position of reversed_date in Ori_Tradingdate
            match_idx = Ori_Tradingdate[Ori_Tradingdate.str_date == reversed_date].index
            if not match_idx.empty:
                Tradingdate = Ori_Tradingdate.iloc[match_idx[0] + 1:]
            else:
                Tradingdate = Ori_Tradingdate  # fallback
            last_date = None
        else:
            Tradingdate = Ori_Tradingdate

        # Prepare the date list
        str_date_list = Tradingdate['str_date'].tolist()
        data_tables = []

        # Fetch data in chunks of 10 dates
        for i in range(0, len(str_date_list), 10):
            date_slice = str_date_list[i:i+10]
            logger.info(
                f"========== Processing Broker: {branch_code}, "
                f"Dates: {date_slice[0]} to {date_slice[-1]} ============"
            )

            # Download the HTML
            soup_date_list = new_download_broker_data(broker_code, branch_code, date_slice)

            # Parse data and store in data_tables
            for html_content, date_str in soup_date_list:
                if not html_content:
                    # If html_content is None, it means an error occurred
                    # Already logged in fetch_async, but you can add more if needed
                    continue

                real_soup = BeautifulSoup(html_content, 'html.parser')
                try:
                    # Attempt to find the relevant table(s)
                    table_rows = real_soup.find('table')
                    if not table_rows:
                        logger.warning(
                            f"No main <table> found for Broker {branch_code}, date {date_str}"
                        )
                        continue
                    
                    # The indexing logic might need to be more robust in case the structure changes
                    data_row = table_rows.find_all('tr')[5]
                    if not data_row:
                        logger.warning(
                            f"Cannot find row index 5 for Broker {branch_code}, date {date_str}"
                        )
                        continue

                    # Extract numbers (t3n1) and names (t4t1)
                    numbers = data_row.find('table').find_all('td', class_='t3n1')
                    names = data_row.find('table').find_all('td', class_='t4t1')

                    # Construct the partial dataframe
                    data_table = pd.concat([extract_name(names), extract_number(numbers)], axis=1)
                    data_table['Branch'] = branch_name
                    data_table['Date'] = date_str
                    data_table['Branch_Code'] = branch_code
                    data_tables.append(data_table)

                except Exception as e:
                    logger.error(
                        f"Error parsing HTML for Broker {branch_code}, date {date_str}: {e}",
                        exc_info=True
                    )

            # Write partial results to disk if we have at least 20 items
            if len(data_tables) > 20:
                combined_df = pd.concat(data_tables, ignore_index=True)
                if not os.path.isfile(Saved_path):
                    combined_df.to_csv(Saved_path, index=False)
                else:
                    combined_df.to_csv(Saved_path, mode='a', index=False, header=False)
                
                # Clear the list
                data_tables = []

            time.sleep(0.25)  # being nice to the server

        # After finishing the broker, write remaining data (if any)
        if data_tables:
            combined_df = pd.concat(data_tables, ignore_index=True)
            if not os.path.isfile(Saved_path):
                combined_df.to_csv(Saved_path, index=False)
            else:
                combined_df.to_csv(Saved_path, mode='a', index=False, header=False)
            data_tables = []

# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    Tradingdatefile_path = '~/Stock_project/TW_stock_data/Tradingdate.csv'
    Brokerlist_path = '~/Stock_project/TW_stock_data/big_branch_list.csv'
    Saved_path = '~/Stock_project/TW_stock_data/broker_trading_list.csv'

    go_through_dates(Tradingdatefile_path, Brokerlist_path, Saved_path)
