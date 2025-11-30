import os
import re
import sys
import time
import random
import logging
import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

# -------------------------------------------------------------------
# Create the "logs" directory if it doesn't exist yet.
os.makedirs("logs", exist_ok=True)

# Generate a filename that includes the local time (YearMonthDay_HourMinuteSecond).
# Example filename: logs/broker_data_20241226_071759.log
log_filename = time.strftime("logs/broker_data_%Y%m%d_%H%M%S.log", time.localtime())

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Global User Agents, Config
# -------------------------------------------------------------------
MAX_RETRIES = 4       # Number of attempts before giving up
RETRY_DELAY = 2       # Seconds to wait between retries
CHUNK_SIZE = 10       # Number of dates fetched at once per broker
BROKER_CONCURRENCY = 5  # How many brokers to process concurrently

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

# -------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------
def transform_date(date_str):
    """
    Transforms a date string from 'YYYY-MM-DD' to 'YYYY-M-D'.
    """
    year, month, day = date_str.split('-')
    return f"{year}-{int(month)}-{int(day)}"


def transform_date_reverse(date_str):
    """
    Transforms a date string from 'YYYY-M-D' to 'YYYY-MM-DD'.
    """
    year, month, day = date_str.split('-')
    return f"{year}-{int(month):02d}-{int(day):02d}"


def construct_url(brokerHQ_id, broker_id, c, start_date, end_date):
    """
    Constructs the URL with query parameters given broker IDs and dates.
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

# -------------------------------------------------------------------
# Async Fetch
# -------------------------------------------------------------------
async def fetch_async(session, url, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    """
    Asynchronously fetches the text content of the provided URL with retry logic.
    """
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                response.raise_for_status()
                return await response.text(errors="replace")

        except asyncio.TimeoutError as e:
            logger.warning(
                f"TimeoutError on attempt {attempt}/{max_retries} for {url}: {e}"
            )
            if attempt == max_retries:
                logger.error(f"Max retries reached. Giving up on {url}")
                return None
            await asyncio.sleep(retry_delay)

        except asyncio.CancelledError as e:
            logger.error(f"Request cancelled for {url}: {e}")
            raise

        except Exception as e:
            logger.error(
                f"Error fetching {url} on attempt {attempt}/{max_retries}: {e}",
                exc_info=True
            )
            if attempt == max_retries:
                logger.error(f"Max retries reached. Giving up on {url}")
                return None
            await asyncio.sleep(retry_delay)

    return None

# -------------------------------------------------------------------
# Parsing Functions
# -------------------------------------------------------------------
def extract_code_name(html_fragment):
    """
    Extract Ticker and Name from the given HTML fragment.
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

    # --- Case B: Otherwise, look for <script> with GenLink2stk(...) ---
    script_tag = td.find('script')
    if script_tag and script_tag.string:
        match_genlink = re.search(r"GenLink2stk\('(AS)?(.*?)','(.*?)'\);", script_tag.string)
        if match_genlink:
            code = str(match_genlink.group(2))
            name = match_genlink.group(3)
            return pd.Series({'Ticker': code, 'Name': name})

    # Fallback
    return pd.Series({'Ticker': None, 'Name': None})


def extract_name(names):
    """
    Iterate over a list of <td> elements (class='t4t1') and extract Ticker/Name info.
    """
    df_list = []
    for n in names:
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
    """
    number_list = []
    for i in range(0, len(numbers), 3):
        if i + 2 < len(numbers):
            number_list.append({
                'buy':  numbers[i].get_text(strip=True),
                'sell': numbers[i+1].get_text(strip=True),
                'diff': numbers[i+2].get_text(strip=True)
            })
    return pd.DataFrame(number_list)

# -------------------------------------------------------------------
# ASYNC: Download for a single chunk of dates (for one broker)
# -------------------------------------------------------------------
async def download_chunk(session, brokerHQ_id, broker_id, date_slice):
    """
    Given a chunk of dates, concurrently fetch all HTML pages for these dates,
    then return a list of (html_content, date_str).
    """
    tasks = []
    for date_str in date_slice:
        url = construct_url(brokerHQ_id, broker_id, 'E', date_str, date_str)
        tasks.append(asyncio.create_task(fetch_async(session, url)))
    results = await asyncio.gather(*tasks)
    return list(zip(results, date_slice))

# -------------------------------------------------------------------
# ASYNC: Process a single broker
# -------------------------------------------------------------------
async def process_broker(
    session,
    brokerHQ_id,
    branch_name,
    branch_code,
    new_dates_df,
    Saved_path
):
    """
    For a single broker (branch_code), fetch data for all `new_dates_df`,
    parse them chunk-by-chunk, and append to CSV.
    """
    str_date_list = new_dates_df['str_date'].tolist()
    data_tables = []

    for i in range(0, len(str_date_list), CHUNK_SIZE):
        date_slice = str_date_list[i:i + CHUNK_SIZE]
        logger.info(
            f"========== Processing Broker: {branch_code}, "
            f"Dates: {date_slice[0]} to {date_slice[-1]} ============"
        )

        # Download concurrently for these date_slice
        soup_date_list = await download_chunk(session, brokerHQ_id, branch_code, date_slice)

        # Parse each date's HTML
        for html_content, date_str in soup_date_list:
            if not html_content:
                # If None, an error or timeout occurred
                continue

            real_soup = BeautifulSoup(html_content, 'html.parser')
            try:
                table_rows = real_soup.find('table')
                if not table_rows:
                    logger.warning(f"No main <table> found for Broker {branch_code}, date {date_str}")
                    continue

                data_row = table_rows.find_all('tr')[5]
                if not data_row:
                    logger.warning(f"Cannot find row index 5 for Broker {branch_code}, date {date_str}")
                    continue

                numbers = data_row.find('table').find_all('td', class_='t3n1')
                names = data_row.find('table').find_all('td', class_='t4t1')

                # Construct partial dataframe
                data_table = pd.concat([
                    extract_name(names),
                    extract_number(numbers)
                ], axis=1)
                data_table['Branch'] = branch_name
                data_table['Date'] = date_str
                data_table['Branch_Code'] = branch_code
                data_tables.append(data_table)

            except Exception as e:
                logger.error(
                    f"Error parsing HTML for Broker {branch_code}, date {date_str}: {e}",
                    exc_info=True
                )

        # PARTIAL SAVE if there's a significant amount of data
        if len(data_tables) > 20:
            combined_df = pd.concat(data_tables, ignore_index=True)
            if not os.path.isfile(Saved_path):
                combined_df.to_csv(Saved_path, index=False)
            else:
                combined_df.to_csv(Saved_path, mode='a', index=False, header=False)
            data_tables.clear()

        # Be kind to the server
        await asyncio.sleep(0.25)

    # After finishing all chunks for this broker, do a final save if there's leftover data
    if data_tables:
        combined_df = pd.concat(data_tables, ignore_index=True)
        if not os.path.isfile(Saved_path):
            combined_df.to_csv(Saved_path, index=False)
        else:
            combined_df.to_csv(Saved_path, mode='a', index=False, header=False)
        data_tables.clear()

    logger.info(f"--- Finished broker: {branch_code} ---")

# -------------------------------------------------------------------
# ASYNC: A wrapper to run the actual process_broker with concurrency
# -------------------------------------------------------------------
async def run_with_semaphore(
    session,
    semaphore,
    brokerHQ_id,
    branch_name,
    branch_code,
    new_dates_df,
    Saved_path
):
    """
    Wrap the process_broker logic in a semaphore to limit concurrency
    across all brokers.
    """
    async with semaphore:
        await process_broker(
            session,
            brokerHQ_id,
            branch_name,
            branch_code,
            new_dates_df,
            Saved_path
        )

# -------------------------------------------------------------------
# ASYNC: Main logic to process all brokers in parallel
# -------------------------------------------------------------------
async def main_async(Ori_Tradingdate, Brockerlist, saved_df, Saved_path):
    """
    Creates tasks for each broker and runs them concurrently with a semaphore limit.
    """
    async with aiohttp.ClientSession() as session:
        # We will limit concurrency to avoid overloading the server
        semaphore = asyncio.Semaphore(BROKER_CONCURRENCY)
        tasks = []

        for idx, row in Brockerlist.iterrows():
            broker_code = row['Broker_Code']
            branch_name = row['Branch_Name']
            branch_code = row['Branch_Code']

            # 1) Find the max date we have already
            broker_saved = saved_df[saved_df['Branch_Code'] == branch_code]
            if not broker_saved.empty:
                max_broker_date = broker_saved['Date'].max()  # a Timestamp
            else:
                max_broker_date = None

            logger.info(f"--- Checking broker: {branch_code}. Last known date: {max_broker_date} ---")

            # 2) Filter the trading dates strictly after max_broker_date
            if max_broker_date is not None and pd.notnull(max_broker_date):
                new_dates_df = Ori_Tradingdate[Ori_Tradingdate['str_date_dt'] > max_broker_date]
            else:
                new_dates_df = Ori_Tradingdate

            if new_dates_df.empty:
                logger.info(f"No new dates for broker {branch_code}. Skipping.")
                continue

            # 3) Create a task for each broker
            task = asyncio.create_task(
                run_with_semaphore(
                    session,
                    semaphore,
                    broker_code,
                    branch_name,
                    branch_code,
                    new_dates_df,
                    Saved_path
                )
            )
            tasks.append(task)

        # Wait for all brokers to finish
        await asyncio.gather(*tasks)

    logger.info("=== All brokers processed ===")

# -------------------------------------------------------------------
# SYNC: Entry function
# -------------------------------------------------------------------
def go_through_dates(Tradingdatefile_path, Brokerlist_path, Saved_path):
    """
    Main driver function (synchronous) that:
      1) Reads CSV data
      2) Determines new dates per broker
      3) Runs the async logic to fetch/parse in parallel
      4) Saves results to CSV (incrementally).
    """
    # Expand user paths ~ => /home/<user>, etc.
    Tradingdatefile_path = os.path.expanduser(Tradingdatefile_path)
    Brokerlist_path = os.path.expanduser(Brokerlist_path)
    Saved_path = os.path.expanduser(Saved_path)

    # 1) Read the trading dates
    Ori_Tradingdate = pd.read_csv(Tradingdatefile_path)
    if 'str_date' not in Ori_Tradingdate.columns:
        raise ValueError("Tradingdate CSV must have a column named 'str_date'.")

    Ori_Tradingdate['str_date_dt'] = pd.to_datetime(
        Ori_Tradingdate['str_date'],
        format='%Y-%m-%d',
        errors='coerce'
    )
    if Ori_Tradingdate['str_date_dt'].isnull().any():
        raise ValueError("Some dates in Tradingdatefile are not parseable as YYYY-MM-DD.")

    # 2) Read the broker list
    Brockerlist = pd.read_csv(Brokerlist_path,
                              dtype={"Broker_Code":"string",
                                     "Broker_Name":"string",
                                     "Branch_Code":"string",
                                     "Branch_Name":"string"})
    required_cols = {'Broker_Code', 'Branch_Name', 'Branch_Code'}
    if not required_cols.issubset(set(Brockerlist.columns)):
        raise ValueError(f"Brokerlist CSV must have columns: {required_cols}")

    # 3) Load or init the saved data
    if os.path.isfile(Saved_path):
        saved_df = pd.read_csv(Saved_path,
                               thousands=',',
                               dtype={"Ticker":"string",
                                    "Name":"string",
                                    "buy":int,
                                    "sell":int,
                                    "diff":int,
                                    "Branch":"string",
                                    "Date":"string",
                                    "Branch_Code":"string"})
        if not saved_df.empty:
            if 'Date' not in saved_df.columns:
                raise ValueError("Saved CSV must have a 'Date' column.")
            saved_df['Date'] = pd.to_datetime(saved_df['Date'], format='%Y-%m-%d', errors='coerce')
        else:
            saved_df = pd.DataFrame(columns=['Branch_Code', 'Date'])
    else:
        saved_df = pd.DataFrame(columns=['Branch_Code', 'Date'])

    logger.info("=== Starting daily update process (async version) ===")

    # 4) Run async logic
    asyncio.run(main_async(Ori_Tradingdate, Brockerlist, saved_df, Saved_path))


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
if __name__ == '__main__':
    Tradingdatefile_path = '~/Documents/Dev/Cheater_finder/Stock_project/TW_stock_data/Tradingdate.csv'
    Brokerlist_path       = '~/Documents/Dev/Cheater_finder/Stock_project/TW_stock_data/big_branch_list.csv'
    Saved_path            = '~/Documents/Dev/Cheater_finder/Stock_project/TW_stock_data/broker_trading_list.csv'

    go_through_dates(Tradingdatefile_path, Brokerlist_path, Saved_path)
