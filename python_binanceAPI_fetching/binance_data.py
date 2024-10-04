# binance_data.py

# Import necessary standard libraries
import os
import threading
import time
from datetime import datetime, timedelta

# Import third-party libraries
import pandas as pd
from binance.client import Client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the Binance API keys from environment variables
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

# Initialize the Binance client with API keys
client = Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_SECRET_KEY, tld="com")

def get_interval_milliseconds(interval):
    """
    Convert Binance interval strings to milliseconds.

    Args:
        interval (str): Interval string (e.g., '1m', '1h')

    Returns:
        int: Interval in milliseconds
    """
    ms = {
        '1m': 60 * 1000,
        '3m': 3 * 60 * 1000,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '30m': 30 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '2h': 2 * 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '6h': 6 * 60 * 60 * 1000,
        '8h': 8 * 60 * 60 * 1000,
        '12h': 12 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
        '3d': 3 * 24 * 60 * 60 * 1000,
        '1w': 7 * 24 * 60 * 60 * 1000,
        '1M': 30 * 24 * 60 * 60 * 1000,  # Approximate month as 30 days
    }
    return ms.get(interval, 60 * 1000)  # Default to 1 minute if interval not found

def get_history(symbol, interval, start_str, end_str=None):
    """
    Fetch historical candlestick data for a specific symbol and interval.

    Args:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
        interval (str): Time interval between data points (e.g., '1m', '1h')
        start_str (str): Start datetime in '%Y-%m-%d %H:%M:%S' format
        end_str (str): End datetime in '%Y-%m-%d %H:%M:%S' format (optional)

    Returns:
        pd.DataFrame: DataFrame containing historical data
    """
    try:
        print(f"Fetching data for {symbol} with interval {interval} from {start_str} to {end_str}")
        limit = 1000  # Maximum number of records per API request
        df_list = []

        # Convert start and end times to milliseconds
        start_ts = int(pd.to_datetime(start_str).timestamp() * 1000)
        end_ts = int(pd.to_datetime(end_str).timestamp() * 1000) if end_str else None

        while True:
            # Fetch data from Binance API
            bars = client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts
            )

            if not bars:
                # No more data to fetch
                break
            print(f"Number of items in each kline: {len(bars[0])}")

            # Convert API response to DataFrame
            df = pd.DataFrame(bars)

            # Rename columns to match the data
            if len(bars[0]) == 12:
                df.columns = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"]

            elif len(bars[0]) == 11:
                df.columns = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"]

            else:
                print(f"Unexpected number of columns: {len(bars[0])}")
                return pd.DataFrame()

            # Update start_ts for the next batch
            last_open_time = int(df.iloc[-1]['open_time'])
            start_ts = last_open_time + get_interval_milliseconds(interval)

            # Convert 'open_time' to datetime
            df['time'] = pd.to_datetime(df['open_time'], unit='ms')

            # Select relevant columns
            df = df[["time", "open", "high", "low", "close", "volume"]].copy()
            df_list.append(df)

            # Check if we have reached or passed the end time
            if end_ts and start_ts >= end_ts:
                break

        if df_list:
            # Concatenate all DataFrames into one
            result_df = pd.concat(df_list, ignore_index=True)
            return result_df
        else:
            print(f"No data returned for {symbol} from {start_str} to {end_str}")
            return pd.DataFrame()
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()

def fetch_historical_data(event, context):
    """
    Fetch historical data based on parameters from the event dictionary.

    Args:
        event (dict): Contains 'symbol', 'interval', 'start', and 'end' keys
        context: Not used (included for compatibility)

    Returns:
        pd.DataFrame: DataFrame containing the historical data
    """
    symbol = event.get('symbol', 'BTCUSDT')
    interval = event.get('interval', '1m')
    start_str = event.get('start', '2024-09-01 00:00:00')
    end_str = event.get('end', None)

    # Parse start and end dates
    start_time = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    if end_str:
        end_time = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
    else:
        end_time = datetime.utcnow()

    # Fetch historical data
    df = get_history(
        symbol=symbol,
        interval=interval,
        start_str=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        end_str=end_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    return df

def fetch_realtime_data(event, context):
    """
    Continuously fetch real-time data at the specified interval.

    Args:
        event (dict): Contains 'symbol' and 'interval' keys
        context: Not used (included for compatibility)
    """
    symbol = event.get('symbol', 'BTCUSDT')
    interval = event.get('interval', '1m')
    
    def fetch_data():
        while True:
            # Calculate the start and end times for the last interval
            current_time = datetime.utcnow()
            interval_ms = get_interval_milliseconds(interval)
            start_time = (current_time - timedelta(milliseconds=interval_ms)).strftime("%Y-%m-%d %H:%M:%S")
            end_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

            # Fetch the latest data
            df = get_history(symbol, interval, start_time, end_time)
            print(df)

            # Sleep until the next interval
            time.sleep(interval_ms / 1000)
    
    # Start the data fetching in a separate thread
    thread = threading.Thread(target=fetch_data)
    thread.daemon = True  # Allows the program to exit even if thread is running
    thread.start()
