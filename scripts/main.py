from dotenv import load_dotenv
import os
import ccxt
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

# Explicitly load the .env file from the root directory
load_dotenv(dotenv_path="/app/.env")

# Fetch environment variables
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_PORT = int(os.getenv("QUESTDB_PORT", 8812))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

# Connect to QuestDB
def connect_to_questdb():
    try:
        conn = psycopg2.connect(
            host=QUESTDB_HOST,
            port=QUESTDB_PORT,
            user=QUESTDB_USER,
            password=QUESTDB_PASSWORD,
            database=QUESTDB_DB,
        )
        print("Connected to QuestDB successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to QuestDB: {e}")
        exit()

# Create table in QuestDB
def create_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS btc_usdt (
        time TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE
    ) TIMESTAMP(time) PARTITION BY DAY;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            print("Table btc_usdt ensured in QuestDB.")
    except Exception as e:
        print(f"Error creating table in QuestDB: {e}")
        conn.rollback()
        exit()

# Transform raw OHLCV data into a DataFrame
def transform_to_dataframe(ohlcv):
    try:
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")  # Convert to datetime
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
        print("Transformed raw data into DataFrame.")
        return df
    except Exception as e:
        print(f"Error transforming data to DataFrame: {e}")
        exit()

# Fetch historical data from Binance
def fetch_historical_data(symbol, since, limit=1000):
    try:
        exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_SECRET_KEY,
        })

        # Fetch OHLCV data
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="1d", since=since, limit=limit)
        print(f"Fetched {len(ohlcv)} rows of historical data.")
        return ohlcv
    except Exception as e:
        print(f"Error fetching historical data: {e}")
        exit()

# Insert DataFrame into QuestDB
def insert_dataframe_to_questdb(conn, df):
    insert_query = """
    INSERT INTO btc_usdt (time, open, high, low, close, volume)
    VALUES %s;
    """
    try:
        data = df.values.tolist()  # Convert DataFrame rows to a list of tuples
        with conn.cursor() as cursor:
            execute_values(cursor, insert_query, data)
            conn.commit()
            print(f"Inserted {len(data)} rows into QuestDB.")
    except Exception as e:
        print(f"Error inserting data into QuestDB: {e}")
        conn.rollback()

# Main function
def main():
    # Validate API keys
    if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
        print("Error: Binance API Key or Secret Key is missing.")
        exit()

    # Initialize parameters
    symbol = "BTC/USDT"
    now = datetime.utcnow()
    ten_years_ago = now - timedelta(days=365 * 10 + 1)  # Include one-day delay
    since = int(ten_years_ago.timestamp() * 1000)  # Convert to milliseconds

    # Fetch raw data
    print("Fetching historical data from Binance...")
    ohlcv = fetch_historical_data(symbol, since)

    # Transform raw data into a DataFrame
    print("Transforming raw data into a DataFrame...")
    df = transform_to_dataframe(ohlcv)

    # Connect to QuestDB
    conn = connect_to_questdb()

    # Ensure table exists
    create_table(conn)

    # Insert DataFrame into QuestDB
    print(f"Inserting {len(df)} rows into QuestDB...")
    insert_dataframe_to_questdb(conn, df)

    # Close connection
    conn.close()
    print("Data insertion complete!")

if __name__ == "__main__":
    main()

