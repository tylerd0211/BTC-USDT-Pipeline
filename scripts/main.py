from datetime import datetime, timedelta
import os
import ccxt
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Environment Variables
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
QUESTDB_HOST = os.getenv("QUESTDB_HOST", "questdb")
QUESTDB_PORT = int(os.getenv("QUESTDB_PORT", 8812))
QUESTDB_USER = os.getenv("QUESTDB_USER", "admin")
QUESTDB_PASSWORD = os.getenv("QUESTDB_PASSWORD", "quest")
QUESTDB_DB = os.getenv("QUESTDB_DB", "qdb")

# Connect to QuestDB
def connect_to_questdb():
    return psycopg2.connect(
        host=QUESTDB_HOST,
        port=QUESTDB_PORT,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        database=QUESTDB_DB,
    )

# Fetch Historical Data Incrementally
def fetch_and_store_data(symbol, timeframe="1m", batch_size=1000):
    conn = connect_to_questdb()
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS btc_usdt (
            time TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            readable_time STRING
        ) TIMESTAMP(time) PARTITION BY DAY;
    """)
    conn.commit()

    # Fetch data
    exchange = ccxt.binance({
        "apiKey": BINANCE_API_KEY,
        "secret": BINANCE_SECRET_KEY,
    })

    # Start fetching from Binance inception
    since = int((datetime(2010, 1, 1)).timestamp() * 1000)
    print(f"Fetching data starting from {datetime.fromtimestamp(since / 1000)}")

    while True:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=batch_size)
        if not ohlcv:
            break

        # Transform to DataFrame
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["readable_time"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Insert into QuestDB
        data = df.values.tolist()
        execute_values(cursor, """
            INSERT INTO btc_usdt (time, open, high, low, close, volume, readable_time)
            VALUES %s;
        """, data)
        conn.commit()
        print(f"Inserted {len(data)} rows into QuestDB.")

        # Update since to the last fetched timestamp
        since = ohlcv[-1][0] + 60000  # Move to the next minute

    cursor.close()
    conn.close()
    print("Data fetching and insertion complete!")

if __name__ == "__main__":
    fetch_and_store_data("BTC/USDT")

