import pandas as pd
import psycopg2
from binance.client import Client

def fetch_binance_data(symbol, interval, start_time, end_time, api_key, secret_key):
    """
    Fetch Binance data and return it as a DataFrame.
    """
    client = Client(api_key=api_key, api_secret=secret_key, tld="com")

    start_ts = int(pd.to_datetime(start_time).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_time).timestamp() * 1000)

    try:
        raw_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            startTime=start_ts,
            endTime=end_ts,
        )
        df = pd.DataFrame(raw_data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ])
        df["time"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df[["time", "open", "high", "low", "close", "volume"]]
        df[["open", "high", "low", "close", "volume"]] = df[[
            "open", "high", "low", "close", "volume"]].astype(float)
        return df
    except Exception as e:
        print(f"Error fetching Binance data: {e}")
        return pd.DataFrame()


def dump_to_questdb(df, table_name, db_config):
    """
    Insert the DataFrame into QuestDB.
    """
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            time TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
        ) TIMESTAMP(time);
        """
        cur.execute(create_table_query)

        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (time, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            cur.execute(insert_query, tuple(row))

        conn.commit()
        cur.close()
        conn.close()

        print(f"Successfully inserted {len(df)} rows into QuestDB table '{table_name}'.")

    except Exception as e:
        print(f"Error inserting data into QuestDB: {e}")

