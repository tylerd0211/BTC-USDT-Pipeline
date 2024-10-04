# main.py

# Import necessary modules
import time
from binance_data import fetch_historical_data, fetch_realtime_data

if __name__ == "__main__":
    # Define the parameters for data fetching
    event = {
        'symbol': 'BTCUSDT',
        'interval': '1m',
        'start': '2023-09-29 00:00:00',
        'end': '2023-09-30 00:00:00'  # Specify the end date if needed
    }

    # Fetch and print historical data
    historical_data = fetch_historical_data(event, None)
    print(historical_data)

    # Start real-time data fetching (optional)
    fetch_realtime_data({'symbol': 'BTCUSDT', 'interval': '1m'}, None)

    # Keep the main thread alive to allow the real-time data fetching thread to run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Program terminated by user.")
