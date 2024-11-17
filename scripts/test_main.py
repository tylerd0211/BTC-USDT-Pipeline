import os
from dotenv import load_dotenv
import ccxt

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Fetch API keys from environment variables
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
    BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

    # Check if keys are loaded properly
    if not BINANCE_API_KEY or not BINANCE_SECRET_KEY:
        print("Error: API keys are not set in the .env file.")
        return

    print("API Key and Secret Key successfully loaded.")

    # Initialize Binance exchange
    try:
        exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_SECRET_KEY,
        })

        # Test authenticated API call: Fetch account balance
        print("Testing account balance API...")
        balance = exchange.fetch_balance()
        print("Account balance fetched successfully.")
        print("Available balance:", balance['total'])  # Print total balance

        # Test public API call: Fetch historical data for BTC/USDT
        print("\nTesting historical data API for BTC/USDT...")
        symbol = "BTC/USDT"
        since = exchange.parse8601("2023-01-01T00:00:00Z")  # Fetch data since Jan 1, 2023
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="1d", since=since, limit=5)  # Fetch 5 days of data
        print("Historical data fetched successfully:")
        for candle in ohlcv:
            print({
                "timestamp": exchange.iso8601(candle[0]),
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "volume": candle[5],
            })

    except Exception as e:
        print(f"Error during API test: {e}")

if __name__ == "__main__":
    main()

