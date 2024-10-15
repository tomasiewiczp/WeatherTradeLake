import yfinance as yf
import logging
import time
import os
from datetime import datetime
import pandas as pd

tickers = {
    "nasdaq": "^IXIC",
    "sp500": "^GSPC",
    "dowjones": "^DJI",
}
# Configure logging
logging.basicConfig(level=logging.INFO)

class FinancialDataFetcher():
    """
    A class to manage market data fetched from Yahoo Finance.
    """
    def __init__(self):
        """
        Initializes the FinancialDataFetcher instance without fetching data immediately.
        """
        logging.info("Initializing FinancialDataFetcher instance...")
        self._market_data = None
        logging.info("FinancialDataFetcher instance initialized.")

    def _get_last_loaded_date(self):
        """
        Reads the last loaded date from a file in the data lake.

        Returns
        -------
        datetime or None
            The datetime of the last data load or None if the file does not exist.
        """
        file_path = os.path.join( "data_lake", "config", "last_loaded_date.txt")
        print(f"!!!!!!!{file_path}")
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                last_date_str = file.read().strip()
                return datetime.fromisoformat(last_date_str).date()
        return None

    def _update_last_loaded_date(self, date):
        """
        Updates the last loaded date in a file in the data lake.

        Parameters
        ----------
        date : datetime
            The date to be saved as the last loaded date.
        """
        file_path = os.path.join("data_lake", "config", "last_loaded_date.txt")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            file.write(date.isoformat())

    def _fetch_yahoo_finance_data(self):
        """
        Fetches historical market data for tickers defined in the variables module.
        Tries multiple times in case of transient network errors.

        Returns
        -------
        dict
            A dictionary containing historical data for each ticker.
        """
        data = {}
        logging.info("Fetching Yahoo Finance data for tickers...")
        last_loaded_date = self._get_last_loaded_date()
        start_date_str = last_loaded_date.strftime("%Y-%m-%d") if last_loaded_date else "2023-01-01"

        for name, ticker in tickers.items():
            retries = 3
            for attempt in range(retries):
                try:
                    logging.info(f"Creating Ticker object for {name} ({ticker})...")
                    ticker_obj = yf.Ticker(ticker)
                    logging.info(f"Fetching historical data for {name} ({ticker}) starting from {start_date_str}...")
                    # Fetch historical data using a specific start date with hourly granularity
                    data[name] = ticker_obj.history(start=start_date_str, end=None, interval="1h")
                    logging.info(f"Successfully fetched data for {name} ({ticker})")
                    break
                except Exception as e:
                    logging.error(f"Failed to fetch data for {name} ({ticker}) on attempt {attempt + 1}: {e}")
                    if attempt < retries - 1:
                        logging.info(f"Retrying fetching data for {name} ({ticker})...")
                        time.sleep(2)
                    else:
                        logging.error(f"All retries failed for {name} ({ticker})")
        # Update the last loaded date to now after successful data load
        self._update_last_loaded_date(datetime.now().date())
        logging.info("Finished fetching Yahoo Finance data.")
        return data
    
    def _save_new_market_data_to_data_lake(self):
        """
        Saves the new market data into the data lake, organized by index, year, month, and day.
        """
        if self._market_data is None:
            logging.error("No market data available to save. Please fetch the data first.")
            return

        logging.info("Saving new market data to data lake...")
        for name, df in self._market_data.items():
            # Group data by day
            df['date'] = df.index.normalize()
            grouped = df.groupby('date')
            for date, group in grouped:
                year_str = date.strftime("%Y")
                month_str = date.strftime("%m")
                day_str = date.strftime("%Y-%m-%d")
                file_path = os.path.join("data_lake", "raw", "market_data", name, year_str, month_str, f"{name}_{day_str}.csv")
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                # Save all hourly data for the day in a single file
                group.to_csv(file_path, index=False)
        logging.info("Finished saving new market data to data lake.")

    def download_new_market_data(self):
        """
        Downloads new market data from Yahoo Finance, processes it, and saves it to the data lake.
        
        This method orchestrates the entire workflow for retrieving and storing market data. It first fetches the data
        for all defined tickers, retries in case of transient issues, and ensures the data is saved to the data lake
        in a structured format. The data is organized by index, year, month, and day, making it easy to access and
        manage for further analysis.
        """
        logging.info("Downloading new market data...")
        self._market_data = self._fetch_yahoo_finance_data()
        self._save_new_market_data_to_data_lake()
        logging.info("New market data downloaded and saved to data lake.")