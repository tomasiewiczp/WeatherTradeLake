import requests
import logging
import time
import os
from datetime import datetime
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)

class WeatherDataFetcher():
    """
    A class to manage weather data fetched from a free weather API for New York City.
    """
    def __init__(self):
        """
        Initializes the WeatherDataFetcher instance without fetching data immediately.
        """
        logging.info("Initializing WeatherDataFetcher instance...")
        self._weather_data = None
        logging.info("WeatherDataFetcher instance initialized.")

    def _get_last_loaded_date(self):
        """
        Reads the last loaded date from a file in the data lake.

        Returns
        -------
        datetime or None
            The datetime of the last data load or None if the file does not exist.
        """
        file_path = os.path.join("data_lake", "config", "last_loaded_weather_date.txt")
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
        file_path = os.path.join( "data_lake", "config", "last_loaded_weather_date.txt")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as file:
            file.write(date.isoformat())

    def _fetch_weather_data(self):
        """
        Fetches historical weather data for New York City using a free weather API.
        Tries multiple times in case of transient network errors.

        Returns
        -------
        DataFrame
            A DataFrame containing historical weather data.
        """
        api_url = "https://api.open-meteo.com/v1/forecast"
        data = []

        logging.info("Fetching weather data for New York City...")
        last_loaded_date = self._get_last_loaded_date()
        start_date = last_loaded_date if last_loaded_date else datetime(2024, 7, 11).date()
        end_date = datetime.now().date()

        current_date = start_date
        while current_date <= end_date:
            retries = 3
            for attempt in range(retries):
                try:
                    logging.info(f"Fetching weather data for {current_date}...")
                    response = requests.get(api_url, params={
                        "latitude": 40.7128,
                        "longitude": -74.0060,
                        "hourly": "temperature_2m,precipitation,cloudcover,windspeed_10m",
                        "start_date": current_date.strftime("%Y-%m-%d"),
                        "end_date": current_date.strftime("%Y-%m-%d")
                    })
                    response.raise_for_status()
                    hourly_data = response.json()["hourly"]
                    for i in range(len(hourly_data["temperature_2m"])):
                        data.append({
                            "date": current_date.strftime("%Y-%m-%d"),
                            "hour": i,
                            "temperature_2m": hourly_data["temperature_2m"][i],
                            "precipitation": hourly_data["precipitation"][i],
                            "cloudcover": hourly_data["cloudcover"][i],
                            "windspeed_10m": hourly_data["windspeed_10m"][i]
                        })
                    logging.info(f"Successfully fetched weather data for {current_date}")
                    break
                except Exception as e:
                    logging.error(f"Failed to fetch weather data for {current_date} on attempt {attempt + 1}: {e}")
                    if attempt < retries - 1:
                        logging.info(f"Retrying fetching weather data for {current_date}...")
                        time.sleep(2)
                    else:
                        logging.error(f"All retries failed for {current_date}")
            current_date += pd.Timedelta(days=1).to_pytimedelta()

        df = pd.DataFrame(data)
        self._update_last_loaded_date(end_date)
        logging.info("Finished fetching weather data.")
        return df

    def _save_new_weather_data_to_data_lake(self):
        """
        Saves the new weather data into the data lake, organized by year, month, and day.
        """
        if self._weather_data is None:
            logging.error("No weather data available to save. Please fetch the data first.")
            return

        logging.info("Saving new weather data to data lake...")
        self._weather_data['date'] = pd.to_datetime(self._weather_data['date'])
        grouped = self._weather_data.groupby('date')
        for date, group in grouped:
            year_str = date.strftime("%Y")
            month_str = date.strftime("%m")
            day_str = date.strftime("%Y-%m-%d")
            file_path = os.path.join( "data_lake", "raw", "weather_data", year_str, month_str, f"weather_{day_str}.csv")
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # Save all hourly data for the day in a single file
            group.to_csv(file_path, index=False)
        logging.info("Finished saving new weather data to data lake.")

    def download_new_weather_data(self):
        """
        Downloads new weather data for New York City, processes it, and saves it to the data lake.
        
        This method orchestrates the entire workflow for retrieving and storing weather data. It first fetches the data
        from the weather API, retries in case of transient issues, and ensures the data is saved to the data lake in a
        structured format. The data is organized by year, month, and day, making it easy to access and manage for
        further analysis.
        """
        logging.info("Downloading new weather data...")
        self._weather_data = self._fetch_weather_data()
        self._save_new_weather_data_to_data_lake()
        logging.info("New weather data downloaded and saved to data lake.")
