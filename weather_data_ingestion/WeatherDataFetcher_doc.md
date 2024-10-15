# Weather Data Module Documentation

## Overview
The Weather Data module (`WeatherData`) is designed to fetch, process, and store historical weather data for New York City. The data is retrieved using the Open-Meteo API, and it is saved in a structured format into a data lake. This module allows incremental loading of data to maintain up-to-date information for analysis purposes.

## Features
- Fetches historical weather data, including temperature, precipitation, cloud cover, and wind speed.
- Automatically retries in case of transient network issues to improve reliability.
- Maintains a record of the last loaded date, allowing incremental loading of new data.
- Saves data in a structured format, organized by year, month, and day.

## Installation
To use this module, ensure the following Python packages are installed:
- `requests`
- `pandas`
- `logging`

You can install the required packages with:
```sh
pip install requests pandas
```

## Usage
### Example Usage
```python
from weather_data import WeatherData

# Create an instance of WeatherData
c2 = WeatherData()

# Download new weather data and save it to the data lake
c2.download_new_weather_data()
```

### Methods
1. **`download_new_weather_data()`**: Orchestrates the entire process of fetching new weather data and saving it to the data lake. It ensures the data is saved in a structured format organized by year, month, and day.
2. **Private Methods**:
   - **`_fetch_weather_data()`**: Fetches the weather data from the Open-Meteo API for each day.
   - **`_get_last_loaded_date()`**: Reads the last loaded date from the data lake configuration file.
   - **`_update_last_loaded_date(date)`**: Updates the last loaded date in the data lake configuration file.
   - **`_save_new_weather_data_to_data_lake()`**: Saves fetched weather data in a structured format.

## Data Lake Structure
The data lake structure for weather data is as follows:
```
data_lake/
  raw/
    weather_data/
      2024/
        10/
          weather_2024-10-10.csv
          weather_2024-10-11.csv
```

## Configuration
The module saves the last loaded date in the following file to keep track of incremental loading:
```
data_lake/
  config/
    last_loaded_weather_date.txt
```

## Important Notes
- Ensure you have internet access while using this module as it relies on the Open-Meteo API for data retrieval.
- Make sure the data lake directory structure exists, or the module will create it automatically.

