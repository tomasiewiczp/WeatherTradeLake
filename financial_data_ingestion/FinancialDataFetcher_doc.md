
# FinancialDataFetcher Module Documentation

## Overview
The Market Data module (`FinancialDataFetcher`) is designed to fetch, process, and store historical financial data for major indices such as the Dow Jones, NASDAQ, and S&P 500. The data is retrieved using Yahoo Finance, and it is saved in a structured format into a data lake. This module allows incremental loading of data to maintain up-to-date information for analysis purposes.

## Features
- Fetches historical market data for a defined set of indices using Yahoo Finance.
- Automatically retries in case of transient network issues to improve reliability.
- Maintains a record of the last loaded date, allowing incremental loading of new data.
- Saves data in a structured format, organized by year, month, and day.

## Installation
To use this module, ensure the following Python packages are installed:
- `yfinance`
- `pandas`
- `logging`

You can install the required packages with:
```sh
pip install yfinance pandas
```

## Usage
### Example Usage
```python
from market_data import MarketData

# Create an instance of MarketData
c1 = MarketData()

# Download new market data and save it to the data lake
c1.download_new_market_data()
```

### Methods
1. **`download_new_market_data()`**: Orchestrates the entire process of fetching new market data and saving it to the data lake. It ensures the data is saved in a structured format organized by index, year, month, and day.
2. **Private Methods**:
   - **`_fetch_yahoo_finance_data()`**: Fetches the market data from Yahoo Finance for each index.
   - **`_get_last_loaded_date()`**: Reads the last loaded date from the data lake configuration file.
   - **`_update_last_loaded_date(date)`**: Updates the last loaded date in the data lake configuration file.
   - **`_save_new_market_data_to_data_lake()`**: Saves fetched market data in a structured format.

## Data Lake Structure
The data lake structure for market data is as follows:
```
data_lake/
  raw/
    market_data/
      dowjones/
        2024/
          10/
            dowjones_2024-10-10.csv
            dowjones_2024-10-11.csv
      nasdaq/
        2024/
          10/
            nasdaq_2024-10-10.csv
            nasdaq_2024-10-11.csv
      sp500/
        2024/
          10/
            sp500_2024-10-10.csv
            sp500_2024-10-11.csv
```

## Configuration
The module saves the last loaded date in the following file to keep track of incremental loading:
```
data_lake/
  config/
    last_loaded_date.txt
```

## Important Notes
- Ensure you have internet access while using this module as it relies on Yahoo Finance for data retrieval.
- Make sure the data lake directory structure exists, or the module will create it automatically.

