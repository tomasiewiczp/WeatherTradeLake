import logging
import os
from financial_data_ingestion.FinancialDataFetcher import FinancialDataFetcher
from weather_data_ingestion.WeatherDataFetcher import WeatherDataFetcher
from transformations.IntermediateDataProcessor import IntermediateDataProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)



def main():
    """
    Main script to orchestrate the downloading of both market and weather data,
    and saving them to the data lake.
    """
    logging.info("Starting the data collection process...")
    
    # Download market data
    try:
        market_data = FinancialDataFetcher()
        market_data.download_new_market_data()
        logging.info("Market data download completed.")
    except Exception as e:
        logging.error(f"An error occurred while downloading market data: {e}")
    
    # Download weather data
    try:
        weather_data = WeatherDataFetcher()
        weather_data.download_new_weather_data()
        logging.info("Weather data download completed.")
    except Exception as e:
        logging.error(f"An error occurred while downloading weather data: {e}")
    
    # Process raw data and load to silver layer
    logging.info("Data collection process completed.")
    processor = IntermediateDataProcessor()
    processor.process_weather_values()
    processor.process_market_values()

if __name__ == "__main__":
    main()