import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class IntermediateDataProcessor():
    """
    A class to process and aggregate market and weather data using PySpark.
    """

    def __init__(self):
        """
        Initialize Spark session and set up paths and indexes.
        """
        self.spark = SparkSession.builder.appName("Spark_intermediate_calculation").getOrCreate()
        self.config_path = "data_lake/config/last_processed_date.txt"
        self.indexes = {
            "dowjones": "data_lake/raw/market_data/dowjones/",
            "nasdaq": "data_lake/raw/market_data/nasdaq/",
            "sp500": "data_lake/raw/market_data/sp500/"
        }
        self.last_processed_date = self._get_last_processed_date()
        self.raw_weather_path = "data_lake/raw/weather_data/"
        self.silver_weather_path = "data_lake/silver/weather_data/"
        logging.info(f"Initialized IntermediateDataProcessor with last_processed_date: {self.last_processed_date}")

    def _get_last_processed_date(self):
        """
        Load the last processed date from the config file, or use a default date if not found.

        Returns:
            str: The last processed date.
        """
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as file:
                date = file.read().strip()
                logging.info(f"Loaded last processed date: {date}")
                return date
        else:
            logging.info("No last processed date found. Using default date: 2024-01-01")
            return "2024-01-01"

    def _update_last_processed_date(self, date):
        """
        Update the config file with the new last processed date.

        Args:
            date (datetime): The new last processed date.
        """
        with open(self.config_path, "w") as file:
            file.write(date.strftime("%Y-%m-%d"))
            logging.info(f"Updated last processed date to: {date.strftime('%Y-%m-%d')}")

    def process_market_values(self):
        """
        Process and aggregate market data for each index and save the results to the silver layer.
        """
        for index_name, index_path in self.indexes.items():
            logging.info(f"Processing market values for index: {index_name}")
            raw_data_path = f"{index_path}*/**/*.csv"
            new_market_data = self.spark.read.csv(raw_data_path, header=True, inferSchema=True)
            new_market_data = new_market_data.filter(F.col("date") > self.last_processed_date)

            if new_market_data.rdd.isEmpty():
                logging.info(f"No new data to process for {index_name}")
                continue

            logging.info(f"New data found for {index_name}, proceeding with processing")
            # Define window specification for calculating start and end values
            window_spec = Window.partitionBy("date").orderBy("date")
            df_with_open_close = new_market_data.withColumn("start_open", F.first("Open").over(window_spec))\
                                                .withColumn("end_close", F.last("Close").over(window_spec))

            # Add year_month column for monthly aggregation
            new_market_data = new_market_data.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))
            aggregated_df = new_market_data.groupBy("year_month").agg(
                F.sum("Volume").alias("max_volume"),            # Sum volume for each month
                F.first("Open").alias("start_open"),            # First open value as start_open
                F.last("Close").alias("end_close")              # Last close value as end_close
            )

            # Write aggregated data to Parquet files for each month
            for row in aggregated_df.collect():
                year, month = row['year_month'].split('-')
                output_path = f"data_lake/silver/market_data/{index_name}/{year}/{month}"
                logging.info(f"Writing aggregated data for {index_name}, year: {year}, month: {month} to {output_path}")
                aggregated_df.filter(F.col("year_month") == row['year_month']).drop("year_month")\
                    .write.mode("overwrite").parquet(output_path)

            # Update the last processed date
            max_date = new_market_data.agg(F.max("date")).collect()[0][0]
            if max_date:
                self._update_last_processed_date(max_date)

    def process_weather_values(self):
        """
        Process and aggregate weather data and save the results to the silver layer.
        """
        logging.info("Processing weather values")
        raw_data_path = f"{self.raw_weather_path}*/**/*.csv"
        new_weather_data = self.spark.read.csv(raw_data_path, header=True, inferSchema=True)

        if new_weather_data.rdd.isEmpty():
            logging.info("No new data to process for weather data")
            return

        logging.info("New weather data found, proceeding with processing")
        # Add year_month column for monthly aggregation
        new_weather_data = new_weather_data.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))

        # Aggregate data to monthly level
        aggregated_df = new_weather_data.groupBy("year_month", "date").agg(
            F.round(F.avg("temperature_2m"), 2).alias("avg_temperature"),            
            F.round(F.avg("precipitation"), 2).alias("avg_precipitation"),
            F.round(F.avg("cloudcover"), 2).alias("avg_cloudcover"),
            F.round(F.avg("windspeed_10m"), 2).alias("avg_windspeed")
        )

        # Write aggregated weather data to Parquet files for each month
        for row in aggregated_df.select("year_month").distinct().collect():
            year, month = row['year_month'].split('-')
            output_path = f"{self.silver_weather_path}{year}/{month}"
            logging.info(f"Writing aggregated weather data for year: {year}, month: {month} to {output_path}")
            aggregated_df.filter(F.col("year_month") == row['year_month']).write.mode("overwrite").parquet(output_path)