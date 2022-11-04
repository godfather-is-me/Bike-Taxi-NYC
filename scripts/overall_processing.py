import os
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, hour, dayofyear, dayofweek, monotonically_increasing_id, lit
from process_udfs import peak_hours
class Processing():
    def __init__(self):
        """
        Function to initialize the class and create sessions and use her
        """
        self.sp = (
            SparkSession
            .builder.appName("Processing")
            .config("spark.sql.session.timeZone", "-04")
            .getOrCreate()
            )
  
    def __del__(self):
        """
        Function to create desctructor class
        """
        self.sp.stop
        print("\nProcessing completed\n")

    def process(self):
        """
        Function to call all preprocessing functions
        """
        taxis = self.merge_taxis()
        bikes = self.merge_folder("citi")
        weather = self.read_weather()

        frames = {
            "taxis" : taxis,                    # Taking a 25% random sample due to Java Heap Space issues
            "bikes" :  self.match_columns(bikes)
        }

        # Similar steps for both
        for k in frames.keys():

            print("Starting processing for " + k)

            # Extract datetime feature
            frames[k] = self.datetime_features(frames[k])

            # Add Peaks UDFs
            frames[k] = frames[k].withColumn("peak_hours", peak_hours(col("time_bin")))
            # Merge traffic counts with data
            frames[k] = self.merge_traffic(frames[k])
            # Merge weather with data
            frames[k] = self.merge_on_date(frames[k], weather)

            # Filter further
            frames[k] = self.filter_processed(frames[k])

            # Drop unnecessary columns
            frames[k] = frames[k].drop("Date", "pickup_datetime", "dropoff_datetime")

            # Write dataframe
            self.write_processed(k, frames[k])

            print("The number of " + k + " after processing is: ", frames[k].count())

            # Show data
            frames[k].show(2)
  
    def merge_taxis(self):
        """
        Function to to merge all taxi columns
        """
        taxi_list = []
        for taxi in ["yellow", "green", "fhvhv"]:
            taxi_list.append(self.merge_folder(taxi))

        return reduce(DataFrame.unionAll, taxi_list)

    def merge_folder(self, taxi_type: str):
        """
        Function to merge everything within yellow or green or fhvhv
        """
        dir = "../data/curated/" + taxi_type +"/"
        folder_locs = os.listdir(dir)
        taxi_list = []
        for folder in folder_locs:
            path = dir + "/" + folder
            taxi_list.append(self.sp.read.parquet(path).sample(0.25, 69))

        return reduce(DataFrame.unionAll, taxi_list)
  
    def datetime_features(self, data):
        """
        Function to extract features directly from the datetime object
        """
        time_diff = col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")
        processed = (
            data
            .withColumn("year", year(data.pickup_datetime))
            .withColumn("month", month(data.pickup_datetime))
            .withColumn("day", dayofyear(data.pickup_datetime))
            .withColumn("time_bin", hour(data.pickup_datetime))
            .withColumn("day_of_week", dayofweek(data.pickup_datetime))
            .withColumn("trip_duration", time_diff)
        )
        return processed

    def match_columns(self, citi: DataFrame):
        """
        Function for citibike data to match the same columns names so 
        it can go through the same feature processing  
        """
        citi = citi.withColumnRenamed("started_at", "pickup_datetime")
        return citi.withColumnRenamed("ended_at", "dropoff_datetime")
    
    def merge_traffic(self, data):
        """
        Function to calculate the traffic count and merge it to respective data
        """
        data_count = data.groupBy(["year", "month", "day", "time_bin"]).count()
        return self.merge_on_date(data, data_count)
    
    def merge_on_date(self, left: DataFrame, right: DataFrame):
        """
        Function to merge right to left on inner left, on year, month, day, and time bin
        """
        date_merge = left.join(
            right,
            ["year", "month", "day", "time_bin"],
            how = "left"
        )
        # Drop duplicated columns
        return date_merge
    
    def read_weather(self):
        """
        Function to process the weather data in similar format so that it can be joined
        """
        weather = self.sp.read.option("header", True).option("inferSchema", True).csv("../data/raw/weather/weather.csv")
        weather = weather.withColumnRenamed("Time bin", "time_bin")
        categories = list(weather.select("Condition").distinct().toPandas()["Condition"])
        # Extract features
        weather = (weather
            .withColumn("year", year(weather.Date))
            .withColumn("month", month(weather.Date))
            .withColumn("day", dayofyear(weather.Date))
        ).drop("Condition")
        return weather
        
    def write_processed(self, data_name: str, data: DataFrame):
        """
        Function to write the processed data
        """
        # Check for parent directory
        output_dir = "../data/processed"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        # Split training and testing to test for 2022
        data.filter(col("year") != 2022).write.parquet(output_dir + "/" + data_name + "_train", mode="overwrite")
        data.filter(col("year") == 2022).write.parquet(output_dir + "/" + data_name + "_test", mode="overwrite")

        print("Finished processing the data for " + data_name)

    def filter_processed(self, data: DataFrame):
        """
        Function to further clean data and drop rows and columns using
        the information from processed data
        """
        # Filter by weekdays
        data = self.filter_weekdays(data)
        data = self.filter_triptime(data)

        return data

    def filter_weekdays(self, data: DataFrame):
        """
        Function to return only data from weekdays
        """
        return data.filter((col("day_of_week") > 1) & (col("day_of_week") < 7))

    def filter_triptime(self, data: DataFrame):
        """
        Function to filter trips that are over 2 hours
        """
        return data.filter(col("trip_duration") < 10800)