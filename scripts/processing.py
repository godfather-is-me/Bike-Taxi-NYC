import os

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, hour, dayofyear, dayofweek, monotonically_increasing_id, lit

from process_udfs import peak_hours, weather_pattern, better_travel_type

class Processing():

    def __init__(self):
        """
        Function to initialize the class and create sessions and use her
        """
        self.sp = (
            SparkSession
            .builder.appName("Processing")
            .config("spark.sql.session.timeZone", "-04")
            #.config("spark.driver.memory", "8g")
            #.config("spark.executor.memory", "12g")
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
        Function to process data and merge the two datasets on common features
        """
        # Read data
        taxis = self.merge_taxis()
        bikes = self.merge_folder("citi")
        weather = self.read_weather()

        # Dataframes
        frames = {
            "taxis" : taxis,              
            "bikes" : self.match_columns(bikes)
        }

        for k in frames.keys():
            print("Start processing datetime and traffic for " + k)

            # Extract datetime feature
            frames[k] = self.datetime_features(frames[k], k)
            # Add traffic counts
            frames[k] = self.merge_traffic(frames[k], k)
            # Drop unnecessary columns
            frames[k] = frames[k].drop("pickup_datetime", "dropoff_datetime")

            print("The count for " + k + " is ", frames[k].count())

        # Merged dataset
        merged = self.merge_dataframes(frames["taxis"], frames["bikes"])

        # Add UDF data
        merged = self.add_udfs(merged)

        # Merge data with weather
        merged = self.merge_on_date(merged, weather)

        # Filter further on processed values ()
        merged = self.filter_processed(merged)

        # Drop weather's date column
        merged = merged.drop("Date")

        # Process merged dataset
        print("The count for the merged data is ", merged.count())

        # Write merged data into folders
        self.write_processed(merged)
     

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
            if taxi_type == "citi":
                taxi_list.append(self.sp.read.parquet(path).sample(0.8, 69)) # Sample 100%
            else:
                taxi_list.append(self.sp.read.parquet(path).sample(0.15, 69))

        return reduce(DataFrame.unionAll, taxi_list)
        
    def datetime_features(self, data: DataFrame, col_name: str):
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
            .withColumn("trip_duration_"+col_name, time_diff)
        )
        return processed

    def match_columns(self, citi: DataFrame):
        """
        Function for citibike data to match the same columns names so 
        it can go through the same feature processing
        """
        citi = citi.withColumnRenamed("started_at", "pickup_datetime")
        return citi.withColumnRenamed("ended_at", "dropoff_datetime")

    def merge_traffic(self, data: DataFrame, col_name: str):
        """
        Function to calculate the traffic count and merge it to respective data
        Data only used to graph and analyse trends
        """
        data_count = data.groupBy(["year", "month", "day", "time_bin"]).count()
        return self.merge_on_date(data, data_count.withColumnRenamed("count", "count_"+col_name))

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

    def merge_dataframes(self, taxi: DataFrame, bike: DataFrame):
        """
        Merge the taxi and bike dataframe on date variables, PULocationID, and DOLocationID
        """
        return taxi.join(
            bike,
            ["year", "month", "day", "time_bin", "PULocationID", "DOLocationID", "day_of_week"],
            how="inner"
        )

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

    def write_processed(self, data: DataFrame):
        """
        Function to write processed data
        """
        # Check for parent directory
        output_dir = "../data/processed"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Split training and testing to test for 2022
        data.write.parquet(output_dir + "/merged", mode="overwrite")

        print("Finished writing the merged data into folder")

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
        data = data.filter((col("trip_duration_taxis") < 10800) & (col("trip_duration_taxis") > 0))
        return data.filter((col("trip_duration_bikes") < 10800) & (col("trip_duration_bikes") > 0))

    def add_udfs(self, data: DataFrame):
        """
        Function to call all UDFs
        """
        data = data.withColumn("trip_difference", col("trip_duration_taxis") - col("trip_duration_bikes"))
        data = data.withColumn("peak_hours", peak_hours(col("time_bin")))
        data = data.withColumn("weather_pattern", weather_pattern(col("day")))
        data = data.withColumn("better_travel_type", better_travel_type(col("trip_difference")))

        return data


        