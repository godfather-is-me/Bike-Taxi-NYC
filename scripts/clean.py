import os
import cleanup_helpers

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

class cleanup():
    """
    A class dedicated to clean up and clean-up related functions for taxis and bikes
    """

    def __init__(self):
        self.sp = (
            SparkSession.builder.appName("Yellow/Green taxi clean-up")
            .config("spark.sql.session.timeZone", "-04")
            .getOrCreate()
        )
        self.bike_zones = cleanup_helpers.read_data(self.sp, "b", 0, 0)
        self.bikes_zones_sp = self.sp.createDataFrame(self.bike_zones)
        self.dates = [
            #(1, 1, 2022)
            (7, 12, 2020),
            (1, 12, 2021),
            (1, 4, 2022)
        ]

    def __del__(self):
        """
        Destructor for clean up class. Stop spark session.
        """
        self.sp.stop
        print("Clean up completed")

    def yellow_cleanup(self):
        """
        Function to clean up yellow taxi specific data for a given month and year,
        and outputs it to a cleaned folder
        """
        output_dir = "../data/curated/yellow"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)


        for date in self.dates:
            start_m, end_m, y = date
            for month in range(start_m, end_m + 1):
                print("\nStarting to process yellow taxi data from: " + str(y) + "-" + str(month))
                yellow_data = cleanup_helpers.read_data(self.sp, "y", month, y)
                yellow_data = self.drop_unknown_values(yellow_data)
                yellow_data = self.filter_by_limits(yellow_data)
                yellow_data = self.drop_columns(yellow_data, "y")
                yellow_data = yellow_data.na.drop()

                # Write data (into folder)
                yellow_data.write.parquet(output_dir + "/" + str(y) + "-" + str(month), mode="overwrite")

                print("Processed!\n")

    def green_cleanup(self):
        """
        Function to clean up green taxi specific data for a given month and year,
        and output it to a cleaned folder
        """
        output_dir = "../data/curated/green"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for date in self.dates:
            start_m, end_m, y = date
            for month in range(start_m, end_m + 1):
                print("\nStarting to process green taxi data from: " + str(y) + "-" + str(month))
                green_data = cleanup_helpers.read_data(self.sp, "g", month, y)

                # Rename columns
                green_data = green_data.withColumnRenamed("payment_type", "Payment_type")
                green_data = green_data.withColumnRenamed("RatecodeID", "RateCodeID")

                green_data = self.drop_unknown_values(green_data)
                green_data = self.green_specific(green_data)
                green_data = self.filter_by_limits(green_data)
                green_data = self.drop_columns(green_data, "g")
                green_data = green_data.na.drop()

                # Write data (into folder)
                green_data.write.parquet(output_dir + "/" + str(y) + "-" + str(month), mode="overwrite")

                print("Processed!\n")

    def fhvhv_cleanup(self):
        """
        Function to clean fhvhv specific data for a given month and year,
        and output it to a clean folder
        """
        output_dir = "../data/curated/fhvhv"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for date in self.dates:
            start_m, end_m, y = date
            for month in range(start_m, end_m + 1):
                print("\nStarting to process fhvhv taxi data from: " + str(y) + "-" + str(month))
                fhvhv_data = cleanup_helpers.read_data(self.sp, "hv", month, y)
                # No categorical data with unknown values
                fhvhv_data = self.filter_fhvhv_by_limits(fhvhv_data)
                fhvhv_data = self.drop_columns(fhvhv_data, "hv")
                fhvhv_data = fhvhv_data.na.drop()

                fhvhv_data.write.parquet(output_dir + "/" + str(y) + "-" + str(month), mode="overwrite")
                
                print("Processed!\n")

    def citibike_cleanup(self):
        """
        Function to clean citibike data for a given month and year,
        and output it to a clean folder
        """
        output_dir = "../data/curated/citi"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        feb_2021 = False

        for date in self.dates:
            start_m, end_m, y = date
            for month in range(start_m, end_m + 1):
                # To filter by station ID
                if y == 2021 and month == 2:
                    feb_2021 = True
                print("\nStarting to process Citi Bike data from: " + str(y) + "-" + str(month))
                citi_data = cleanup_helpers.read_data(self.sp, "c", month, y)
                citi_data = self.filter_by_stations(citi_data, feb_2021)
                citi_data = self.drop_citi_columns(citi_data, feb_2021)
                citi_data = citi_data.na.drop()

                citi_data.write.parquet(output_dir + "/" + str(y) + "-" + str(month), mode="overwrite")

                print("Processed!")

    def filter_by_stations(self, data, feb_21: bool):
        """
        Function to filter citibike data based on station number
        """
        stations = None
        if feb_21:
            stations = self.bike_zones["new_id"].to_list()
            # Create a spark dataframe for left join
            bikes_sp = self.bikes_zones_sp[["new_id", "Taxi area code"]]
            data = data.filter((data["start_station_id"].isin(stations)) & (data["end_station_id"].isin(stations)))

            # Pick up location ID
            data = data.alias("a").join(bikes_sp.alias("b"), col("a.start_station_id") == col("b.new_id"), how="left")
            data = data.withColumnRenamed("Taxi area code", "PULocationID")

            # Drop off location ID
            data = data.alias("c").join(bikes_sp.alias("d"), col("c.end_station_id") == col("d.new_id"), how = "left")
            data = data.withColumnRenamed("Taxi area code", "DOLocationID")

            data = data.filter((data["PULocationID"] != data["DOLocationID"]))
        else:
            stations = self.bike_zones["id"].to_list()
            # Create dataframe for left join
            bikes_sp = self.bikes_zones_sp[["id", "Taxi area code"]]
            data = data.filter((data["start station id"].isin(stations)) & (data["end station id"].isin(stations)))
            
            # Rename columns
            data = data.withColumnRenamed("start station id", "start_station_id")
            data = data.withColumnRenamed("end station id", "end_station_id")

            # Pick up location ID
            data = data.alias("a").join(bikes_sp.alias("b"), col("a.start_station_id") == col("b.id"), how="left")
            data = data.withColumnRenamed("Taxi area code", "PULocationID")

            # Drop off location ID
            data = data.alias("c").join(bikes_sp.alias("d"), col("c.end_station_id") == col("d.id"), how = "left")
            data = data.withColumnRenamed("Taxi area code", "DOLocationID")

            data = data.filter((data["PULocationID"] != data["DOLocationID"]))

        return data
        
    def drop_unknown_values(self, data):
        """
        Function to drop values not present in the given data dictionary
        """
        # Filter vendorID
        data = data.filter((data["VendorID"] == 1) | (data["VendorID"] == 2))
        payment_list = list(range(1, 7))

        # Filter payment type
        data = data.filter((data["Payment_type"].isin(payment_list)))

        # Filter rate code
        ratecode_list = [1, 4, 5, 6]    # No airport rides
        data = data.filter((data["RateCodeID"].isin(ratecode_list)))

        return data

    def filter_by_limits(self, data):
        """
        Function to filter values set by the domain of the research question
        """
        # Consider taxis that start and end in a zone with a citibike station
        data = self.filter_bike_zones(data)
        # Consider taxis with 1-4 passengers
        data = data.filter((data["passenger_count"] > 0) & (data["passenger_count"] < 5))
        # Consider trip distances of 25 miles or less
        data = data.filter((data["trip_distance"] > 0) & (data["trip_distance"] < 25))
        # Consider trips which start and end in different zones
        data = data.filter((data["PULocationID"] != data["DOLocationID"]))

        return data

    def filter_bike_zones(self, data):
        """
        Filter pick up and drop off locations w.r.t. citibike station zones
        """
        zones_list = self.bike_zones["Taxi area code"].to_list()
        return data.filter((data["PULocationID"].isin(zones_list)) & (data["DOLocationID"].isin(zones_list)))

    def filter_fhvhv_by_limits(self, data):
            """
            Function to work with high volume for-hire data specific columns to filter
            based on the research question
            """
            data = self.filter_bike_zones(data)
            # Consider trip distances of 25 miles or less
            data = data.filter((data["trip_miles"] > 0) & (data["trip_miles"] < 25))
            data = data.withColumnRenamed("trip_miles", "trip_distance")
            # Consider trips which start and end in different zones
            data = data.filter((data["PULocationID"] != data["DOLocationID"]))
            # Consider trips that are not shared
            data = data.filter((data["shared_match_flag"] == "N"))
            # Consider trips which are not specific to disability access
            data = data.filter((data["wav_match_flag"] == "N"))

            return data

    def drop_columns(self, data, dtype: str):
        """
        After using extra information to filter out the data, most of the columns are unncessary
        and therefore can be dropped
        """
        if dtype == "y":
            data = data.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
            data = data.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        elif dtype == "g":
            data = data.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
            data = data.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        # Nothing for "hv"

        columns = ["pickup_datetime", "dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID"]
        return data[[columns]].drop("trip_distance")

    def drop_citi_columns(self, data, feb_21: bool):
        """
        Function to drop unnecessary columns from citibike data
        """
        # New data
        if feb_21:
            data = data.select("started_at", "ended_at", "PULocationID", "DOLocationID")
        else:
            data = data.select("starttime", "stoptime", "PULocationID", "DOLocationID")
            data = data.withColumnRenamed("starttime", "started_at")
            data = data.withColumnRenamed("stoptime", "ended_at")            
        return data

    def green_specific(self, data):
        """
        Function to clean data that is particularly related to green taxis
        """
        data = data.filter((data["trip_type"] == 1) | (data["trip_type"] == 2))
        return data


