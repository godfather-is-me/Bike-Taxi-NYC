import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

def read_data(sp: SparkSession, dtype: str, month: int, year: int):
    """
    Helper function to read from the raw data folder

    sp - Spark session
    type - y (yellow)/ g (green)/ hv (high volume)/ c (citi)/ w (weather)/ z (zones)/ b (bike stations)
    month - month
    year - year
    """

    # Describables
    source = "../data/raw/"
    y = str(year)
    m = str(month).zfill(2)

    if dtype == "y":
        return read_parquet("yellow", y, m, source, sp)
    elif dtype == "g":
        return read_parquet("green", y, m, source, sp)
    elif dtype == "hv":
        return read_parquet("fhvhv", y, m, source,sp)
    elif dtype == "c":
        source += "citi/" + y + m + "-citibike-tripdata.csv"
        return sp.read.option("header", True).option("inferSchema", True).csv(source)
    elif dtype == "w":
        source += "weather/weather.csv"
        return pd.read_csv(source)
    elif dtype == "b":
        source += "citi/bike stations.csv"
        return pd.read_csv(source)
    else:
        print("Wrong choice, please check code")
    return


def read_parquet(dtype: str, y: str, m: str, pre: str, sp: SparkSession):
    """
    Function to read and return parquet files
    """
    source = pre + dtype + "/" + dtype + "-" + y + "-" + m + ".parquet"
    return sp.read.parquet(source)