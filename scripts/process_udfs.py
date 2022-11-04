from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, udf

@udf(IntegerType())
def peak_hours(time_bin: int):
    """
    Function to assign peak hours category to the data

    All days (including weekends) from 8 am - 11 pm, and 4pm - 7pm
    """
    if ((time_bin >= 8) and (time_bin < 11)) or ((time_bin >= 16) and (time_bin < 19)):
        return 1
    return 0

@udf(IntegerType())
def weather_pattern(days: int):
    """
    Function to partition the days of the year by season
    0-60/330-365 "cold"
    60-150/240-330 "warm"
    150-240 "hot"
    """
    if (days < 60) or (days >= 330):
        return 0    # Cold
    elif (days < 240) and (days >= 150):
        return 1    # Hot
    return 2        # Warm

@udf(IntegerType())
def better_travel_type(diff: int):
    """
    Function that gets the trip difference and indicates whether the taxi or bike has performed better
    Diff = Taxis - Bikes
    """
    # If taxis take longer
    if diff > 0:
        return 1
    return 0
