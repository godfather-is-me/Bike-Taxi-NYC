from urllib.request import urlretrieve
from zipfile import ZipFile
import os

def download_all():
    """
    Defines all the downloads and their parameters
    """
    # TLC data
    download_tlc(7, 12, 2020)
    download_tlc(1, 12, 2021)
    download_tlc(1, 4, 2022)

    print("Download from TLC complete")

    # Citi data
    download_citi(7, 12, 2020)
    download_citi(1, 12, 2021)
    download_citi(1, 4, 2022)

    print("Download from CitiBike complete")

    # Download helper datasets
    download_weather()
    download_citi_stations()

def download_tlc(start_month: int, end_month: int, year: int):
    """
    Function to download data from the TLC Website

    start_month : The starting month from when data is required (int)
    end_month : The end month from when data is required (int)
    year : The year of required data
    """

    output_dir = "../data/raw/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Sources of TLC data
    sources = {
        "yellow" : "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_",
        "green" : "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_",
        "fhv" : "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_",
        "fhvhv" : "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_",
        "taxi_zones" : "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
        "taxi_zones_lookup" : "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    }

    for key in sources.keys():
        # Create a directory for source group
        target_dir = output_dir + key
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # Data that is over months
        if "_tripdata_" in sources[key]:
            for month in range(start_month, end_month + 1):
                m = str(month).zfill(2)
                print("Downloading TLC " + key + " data from : Year: " + str(year) + " Month: " + m)

                extension = str(year) + "-" + m + ".parquet"
                url = sources[key] + extension
                file_location = target_dir + "/" + key + "-" + extension
                urlretrieve(url, file_location)

        # Zone data
        elif "zones" in key:
            if "lookup" in key:
                file_location = target_dir + key + ".csv"
                urlretrieve(sources[key], file_location)
            else:
                file_location = target_dir + "taxi_zones.zip"
                urlretrieve(sources[key], file_location)

                with ZipFile(file_location, 'r') as f:
                    f.extractall(target_dir)
                    os.remove(file_location)

def download_citi(start_month: int, end_month: int, year: int):
    """
    Function to download data from citibike website
    """

    # Safety check
    output_dir = "../data/raw/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # "https://s3.amazonaws.com/tripdata/202007-citibike-tripdata.csv.zip"
    source = "https://s3.amazonaws.com/tripdata/"
    extension = "-citibike-tripdata.csv.zip"
    y = str(year)

    # Create a directory for source group
    target_dir = output_dir + "citi"
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for month in range(start_month, end_month + 1):
        m = str(month).zfill(2)
        print("Downloading CitiBike data from year: " + y + " month: " + m)
        
        url = source + y + m + extension
        file_location = target_dir + "/" + y + m + ".csv.zip"
        urlretrieve(url, file_location)

        # Unzip
        with ZipFile(file_location, 'r') as f:
            f.extractall(target_dir)
            os.remove(file_location)


def download_weather():
    """
    Function to download NYC weather data over specified time period
    """
    # Safety check
    output_dir = "../data/raw/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create a directory for source group
    target_dir = output_dir + "weather"
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    source = "https://drive.google.com/uc?export=download&id=15BKMBk-vGvYRQkydKuc1hI4UFbMQSezc"

    file_location = target_dir + "/weather.csv"
    urlretrieve(source, file_location)

    print("Download from OneDrive - Weather complete")

def download_citi_stations():
    """
    Function to download processed data on citibike stations associated with the taxi shape file
    """
    # Safety check
    output_dir = "../data/raw/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Create a directory for source group
    target_dir = output_dir + "citi"
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # Insert drive source for bike stations data
    source = "https://drive.google.com/uc?export=download&id=1p4wmPL8mBjPYd9mtjwurOsCDtGAfpwXo"
    file_location = target_dir + "/bike stations.csv"
    urlretrieve(source, file_location)

    print("Download Citi Bike Stations complete")

# Function to be called in main.py
# download_all()
