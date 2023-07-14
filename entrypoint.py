import json
import pandas as pd
from sodapy import Socrata
from data_handler import pq_to_df, get_avg_trip_length, show_schema, get_rolling_avg_trip_length, export_rolling_avg
from fetch_data import get_taxi_trip_data

if __name__ == "__main__":
    # Fetch trip data from website
    get_taxi_trip_data()
    
    # Get average trip length for a given month
    get_avg_trip_length(4, year=2023)

    # Get rolling average trip length and save it to parquet file
    get_rolling_avg_trip_length(window_size=45)
    
    # Export rolling average to csv file
    export_rolling_avg("rolling_20_days")
