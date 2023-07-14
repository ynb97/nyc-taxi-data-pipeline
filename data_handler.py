import pyarrow.parquet as pq
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date
spark = (SparkSession.builder.appName("pyspark_parquet")
     .config("spark.sql.crossJoin.enabled","true")
     .getOrCreate())


def pq_to_df(file_path):
    df = None

    try:

        df = pq.read_table(file_path).to_pandas()
    except Exception as e:
        print(e)

    return df


def show_schema(year=None):
    taxis = spark.read.format("parquet").load(f"data/{year}/yellow_tripdata_*.parquet")

    print(type(taxis))
    taxis.printSchema()
    print(f"Rows: {taxis.count()}")


def get_avg_trip_length(month, year=None):

    if not year:
        year = "2023"
    
    avg_trip_duration = spark.sql(
        f"select avg(trip_duration) as `Average Trip Duration` \
        from (select ( (bigint(to_timestamp(tpep_dropoff_datetime)))-(bigint(to_timestamp(tpep_pickup_datetime))) )/(60) as trip_duration \
            from parquet.`data/{year}/yellow_*.parquet` where month(tpep_dropoff_datetime) == {month} and year(tpep_dropoff_datetime) == {year})\
        mytable"
    )
    avg_trip_duration.show()


def get_rolling_avg_trip_length(window_size=None, year=None):

    if not window_size:
        window_size = 3
    
    if not year:
        year = "2023"
    

    try:

        earliest_trip_record = spark.sql(f"select `Created At` from parquet.`data/{year}/rolling_{window_size}_days.parquet` order by `Created At` desc limit 1").collect()[0][0] - timedelta(days=window_size - 2)
    except Exception as e:
        earliest_trip_record = None
        print(e)

    if not earliest_trip_record:
        earliest_trip_record = date(2023, 1, 1)

    latest_trip_record = spark.sql(
        f"select date(tpep_pickup_datetime) from parquet.`data/{year}/yellow_*.parquet` order by tpep_pickup_datetime desc limit 1"
    ).collect()[0][0]

    print(earliest_trip_record, latest_trip_record)

    number_of_rolling_avgs = (latest_trip_record - earliest_trip_record).days - window_size + 1

    print(number_of_rolling_avgs)

    for i in range(number_of_rolling_avgs):
        print(i)
        avg_window_start = earliest_trip_record + timedelta(days = i)

        avg_window_end = avg_window_start + timedelta(days = window_size)

        print(avg_window_start, avg_window_end)

        avg_trip_duration = spark.sql(
            f"select avg(trip_duration1) as `Average Trip Duration`, max(created_at) as `Created At` \
            from (select date(tpep_pickup_datetime) as created_at, ( (bigint(to_timestamp(tpep_dropoff_datetime)))-(bigint(to_timestamp(tpep_pickup_datetime))) )/(60) as trip_duration1 \
                from parquet.`data/{year}/yellow_*.parquet` \
                where date(tpep_pickup_datetime) >= '{avg_window_start.strftime('%Y-%m-%d')}' and date(tpep_dropoff_datetime) < '{avg_window_end.strftime('%Y-%m-%d')}')\
            mytable"
        )
        avg_trip_duration.write.parquet(f"data/{year}/rolling_{window_size}_days.parquet", mode="append")
        # avg_trip_duration.show()


def export_rolling_avg(file_name):

    df = pd.read_parquet(f"data/{file_name}.parquet")
    df.to_csv(f"data/{file_name}.csv")