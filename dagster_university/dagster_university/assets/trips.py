from dagster import asset, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource
import requests
from . import constants
from ..partitions import monthly_partitions
import os
import polars as pl
import time



@asset(partitions_def=monthly_partitions,
        group_name="raw_files")
def taxi_trips_file(context):
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    # month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

    num_rows = pl.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)).shape[0]

    return MaterializeResult(
    metadata={
        'Number of records': MetadataValue.int(num_rows)
    }
)


@asset(description="The raw CSV file for the taxi zones dataset",
        group_name="raw_files",)
def taxi_zones_file():
    """
    This asset will contain a unique identifier and name for each part of NYC as a distinct taxi zone
    """
    taxi_zones = requests.get(
        f"https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(taxi_zones.content)

    num_rows = pl.read_csv(constants.TAXI_ZONES_FILE_PATH).shape[0]


    created_on = time.ctime(os.path.getctime(constants.TAXI_ZONES_FILE_PATH))

    return MaterializeResult(
    metadata={
        'Number of records': MetadataValue.int(num_rows),
        'File created on': MetadataValue.text(created_on)
    }
    )


@asset(deps=["taxi_trips_file"],
            partitions_def=monthly_partitions,
            group_name="ingested")
def taxi_trips(context, database: DuckDBResource):
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.asset_partition_key_for_output()
    month_to_fetch = partition_date_str[:-3]

    sql_query = f"""
        create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
        );

        delete from trips where partition_date = '{month_to_fetch}';

        insert into trips
        select
            VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
            tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """
    with database.get_connection() as conn:
        conn.execute(sql_query)




@asset(deps=["taxi_zones_file"],
            group_name="ingested")
def taxi_zones(database: DuckDBResource):
    """
    Zones table in a DuckDB database
    """
    sql_query = """
        create or replace table zones as (
        select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry
        from 'data/raw/taxi_zones.csv'
        );
    """
    with database.get_connection() as conn:
        conn.execute(sql_query)

        # generate code to read jupyter notebook
        #
