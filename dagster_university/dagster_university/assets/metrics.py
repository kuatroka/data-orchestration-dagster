from dagster import asset
from dagster_duckdb import DuckDBResource
import plotly.express as px
import plotly.io as pio
import geopandas as gpd
import datetime

from . import constants

# @asset
# def test_func(database: DuckDBResource):
#     query = """
#         select
#             zones.zone,
#             zones.borough,
#             zones.geometry,
#             count(1) as num_trips,
#         from trips
#         left join zones on trips.pickup_zone_id = zones.zone_id
#         where borough = 'Manhattan' and geometry is not null
#         group by zone, borough, geometry
#     """

#     with database.get_connection() as conn:
#         trips_by_zone = conn.execute(query).fetch_df()    

#     trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
#     trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

#     with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
#         output_file.write(trips_by_zone.to_json())


@asset(deps=["taxi_trips", "taxi_zones"])
def manhattan_stats(database: DuckDBResource):
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@asset(deps=["taxi_trips"])
def trips_by_week(database: DuckDBResource):
    current_date = datetime.datetime.strptime("2023-03-01", constants.DATE_FORMAT)
    end_date = datetime.datetime.strptime("2023-04-01", constants.DATE_FORMAT)

    sql_query = f"""
    COPY (
        SELECT 
            date_trunc('week', pickup_datetime) as period,
            COUNT(vendor_id) AS num_trips,
            SUM(passenger_count) AS passenger_count,
            SUM(total_amount) AS total_amount,
            SUM(trip_distance) AS trip_distance
        FROM trips
        WHERE date_trunc('week', pickup_datetime) BETWEEN '{current_date}' AND '{end_date}'
        GROUP BY period
        ORDER BY period DESC
    ) TO '{constants.TRIPS_BY_WEEK_FILE_PATH}' WITH (FORMAT CSV, HEADER)
                """

    with database.get_connection() as conn:
        conn.execute(sql_query)
        # trips_by_week = conn.execute(sql_query).pl()    ## using polars to write to csv (remove COPY from sql_query and add .pl())
        # trips_by_week.write_csv(constants.TRIPS_BY_WEEK_FILE_PATH, batch_size=10000)



@asset(deps=["manhattan_stats"])
def manhattan_map():
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        labels={'num_trips': 'Number of Trips'}
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)



