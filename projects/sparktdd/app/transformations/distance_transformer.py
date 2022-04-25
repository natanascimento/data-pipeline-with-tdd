from math import radians, cos, sin, asin, sqrt

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DoubleType

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE

def calculate_distance(start_latitude:float, start_longitude:float, end_latitude:float, end_longitude:float):

    if start_latitude is None or start_longitude is None and end_latitude is None and end_longitude is None:
        raise ValueError("Parameters must be required")
    
    start_latitude_rad, start_longitude_rad, end_latitude_rad, end_longitude_rad = map(radians, [start_latitude, 
                                                                                                 start_longitude, 
                                                                                                 end_latitude, 
                                                                                                 end_longitude])

    d_long = end_longitude_rad - start_longitude_rad
    d_lat = end_latitude_rad - start_latitude_rad
    
    distance = sin(d_lat/2)**2 + cos(start_latitude_rad) * cos(end_latitude_rad) * sin(d_long/2)**2
    
    c = 2 * asin(sqrt(distance))
    
    return round((c * EARTH_RADIUS_IN_METERS) / METERS_PER_MILE, 6)

def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    
    calculate_distance_udf = udf(lambda start_latitude, start_longitude, end_latitude, end_longitude:  calculate_distance(start_latitude, start_longitude, end_latitude, end_longitude), DoubleType())
    
    df_with_distance = dataframe.withColumn("distance", calculate_distance_udf(col("start_station_latitude").cast(DoubleType()),
                                                                               col("start_station_longitude").cast(DoubleType()),
                                                                               col("end_station_latitude").cast(DoubleType()),
                                                                               col("end_station_longitude").cast(DoubleType())))
    
    return df_with_distance


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')