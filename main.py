
#Question 1. Install Spark and PySpark
import pyspark
from pyspark.sql import SparkSession, types, functions as f

spark = SparkSession.builder \
    .appName("spark_local_cluster") \
    .getOrCreate()

spark.version

#Question 2. Yellow November 2025
yellow_df = spark.read.parquet("homework/data/yellow_tripdata_2025-11.parquet")
yellow_df.repartition(4).write.parquet("data/parts/")
yellow_df.show(5)



#Question 3. Count records 
yellow_df.createOrReplaceTempView("yellow_trips")

nov_15_trips = spark.sql("""
    SELECT COUNT(*)
    FROM yellow_trips   
    WHERE DATE(tpep_pickup_datetime) = '2025-11-15'           
    """).show()

#Question 4. Longest trip
longest_trip = spark.sql("""
    SELECT 
        trip_distance,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60 AS minutes_diff,
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600 AS hours_diff
    FROM yellow_trips
    ORDER BY hours_diff DESC
    LIMIT 10
""").show()

#Question 6. Least frequent pickup location zone
zones_df = spark.read.csv("homework/data/taxi_zone_lookup.csv", header=True)
zones_df.write.parquet("homework/zones")

joind_trips = yellow_df.join(zones_df, yellow_df.DOLocationID == zones_df.LocationID, how='outer')
joind_trips.select("DOLocationID", "Zone").show(5)

joind_trips.createOrReplaceTempView("trips")

LeastFrequentPickupLocationZone = spark.sql("""
    SELECT 
        Zone,
        COUNT(*) as DOLocation_Freq
    FROM trips
    WHERE tpep_pickup_datetime > '2025-11-01 00:00:00' 
    GROUP BY Zone
    ORDER BY DOLocation_Freq ASC
    """).show()


