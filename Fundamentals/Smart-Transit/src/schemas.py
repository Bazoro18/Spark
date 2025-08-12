from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

route_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("start_point", StringType(), True),
    StructField("end_point", StringType(), True),
    StructField("capacity", IntegerType(), True)
])

ticket_schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("passenger_id", StringType(), True),
    StructField("booking_time", TimestampType(), True),
    StructField("route_id", StringType(), True)
])

gps_schema = StructType([
    StructField("bus_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])
