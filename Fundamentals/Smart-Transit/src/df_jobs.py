"""
Batch DataFrame & SparkSQL job: joins, UDFs, temp views, window functions,
broadcast join, catalyst explain, predicate pushdown, partition writes, schema evolution demo.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from schemas import route_schema

# =========================
# UDF (moved to top level)
# =========================
@F.udf(returnType=DoubleType())
def occupancy_udf(passengers, capacity):
    if capacity is None or capacity == 0:
        return 0.0
    return float(passengers) / float(capacity) * 100.0


def batch_analytics(spark, seed_dir, output_dir):
    # Explicit schema for routes
    routes = spark.read.schema(route_schema).parquet(f"{seed_dir}/routes.parquet")

    # CSV with inferred schema
    tickets = spark.read.option("header", True) \
                        .csv(f"{seed_dir}/tickets.csv", inferSchema=True)

    # JSON inferred schema
    gps = spark.read.json(f"{seed_dir}/gps_json")

    # === RDD example -> DF ===
    ticket_rdd = spark.sparkContext.textFile(f"{seed_dir}/tickets.csv")
    header = ticket_rdd.first()
    ticket_pairs = ticket_rdd.filter(lambda x: x != header) \
                              .map(lambda x: x.split(",")) \
                              .map(lambda arr: (arr[3].strip(), 1))
    counts = ticket_pairs.reduceByKey(lambda a, b: a + b)
    counts_df = counts.toDF(["route_id", "passenger_count"])

    # Broadcast join
    joined = counts_df.join(F.broadcast(routes), "route_id", "left")

    # Apply UDF
    joined = joined.withColumn(
        "occupancy_pct",
        occupancy_udf(F.col("passenger_count"), F.col("capacity"))
    )

    # Temp view
    joined.createOrReplaceTempView("route_status")
    sql_df = spark.sql("""
        SELECT route_id,
               AVG(occupancy_pct) as avg_occupancy,
               COUNT(*) as samples
        FROM route_status
        GROUP BY route_id
    """)

    # Window function
    w = Window.orderBy(F.col("avg_occupancy").desc())
    ranked = sql_df.withColumn("rank", F.rank().over(w))

    # Debug: explain physical/logical plans
    ranked.explain(True)

    # Partitioned write
    ranked.write.mode("overwrite") \
          .partitionBy("route_id") \
          .parquet(f"{output_dir}/route_metrics")

    # Predicate pushdown example
    _ = spark.read.parquet(f"{output_dir}/route_metrics") \
                  .filter(F.col("route_id") == "R1")

    return ranked
