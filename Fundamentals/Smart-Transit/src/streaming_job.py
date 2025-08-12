import os
from pyspark.sql import functions as F
from config import CONFIG
from schemas import gps_schema

def start_streaming(spark, input_dir=None, seed_dir=None, output_dir=None, checkpoint_dir=None):
    input_dir = input_dir or CONFIG["paths"].get("input_dir")
    seed_dir = seed_dir or CONFIG["paths"].get("seed_dir")
    output_dir = output_dir or CONFIG["paths"].get("output_dir")
    checkpoint_dir = checkpoint_dir or CONFIG["paths"].get("checkpoint_dir")

    # Read small static table for joins (gps seed can act as small static table)
    gps_json_dir = os.path.join(seed_dir, "gps_json")
    if os.path.isdir(gps_json_dir):
        static_gps = spark.read.schema(gps_schema).json(gps_json_dir)
    else:
        static_gps = None

    # Streaming DataFrame (file source)
    stream_df = spark.readStream.schema(gps_schema).option("maxFilesPerTrigger", 1).json(input_dir)

    # Event time processing + watermark
    ev = stream_df.withColumn("event_time", F.col("timestamp"))

    # Basic aggregation: count per route in sliding window (event-time)
    agg = ev.withWatermark("event_time", "2 minutes").groupBy(
        F.window("event_time", "2 minutes", "1 minute"),
        F.col("route_id")
    ).agg(
        F.count("*").alias("events"),
        F.avg("speed").alias("avg_speed")
    )

    # Write with foreachBatch so we can atomically write to Parquet and also update other stores
    def foreach_batch_function(df, epoch_id):
        out_path = os.path.join(output_dir, "stream_route_metrics")
        df = df.repartition(1)
        df.write.mode("append").parquet(out_path)
        # keep the print for quick local debugging
        print(f"foreachBatch: wrote epoch {epoch_id} rows={df.count()}")

    query = agg.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime="10 seconds") \
        .foreachBatch(foreach_batch_function) \
        .start()

    return query
