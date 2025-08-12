#!/usr/bin/env bash
# Example spark-submit for batch job
SPARK_SUBMIT=/path/to/spark/bin/spark-submit
APP_JAR=src/main.py
$SPARK_SUBMIT \
  --master local[*] \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=4 \
  src/main.py --mode batch
