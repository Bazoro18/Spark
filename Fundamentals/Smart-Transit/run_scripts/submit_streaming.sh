#!/usr/bin/env bash
# Start streaming job (run in its own terminal)
$SPARK_SUBMIT \
  --master local[*] \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=4 \
  src/main.py --mode stream
