from pyspark import StorageLevel
import os

def ticket_counts_rdd(sc, tickets_csv_path, checkpoint_dir):
    rdd = sc.textFile(tickets_csv_path)
    header = rdd.first()
    rows = rdd.filter(lambda x: x != header)

    # narrow: map -> create (route_id, 1)
    pairs = rows.map(lambda line: line.split(",")).map(lambda arr: (arr[3].strip(), 1))

    # wide: reduceByKey (shuffle)
    counts = pairs.reduceByKey(lambda a, b: a + b)

    # Cache (StorageLevel)
    counts.persist(StorageLevel.MEMORY_ONLY)

    # Checkpoint
    sc.setCheckpointDir(checkpoint_dir)
    counts.checkpoint()  # will be materialized on action

    # Action
    result = counts.collect()
    counts.unpersist()
    return result
