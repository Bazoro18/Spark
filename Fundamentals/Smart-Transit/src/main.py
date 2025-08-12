"""
Orchestrator: run batch jobs (RDD, DF, SQL) and/or start streaming.
Run as a package: python3 -m src.main --mode batch
"""

import argparse
import inspect
import os
from utils import build_spark, setup_logger
from config import CONFIG
from data_generator import write_seed_csv, load_seed_data

# optional job imports
try:
    from .rdd_jobs import ticket_counts_rdd
except Exception:
    ticket_counts_rdd = None

try:
    from .df_jobs import batch_analytics
except Exception:
    batch_analytics = None

try:
    from .streaming_job import start_streaming
    HAS_STREAMING = True
except Exception:
    start_streaming = None
    HAS_STREAMING = False

logger = setup_logger("smart-transit-main")

def _get_paths_from_config():
    base = os.path.dirname(os.path.abspath(__file__))
    if "paths" in CONFIG:
        seed_dir = CONFIG["paths"].get("seed_dir")
        output_dir = CONFIG["paths"].get("output_dir")
        checkpoint_dir = CONFIG["paths"].get("checkpoint_dir")
    else:
        seed_dir = CONFIG.get("seed_dir") or os.path.join(base, "../data/seed")
        output_dir = CONFIG.get("output_dir") or os.path.join(base, "../data/output")
        checkpoint_dir = CONFIG.get("checkpoint_dir") or os.path.join(base, "../checkpoints")
    return seed_dir, output_dir, checkpoint_dir

def _call_maybe_with_spark(func, spark=None, *args, **kwargs):
    if func is None:
        return None
    sig = inspect.signature(func)
    # if func expects zero args, call without spark
    if len(sig.parameters) == 0:
        return func()
    # else pass spark if available
    return func(spark, *args, **kwargs) if spark is not None else func(*args, **kwargs)

def run_batch(spark):
    seed_dir, output_dir, checkpoint_dir = _get_paths_from_config()
    logger.info("Using seed_dir=%s output_dir=%s checkpoint_dir=%s", seed_dir, output_dir, checkpoint_dir)

    # ensure seed files exist; write_seed_csv optionally accepts spark
    _call_maybe_with_spark(write_seed_csv, spark)

    # RDD demo
    if ticket_counts_rdd:
        tickets_csv = os.path.join(seed_dir, "tickets.csv")
        try:
            rdd_res = ticket_counts_rdd(spark.sparkContext, tickets_csv, checkpoint_dir)
            logger.info("RDD passenger counts: %s", rdd_res)
        except Exception:
            logger.exception("RDD job failed.")

    # DF job
    if batch_analytics:
        try:
            ranked = batch_analytics(spark, seed_dir, output_dir)
            try:
                ranked.show()
            except Exception:
                pass
        except Exception:
            logger.exception("DF job failed.")

    logger.info("Batch complete. outputs in %s", output_dir)

def run_stream(spark):
    if not HAS_STREAMING:
        logger.error("Streaming module not available.")
        return
    seed_dir, output_dir, checkpoint_dir = _get_paths_from_config()
    _call_maybe_with_spark(write_seed_csv, spark)
    logger.info("Starting streaming job (checkpoint -> %s)", checkpoint_dir)
    query = start_streaming(spark, input_dir=CONFIG["paths"].get("input_dir"), seed_dir=seed_dir, output_dir=output_dir, checkpoint_dir=checkpoint_dir)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming...")
        query.stop()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "stream", "both"], default="batch")
    args = parser.parse_args()

    spark = build_spark()
    try:
        if args.mode == "batch":
            run_batch(spark)
        elif args.mode == "stream":
            run_stream(spark)
        elif args.mode == "both":
            run_batch(spark)
            run_stream(spark)
    finally:
        try:
            spark.stop()
        except Exception:
            pass
        logger.info("Spark stopped, exiting.")

if __name__ == "__main__":
    main()
