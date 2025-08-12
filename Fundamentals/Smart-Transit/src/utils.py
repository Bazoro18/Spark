import logging
from pyspark.sql import SparkSession
from config import CONFIG

def setup_logger(name="smarttransit"):
    # safe, idempotent logger setup
    logger = logging.getLogger(name)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def build_spark(app_name=None, extra_conf=None):
    conf = CONFIG["spark"]
    app = app_name or conf.get("app_name", "SmartTransit")
    builder = SparkSession.builder.appName(app).master(conf.get("master", "local[*]"))
    builder = builder.config("spark.driver.memory", conf.get("driver_memory", "2g"))
    builder = builder.config("spark.executor.memory", conf.get("executor_memory", "2g"))
    builder = builder.config("spark.sql.shuffle.partitions", conf.get("shuffle_partitions", "4"))
    spark = builder.getOrCreate()
    if extra_conf:
        for k, v in extra_conf.items():
            spark.conf.set(k, v)
    # try enabling adaptive execution but ignore failures
    try:
        spark.conf.set("spark.sql.adaptive.enabled", "true")
    except Exception:
        pass
    spark.sparkContext.setLogLevel("WARN")
    return spark
