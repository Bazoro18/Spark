import os
import json
from datetime import datetime
from config import CONFIG
from schemas import route_schema, ticket_schema, gps_schema
import seed_data

# convenience path lookups
SEED_DIR = CONFIG["paths"]["seed_dir"]
INPUT_DIR = CONFIG["paths"].get("input_dir", os.path.join(os.path.dirname(__file__), "../data/input"))

def write_seed_csv(spark=None):
    """
    Write seed data to disk. Accepts optional spark to additionally write Parquet/JSON
    that some jobs expect (routes.parquet, gps_json). This avoids sending Python lists
    to executors (no pickling).
    """
    os.makedirs(SEED_DIR, exist_ok=True)

    # Routes CSV
    routes = seed_data.get_routes()
    routes_csv = os.path.join(SEED_DIR, "routes.csv")
    with open(routes_csv, "w") as f:
        f.write("route_id,start_point,end_point,capacity\n")
        for r in routes:
            f.write(",".join(map(str, r)) + "\n")

    # Tickets CSV
    tickets = seed_data.get_tickets()
    tickets_csv = os.path.join(SEED_DIR, "tickets.csv")
    with open(tickets_csv, "w") as f:
        f.write("ticket_id,passenger_id,booking_time,route_id\n")
        for t in tickets:
            # booking_time may be datetime; convert to ISO
            booking_time = t[2].isoformat() if hasattr(t[2], "isoformat") else str(t[2])
            f.write(",".join([t[0], t[1], booking_time, t[3]]) + "\n")

    # GPS CSV and small JSON partition (for stream)
    gps = seed_data.get_gps()
    gps_csv = os.path.join(SEED_DIR, "gps.csv")
    with open(gps_csv, "w") as f:
        f.write("bus_id,route_id,latitude,longitude,speed,timestamp\n")
        for g in gps:
            ts = g[5].isoformat() if hasattr(g[5], "isoformat") else str(g[5])
            f.write(",".join([g[0], g[1], str(g[2]), str(g[3]), str(g[4]), ts]) + "\n")

    # If a SparkSession was provided, also write the formats other modules expect
    if spark is not None:
        # write routes parquet (explicit schema)
        try:
            df_routes = spark.createDataFrame(routes, schema=route_schema)
            df_routes.write.mode("overwrite").parquet(os.path.join(SEED_DIR, "routes.parquet"))
        except Exception:
            # If any issue, ignore — CSV still exists
            pass

        # write tickets CSV via spark (ensures same schema types)
        try:
            df_tickets = spark.createDataFrame(tickets, schema=ticket_schema)
            df_tickets.write.mode("overwrite").option("header", True).csv(os.path.join(SEED_DIR, "tickets.csv"))
        except Exception:
            pass

        # write GPS JSON
        try:
            df_gps = spark.createDataFrame(gps, schema=gps_schema)
            gps_json_dir = os.path.join(SEED_DIR, "gps_json")
            df_gps.write.mode("overwrite").json(gps_json_dir)
        except Exception:
            pass

    # Also create input dir for streaming simulator
    os.makedirs(INPUT_DIR, exist_ok=True)
    return {
        "routes_csv": routes_csv,
        "tickets_csv": tickets_csv,
        "gps_csv": gps_csv
    }

def load_seed_data(spark):
    """
    Read seed files into DataFrames using explicit schemas (so types are correct).
    """
    # Read using spark to get typed DataFrames
    routes_path = os.path.join(SEED_DIR, "routes.parquet")
    tickets_path = os.path.join(SEED_DIR, "tickets.csv")
    gps_json_path = os.path.join(SEED_DIR, "gps_json")

    # Prefer Parquet for routes if exists, otherwise CSV
    if spark._jsparkSession.sessionState().catalog().listTables():  # cheap guard (no-op, safe)
        pass

    if os.path.exists(routes_path):
        df_routes = spark.read.schema(route_schema).parquet(routes_path)
    else:
        df_routes = spark.read.schema(route_schema).option("header", True).csv(os.path.join(SEED_DIR, "routes.csv"))

    df_tickets = spark.read.schema(ticket_schema).option("header", True).csv(tickets_path)
    # GPS: prefer JSON dir if it exists
    if os.path.isdir(gps_json_path):
        df_gps = spark.read.schema(gps_schema).json(gps_json_path)
    else:
        df_gps = spark.read.schema(gps_schema).option("header", True).csv(os.path.join(SEED_DIR, "gps.csv"))

    return df_routes, df_tickets, df_gps
