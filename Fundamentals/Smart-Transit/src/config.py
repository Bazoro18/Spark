import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG = {
    "paths": {
        "seed_dir": os.path.join(BASE_DIR, "../data/seed"),
        "output_dir": os.path.join(BASE_DIR, "../data/output"),
        "checkpoint_dir": os.path.join(BASE_DIR, "..", "checkpoints"),
        "log_dir": os.path.join(BASE_DIR, "..", "logs"),
        # streaming input directory (used by stream simulator / streaming job)
        "input_dir": os.path.join(BASE_DIR, "../data/input")
    },
    "spark": {
        "app_name": "SmartTransit",
        "master": "local[*]",
        "driver_memory": "2g",
        "executor_memory": "2g",
        "shuffle_partitions": "4"
    }
}
