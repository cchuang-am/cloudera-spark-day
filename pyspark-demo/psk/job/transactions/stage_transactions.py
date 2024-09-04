
from psk.config import core_config


def load_transactions(spark):
    files = f"{core_config.DATA_SOURCE()}/transactions"

    return spark.read.parquet(files)
