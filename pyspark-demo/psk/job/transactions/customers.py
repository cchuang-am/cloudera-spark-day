from psk.config import core_config


def load_customers(spark):
    files = f"{core_config.DATA_SOURCE()}/customers"

    return spark.read.parquet(files)
