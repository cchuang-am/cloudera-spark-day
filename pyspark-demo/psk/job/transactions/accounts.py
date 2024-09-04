from psk.config import core_config


def load_accounts(spark):
    files = f"{core_config.DATA_SOURCE()}/accounts"

    return spark.read.parquet(files)
