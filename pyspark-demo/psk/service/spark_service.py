from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from psk.service import core_service


def create_config():
    config = SparkConf() \
        .setAppName(core_service.get_app_name()) \
        .set("spark.sql.parquet.enableVectorizedReader", "false")

    return config


def create_spark():
    config = create_config()

    spark = SparkSession.builder \
        .config(conf=config) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def stop_spark(spark):
    spark.stop()
