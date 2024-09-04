from psk.config import core_config

DEST_TABLE = "stat_transactions"
DEST_FOLDER = core_config.DATA_DESTINATION()


def save_transactions(df_result):
    df_result.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("txn_year_month") \
        .option('partitionOverwriteMode', 'dynamic') \
        .save(f"{DEST_FOLDER}/{DEST_TABLE}")