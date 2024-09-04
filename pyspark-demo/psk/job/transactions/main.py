from pyspark.sql import functions as f
import databricks.koalas as ks

from psk.job.transactions import customers, accounts, stage_transactions, stat_transactions


def ks_transform(df_customers, df_accounts, df_transactions):
    df_customers = df_customers.to_koalas()
    df_accounts = df_accounts.to_koalas()
    df_transactions = df_transactions.to_koalas()

    df_oppo_accounts = df_accounts.rename(columns={
        "account_key": "oppo_account_key",
        "account_no": "oppo_account_no"})

    df_result = ks.merge(df_transactions, df_customers, on="customer_key", how="inner")
    df_result = ks.merge(df_result, df_accounts, on="account_key", how="inner")
    df_result = ks.merge(df_result, df_oppo_accounts, on="oppo_account_key", how="left")

    df_result = df_result[["txn_key",
                           "customer_id",
                           "account_no",
                           "oppo_account_no",
                           "txn_date_time",
                           "txn_direction",
                           "txn_amount"]]

    df_result["txn_year_month"] = df_result["txn_date_time"].dt.strftime("%Y%m")

    return df_result.to_spark()


def ps_transform(df_customers, df_accounts, df_transactions):
    df_result = df_transactions.alias("transactions") \
        .join(df_customers.alias("customers"), on=["customer_key"], how="inner") \
        .join(df_accounts.alias("accounts"), on=["account_key"], how="inner") \
        .join(df_accounts.alias("oppo_accounts"),
              f.col("transactions.oppo_account_key") == f.col("oppo_accounts.account_key"),
              how="left") \
        .select("transactions.txn_key",
                "customers.customer_id",
                "accounts.account_no",
                f.col("oppo_accounts.account_no").alias("oppo_account_no"),
                "transactions.txn_date_time",
                "transactions.txn_direction",
                "transactions.txn_amount") \
        .withColumn("txn_year_month", f.date_format("transactions.txn_date_time", "yyyyMM"))

    return df_result


def sql_transform(spark, df_customers, df_accounts, df_transactions):
    df_customers.createOrReplaceTempView("customers")
    df_accounts.createOrReplaceTempView("accounts")
    df_transactions.createOrReplaceTempView("transactions")

    df_result = spark.sql("""
        SELECT
            transactions.txn_key,
            customers.customer_id,
            accounts.account_no,
            oppo_accounts.account_no AS oppo_account_no,
            transactions.txn_date_time,
            transactions.txn_direction,
            transactions.txn_amount,
            date_format(transactions.txn_date_time, 'yyyyMM') AS txn_year_month
        FROM
            transactions
            INNER JOIN customers ON transactions.customer_key = customers.customer_key
            INNER JOIN accounts ON transactions.account_key = accounts.account_key
            LEFT JOIN accounts AS oppo_accounts ON transactions.oppo_account_key = oppo_accounts.account_key
    """)

    return df_result


def run(spark):
    df_customers = customers.load_customers(spark)
    df_accounts = accounts.load_accounts(spark)
    df_transactions = stage_transactions.load_transactions(spark)

    df_result = ps_transform(df_customers, df_accounts, df_transactions)
    # df_result = ks_transform(df_customers, df_accounts, df_transactions)
    # df_result = sql_transform(spark, df_customers, df_accounts, df_transactions)

    stat_transactions.save_transactions(df_result)
