import pytest

from psk.service import spark_service


@pytest.fixture
def spark():
    return spark_service.create_spark()


def test_load_customers(spark):
    from psk.job.transactions import customers

    customers_df = customers.load_customers(spark)

    assert customers_df is not None
    assert customers_df.count() != 0

    customers_df.show()


def test_load_accounts(spark):
    from psk.job.transactions import accounts

    accounts_df = accounts.load_accounts(spark)

    assert accounts_df is not None
    assert accounts_df.count() != 0

    accounts_df.show()

def test_load_transactions(spark):
    from psk.job.transactions import stage_transactions

    transactions_df = stage_transactions.load_transactions(spark)

    assert transactions_df is not None
    assert transactions_df.count() != 0

    transactions_df.show()

def test_run(spark):
    from psk.job.transactions import main

    main.run(spark)