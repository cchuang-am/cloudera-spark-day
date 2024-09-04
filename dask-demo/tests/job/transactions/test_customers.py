def test_load_customers():
    from dsk.job.transactions import customers

    customers_df = customers.load_customers()

    assert customers_df is not None
    assert len(customers_df) != 0

    print(customers_df)
