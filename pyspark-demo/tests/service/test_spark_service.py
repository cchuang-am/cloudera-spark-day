from psk.service import spark_service

def test_create_config():
    config = spark_service.create_config()
    assert config is not None

def test_create_spark():
    spark = spark_service.create_spark()
    assert spark is not None