import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("addons_daily_tests").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark
    spark.stop()
