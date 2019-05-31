from addons_daily.utils.telemetry_data import *
from addons_daily.addons_report import agg_addons_report
from addons_daily.utils.helpers import is_same
from pyspark.sql.types import *
from pyspark.sql import Row
import pytest
import json
import os


BASE_DATE = "20190515"


def load_json(filename):
    root = os.path.dirname(__file__)
    path = os.path.join(root, "resources", filename)
    with open(path) as f:
        d = json.load(f)
    return d


def load_df_from_json(prefix, spark):
    root = os.path.dirname(__file__)
    schema_path = os.path.join(root, "resources", "{}_schema.json".format(prefix))
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = os.path.join(root, "resources", "{}.json".format(prefix))
    # FAILFAST causes us to abort early if the data doesn't match
    # the given schema. Without this there was as very annoying
    # problem where dataframe.collect() would return an empty set.
    frame = spark.read.json(rows_path, schema, mode="FAILFAST")
    return frame


def df_to_json(df):
    return [i.asDict() for i in df.collect()]


@pytest.fixture()
def spark():
    spark_session = SparkSession.builder.appName("addons_daily_tests").getOrCreate()
    return spark_session


@pytest.fixture()
def main_summary(spark):
    return load_df_from_json("main_summary", spark)


@pytest.fixture()
def search_clients_daily(spark):
    return load_df_from_json("search_clients_daily", spark)


@pytest.fixture()
def events(spark):
    return load_df_from_json("events", spark)


@pytest.fixture()
def raw_pings():
    sc = SparkContext.getOrCreate()
    return sc.parallelize(load_json("raw_pings.json"))


def test_agg(main_summary, search_clients_daily, events, raw_pings, spark):
    agg = agg_addons_report(
        spark=spark,
        date=BASE_DATE,
        main_summary=main_summary,
        search_clients_daily=search_clients_daily,
        events=events,
        raw_pings=raw_pings,
    )
    result = df_to_json(agg)
    expected_result = load_json("expected_results.json")
    assert result == expected_result
