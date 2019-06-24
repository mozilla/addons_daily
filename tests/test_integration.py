import addons_daily
from addons_daily.utils.telemetry_data import *
from addons_daily.addons_report import agg_addons_report, main
from addons_daily.utils.helpers import is_same
from pyspark.sql.types import *
from pyspark.sql import Row, functions as F
import pytest
import json
import os
from click.testing import CliRunner


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
    # uncomment for test dev
    with open("TEST.json", "w") as f:
        import re

        f.write(
            re.sub(
                "None",
                "null",
                re.sub(
                    "True",
                    "true",
                    re.sub("False", "false", re.sub("'", '"', str(df_to_json(agg)))),
                ),
            )
        )

    result = df_to_json(agg)
    expected_result = load_json("expected_results.json")
    assert result == expected_result


@pytest.fixture()
def mock_main_dependencies(
    main_summary, search_clients_daily, events, raw_pings, spark, monkeypatch
):
    def mock_get_spark(*args, **kwargs):
        return spark

    def mock_load_data_s3(*args, **kwargs):
        prefix = kwargs["input_prefix"]
        if prefix == "main_summary":
            df = main_summary.withColumn("normalized_channel", F.lit("release"))
        elif prefix == "search_clients_daily":
            df = search_clients_daily
        elif prefix == "events":
            df = events
        else:
            raise NotImplementedError
        return df.withColumn("sample_id", F.lit("0"))

    def mock_load_raw_pings(*args, **kwargs):
        return raw_pings

    monkeypatch.setattr(addons_daily.addons_report, "get_spark", mock_get_spark)
    monkeypatch.setattr(addons_daily.addons_report, "load_data_s3", mock_load_data_s3)
    monkeypatch.setattr(
        addons_daily.addons_report, "load_raw_pings", mock_load_raw_pings
    )


def test_main_end_to_end(mock_main_dependencies, tmpdir):
    runner = CliRunner()
    result = runner.invoke(
        main, ["--date", BASE_DATE, "--output", "file://" + str(tmpdir)]
    )
    assert result.exit_code == 0
    submission_path = tmpdir / "submission_date_s3={}".format(BASE_DATE)
    assert os.path.exists(submission_path)
    assert len([p for p in os.listdir(submission_path) if p.endswith(".parquet")]) > 0


def test_main_invalid_prefix(mock_main_dependencies, tmpdir):
    runner = CliRunner()

    # missing "file://"
    result = runner.invoke(main, ["--date", BASE_DATE, "--output", str(tmpdir)])
    assert result.exit_code == 1
    assert isinstance(result.exception, ValueError)

    # not a supported path
    result = runner.invoke(
        main, ["--date", BASE_DATE, "--output", "gs://" + str(tmpdir)]
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, ValueError)
