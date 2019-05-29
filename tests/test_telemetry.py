from addons_daily.utils.telemetry_data import *
from addons_daily.utils.helpers import is_same
from pyspark.sql.types import *
from pyspark.sql import Row
import pytest
import json
import os


def load_expected_data(filename, spark):
    root = os.path.dirname(__file__)
    path = os.path.join(root, "resources", filename)
    with open(path) as f:
        d = json.load(f)
    return d


@pytest.fixture()
def spark():
    spark_session = SparkSession.builder.appName("addons_daily").getOrCreate()
    return spark_session


@pytest.fixture()
def addons_expanded(spark):
    d = load_expected_data("telemetry.json", spark)
    addons_schema = StructType(
        [
            StructField("submission_date", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("addon_id", StringType(), True),
            StructField("blocklisted", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("user_disabled", BooleanType(), True),
            StructField("app_disabled", BooleanType(), True),
            StructField("version", StringType(), True),
            StructField("scope", IntegerType(), True),
            StructField("type", StringType(), True),
            StructField(
                "scalar_parent_browser_engagement_tab_open_event_count",
                IntegerType(),
                True,
            ),
            StructField("foreign_install", BooleanType(), True),
            StructField("has_binary_components", BooleanType(), True),
            StructField("install_day", IntegerType(), True),
            StructField("update_day", IntegerType(), True),
            StructField("signed_state", IntegerType(), True),
            StructField("is_system", BooleanType(), True),
            StructField("is_web_extension", BooleanType(), True),
            StructField("multiprocess_compatible", BooleanType(), True),
            StructField("os", StringType(), True),
            StructField("country", StringType(), True),
            StructField("subsession_length", LongType(), True),
            StructField("places_pages_count", IntegerType(), True),
            StructField("places_bookmarks_count", IntegerType(), True),
            StructField(
                "scalar_parent_browser_engagement_total_uri_count", IntegerType(), True
            ),
            StructField("devtools_toolbox_opened_count", IntegerType(), True),
            StructField("active_ticks", IntegerType(), True),
            StructField(
                "histogram_parent_tracking_protection_enabled",
                MapType(StringType(), IntegerType(), True),
                True,
            ),
            StructField(
                "histogram_parent_webext_background_page_load_ms",
                MapType(StringType(), IntegerType(), True),
                True,
            ),
        ]
    )
    return spark.createDataFrame(d, addons_schema)


@pytest.fixture()
def main_summary_uem(spark):
    schema = StructType(
        [
            StructField("disabled_addons_ids", ArrayType(StringType(), True), True),
            StructField("client_id", StringType(), True),
        ]
    )
    d = load_expected_data("uem.json", spark)
    uem_df = spark.createDataFrame(d, schema)
    return uem_df


@pytest.fixture()
def main_summary_tto(spark):
    d = load_expected_data("mstto.json", spark)
    schema = StructType(
        [
            StructField("client_id", StringType(), True),
            StructField(
                "active_addons",
                ArrayType(
                    StructType(
                        [
                            StructField("addon_id", StringType(), True),
                            StructField("blocklisted", BooleanType(), True),
                            StructField("name", StringType(), True),
                            StructField("user_disabled", BooleanType(), True),
                            StructField("app_disabled", BooleanType(), True),
                            StructField("version", StringType(), True),
                            StructField("scope", IntegerType(), True),
                            StructField("type", StringType(), True),
                            StructField("foreign_install", BooleanType(), True),
                            StructField("has_binary_components", BooleanType(), True),
                            StructField("install_day", IntegerType(), True),
                            StructField("update_day", IntegerType(), True),
                            StructField("signed_state", IntegerType(), True),
                            StructField("is_system", BooleanType(), True),
                            StructField("is_web_extension", BooleanType(), True),
                            StructField("multiprocess_compatible", BooleanType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    return spark.createDataFrame(d, schema)


def test_browser_metrics(addons_expanded, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param addons_expanded: pytest fixture defined above
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_browser_metrics(addons_expanded)

    schema = StructType(
        [
            StructField("addon_id", StringType(), False),
            StructField("avg_bookmarks", FloatType(), True),
            StructField("avg_tabs", FloatType(), True),
            StructField("avg_toolbox_opened_count", FloatType(), True),
            StructField("avg_uri", FloatType(), True),
            StructField("pct_w_tracking_prot_enabled", FloatType(), True),
        ]
    )

    d = load_expected_data("browser.json", spark)
    expected_output = spark.createDataFrame(d, schema)

    is_same(output, expected_output, True)


def test_user_demo_metrics(addons_expanded, spark):
    # aggregate nested maps before calling intersect
    def agg_map(df):
        return df.select("addon_id", "os_dist.Windows_NT", "country_dist.ES")

    output = agg_map(get_user_demo_metrics(addons_expanded))

    schema = StructType(
        [
            StructField("addon_id", StringType(), False),
            StructField("os_dist", MapType(StringType(), FloatType()), True),
            StructField("country_dist", MapType(StringType(), FloatType()), True),
        ]
    )

    d = load_expected_data("demo.json", spark)
    expected_output = agg_map(spark.createDataFrame(d, schema))
    is_same(output, expected_output, True)


def test_trend_metrics(addons_expanded, spark):

    output = get_trend_metrics(addons_expanded)

    schema = StructType(
        [
            StructField("addon_id", StringType(), True),
            StructField("dau", LongType(), True),
            StructField("mau", LongType(), True),
            StructField("wau", LongType(), True),
        ]
    )

    rows = [
        Row(addon_id="screenshots@mozilla.org", mau=1, wau=None, dau=None),
        Row(addon_id="fxmonitor@mozilla.org", mau=1, wau=1, dau=1),
        Row(addon_id="webcompat-reporter@mozilla.org", mau=1, wau=None, dau=None),
        Row(addon_id=u"webcompat@mozilla.org", dau=None, mau=1, wau=None),
    ]

    expected_output = spark.createDataFrame(rows, schema)

    is_same(output, expected_output, True)


@pytest.mark.skip(reason="skipping while sorting out py4j issue")
def test_top_ten_others(main_summary_tto, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param main_summary_tto: pytest fixture defined above, sample data from main_summary
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_top_ten_others(main_summary_tto)

    schema = StructType(
        [
            StructField("addon_id", StringType(), True),
            StructField("top_ten_others", ArrayType(StringType(), True), True),
        ]
    )

    d = load_expected_data("top_ten", spark)
    expected_output = spark.createDataFrame(d, schema)

    is_same(output, expected_output, True)


def test_engagement_metrics(addons_expanded, main_summary_uem, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param addons_expanded: pytest fixture defined above
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_engagement_metrics(addons_expanded, main_summary_uem)

    schema = StructType(
        [
            StructField("active_hours", DoubleType(), True),
            StructField("addon_id", StringType(), True),
            StructField("avg_time_total", DoubleType(), True),
            StructField("disabled", LongType(), True),
        ]
    )
    d = load_expected_data("engagement.json", spark)
    expected_output = spark.createDataFrame(d, schema)

    is_same(output, expected_output, True)
