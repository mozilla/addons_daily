from addons_daily.utils.telemetry_data import *
from addons_daily.addons_report import expand_addons
from addons_daily.utils.helpers import is_same
from pyspark.sql.types import *
from pyspark.sql import Row
import pytest
import json
import os


BASE_DATE = "20190515"


def df_to_json(df):
    return [i.asDict() for i in df.collect()]


# def load_expected_data(filename):
#     root = os.path.dirname(__file__)
#     path = os.path.join(root, "resources", filename)
#     with open(path) as f:
#         d = json.load(f)
#     return d


@pytest.fixture()
def spark():
    spark_session = SparkSession.builder.appName("addons_daily_tests").getOrCreate()
    return spark_session


# @pytest.fixture()
# def addons_expanded(spark):
#     d = load_expected_data("telemetry.json")
#     addons_schema = StructType(
#         [
#             StructField("submission_date", StringType(), True),
#             StructField("client_id", StringType(), True),
#             StructField("addon_id", StringType(), True),
#             StructField("blocklisted", BooleanType(), True),
#             StructField("name", StringType(), True),
#             StructField("user_disabled", BooleanType(), True),
#             StructField("app_disabled", BooleanType(), True),
#             StructField("version", StringType(), True),
#             StructField("scope", IntegerType(), True),
#             StructField("type", StringType(), True),
#             StructField(
#                 "scalar_parent_browser_engagement_tab_open_event_count",
#                 IntegerType(),
#                 True,
#             ),
#             StructField("foreign_install", BooleanType(), True),
#             StructField("has_binary_components", BooleanType(), True),
#             StructField("install_day", IntegerType(), True),
#             StructField("update_day", IntegerType(), True),
#             StructField("signed_state", IntegerType(), True),
#             StructField("is_system", BooleanType(), True),
#             StructField("is_web_extension", BooleanType(), True),
#             StructField("multiprocess_compatible", BooleanType(), True),
#             StructField("os", StringType(), True),
#             StructField("country", StringType(), True),
#             StructField("subsession_length", LongType(), True),
#             StructField("places_pages_count", IntegerType(), True),
#             StructField("places_bookmarks_count", IntegerType(), True),
#             StructField(
#                 "scalar_parent_browser_engagement_total_uri_count", IntegerType(), True
#             ),
#             StructField("devtools_toolbox_opened_count", IntegerType(), True),
#             StructField("active_ticks", IntegerType(), True),
#             StructField(
#                 "histogram_parent_tracking_protection_enabled",
#                 MapType(StringType(), IntegerType(), True),
#                 True,
#             ),
#             StructField(
#                 "histogram_parent_webext_background_page_load_ms",
#                 MapType(StringType(), IntegerType(), True),
#                 True,
#             ),
#         ]
#     )
#     return spark.createDataFrame(d, addons_schema)


# @pytest.fixture()
# def main_summary_uem(spark):
#     schema = StructType(
#         [
#             StructField("disabled_addons_ids", ArrayType(StringType(), True), True),
#             StructField("client_id", StringType(), True),
#         ]
#     )
#     d = load_expected_data("uem.json")
#     uem_df = spark.createDataFrame(d, schema)
#     return uem_df


# @pytest.fixture()
# def main_summary_tto(spark):
#     d = load_expected_data("mstto.json")
#     schema = StructType(
#         [
#             StructField("client_id", StringType(), True),
#             StructField(
#                 "active_addons",
#                 ArrayType(
#                     StructType(
#                         [
#                             StructField("addon_id", StringType(), True),
#                             StructField("blocklisted", BooleanType(), True),
#                             StructField("name", StringType(), True),
#                             StructField("user_disabled", BooleanType(), True),
#                             StructField("app_disabled", BooleanType(), True),
#                             StructField("version", StringType(), True),
#                             StructField("scope", IntegerType(), True),
#                             StructField("type", StringType(), True),
#                             StructField("foreign_install", BooleanType(), True),
#                             StructField("has_binary_components", BooleanType(), True),
#                             StructField("install_day", IntegerType(), True),
#                             StructField("update_day", IntegerType(), True),
#                             StructField("signed_state", IntegerType(), True),
#                             StructField("is_system", BooleanType(), True),
#                             StructField("is_web_extension", BooleanType(), True),
#                             StructField("multiprocess_compatible", BooleanType(), True),
#                         ]
#                     ),
#                     True,
#                 ),
#                 True,
#             ),
#         ]
#     )
#     return spark.createDataFrame(d, schema)


@pytest.fixture()
def main_summary(spark):
    root = os.path.dirname(__file__)
    schema_path = os.path.join(root, "resources", "main_summary_schema.json")
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = os.path.join(root, "resources", "main_summary.json")
    # FAILFAST causes us to abort early if the data doesn't match
    # the given schema. Without this there was as very annoying
    # problem where dataframe.collect() would return an empty set.
    frame = spark.read.json(rows_path, schema, mode="FAILFAST")
    return frame


@pytest.fixture()
def addons_expanded(main_summary):
    return expand_addons(main_summary)


@pytest.fixture()
def addons_expanded_day(addons_expanded):
    return addons_expanded.filter("submission_date_s3 = '{}'".format(BASE_DATE))


def test_browser_metrics(addons_expanded_day, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param addons_expanded: pytest fixture defined above
    :return: assertion whether the expected output indeed matches the true output
    """
    output = df_to_json(get_browser_metrics(addons_expanded_day))
    expected = [
        {
            "addon_id": u"baidu-code-update@mozillaonline.com",
            "avg_bookmarks": None,
            "avg_tabs": None,
            "avg_toolbox_opened_count": None,
            "avg_uri": 33.0,
            "pct_w_tracking_prot_enabled": 0.0,
        },
        {
            "addon_id": u"screenshots@mozilla.org",
            "avg_bookmarks": None,
            "avg_tabs": None,
            "avg_toolbox_opened_count": None,
            "avg_uri": 33.0,
            "pct_w_tracking_prot_enabled": 0.0,
        },
        {
            "addon_id": u"hotfix-update-xpi-intermediate@mozilla.com",
            "avg_bookmarks": None,
            "avg_tabs": None,
            "avg_toolbox_opened_count": None,
            "avg_uri": 33.0,
            "pct_w_tracking_prot_enabled": 0.0,
        },
        {
            "addon_id": u"fxmonitor@mozilla.org",
            "avg_bookmarks": None,
            "avg_tabs": None,
            "avg_toolbox_opened_count": None,
            "avg_uri": 33.0,
            "pct_w_tracking_prot_enabled": 0.0,
        },
        {
            "addon_id": u"formautofill@mozilla.org",
            "avg_bookmarks": None,
            "avg_tabs": None,
            "avg_toolbox_opened_count": None,
            "avg_uri": 33.0,
            "pct_w_tracking_prot_enabled": 0.0,
        },
        {
            "addon_id": u"webcompat@mozilla.org",
            "avg_bookmarks": None,
            "avg_tabs": None,
            "avg_toolbox_opened_count": None,
            "avg_uri": 33.0,
            "pct_w_tracking_prot_enabled": 0.0,
        },
    ]
    assert output == expected


def test_user_demo_metrics(addons_expanded_day, spark):
    output = df_to_json(get_user_demo_metrics(addons_expanded_day))
    expected = [
        {
            "addon_id": u"baidu-code-update@mozillaonline.com",
            "country_dist": {u"GB": 1.0},
            "os_dist": {u"Windows_NT": 1.0},
        },
        {
            "addon_id": u"screenshots@mozilla.org",
            "country_dist": {u"GB": 1.0},
            "os_dist": {u"Windows_NT": 1.0},
        },
        {
            "addon_id": u"hotfix-update-xpi-intermediate@mozilla.com",
            "country_dist": {u"GB": 1.0},
            "os_dist": {u"Windows_NT": 1.0},
        },
        {
            "addon_id": u"fxmonitor@mozilla.org",
            "country_dist": {u"GB": 1.0},
            "os_dist": {u"Windows_NT": 1.0},
        },
        {
            "addon_id": u"formautofill@mozilla.org",
            "country_dist": {u"GB": 1.0},
            "os_dist": {u"Windows_NT": 1.0},
        },
        {
            "addon_id": u"webcompat@mozilla.org",
            "country_dist": {u"GB": 1.0},
            "os_dist": {u"Windows_NT": 1.0},
        },
    ]
    print("output", output)
    print("expected", expected)
    assert output == expected


@pytest.mark.skip(reason="skipping while sorting out py4j issue")
def test_trend_metrics(addons_expanded, spark):
    output = df_to_json(get_trend_metrics(addons_expanded, BASE_DATE))
    expected_output = load_expected_data("trend.json")
    addons_expanded.unpersist()
    assert output == expected_output


@pytest.mark.skip(reason="skipping while sorting out py4j issue")
def test_top_ten_others(main_summary_tto, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param main_summary_tto: pytest fixture defined above, sample data from main_summary
    :return: assertion whether the expected output indeed matches the true output
    """
    output = df_to_json(get_top_ten_others(main_summary_tto))
    expected_output = load_expected_data("top_ten", spark)
    main_summary_tto.unpersist()
    assert output == expected_output


@pytest.mark.skip(reason="skipping while sorting out py4j issue")
def test_engagement_metrics(addons_expanded, main_summary_uem, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param addons_expanded: pytest fixture defined above
    :return: assertion whether the expected output indeed matches the true output
    """
    output = df_to_json(get_engagement_metrics(addons_expanded, main_summary_uem))
    expected_output = load_expected_data("engagement.json")
    addons_expanded.unpersist()
    main_summary_uem.unpersist()
    assert output == expected_output
