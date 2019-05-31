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


@pytest.fixture()
def spark():
    spark_session = SparkSession.builder.appName("addons_daily_tests").getOrCreate()
    return spark_session


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
def main_summary_day(main_summary):
    return main_summary.filter("submission_date_s3 = '{}'".format(BASE_DATE))


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


def test_trend_metrics(addons_expanded, spark):
    output = df_to_json(get_trend_metrics(addons_expanded, BASE_DATE))
    expected_output = [
        {
            "addon_id": u"baidu-code-update@mozillaonline.com",
            "dau": 1,
            "mau": 1,
            "wau": 1,
        },
        {
            "addon_id": u"tls13-version-fallback-rollout-bug1462099@mozilla.org",
            "dau": None,
            "mau": 1,
            "wau": None,
        },
        {"addon_id": u"screenshots@mozilla.org", "dau": 1, "mau": 2, "wau": 1},
        {"addon_id": u"firefox@getpocket.com", "dau": None, "mau": 1, "wau": None},
        {
            "addon_id": u"hotfix-update-xpi-intermediate@mozilla.com",
            "dau": 1,
            "mau": 1,
            "wau": 1,
        },
        {"addon_id": u"fxmonitor@mozilla.org", "dau": 1, "mau": 1, "wau": 1},
        {"addon_id": u"aushelper@mozilla.org", "dau": None, "mau": 1, "wau": None},
        {"addon_id": u"onboarding@mozilla.org", "dau": None, "mau": 1, "wau": None},
        {
            "addon_id": u"activity-stream@mozilla.org",
            "dau": None,
            "mau": 1,
            "wau": None,
        },
        {"addon_id": u"followonsearch@mozilla.com", "dau": None, "mau": 1, "wau": None},
        {"addon_id": u"formautofill@mozilla.org", "dau": 1, "mau": 2, "wau": 1},
        {"addon_id": u"webcompat@mozilla.org", "dau": 1, "mau": 2, "wau": 1},
    ]
    assert output == expected_output


def test_top_ten_others(main_summary_day, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param main_summary_tto: pytest fixture defined above, sample data from main_summary
    :return: assertion whether the expected output indeed matches the true output
    """
    pass


def test_engagement_metrics(addons_expanded_day, main_summary_day, spark):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param addons_expanded: pytest fixture defined above
    :return: assertion whether the expected output indeed matches the true output
    """
    output = df_to_json(get_engagement_metrics(addons_expanded_day, main_summary_day))
    expected_output = [
        {
            "active_hours": 0.18194444444444444,
            "addon_id": u"baidu-code-update@mozillaonline.com",
            "avg_time_total": 747.0,
            "disabled": None,
        },
        {
            "active_hours": 0.18194444444444444,
            "addon_id": u"screenshots@mozilla.org",
            "avg_time_total": 747.0,
            "disabled": None,
        },
        {
            "active_hours": 0.18194444444444444,
            "addon_id": u"hotfix-update-xpi-intermediate@mozilla.com",
            "avg_time_total": 747.0,
            "disabled": None,
        },
        {
            "active_hours": 0.18194444444444444,
            "addon_id": u"fxmonitor@mozilla.org",
            "avg_time_total": 747.0,
            "disabled": None,
        },
        {
            "active_hours": 0.18194444444444444,
            "addon_id": u"formautofill@mozilla.org",
            "avg_time_total": 747.0,
            "disabled": None,
        },
        {
            "active_hours": 0.18194444444444444,
            "addon_id": u"webcompat@mozilla.org",
            "avg_time_total": 747.0,
            "disabled": None,
        },
    ]
    assert output == expected_output
