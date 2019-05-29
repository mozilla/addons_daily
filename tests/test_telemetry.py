from pyspark.sql.types import *
from pyspark.sql import Row
from .data_generators import main_summary_for_user_engagement, load_expected_data
from addons_daily.utils.telemetry_data import *
from addons_daily.utils.helpers import is_same
import pytest
import json
import os


@pytest.fixture()
def spark():
    spark_session = SparkSession.builder.appName("addons_daily").getOrCreate()
    return spark_session


@pytest.fixture()
def addons_expanded(spark):
    d = load_expected_data("telemetry", spark)
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
    rows, schema = main_summary_for_user_engagement()
    main_rows = [row.asDict() for row in rows]
    uem_df = spark.createDataFrame(main_rows, schema)
    return uem_df


@pytest.fixture()
def main_summary_tto(spark):
    d = load_expected_data("mstto", spark)
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

    rows = [
        Row(
            addon_id="screenshots@mozilla.org",
            avg_tabs=None,
            avg_bookmarks=None,
            avg_toolbox_opened_count=None,
            avg_uri=220.0,
            pct_w_tracking_prot_enabled=0.0,
        ),
        Row(
            addon_id="fxmonitor@mozilla.org",
            avg_tabs=10.0,
            avg_bookmarks=None,
            avg_toolbox_opened_count=None,
            avg_uri=220.0,
            pct_w_tracking_prot_enabled=0.0,
        ),
        Row(
            addon_id="formautofill@mozilla.org",
            avg_tabs=10.0,
            avg_bookmarks=5.0,
            avg_toolbox_opened_count=None,
            avg_uri=220.0,
            pct_w_tracking_prot_enabled=0.0,
        ),
        Row(
            addon_id="webcompat-reporter@mozilla.org",
            avg_tabs=100.0,
            avg_bookmarks=None,
            avg_toolbox_opened_count=None,
            avg_uri=220.0,
            pct_w_tracking_prot_enabled=0.0,
        ),
        Row(
            addon_id="webcompat@mozilla.org",
            avg_tabs=120.0,
            avg_bookmarks=None,
            avg_toolbox_opened_count=None,
            avg_uri=220.0,
            pct_w_tracking_prot_enabled=0.0,
        ),
    ]

    expected_output = spark.createDataFrame(rows, schema)

    is_same(output, expected_output, True)


def _test_user_demo_metrics(addons_expanded, spark):
    output = get_user_demo_metrics(addons_expanded)

    schema = StructType(
        [
            StructField("addon_id", StringType(), False),
            StructField("os_dist", MapType(StringType(), FloatType()), True),
            StructField("country_dist", MapType(StringType(), FloatType()), True),
        ]
    )

    rows = [
        Row(
            addon_id="screenshots@mozilla.org",
            os_dist={"Windows_NT": 1.0},
            country_dist={"ES": 1.0},
        ),
        Row(
            addon_id="fxmonitor@mozilla.org",
            os_dist={"Windows_NT": 1.0},
            country_dist={"ES": 1.0},
        ),
        Row(
            addon_id="formautofill@mozilla.org",
            os_dist={"Windows_NT": 1.0},
            country_dist={"ES": 1.0},
        ),
        Row(
            addon_id="webcompat-reporter@mozilla.org",
            os_dist={"Windows_NT": 1.0},
            country_dist={"ES": 1.0},
        ),
        Row(
            addon_id="webcompat@mozilla.org",
            os_dist={"Windows_NT": 1.0},
            country_dist={"ES": 1.0},
        ),
    ]

    expected_output = spark.createDataFrame(rows, schema)
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

    rows = [
        Row(
            addon_id="screenshots@mozilla.org",
            top_ten_others=[
                "{webcompat@mozilla.org=10}",
                "{followonsearch@mozilla.com=10}",
                "{formautofill@mozilla.org=10}",
                "{firefox@getpocket.com=10}",
                "{aushelper@mozilla.org=10}",
                "{onboarding@mozilla.org=10}",
                "{webcompat-reporter@mozilla.org=10}",
                "{activity-stream@mozilla.org=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{ciscowebexstart1@cisco.com=1}",
            ],
        ),
        Row(
            addon_id="firefox@getpocket.com",
            top_ten_others=[
                "{activity-stream@mozilla.org=10}",
                "{onboarding@mozilla.org=10}",
                "{formautofill@mozilla.org=10}",
                "{webcompat-reporter@mozilla.org=10}",
                "{screenshots@mozilla.org=10}",
                "{webcompat@mozilla.org=10}",
                "{followonsearch@mozilla.com=10}",
                "{aushelper@mozilla.org=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{uBlock0@raymondhill.net=1}",
            ],
        ),
        Row(
            addon_id="mozilla_cc3@internetdownloadmanager.com",
            top_ten_others=[
                "{firefox@getpocket.com=1}",
                "{onboarding@mozilla.org=1}",
                "{formautofill@mozilla.org=1}",
                "{activity-stream@mozilla.org=1}",
                "{screenshots@mozilla.org=1}",
                "{webcompat-reporter@mozilla.org=1}",
                "{followonsearch@mozilla.com=1}",
                "{webcompat@mozilla.org=1}",
                "{aushelper@mozilla.org=1}",
            ],
        ),
        Row(
            addon_id="ciscowebexstart1@cisco.com",
            top_ten_others=[
                "{onboarding@mozilla.org=1}",
                "{aushelper@mozilla.org=1}",
                "{screenshots@mozilla.org=1}",
                "{webcompat@mozilla.org=1}",
                "{formautofill@mozilla.org=1}",
                "{activity-stream@mozilla.org=1}",
                "{followonsearch@mozilla.com=1}",
                "{webcompat-reporter@mozilla.org=1}",
                "{firefox@getpocket.com=1}",
            ],
        ),
        Row(
            addon_id="aushelper@mozilla.org",
            top_ten_others=[
                "{formautofill@mozilla.org=10}",
                "{firefox@getpocket.com=10}",
                "{onboarding@mozilla.org=10}",
                "{webcompat-reporter@mozilla.org=10}",
                "{activity-stream@mozilla.org=10}",
                "{screenshots@mozilla.org=10}",
                "{webcompat@mozilla.org=10}",
                "{followonsearch@mozilla.com=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{browsec@browsec.com=1}",
            ],
        ),
        Row(
            addon_id="browsec@browsec.com",
            top_ten_others=[
                "{aushelper@mozilla.org=1}",
                "{onboarding@mozilla.org=1}",
                "{screenshots@mozilla.org=1}",
                "{webcompat@mozilla.org=1}",
                "{webcompat-reporter@mozilla.org=1}",
                "{followonsearch@mozilla.com=1}",
                "{firefox@getpocket.com=1}",
                "{formautofill@mozilla.org=1}",
                "{activity-stream@mozilla.org=1}",
            ],
        ),
        Row(
            addon_id="onboarding@mozilla.org",
            top_ten_others=[
                "{firefox@getpocket.com=10}",
                "{webcompat-reporter@mozilla.org=10}",
                "{webcompat@mozilla.org=10}",
                "{aushelper@mozilla.org=10}",
                "{followonsearch@mozilla.com=10}",
                "{screenshots@mozilla.org=10}",
                "{activity-stream@mozilla.org=10}",
                "{formautofill@mozilla.org=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{uBlock0@raymondhill.net=1}",
            ],
        ),
        Row(
            addon_id="activity-stream@mozilla.org",
            top_ten_others=[
                "{screenshots@mozilla.org=10}",
                "{formautofill@mozilla.org=10}",
                "{followonsearch@mozilla.com=10}",
                "{webcompat@mozilla.org=10}",
                "{onboarding@mozilla.org=10}",
                "{firefox@getpocket.com=10}",
                "{aushelper@mozilla.org=10}",
                "{webcompat-reporter@mozilla.org=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{uBlock0@raymondhill.net=1}",
            ],
        ),
        Row(
            addon_id="{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}",
            top_ten_others=[
                "{firefox@getpocket.com=2}",
                "{webcompat@mozilla.org=2}",
                "{followonsearch@mozilla.com=2}",
                "{screenshots@mozilla.org=2}",
                "{webcompat-reporter@mozilla.org=2}",
                "{onboarding@mozilla.org=2}",
                "{aushelper@mozilla.org=2}",
                "{formautofill@mozilla.org=2}",
                "{activity-stream@mozilla.org=2}",
            ],
        ),
        Row(
            addon_id="followonsearch@mozilla.com",
            top_ten_others=[
                "{webcompat-reporter@mozilla.org=10}",
                "{firefox@getpocket.com=10}",
                "{aushelper@mozilla.org=10}",
                "{activity-stream@mozilla.org=10}",
                "{onboarding@mozilla.org=10}",
                "{webcompat@mozilla.org=10}",
                "{formautofill@mozilla.org=10}",
                "{screenshots@mozilla.org=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{uBlock0@raymondhill.net=1}",
            ],
        ),
        Row(
            addon_id="formautofill@mozilla.org",
            top_ten_others=[
                "{screenshots@mozilla.org=10}",
                "{webcompat-reporter@mozilla.org=10}",
                "{activity-stream@mozilla.org=10}",
                "{firefox@getpocket.com=10}",
                "{onboarding@mozilla.org=10}",
                "{aushelper@mozilla.org=10}",
                "{webcompat@mozilla.org=10}",
                "{followonsearch@mozilla.com=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{uBlock0@raymondhill.net=1}",
            ],
        ),
        Row(
            addon_id="webcompat-reporter@mozilla.org",
            top_ten_others=[
                "{firefox@getpocket.com=10}",
                "{onboarding@mozilla.org=10}",
                "{aushelper@mozilla.org=10}",
                "{formautofill@mozilla.org=10}",
                "{followonsearch@mozilla.com=10}",
                "{activity-stream@mozilla.org=10}",
                "{webcompat@mozilla.org=10}",
                "{screenshots@mozilla.org=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{ciscowebexstart1@cisco.com=1}",
            ],
        ),
        Row(
            addon_id="webcompat@mozilla.org",
            top_ten_others=[
                "{webcompat-reporter@mozilla.org=10}",
                "{firefox@getpocket.com=10}",
                "{followonsearch@mozilla.com=10}",
                "{aushelper@mozilla.org=10}",
                "{activity-stream@mozilla.org=10}",
                "{onboarding@mozilla.org=10}",
                "{formautofill@mozilla.org=10}",
                "{screenshots@mozilla.org=10}",
                "{{d10d0bf8-f5b5-c8b4-a8b2-2b9879e08c5d}=2}",
                "{{b9db16a4-6edc-47ec-a1f4-b86292ed211d}=1}",
            ],
        ),
        Row(
            addon_id="{b9db16a4-6edc-47ec-a1f4-b86292ed211d}",
            top_ten_others=[
                "{webcompat-reporter@mozilla.org=1}",
                "{firefox@getpocket.com=1}",
                "{activity-stream@mozilla.org=1}",
                "{onboarding@mozilla.org=1}",
                "{webcompat@mozilla.org=1}",
                "{screenshots@mozilla.org=1}",
                "{formautofill@mozilla.org=1}",
                "{aushelper@mozilla.org=1}",
                "{followonsearch@mozilla.com=1}",
            ],
        ),
        Row(
            addon_id="uBlock0@raymondhill.net",
            top_ten_others=[
                "{aushelper@mozilla.org=1}",
                "{firefox@getpocket.com=1}",
                "{activity-stream@mozilla.org=1}",
                "{followonsearch@mozilla.com=1}",
                "{screenshots@mozilla.org=1}",
                "{onboarding@mozilla.org=1}",
                "{formautofill@mozilla.org=1}",
                "{webcompat-reporter@mozilla.org=1}",
                "{webcompat@mozilla.org=1}",
            ],
        ),
    ]

    expected_output = spark.createDataFrame(rows, schema)

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

    rows = [
        Row(
            addon_id="screenshots@mozilla.org",
            avg_time_total=3392.0,
            active_hours=0.5486111111111112,
            disabled=None,
        ),
        Row(
            addon_id="firefox@getpocket.com",
            avg_time_total=None,
            active_hours=None,
            disabled=1,
        ),
        Row(
            addon_id="fxmonitor@mozilla.org",
            avg_time_total=3392.0,
            active_hours=0.5486111111111112,
            disabled=None,
        ),
        Row(
            addon_id="{CAFEEFAC-0016-0000-0039-ABCDEFFEDCBA}",
            avg_time_total=None,
            active_hours=None,
            disabled=1,
        ),
        Row(
            addon_id="ca@dictionaries.addons.mozilla.org",
            avg_time_total=None,
            active_hours=None,
            disabled=1,
        ),
        Row(
            addon_id="383882@modext.tech",
            avg_time_total=None,
            active_hours=None,
            disabled=1,
        ),
        Row(
            addon_id="en-GB@dictionaries.addons.mozilla.org",
            avg_time_total=None,
            active_hours=None,
            disabled=1,
        ),
        Row(
            addon_id="{972ce4c6-7e08-4474-a285-3208198ce6fd}",
            avg_time_total=None,
            active_hours=None,
            disabled=5,
        ),
        Row(
            addon_id="formautofill@mozilla.org",
            avg_time_total=3392.0,
            active_hours=0.5486111111111112,
            disabled=None,
        ),
        Row(
            addon_id="webcompat-reporter@mozilla.org",
            avg_time_total=3392.0,
            active_hours=0.5486111111111112,
            disabled=None,
        ),
        Row(
            addon_id="webcompat@mozilla.org",
            avg_time_total=3392.0,
            active_hours=0.5486111111111112,
            disabled=None,
        ),
        Row(
            addon_id="es-es@dictionaries.addons.mozilla.org",
            avg_time_total=None,
            active_hours=None,
            disabled=1,
        ),
    ]

    expected_output = spark.createDataFrame(rows, schema)

    is_same(output, expected_output, True)
