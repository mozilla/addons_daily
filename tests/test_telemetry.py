from pyspark.sql.types import *
from pyspark.sql import Row
import datetime
from utils.telemetry_data import *
from utils.helpers import get_spark
import pytest

@pytest.fixture()
def addons_expanded():
    addons_expanded_sample = [Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0),
                                  client_id=u'9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id=u'fxmonitor@mozilla.org',
                                  blocklisted=False, name=u'Firefox Monitor', user_disabled=False, app_disabled=False,
                                  version=u'2.8', scope=1, type=u'extension', foreign_install=False,
                                  has_binary_components=False, install_day=17877, update_day=17877, signed_state=3,
                                  is_system=True, is_web_extension=True, multiprocess_compatible=True, os=u'Windows_NT',
                                  country=u'ES', subsession_length=3392, places_pages_count=None,
                                  places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220,
                                  devtools_toolbox_opened_count=None, active_ticks=395,
                                  histogram_parent_tracking_protection_enabled={0: 1, 1: 0},
                                  histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0, 964: 0,
                                                                                   1429: 1, 1174: 1}),
                              Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0),
                                  client_id=u'9ad5490a-6fd8-47e8-9a1e-68e759d7f073',
                                  addon_id=u'webcompat-reporter@mozilla.org', blocklisted=False, name=u'WebCompat Reporter',
                                  user_disabled=False, app_disabled=False, version=u'1.1.0', scope=1, type=u'extension',
                                  foreign_install=False, has_binary_components=False, install_day=17850, update_day=17876,
                                  signed_state=None, is_system=True, is_web_extension=True, multiprocess_compatible=True,
                                  os=u'Windows_NT', country=u'ES', subsession_length=3392, places_pages_count=None,
                                  places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220,
                                  devtools_toolbox_opened_count=None, active_ticks=395,
                                  histogram_parent_tracking_protection_enabled={0: 1, 1: 0},
                                  histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0,
                                                                                   964: 0, 1429: 1, 1174: 1}),
                              Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0),
                                  client_id=u'9ad5490a-6fd8-47e8-9a1e-68e759d7f073',
                                  addon_id=u'webcompat@mozilla.org', blocklisted=False, name=u'Web Compat',
                                  user_disabled=False, app_disabled=False, version=u'3.0.0', scope=1, type=u'extension',
                                  foreign_install=False, has_binary_components=False, install_day=17850, update_day=17876,
                                  signed_state=None, is_system=True, is_web_extension=True, multiprocess_compatible=True,
                                  os=u'Windows_NT', country=u'ES', subsession_length=3392, places_pages_count=None,
                                  places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220,
                                  devtools_toolbox_opened_count=None, active_ticks=395,
                                  histogram_parent_tracking_protection_enabled={0: 1, 1: 0},
                                  histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0,
                                                                                   964: 0, 1429: 1, 1174: 1}),
                              Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0),
                                  client_id=u'9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id=u'screenshots@mozilla.org',
                                  blocklisted=False, name=u'Firefox Screenshots', user_disabled=False, app_disabled=False,
                                  version=u'35.0.0', scope=1, type=u'extension', foreign_install=False,
                                  has_binary_components=False, install_day=17850, update_day=17876, signed_state=None,
                                  is_system=True, is_web_extension=True, multiprocess_compatible=True, os=u'Windows_NT',
                                  country=u'ES', subsession_length=3392, places_pages_count=None,
                                  places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220,
                                  devtools_toolbox_opened_count=None, active_ticks=395,
                                  histogram_parent_tracking_protection_enabled={0: 1, 1: 0},
                                  histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0,
                                                                                   964: 0, 1429: 1, 1174: 1}),
                              Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0),
                                  client_id=u'9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id=u'formautofill@mozilla.org',
                                  blocklisted=False, name=u'Form Autofill', user_disabled=False, app_disabled=False,
                                  version=u'1.0', scope=1, type=u'extension', foreign_install=False,
                                  has_binary_components=False, install_day=17850, update_day=17876,
                                  signed_state=None, is_system=True, is_web_extension=True, multiprocess_compatible=True,
                                  os=u'Windows_NT', country=u'ES', subsession_length=3392, places_pages_count=10,
                                  places_bookmarks_count=5, scalar_parent_browser_engagement_total_uri_count=220,
                                  devtools_toolbox_opened_count=None, active_ticks=395,
                                  histogram_parent_tracking_protection_enabled={0: 1, 1: 0},
                                  histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0,
                                                                                   964: 0, 1429: 1, 1174: 1})]

    addons_schema = StructType([StructField('Submission_date', TimestampType(), True),
                                StructField('client_id', StringType(), True), StructField('addon_id', StringType(), True),
                                StructField('blocklisted', BooleanType(), True), StructField('name', StringType(), True),
                                StructField('user_disabled', BooleanType(), True),
                                StructField('app_disabled', BooleanType(), True),
                                StructField('version', StringType(), True), StructField('scope', IntegerType(), True),
                                StructField('type', StringType(), True),
                                StructField('foreign_install', BooleanType(), True),
                                StructField('has_binary_components', BooleanType(), True),
                                StructField('install_day', IntegerType(), True),
                                StructField('update_day', IntegerType(), True),
                                StructField('signed_state', IntegerType(), True),
                                StructField('is_system', BooleanType(), True),
                                StructField('is_web_extension', BooleanType(), True),
                                StructField('multiprocess_compatible', BooleanType(), True),
                                StructField('os', StringType(), True),
                                StructField('country', StringType(), True),
                                StructField('subsession_length', LongType(), True),
                                StructField('places_pages_count', IntegerType(), True),
                                StructField('places_bookmarks_count', IntegerType(), True),
                                StructField('scalar_parent_browser_engagement_total_uri_count', IntegerType(), True),
                                StructField('devtools_toolbox_opened_count', IntegerType(), True),
                                StructField('active_ticks', IntegerType(), True),
                                StructField('histogram_parent_tracking_protection_enabled',
                                            MapType(IntegerType(), IntegerType(), True), True),
                                StructField('histogram_parent_webext_background_page_load_ms',
                                            MapType(IntegerType(), IntegerType(), True), True)])

    addons_expanded_sample = [row.asDict() for row in addons_expanded_sample]
    spark = get_spark()
    addons_df = spark.createDataFrame(addons_expanded_sample, addons_schema)
    return addons_df


def test_pct_tracking_enabled(addons_expanded):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param addons_expanded: pytest fixture defined above
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_pct_tracking_enabled(addons_expanded).collect()
    expected_output = [Row(addon_id=u'screenshots@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id=u'fxmonitor@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id=u'formautofill@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id=u'webcompat-reporter@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id=u'webcompat@mozilla.org', pct_w_tracking_prot_enabled=0.0)]
    assert output == expected_output


def test_country_distribution(addons_expanded):
    """
    Given a dataframe of actual sampled data, ensure that the get_ct_dist outputs the correct dataframe
    :param addons_expanded: pytest fixture that generates addons_expanded sample
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_ct_dist(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', country_dist={'ES': 1.0}),
                       Row(addon_id='fxmonitor@mozilla.org', country_dist={'ES': 1.0}),
                       Row(addon_id='formautofill@mozilla.org', country_dist={'ES': 1.0}),
                       Row(addon_id='webcompat-reporter@mozilla.org', country_dist={'ES': 1.0}),
                       Row(addon_id='webcompat@mozilla.org', country_dist={'ES': 1.0})]

    assert output == expected_output


def test_tabs(addons_expanded):
    """
    Given a dataframe of actual sampled data, ensure that the get_bookmarks_and_tabs outputs the correct dataframe
    :param addons_expanded: pytest fixture that generates addons_expanded sample
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_tabs(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_tabs=None),
                       Row(addon_id='fxmonitor@mozilla.org', avg_tabs=None),
                       Row(addon_id='formautofill@mozilla.org', avg_tabs=10.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_tabs=None),
                       Row(addon_id='webcompat@mozilla.org', avg_tabs=None)]
    print(output)
    assert output == expected_output


def test_bookmarks(addons_expanded):
    """
    Given a dataframe of actual sampled data, ensure that the get_bookmarks_and_tabs outputs the correct dataframe
    :param addons_expanded: pytest fixture that generates addons_expanded sample
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_bookmarks(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_bookmarks=None),
                       Row(addon_id='fxmonitor@mozilla.org', avg_bookmarks=None),
                       Row(addon_id='formautofill@mozilla.org', avg_bookmarks=5.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_bookmarks=None),
                       Row(addon_id='webcompat@mozilla.org', avg_bookmarks=None)]
    print(output)
    assert output == expected_output


# def test_active_hours(addons_expanded):
#    output = get_active_hours(addons_expanded).collect()
#    expected_output = [Row(addon_id='screenshots@mozilla.org', active_hours=0.5486111111111112),
#                       Row(addon_id='fxmonitor@mozilla.org', active_hours=0.5486111111111112),
#                       Row(addon_id='formautofill@mozilla.org', active_hours=0.5486111111111112),
#                       Row(addon_id='webcompat-reporter@mozilla.org', active_hours=0.5486111111111112),
#                       Row(addon_id='webcompat@mozilla.org', active_hours=0.5486111111111112)]
#    assert expected_output == output


def test_total_hours(addons_expanded):
    output = get_total_hours(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='fxmonitor@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='formautofill@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='webcompat@mozilla.org', avg_time_active_ms=3392.0)]
    assert expected_output == output


def test_devtools(addons_expanded):
    output = get_devtools_opened_count(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='fxmonitor@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='formautofill@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='webcompat@mozilla.org', avg_toolbox_opened_count=None)]
    assert output == expected_output


def test_uri(addons_expanded):
    output = get_avg_uri(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_uri=220.0),
                       Row(addon_id='fxmonitor@mozilla.org', avg_uri=220.0),
                       Row(addon_id='formautofill@mozilla.org', avg_uri=220.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_uri=220.0),
                       Row(addon_id='webcompat@mozilla.org', avg_uri=220.0)]

    assert output == expected_output


def test_tracking(addons_expanded):
    output = get_pct_tracking_enabled(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='fxmonitor@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='formautofill@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='webcompat@mozilla.org', pct_w_tracking_prot_enabled=0.0)]

    assert output == expected_output




