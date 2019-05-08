from pyspark.sql.types import *
from pyspark.sql import Row
from addons_daily.utils.telemetry_data import *
from .helpers.data_generators import make_telemetry_data
from addons_daily.utils.helpers import get_spark
import pytest

@pytest.fixture()
def addons_expanded():
    addons_expanded_sample,addons_schema = make_telemetry_data()
    addons_expanded_sample = [row.asDict() for row in addons_expanded_sample]
    sc = SparkContext.getOrCreate()
    spark = SQLContext.getOrCreate(sc)
    return spark.createDataFrame(addons_expanded_sample, addons_schema)

def dumb_test(addons_expanded):
    assert addons_expanded.collect() ==[Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0), client_id='9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id='fxmonitor@mozilla.org', blocklisted=False, name='Firefox Monitor', user_disabled=False, app_disabled=False, version='2.8', scope=1, type='extension', scalar_parent_browser_engagement_tab_open_event_count=15, foreign_install=False, has_binary_components=False, install_day=17877, update_day=17877, signed_state=3, is_system=True, is_web_extension=True, multiprocess_compatible=True, os='Windows_NT', country='ES', subsession_length=3392, places_pages_count=10, places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220, devtools_toolbox_opened_count=None, active_ticks=395, histogram_parent_tracking_protection_enabled={0: 1, 1: 0}, histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0, 964: 0, 1429: 1, 1174: 1}), Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0), client_id='9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id='webcompat-reporter@mozilla.org', blocklisted=False, name='WebCompat Reporter', user_disabled=False, app_disabled=False, version='1.1.0', scope=1, type='extension', scalar_parent_browser_engagement_tab_open_event_count=12, foreign_install=False, has_binary_components=False, install_day=17850, update_day=17876, signed_state=None, is_system=True, is_web_extension=True, multiprocess_compatible=True, os='Windows_NT', country='ES', subsession_length=3392, places_pages_count=100, places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220, devtools_toolbox_opened_count=None, active_ticks=395, histogram_parent_tracking_protection_enabled={0: 1, 1: 0}, histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0, 964: 0, 1429: 1, 1174: 1}), Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0), client_id='9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id='webcompat@mozilla.org', blocklisted=False, name='Web Compat', user_disabled=False, app_disabled=False, version='3.0.0', scope=1, type='extension', scalar_parent_browser_engagement_tab_open_event_count=5, foreign_install=False, has_binary_components=False, install_day=17850, update_day=17876, signed_state=None, is_system=True, is_web_extension=True, multiprocess_compatible=True, os='Windows_NT', country='ES', subsession_length=3392, places_pages_count=120, places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220, devtools_toolbox_opened_count=None, active_ticks=395, histogram_parent_tracking_protection_enabled={0: 1, 1: 0}, histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0, 964: 0, 1429: 1, 1174: 1}), Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0), client_id='9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id='screenshots@mozilla.org', blocklisted=False, name='Firefox Screenshots', user_disabled=False, app_disabled=False, version='35.0.0', scope=1, type='extension', scalar_parent_browser_engagement_tab_open_event_count=None, foreign_install=False, has_binary_components=False, install_day=17850, update_day=17876, signed_state=None, is_system=True, is_web_extension=True, multiprocess_compatible=True, os='Windows_NT', country='ES', subsession_length=3392, places_pages_count=None, places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220, devtools_toolbox_opened_count=None, active_ticks=395, histogram_parent_tracking_protection_enabled={0: 1, 1: 0}, histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0, 964: 0, 1429: 1, 1174: 1}), Row(Submission_date=datetime.datetime(2019, 1, 1, 0, 0), client_id='9ad5490a-6fd8-47e8-9a1e-68e759d7f073', addon_id='formautofill@mozilla.org', blocklisted=False, name='Form Autofill', user_disabled=False, app_disabled=False, version='1.0', scope=1, type='extension', scalar_parent_browser_engagement_tab_open_event_count=None, foreign_install=False, has_binary_components=False, install_day=17850, update_day=17876, signed_state=None, is_system=True, is_web_extension=True, multiprocess_compatible=True, os='Windows_NT', country='ES', subsession_length=3392, places_pages_count=10, places_bookmarks_count=5, scalar_parent_browser_engagement_total_uri_count=220, devtools_toolbox_opened_count=None, active_ticks=395, histogram_parent_tracking_protection_enabled={0: 1, 1: 0}, histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0, 964: 0, 1429: 1, 1174: 1})]


def test_browser_metrics(addons_expanded):
    """
    Given a dataframe of some actual sampled data, ensure that
    the get_pct_tracking_enabled outputs the correct dataframe
    :param addons_expanded: pytest fixture defined above
    :return: assertion whether the expected output indeed matches the true output
    """
    output = [row.asDict() for row in get_browser_metrics(addons_expanded).orderBy("addon_id").collect()]
    expected_output = [
                       dict(addon_id='formautofill@mozilla.org', avg_bookmarks=5.0, avg_tabs=10.0,
                           avg_toolbox_opened_count=None, avg_uri=220.0,
                           pct_w_tracking_prot_enabled=0.0),
                       dict(addon_id='fxmonitor@mozilla.org', avg_bookmarks=None, avg_tabs=10.0,
                           avg_toolbox_opened_count=None, avg_uri=220.0, pct_w_tracking_prot_enabled=0.0),
                       dict(addon_id='screenshots@mozilla.org', avg_bookmarks=None, avg_tabs=None,
                           avg_toolbox_opened_count=None, avg_uri=220.0, pct_w_tracking_prot_enabled=0.0),
                       dict(addon_id='webcompat-reporter@mozilla.org', avg_bookmarks=None, avg_tabs=100.0,
                           avg_toolbox_opened_count=None, avg_uri=220.0,
                           pct_w_tracking_prot_enabled=0.0),
                       dict(addon_id='webcompat@mozilla.org', avg_bookmarks=None, avg_tabs=120.0,
                           avg_toolbox_opened_count=None, avg_uri=220.0, pct_w_tracking_prot_enabled=0.0)]
    assert output == expected_output


@pytest.mark.xfail
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


@pytest.mark.xfail
def test_tabs(addons_expanded):
    """
    Given a dataframe of actual sampled data, ensure that the get_bookmarks_and_tabs outputs the correct dataframe
    :param addons_expanded: pytest fixture that generates addons_expanded sample
    :return: assertion whether the expected output indeed matches the true output
    """
    output = get_tabs(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_tabs=None),
                       Row(addon_id='fxmonitor@mozilla.org', avg_tabs=15.0),
                       Row(addon_id='formautofill@mozilla.org', avg_tabs=None),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_tabs=12.0),
                       Row(addon_id='webcompat@mozilla.org', avg_tabs=5.0)]
    assert output == expected_output


@pytest.mark.xfail
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
    assert output == expected_output


@pytest.mark.xfail
def test_active_hours(addons_expanded):
    output = get_active_hours(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='fxmonitor@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='formautofill@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='webcompat@mozilla.org', avg_active_hours=0.5486111111111112)]
    assert expected_output == output


@pytest.mark.xfail
def test_total_hours(addons_expanded):
    output = get_total_hours(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='fxmonitor@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='formautofill@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_time_active_ms=3392.0),
                       Row(addon_id='webcompat@mozilla.org', avg_time_active_ms=3392.0)]
    assert expected_output == output


@pytest.mark.xfail
def test_devtools(addons_expanded):
    output = get_devtools_opened_count(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='fxmonitor@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='formautofill@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_toolbox_opened_count=None),
                       Row(addon_id='webcompat@mozilla.org', avg_toolbox_opened_count=None)]
    assert output == expected_output


@pytest.mark.xfail
def test_uri(addons_expanded):
    output = get_avg_uri(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_uri=220.0),
                       Row(addon_id='fxmonitor@mozilla.org', avg_uri=220.0),
                       Row(addon_id='formautofill@mozilla.org', avg_uri=220.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_uri=220.0),
                       Row(addon_id='webcompat@mozilla.org', avg_uri=220.0)]

    assert output == expected_output


@pytest.mark.xfail
def test_tracking(addons_expanded):
    output = get_pct_tracking_enabled(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='fxmonitor@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='formautofill@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='webcompat-reporter@mozilla.org', pct_w_tracking_prot_enabled=0.0),
                       Row(addon_id='webcompat@mozilla.org', pct_w_tracking_prot_enabled=0.0)]

    assert output == expected_output
