from pyspark.sql.types import *
from pyspark.sql import Row
import datetime
from utils.telemetry_data import get_pct_tracking_enabled
from utils.helpers import get_spark

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
                              os=u'Windows_NT', country=u'ES', subsession_length=3392, places_pages_count=None,
                              places_bookmarks_count=None, scalar_parent_browser_engagement_total_uri_count=220,
                              devtools_toolbox_opened_count=None, active_ticks=395,
                              histogram_parent_tracking_protection_enabled={0: 1, 1: 0},
                              histogram_parent_webext_background_page_load_ms={1064: 3, 1577: 0,
                                                                               964: 0, 1429: 1, 1174: 1})]

spark = get_spark()


def test_pct_tracking_enabled(spark, data):
    df = spark.createDataFrame(data)
    output = get_pct_tracking_enabled(df)
    print(output)

    
test_pct_tracking_enabled(spark, addons_expanded_sample)


