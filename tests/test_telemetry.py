from pyspark.sql.types import *
from pyspark.sql import Row
from utils.telemetry_data import *
from helpers.data_generators import make_telemetry_data
from utils.helpers import get_spark
import pytest

@pytest.fixture()
def addons_expanded():
    addons_expanded_sample,addons_schema = make_telemetry_data()
    addons_expanded_sample = [row.asDict() for row in addons_expanded_sample]
    sc = SparkContext.getOrCreate()
    spark = SQLContext.getOrCreate(sc)
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


def test_active_hours(addons_expanded):
    output = get_active_hours(addons_expanded).collect()
    expected_output = [Row(addon_id='screenshots@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='fxmonitor@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='formautofill@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='webcompat-reporter@mozilla.org', avg_active_hours=0.5486111111111112),
                       Row(addon_id='webcompat@mozilla.org', avg_active_hours=0.5486111111111112)]
    assert expected_output == output


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




