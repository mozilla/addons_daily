from pyspark.sql.types import *
from pyspark.sql import Row
from utils.search_daily_data import *
from utils.telemetry_data import *
from helpers.data_generators import make_search_daily_data, make_telemetry_data
from utils.helpers import get_spark
import pytest

@pytest.fixture()
def search_daily():
    search_daily_sample, search_daily_schema = make_search_daily_data()
    search_daily_sample = [row.asDict() for row in search_daily_sample]

    addons_expanded_sample, addons_expanded_schema = make_telemetry_data()
    addons_expanded_sample = [row.asDict() for row in addons_expanded_sample]

    sc = SparkContext.getOrCreate()
    spark = SQLContext.getOrCreate(sc)

    search_daily_df = spark.createDataFrame(search_daily_sample, search_daily_schema)
    addons_expanded_df = spark.createDataFrame(addons_expanded_sample, addons_expanded_schema)

    return search_daily_df, addons_expanded_df

def test_pct_tracking_enabled(search_daily_df, addons_expanded_df):
    """
    """
    output = get_search_metrics(search_daily_df, addons_expanded_df).collect()

    # TODO figure out expected output

    # expected_output = [Row(addon_id=u'screenshots@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'fxmonitor@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'formautofill@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'webcompat-reporter@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'webcompat@mozilla.org', pct_w_tracking_prot_enabled=0.0)]

    # assert output == expected_output