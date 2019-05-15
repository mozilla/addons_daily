from pyspark.sql.types import *
from pyspark.sql import Row
from addons_daily.utils.search_daily_data import *
from addons_daily.utils.telemetry_data import *
from .helpers.data_generators import make_search_daily_data, make_telemetry_data
from addons_daily.utils.helpers import get_spark
import pytest


@pytest.fixture
def spark():
    sc = SparkContext.getOrCreate()
    return SQLContext.getOrCreate(sc)


@pytest.fixture
def search_daily(spark):
    search_daily_sample, search_daily_schema = make_search_daily_data()
    search_daily_sample = [row.asDict() for row in search_daily_sample]
    return spark.createDataFrame(search_daily_sample, search_daily_schema)


@pytest.fixture
def addons_expanded(spark):
    addons_expanded_sample, addons_expanded_schema = make_telemetry_data()
    addons_expanded_sample = [row.asDict() for row in addons_expanded_sample]
    return spark.createDataFrame(addons_expanded_sample, addons_expanded_schema)


<<<<<<< HEAD
#def test_search_metrics(search_daily, addons_expanded):
 #   """
  #  """
   # output = get_search_metrics(search_daily, addons_expanded)
=======
def test_pct_tracking_enabled(search_daily, addons_expanded):
    """
    """
    output = get_search_metrics(search_daily, addons_expanded).collect()
>>>>>>> 188f7431f348408a78f2926f7c3b8cfa52a0291e

    # TODO figure out expected output

    # expected_output = [Row(addon_id=u'screenshots@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'fxmonitor@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'formautofill@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'webcompat-reporter@mozilla.org', pct_w_tracking_prot_enabled=0.0),
    #                    Row(addon_id=u'webcompat@mozilla.org', pct_w_tracking_prot_enabled=0.0)]

    # assert output == expected_output
