from helper_functions import *
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql import SQLContext

spark = get_spark()


def get_page_load_times(df):
    """

    :param df: raw pings dataframe
    :return:
    """
    hist = "FX_PAGE_LOAD_MS_2"

    avg_page_load = (
      df
      .filter(lambda x: hist in x['payload']['histograms'].keys())
      .flatMap(lambda x: [(item,histogram_mean(x['payload']['histograms'][hist]['values']))
                          for item in x['environment']['addons']['activeAddons'].keys()])
    )

    schema = StructType([StructField('addon_id', StringType(), True),
                         StructField('avg_page_loadtime', FloatType(), True)])

    avg_page_load_df = spark.createDataFrame(data=avg_page_load, schema=schema)

    avg_page_load_agg = (
      avg_page_load_df
      .groupBy("addon_id")
      .agg(F.mean("avg_page_loadtime"))
      .withColumnRenamed("avg(avg_page_loadtime)", "avg_page_load_time")
    )
    return avg_page_load_agg


def get_tab_switch_time(df):
    """
    :param df: raw pings
    :return:
    """

    tab_switch_hist = (
        df
        .filter(lambda x: 'environment' in x.keys())
        .filter(lambda x: 'addons' in x['environment'].keys())
        .filter(lambda x: 'activeAddons' in x['environment']['addons'])
        .filter(lambda x: 'payload' in x.keys())
        .filter(lambda x: 'histograms' in x['payload'].keys())
        .filter(lambda x: 'FX_TAB_SWITCH_TOTAL_E10S_MS' in x['payload']['histograms'].keys())
        .map(lambda x: (x['environment']['addons']['activeAddons'].keys(),
                        x['payload']['histograms']['FX_TAB_SWITCH_TOTAL_E10S_MS']))
        .map(lambda x: [(i, x[1]) for i in x[0]])
        .flatMap(lambda x: x))

    tab_switch = tab_switch_hist.map(lambda x: (x[0], histogram_mean(x[1]['values'])))

    tab_switch_df = (
        spark.createDataFrame(tab_switch, ['addon_id', 'tab_switch_ms'])
        .groupby('addon_id')
        .agg(F.mean('tab_switch_ms').alias('tab_switch_ms')))

    return tab_switch_df
