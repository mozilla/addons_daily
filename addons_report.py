import click
import os
from utils.helpers import load_main_summary,load_raw_pings, get_spark, get_sc, load_keyed_hist, load_bq_data
from utils.telemetry_data import *
from utils.amo_data import *
from utils.bq_data import *
from pyspark.sql import SparkSession

DEFAULT_TZ = 'UTC'


def agg_addons_report(main_summary_data, raw_pings_data, amo_data, bq_data, **kwargs):
    """
    This function will create the addons dataset
    """

    # TODO
    # Join bq_data to amo_data by slug, then drop slug
    # Join to main_summary_data and raw_pings_data by addon ID
    # Return aggregated dataset


def main():
    spark = get_spark(DEFAULT_TZ)
    sc = get_sc()
    ms = load_main_summary(spark,input_bucket='telemetry-parquet', input_prefix='main_summary', input_version='v4')
    main_summary = (
        ms
        .filter("submission_date >= (NOW() - INTERVAL 365 DAYS)")
    )
    raw_pings = load_raw_pings(sc)
    keyed_histograms = load_keyed_hist(raw_pings)


