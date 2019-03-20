import click
import os
from utils.helpers import load_main_summary
from utils.telemetry_data import *
from utils.amo_data import *
from utils.bq_data import *

DEFAULT_TZ = 'UTC'

def get_spark():
    spark = (SparkSession
             .builder
             .appName("usage_report")
             .getOrCreate())

    spark.conf.set('spark.sql.session.timeZone', DEFAULT_TZ)

    return spark

def agg_addons_report(main_summary_data, raw_pings_data, bq_data, **kwargs):
    """
    This function will create the addons dataset
    """

    # TODO

def main():

    # TODO
