import click
import os
from utils.helpers import load_main_summary
from utils.helpers import load_raw_pings
from utils.helpers import load_bq_data
from utils.telemetry_data import *
from utils.amo_data import *
from utils.bq_data import *

DEFAULT_TZ = 'UTC'

# taken from Fx_Usage_Report
def get_spark():
    spark = (SparkSession
             .builder
             .appName("usage_report")
             .getOrCreate())

    spark.conf.set('spark.sql.session.timeZone', DEFAULT_TZ)

    return spark

def agg_addons_report(main_summary_data, raw_pings_data, bq_data, **kwargs):
    # TODO - get individual datasets

    # TODO - aggregate into one dataset

def main():

    # taken from Fx_Usage_Report
    ms = (
            load_main_summary(spark, input_bucket, input_prefix, input_version)
            .filter("submission_date_s3 <= '{}'".format(date))
            .filter("sample_id < {}".format(sample))
            .filter(col("normalized_channel").isin(ALLOWED_CHANNELS))
    .filter("app_name = 'Firefox'"))

    # TODO - load_raw_pings

    # TODO - load_bq_data

    # TODO - agg_addons_report to create dataset

    # TODO - write dataset to s3


if __name__ == '__main__':
    main()
