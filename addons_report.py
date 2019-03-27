import click
import os
from utils.helpers import load_main_summary,load_raw_pings, get_spark, get_sc, load_keyed_hist, load_bq_data
from utils.telemetry_data import *
from utils.amo_data import *
from utils.bq_data import *
from pyspark.sql import SparkSession

DEFAULT_TZ = 'UTC'


def agg_addons_report(main_summary_data, raw_pings_data, bq_data, **kwargs):
    """
    This function will create the addons dataset
    """
    addons_and_users = (
        main_summary_data
        .select("Submission_date", "client_id",
                F.explode("active_addons"),
                "os", "country", "subsession_length",
                "places_pages_count", "places_bookmarks_count",
                "scalar_parent_browser_engagement_total_uri_count",
                "devtools_toolbox_opened_count", "active_ticks",
                "histogram_parent_tracking_protection_enabled",
                "histogram_parent_webext_background_page_load_ms")
    )

    addons_expanded = (
            addons_and_users
            .select("Submission_date", "client_id",
                    "col.*",
                    "os", "country", "subsession_length",
                    "places_pages_count", "places_bookmarks_count",
                    "scalar_parent_browser_engagement_total_uri_count",
                    "devtools_toolbox_opened_count", "active_ticks",
                    "histogram_parent_tracking_protection_enabled",
                    "histogram_parent_webext_background_page_load_ms")
            .cache()
    )

    keyed_histograms = load_keyed_hist(raw_pings_data)

    # telemetry metrics
    os_dist = get_os_dist(addons_expanded)
    ct_dist = get_ct_dist(addons_expanded)
    total_hours = get_total_hours(addons_expanded)
    active_hours = get_active_hours(addons_expanded)
    top_ten_others = get_top_ten_others(addons_expanded)
    avg_uri = get_avg_uri(addons_expanded)
    tabs_and_bookmarks = get_tabs_and_bookmarks(addons_expanded)
    dt_opened_ct = get_devtools_opened_count(addons_expanded)
    pct_tracking = get_pct_tracking_enabled(addons_expanded)
    dau = get_dau(addons_expanded)
    wau = get_wau(addons_expanded)
    mau = get_mau(addons_expanded)
    yau = get_yau(addons_expanded)

    # raw pings metrics




    return data



def main():
    path = '' # need to pass in from command line i think
    # path var is a path to the user credentials.json for BQ
    spark = get_spark(DEFAULT_TZ)
    sc = get_sc()
    ms = load_main_summary(spark,input_bucket='telemetry-parquet', input_prefix='main_summary', input_version='v4')
    main_summary = (
        ms
        .filter("submission_date >= (NOW() - INTERVAL 365 DAYS)")
    )
    raw_pings = load_raw_pings(sc)
    bq_d = load_bq_data(path)
    agg_data = agg_addons_report(main_summary, raw_pings, bq_d)

