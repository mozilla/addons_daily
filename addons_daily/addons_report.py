import click
import os
from .utils.helpers import (
    load_data_s3,
    load_raw_pings,
    get_spark,
    get_sc,
    load_keyed_hist,
)
from .utils.telemetry_data import *
from .utils.search_daily_data import *
from .utils.events_data import *

# from .utils.amo_data import *
from .utils.bq_data import *
from .utils.raw_pings import *
from .utils.events_data import *
from pyspark.sql import SparkSession

DEFAULT_TZ = "UTC"


def expand_addons(main_summary):
    addons_and_users = main_summary.select(
        "submission_date_s3",
        "client_id",
        F.explode("active_addons"),
        "os",
        "country",
        "subsession_length",
        "scalar_parent_browser_engagement_tab_open_event_count",
        "places_bookmarks_count",
        "scalar_parent_browser_engagement_total_uri_count",
        "devtools_toolbox_opened_count",
        "active_ticks",
        "histogram_parent_tracking_protection_enabled",
        "histogram_parent_webext_background_page_load_ms",
    )

    addons_expanded = addons_and_users.select(
        "submission_date_s3",
        "client_id",
        "col.*",
        "os",
        "country",
        "subsession_length",
        "scalar_parent_browser_engagement_tab_open_event_count",
        "places_bookmarks_count",
        "scalar_parent_browser_engagement_total_uri_count",
        "devtools_toolbox_opened_count",
        "active_ticks",
        "histogram_parent_tracking_protection_enabled",
        "histogram_parent_webext_background_page_load_ms",
    )

    return addons_expanded


def agg_addons_report(
    spark, date, main_summary, search_clients_daily, events, raw_pings, **kwargs
):
    """
    This function will create the addons dataset
    """

    addons_expanded = expand_addons(main_summary)
    addons_expanded_day = addons_expanded.filter(
        "submission_date_s3 = '{}'".format(date)
    )

    main_summary_day = main_summary.filter("submission_date_s3 = '{}'".format(date))

    # telemetry metrics
    user_demo_metrics = get_user_demo_metrics(addons_expanded_day)
    engagement_metrics = get_engagement_metrics(addons_expanded_day, main_summary_day)
    browser_metrics = get_browser_metrics(addons_expanded_day)
    search_metrics = get_search_metrics(search_clients_daily, addons_expanded_day)
    # top_ten_others = get_top_ten_others(addons_expanded_day)
    # needs to process 30 days in past, use unfiltered dataframes
    trend_metrics = get_trend_metrics(addons_expanded, date)
    event_metrics = install_flow_events(events)

    # raw pings metrics
    storage_get = get_storage_local_get_time(raw_pings)
    storage_set = get_storage_local_set_time(raw_pings)
    startup_time = get_startup_time(raw_pings)
    bkg_load_time = get_bkgd_load_time(raw_pings)
    ba_popup_lt = get_ba_popup_load_time(raw_pings)
    pa_popup_lt = get_pa_popup_load_time(raw_pings)
    cs_injection_time = get_cs_injection_time(raw_pings)
    mem_total = get_memory_total(raw_pings)

    agg = (
        user_demo_metrics.join(engagement_metrics, on="addon_id", how="left")
        .join(browser_metrics, on="addon_id", how="left")
        # .join(top_ten_others, on="addon_id", how="left")
        .join(trend_metrics, on="addon_id", how="left")
        .join(search_metrics, on="addon_id", how="left")
        .join(event_metrics, on="addon_id", how="left")
        .join(storage_get, on="addon_id", how="left")
        .join(storage_set, on="addon_id", how="left")
        .join(startup_time, on="addon_id", how="left")
        .join(bkg_load_time, on="addon_id", how="left")
        .join(ba_popup_lt, on="addon_id", how="left")
        .join(pa_popup_lt, on="addon_id", how="left")
        .join(cs_injection_time, on="addon_id", how="left")
        .join(mem_total, on="addon_id", how="left")
    )

    return agg


@click.command()
@click.option("--date", required=True)
@click.option("--main_summary_version", default="v4")
@click.option("--search_clients_daily_version", default="v5")
@click.option("--events_version", default="v1")
@click.option("--sample", default=1, help="percent sample as int [1, 100]")
def main(
    date, sample, main_summary_version, search_clients_daily_version, events_version
):
    # path = '' # need to pass in from command line i think
    # path var is a path to the user credentials.json for BQ
    spark = get_spark(DEFAULT_TZ)
    sc = get_sc()

    # leave main_summary unfiltered by
    # date to get trend metrics
    main_summary = load_data_s3(
        spark,
        input_bucket="telemetry-parquet",
        input_prefix="main_summary",
        input_version=main_summary_version,
    ).filter(F.col("sample_id").isin(range(0, sample)))

    search_daily = (
        load_data_s3(
            spark,
            input_bucket="telemetry-parquet",
            input_prefix="search_clients_daily",
            input_version=search_clients_daily_version,
        )
        .filter(F.col("sample_id").isin(range(0, sample)))
        .filter("submission_date_s3 = '{}'".format(date))
    )

    events = (
        load_data_s3(
            spark,
            input_bucket="telemetry-parquet",
            input_prefix="events",
            input_version=events_version,
        )
        .filter(F.col("sample_id").isin(range(0, sample)))
        .filter("submission_date_s3 = '{}'".format(date))
    )

    raw_pings = load_raw_pings(sc, date)

    # bq_d = load_bq_data(datetime.date.today(), path, spark)

    agg_data = agg_addons_report(
        spark, date, main_summary, search_daily, events, raw_pings
    )
    print(agg_data.collect()[0:10])
    # return agg_data


if __name__ == "__main__":
    main()
