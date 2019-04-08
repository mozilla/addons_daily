import click
import os
from utils.helpers import load_main_summary,load_raw_pings, get_spark, get_sc, load_keyed_hist, load_bq_data
from utils.telemetry_data import *
from utils.search_daily_data import *
from utils.amo_data import *
from utils.bq_data import *
from utils.raw_pings import *
from pyspark.sql import SparkSession

DEFAULT_TZ = 'UTC'


def agg_addons_report(main_summary_data, search_daily_data, raw_pings_data, **kwargs):
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
    tabs = get_tabs(addons_expanded)
    bookmarks = get_bookmarks(addons_expanded)
    dt_opened_ct = get_devtools_opened_count(addons_expanded)
    pct_tracking = get_pct_tracking_enabled(addons_expanded)
    dau = get_dau(addons_expanded)
    wau = get_wau(addons_expanded)
    mau = get_mau(addons_expanded)
    yau = get_yau(addons_expanded)

    # search metrics
    # search_daily = get_search_metrics(search_daily_data, addons_expanded)

    # raw pings metrics
    page_load_times = get_page_load_times(raw_pings_data)
    tab_switch_time = get_tab_switch_time(raw_pings_data)
    storage_get = get_storage_local_get_time(keyed_histograms)
    storage_set = get_storage_local_set_time(keyed_histograms)
    startup_time = get_startup_time(keyed_histograms)
    bkg_load_time = get_bkgd_load_time(keyed_histograms)
    ba_popup_lt = get_ba_popup_load_time(keyed_histograms)
    pa_popup_lt = get_pa_popup_load_time(keyed_histograms)
    cs_injection_time = get_cs_injection_time(keyed_histograms)
    mem_total = get_memory_total(keyed_histograms)

    agg_data = (
        os_dist
        .join(ct_dist, on='addon_id', how='left')
        .join(total_hours, on='addon_id', how='left')
        .join(active_hours, on='addon_id', how='left')
        .join(top_ten_others, on='addon_id', how='left')
        .join(avg_uri, on='addon_id', how='left')
        .join(tabs, on='addon_id', how='left')
        .join(bookmarks, on='addon_id', how='left')
        .join(dt_opened_ct, on='addon_id', how='left')
        .join(pct_tracking, on='addon_id', how='left')
        .join(dau, on='addon_id', how='left')
        .join(wau, on='addon_id', how='left')
        .join(mau, on='addon_id', how='left')
        .join(yau, on='addon_id', how='left')
        # .join(search_daily, on='addon_id', how='left')
        .join(page_load_times, on='addon_id', how='left')
        .join(tab_switch_time, on='addon_id', how='left')
        .join(storage_get, on='addon_id', how='left')
        .join(storage_set, on='addon_id', how='left')
        .join(startup_time, on='addon_id', how='left')
        .join(bkg_load_time, on='addon_id', how='left')
        .join(ba_popup_lt, on='addon_id', how='left')
        .join(pa_popup_lt, on='addon_id', how='left')
        .join(cs_injection_time, on='addon_id', how='left')
        .join(mem_total, on='addon_id', how='left')
        # .join(bq_data, on='addon_id', how='left')
    )

    return agg_data


def main():
    #path = '' # need to pass in from command line i think
    # path var is a path to the user credentials.json for BQ
    spark = get_spark(DEFAULT_TZ)
    sc = get_sc()
    ms = load_main_summary(spark, input_bucket='telemetry-parquet', input_prefix='main_summary', input_version='v4')
    main_summary = (
        ms
        .filter("submission_date >= (NOW() - INTERVAL 1 DAYS)")
    )

    # search_daily = load_search_daily()
    
    raw_pings = load_raw_pings(sc)

    #bq_d = load_bq_data(datetime.date.today(), path, spark)
    agg_data = agg_addons_report(main_summary, raw_pings)
    print(agg_data.collect()[0:10])
    #return agg_data


if __name__ == '__main__':
    main()


