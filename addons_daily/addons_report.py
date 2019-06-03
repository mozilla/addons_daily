import click
import os
from .utils.helpers import (
    load_data_s3,
    load_raw_pings,
    get_spark,
    dataframe_joiner,
    get_sc,
    load_keyed_hist,
)
from .utils.telemetry_data import *

# from .utils.amo_data import *
from .utils.bq_data import *
from .utils.raw_pings import *
from pyspark.sql import SparkSession

DEFAULT_TZ = "UTC"

CORE_FIELDS = [
    "submission_date_s3",
    "client_id",
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
]


def expand_addons(main_summary):
    addons_and_users = main_summary.select(CORE_FIELDS + [F.explode("active_addons")])
    addons_expanded = addons_and_users.select(CORE_FIELDS + ["col.*"])
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
    addon_names = get_top_addon_names(addons_expanded_day)
    user_demo_metrics = get_user_demo_metrics(addons_expanded_day)
    engagement_metrics = get_engagement_metrics(addons_expanded_day, main_summary_day)
    browser_metrics = get_browser_metrics(addons_expanded_day)
    search_metrics = get_search_metrics(search_clients_daily, addons_expanded_day)
    top_ten_coinstalls = get_top_10_coinstalls(addons_expanded_day)
    # needs to process 30 days in past, use unfiltered dataframes
    trend_metrics = get_trend_metrics(addons_expanded, date)
    event_metrics = install_flow_events(events)

    keyed_hists = load_keyed_hist(raw_pings)
    # raw pings metrics
    storage_get = get_storage_local_get_time(keyed_hists)
    storage_set = get_storage_local_set_time(keyed_hists)
    startup_time = get_startup_time(keyed_hists)
    bkg_load_time = get_bkgd_load_time(keyed_hists)
    ba_popup_lt = get_ba_popup_load_time(keyed_hists)
    pa_popup_lt = get_pa_popup_load_time(keyed_hists)
    cs_injection_time = get_cs_injection_time(keyed_hists)

    agg = dataframe_joiner(
        [
            addon_names,
            user_demo_metrics,
            engagement_metrics,
            browser_metrics,
            trend_metrics,
            search_metrics,
            event_metrics,
            storage_get,
            storage_set,
            startup_time,
            top_ten_coinstalls,
            bkg_load_time,
            ba_popup_lt,
            pa_popup_lt,
            cs_injection_time,
        ]
    )

    return agg


def load_test_data(spark, sc):
    import json

    schema_path = "/home/hadoop/analyses/src/addons_daily/tests/resources/main_summary_schema.json"
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = "/main_summary.json"
    # FAILFAST causes us to abort early if the data doesn't match
    # the given schema. Without this there was as very annoying
    # problem where dataframe.collect() would return an empty set.
    main_summary = spark.read.json(rows_path, schema, mode="FAILFAST")

    schema_path = "/home/hadoop/analyses/src/addons_daily/tests/resources/search_clients_daily_schema.json"
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = "/search_clients_daily.json"
    # FAILFAST causes us to abort early if the data doesn't match
    # the given schema. Without this there was as very annoying
    # problem where dataframe.collect() would return an empty set.
    search_clients_daily = spark.read.json(rows_path, schema, mode="FAILFAST")

    schema_path = (
        "/home/hadoop/analyses/src/addons_daily/tests/resources/events_schema.json"
    )
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = "/events.json"
    # FAILFAST causes us to abort early if the data doesn't match
    # the given schema. Without this there was as very annoying
    # problem where dataframe.collect() would return an empty set.
    events = spark.read.json(rows_path, schema, mode="FAILFAST")

    with open(
        "/home/hadoop/analyses/src/addons_daily/tests/resources/raw_pings.json"
    ) as f:
        j = json.load(f)
        raw_pings = sc.parallelize(j)
    return main_summary, search_clients_daily, events, raw_pings


@click.command()
@click.option("--date", required=True)
@click.option("--use_test_data", default=False)
@click.option("--main_summary_version", default="v4")
@click.option("--search_clients_daily_version", default="v5")
@click.option("--events_version", default="v1")
@click.option("--sample", default=1, help="percent sample as int [1, 100]")
def main(
    date,
    sample,
    main_summary_version,
    search_clients_daily_version,
    events_version,
    use_test_data,
):
    # path = '' # need to pass in from command line i think
    # path var is a path to the user credentials.json for BQ
    spark = get_spark(DEFAULT_TZ)
    sc = get_sc()
    sc.setLogLevel("INFO")

    if use_test_data:
        main_summary, search_daily, events, raw_pings = load_test_data(spark, sc)
    else:
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
