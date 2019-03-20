from helper_functions import *
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql import SQLContext


# just so pycharm is not mad at me while I type this code
# these will be replaced later


spark = get_spark()

# this script assumes we have the data from main_summary and the raw_pings
# already loaded and processed

""" User Demographics & Usage Metrics """

#########
# os dist
#########


def get_os_dist(df):
    """
    :param df: addons_expanded
    :return:
    """

    client_counts = (
        df
        .select("addon_id", "client_id")
        .groupBy("addon_id")
        .agg(F.countDistinct("client_id"))
        .withColumnRenamed("count(DISTINCT client_id)", "total_clients")
    )


    # count distinct clients for each addon:OS pair,
    # join with total client counts on addon_id
    os_ = (
        df
        .select("addon_id", "os", "client_id")
        .groupBy("addon_id")
        .agg(F.countDistinct("client_id"))
        .withColumnRenamed("count(DISTINCT client_id)", "client_count")
        .join(client_counts,on='addon_id', how='left')
        .withColumn("os_pct", F.col("client_count")/F.col("total_clients"))
    )

    # group joined dataframe on addon_id and take percentage of users per OS,
    # return as a mapping of OS to percentage of users with given OS by addon
    os_distribution = (
        os_
        .select("addon_id", "os", "os_pct")
        .groupBy("addon_id")
        .agg(
            F.collect_list("os").alias("os"),
            F.collect_list("os_pct").alias("os_pct")
        )
        .withColumn("os_dist", make_map(F.col("os"), F.col("os_pct").cast(ArrayType(DoubleType()))))
        .drop("os", "os_pct")
    )

    return os_distribution

##############
# country dist
##############


def get_ct_dist(df):
    """
    :param df: addons_expanded
    :return:
    """

    client_counts = (
            df
            .select("addon_id", "client_id")
            .groupBy("addon_id")
            .agg(F.countDistinct("client_id"))
            .withColumnRenamed("count(DISTINCT client_id)", "total_clients")
    )
    country_ = (
            df
            .select("addon_id", "country", "client_id")
            .groupBy("addon_id", "country")
            .agg(F.countDistinct("client_id"))
            .withColumnRenamed("count(DISTINCT client_id)", "client_count")
            .join(client_counts, on="addon_id", how='left')
            .withColumn("country_pct", F.col("client_count") / F.col("total_clients"))
    )

    # group joined dataframe on addon_id and take percentage of users per country,
    # return as a mapping of country to percentage of users from that country by addon
    ct_dist = (
        country_
        .select("addon_id", "country", "country_pct")
        .groupBy("addon_id")
        .agg(
            F.collect_list("country").alias("country"),
            F.collect_list("country_pct").alias("country_pct")
        )
        .withColumn("country_dist", make_map(F.col("country"), F.col("country_pct")))
        .drop("country", "country_pct")
    )

    return ct_dist

##############
# active hours
##############


def get_active_hours(df):
    """
    :param df: addons_expanded
    :return:
    """

    average_time = (
        df
        .select("addon_id", "client_id", "subsession_length", "Submission_date")
        .groupBy('addon_id', 'client_id', "Submission_date")
        .agg(F.sum('subsession_length').alias('daily_total'))
    )

# group this aggregated dataframe by addon_id and take average
# of all users daily_total time spent active in milliseconds

    agg_avg_time = (
        average_time
        .groupBy("addon_id")
        .agg(F.mean("daily_total"))
        .withColumnRenamed("avg(daily_total)", "avg_time_active_ms")
    )

    return agg_avg_time

############
# total URIs
############


def get_avg_uri(df):
    """
    :param df: addons_expanded
    :return:
    """
    avg_uri = (
        df
        .select('addon_id', 'client_id', 'scalar_parent_browser_engagement_total_uri_count')
        .groupBy('addon_id', 'client_id')
        .agg(F.mean('scalar_parent_browser_engagement_total_uri_count').alias('avg_uri'))
        .groupBy("addon_id")
        .agg(F.mean("avg_uri"))
        .withColumnRenamed("avg(avg_uri)", "avg_uri")
    )

    return avg_uri

##############################
# number of tabs and bookmarks
##############################

def get_tabs_and_bookmarks(df):
    """
    :param df: addons_expanded
    :return:
    """

    tab_counts = (
        df
        .groupby("addon_id")
        .agg(F.avg("places_pages_count"),F.avg("places_bookmarks_count"))
        .withColumnRenamed("avg(places_pages_count)","avg_tabs")
        .withColumnRenamed("avg(places_bookmarks_count)","avg_bookmarks")
    )

    return tab_counts

#######################
# top ten other add ons
#######################


def get_top_ten_others(df):
    """
    :param df: addons_expanded
    :return:
    """

    ttt = F.udf(take_top_ten, ArrayType(StringType()))

    client_to_addon = (
        df
        .rdd
        .filter(lambda x: x['active_addons'] is not None)
        .map(lambda x: (x['client_id'], [y['addon_id'] for y in x['active_addons']]))
        .filter(lambda x: len(x[1]) > 1)
        .map(lambda x: (x[0], [(x[1][i], [y for y in x[1] if y != x[1][i]])
                        for i in range(len(x[1])) if len(x[1]) > 1]))
        .filter(lambda x: len(x[1]) > 2)
        .flatMap(lambda x: [(str(x[0]), element[0], element[1]) for element in x[1]])
        .flatMap(lambda x: [(x[1], j, x[0]) for j in x[2]])
    )

    schema = StructType([StructField("addon_id", StringType(), True),
                         StructField("other_addons", StringType(), True),
                         StructField("client_id", StringType(), True)])

    other_addons = client_to_addon.toDF(schema=schema)

    map_df = (
        other_addons
        .groupBy("addon_id", "other_addons")
        .agg(F.countDistinct("client_id"))
        .rdd
        .map(lambda x: (x[0], (x[1], x[2])))
        .groupByKey()
        .map(lambda x: (x[0], {y[0]: float(y[1]) for y in x[1]}))
    )

    other_addons_schema = StructType([StructField("addon_id", StringType(), True),
                                      StructField("other_addons", MapType(StringType(), FloatType()), True)])

    other_addons_df = (
        map_df
        .toDF(schema=other_addons_schema)
        .withColumn("top_ten_other_addons", ttt("other_addons"))
        .drop("other_addons")
        )

    return other_addons_df
#######################
# devtools opened count
#######################


def get_devtools_opened_count(df):
    """
    :param df: addons_expanded
    :return:
    """
    dev_count = (
        df
        .groupBy("addon_id")
        .agg(F.avg("devtools_toolbox_open_count"))
        .withColumnRenamed("avg(devtools_toolbox_open_count)", "avg_toolbox_opened_count")
    )
    return dev_count

######################################################
# percentage of users with tracking protection enabled
######################################################

def get_pct_tracking_enabled(df):
    """
    :param df: addons_expanded
    :return:
    """
    tracking_enabled = (
        df
        .where('histogram_parent_tracking_protection_enabled is not null')
        .select('addon_id', 'histogram_parent_tracking_protection_enabled.1',
                'histogram_parent_tracking_protection_enabled.0')
        .groupBy('addon_id')
        .agg(F.sum('1'), F.count('0'))
        .withColumn('total', F.col('sum(1)') + F.col('count(0)'))
        .withColumn('pct_w_tracking_prot_enabled', F.col('sum(1)') / F.col('total'))
        .drop('sum(1)', 'count(0)', 'total')
    )
    return tracking_enabled

""" Performance Metrics """

###########################
# background page load time
###########################

def get_bkgd_load_time():

    hist = "WEBEXT_BACKGROUND_PAGE_LOAD_MS_BY_ADDONID"
    return get_hist_avg(hist)

#################################
# browser action pop up load time
#################################

def get_ba_popup_load_time():

    hist = "WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID"

    ba_popup_load_time_df = get_hist_avg(hist)

    ba_popup_load_time_by_addon = (
      ba_popup_load_time_df
      .groupBy("addon_id")
      .agg(F.mean("avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID"))
      .withColumnRenamed("avg(avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID)","avg_ba_popup_load_time")
    )

    return ba_popup_load_time_by_addon

##############################
# page action pop up load time
##############################


def get_pa_popup_load_time():
    hist = "WEBEXT_PAGEACTION_POPUP_OPEN_MS_BY_ADDONID"

    pa_popup_load_time_df = get_hist_avg(hist)

    pa_popup_load_time_by_addon = (
      pa_popup_load_time_df
      .groupBy("addon_id")
      .agg(F.mean("avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID"))
      .withColumnRenamed("avg(avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID)","avg_ba_popup_load_time")
    )
    return pa_popup_load_time_by_addon

###############################
# content script injection time
###############################


def get_cs_injection_time():
    hist = 'WEBEXT_CONTENT_SCRIPT_INJECTION_MS_BY_ADDONID'
    content_script_time_df = get_hist_avg(hist)

    content_script_time_by_addon = (
      content_script_time_df
      .groupBy("addon_id")
      .agg(F.mean("avg_WEBEXT_CONTENT_SCRIPT_INJECTION_MS_BY_ADDONID").alias('avg_content_script_injection_ms'))
    )
    return content_script_time_by_addon

##########################
# storage local 'get' time
##########################

def get_storage_local_get_time():

    storage_local_get_df = get_hist_avg('WEBEXT_STORAGE_LOCAL_GET_MS_BY_ADDONID', just_keyed_hist)

    storage_local_get_by_addon = (
        storage_local_get_df
        .groupBy('addon_id')
        .agg(F.mean('avg_webext_storage_local_get_ms_by_addonid').alias('avg_storage_local_get_ms'))
    )
    return storage_local_get_by_addon

##########################
# storage local 'set' time
##########################

def get_storage_local_set_time():
    storage_local_set_df = get_hist_avg('WEBEXT_STORAGE_LOCAL_SET_MS_BY_ADDONID', just_keyed_hist)

    storage_local_set_by_addon = (
        storage_local_set_df
        .groupBy('addon_id')
        .agg(F.mean('avg_webext_storage_local_get_ms_by_addonid').alias('avg_storage_local_get_ms'))
    )
    return storage_local_set_by_addon
##############
# startup time
##############


def get_startup_time():
    hist = "WEBEXT_EXTENSION_STARTUP_MS_BY_ADDONID"

    ext_startup_df = get_hist_avg(hist)

    startup_time_by_addon = (
      ext_startup_df
      .groupBy("addon_id")
      .agg(F.mean("avg_WEBEXT_EXTENSION_STARTUP_MS_BY_ADDONID"))
      .withColumnRenamed("avg(avg_WEBEXT_EXTENSION_STARTUP_MS_BY_ADDONID)","avg_startup_time")
    )
    return startup_time_by_addon


#########
# crashes
#########

def get_crashes(df):
    """
    :param df: raw crash pings dataframe
    :return: aggregtaed crash by addon df
    """
    addon_time = (
        df
        .filter(lambda x: 'environment' in x.keys())
        .filter(lambda x: 'addons' in x['environment'].keys())
        .filter(lambda x: 'activeAddons' in x['environment']['addons'])
        .map(lambda x: (x['environment']['addons']['activeAddons'].keys(), x['creationDate']))
        .map(lambda x: [(i, x[1]) for i in x[0]])
        .flatMap(lambda x: x)
    )

    dateToHour = F.udf(lambda x: x.hour, IntegerType())

    addon_time_df = (
        spark.createDataFrame(addon_time, ['addon_id', 'time'])
        .withColumn('time_stamp', F.to_timestamp('time', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withColumn('hour', dateToHour('time_stamp'))
        .drop('time', 'time_stamp')
    )

    crashes_df = (
        addon_time_df
        .groupby('addon_id', 'hour')
        .agg(F.count(F.lit(1)).alias('crashes'))
        .groupby('addon_id')
        .agg((F.sum('crashes') / F.sum('hour')).alias('avg_hourly_crashes'))
    )

    return crashes_df
#################
# page load times
#################


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

#################
# tab switch time
#################


def get_tab_switch_time(df):
    """
    :param df: raw pings
    :return:
    """

    tab_switch_hist = (
        raw_pings
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

""" Trend Metrics """

#####
# MAU
#####

def get_mau(df):
    """
    :param df: addons expanded from main summary just last month
    :return:
    """
    mau = (
        addons_expanded
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('mau'))

    )
    return mau

#####
# YAU
#####


def get_yau(df):
    """
    :param df: main_summary addons expanded from last year
    :return:
    """
    mau = (
        addons_expanded_year
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('mau'))

    )

""" AMO-DB Metrics """

db = 'db-slave-amoprod1.amo.us-west-2.prod.mozaws.net:3306'
hostname, port = db.split(":")
port = int(port)
database = 'addons_mozilla_org'

tempdir = "s3n://mozilla-databricks-telemetry-test/amo-mysql/_temp"
jdbcurl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}&ssl=true&sslMode=verify-ca".format(hostname, port, database, dbutils.secrets.get("amo-mysql","amo-mysql-user"), dbutils.secrets.get("amo-mysql","amo-mysql-pass"))

sql_context = SQLContext(sc)

amo_df = sql_context.read \
         .format("jdbc") \
         .option("forward_spark_s3_credentials", True) \
         .option("url", jdbcurl) \
         .option("tempdir", tempdir) \
         .option("query", "select guid, averagerating, totalreviews from addons limit 1000") \
         .load()
