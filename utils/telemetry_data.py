from helper_functions import *
import pyspark.sql.functions as F
import pandas as pd

# just so pycharm is not mad at me while I type this code
# these will be replaced later
addons_expanded = pd.DataFrame()
just_keyed_hist = pd.DataFrame()
addons_expanded_year = pd.DataFrame()
raw_pings = pd.DataFrame()

running_aggregate = (
    addons_expanded
    .select(F.countDistinct("addon_id"))
)

# this script assumes we have the data from main_summary and the raw_pings
# already loaded and processed

""" User Demographics & Usage Metrics """

#########
# os dist
#########

client_counts = (
    addons_expanded
    .select("addon_id", "client_id")
    .groupBy("addon_id")
    .agg(F.countDistinct("client_id"))
    .withColumnRenamed("count(DISTINCT client_id)", "total_clients")
)

# count distinct clients for each addon:OS pair,
# join with total client counts on addon_id
os_ = (
    addons_expanded
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

##############
# country dist
##############

country_ = (
        addons_expanded
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

##############
# active hours
##############

average_time = (
    addons_expanded
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

############
# total URIs
############

avg_uri = (
    addons_expanded
    .select('addon_id', 'client_id', 'scalar_parent_browser_engagement_total_uri_count')
    .groupBy('addon_id', 'client_id')
    .agg(F.mean('scalar_parent_browser_engagement_total_uri_count').alias('avg_uri'))
    .groupBy("addon_id")
    .agg(F.mean("avg_uri"))
    .withColumnRenamed("avg(avg_uri)", "avg_uri")
)

##############################
# number of tabs and bookmarks
##############################
tab_counts = (
    addons_expanded
    .groupby("addon_id")
    .agg(F.avg("places_pages_count"),F.avg("places_bookmarks_count"))
    .withColumnRenamed("avg(places_pages_count)","avg_tabs")
    .withColumnRenamed("avg(places_bookmarks_count)","avg_bookmarks")
)


#######################
# top ten other add ons
#######################

ttt = F.udf(take_top_ten, ArrayType(StringType()))

client_to_addon = (
    addons_expanded
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

#######################
# devtools opened count
#######################

dev_count = (
    addons_expanded
    .groupBy("addon_id")
    .agg(F.avg("devtools_toolbox_open_count"))
    .withColumnRenamed("avg(devtools_toolbox_open_count)", "avg_toolbox_opened_count")
)

######################################################
# percentage of users with tracking protection enabled
######################################################

tracking_enabled = (
    addons_expanded
    .where('histogram_parent_tracking_protection_enabled is not null')
    .select('addon_id', 'histogram_parent_tracking_protection_enabled.1',
            'histogram_parent_tracking_protection_enabled.0')
    .groupBy('addon_id')
    .agg(F.sum('1'), F.count('0'))
    .withColumn('total', F.col('sum(1)') + F.col('count(0)'))
    .withColumn('pct_w_tracking_prot_enabled', F.col('sum(1)') / F.col('total'))
    .drop('sum(1)', 'count(0)', 'total')
)

""" Performance Metrics """

###########################
# background page load time
###########################

hist = "WEBEXT_BACKGROUND_PAGE_LOAD_MS_BY_ADDONID"
bkgd_page_load_time_df = get_hist_avg(hist)

#################################
# browser action pop up load time
#################################

hist = "WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID"

ba_popup_load_time_df = get_hist_avg(hist)

ba_popup_load_time_by_addon = (
  ba_popup_load_time_df
  .groupBy("addon_id")
  .agg(F.mean("avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID"))
  .withColumnRenamed("avg(avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID)","avg_ba_popup_load_time")
)

##############################
# page action pop up load time
##############################

hist = "WEBEXT_PAGEACTION_POPUP_OPEN_MS_BY_ADDONID"

pa_popup_load_time_df = get_hist_avg(hist)

pa_popup_load_time_by_addon = (
  pa_popup_load_time_df
  .groupBy("addon_id")
  .agg(F.mean("avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID"))
  .withColumnRenamed("avg(avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID)","avg_ba_popup_load_time")
)

###############################
# content script injection time
###############################

hist = 'WEBEXT_CONTENT_SCRIPT_INJECTION_MS_BY_ADDONID'
content_script_time_df = get_hist_avg(hist)

content_script_time_by_addon = (
  content_script_time_df
  .groupBy("addon_id")
  .agg(F.mean("avg_WEBEXT_CONTENT_SCRIPT_INJECTION_MS_BY_ADDONID").alias('avg_content_script_injection_ms'))
)

##########################
# storage local 'get' time
##########################

storage_local_get_df = get_hist_avg('WEBEXT_STORAGE_LOCAL_GET_MS_BY_ADDONID', just_keyed_hist)

storage_local_get_by_addon = (
    storage_local_get_df
    .groupBy('addon_id')
    .agg(F.mean('avg_webext_storage_local_get_ms_by_addonid').alias('avg_storage_local_get_ms'))
)

##########################
# storage local 'set' time
##########################

storage_local_set_df = get_hist_avg('WEBEXT_STORAGE_LOCAL_SET_MS_BY_ADDONID', just_keyed_hist)

storage_local_set_by_addon = (
    storage_local_get_df
    .groupBy('addon_id')
    .agg(F.mean('avg_webext_storage_local_get_ms_by_addonid').alias('avg_storage_local_get_ms'))
)
##############
# startup time
##############

hist = "WEBEXT_EXTENSION_STARTUP_MS_BY_ADDONID"

ext_startup_df = get_hist_avg(hist)

startup_time_by_addon = (
  ext_startup_df
  .groupBy("addon_id")
  .agg(F.mean("avg_WEBEXT_EXTENSION_STARTUP_MS_BY_ADDONID"))
  .withColumnRenamed("avg(avg_WEBEXT_EXTENSION_STARTUP_MS_BY_ADDONID)","avg_startup_time")
)

#########
# crashes
#########
addon_time = (
	raw_pings_crash
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

#################
# page load times
#################

hist = "FX_PAGE_LOAD_MS_2"

avg_page_load = (
  raw_pings
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

#################
# tab switch time
#################

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

""" Trend Metrics """

#####
# MAU
#####
mau = (
    addons_expanded
    .groupby('addon_id')
    .agg(F.countDistinct('client_id').alias('mau'))

)

#####
# YAU
#####

mau = (
    addons_expanded_year
    .groupby('addon_id')
    .agg(F.countDistinct('client_id').alias('mau'))

)

