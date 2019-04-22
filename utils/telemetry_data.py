from utils.helpers import *
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql import SQLContext


# just so pycharm is not mad at me while I type this code
# these will be replaced later


# this script assumes we have the data from main_summary and the raw_pings
# already loaded and processed

""" User Demographics & Usage Metrics """

##################################################################
# Get user demographics - country distribution and os distribution
##################################################################


def get_user_demo(df):
    """
    :param df: addons_expanded
    :return: aggregated dataframe by addon_id,
    getting distribution of users with various OS
    and distribution of users in various countries
    """
    client_counts = (
        df
            .select('addon_id','client_id')
            .groupBy('addon_id')
            .agg(F.countDistinct('client_id').alias('total_clients'))
        )
      
        os_dist = (
            df
            .select('addon_id', 'os', 'client_id')
            .groupBy('addon_id', 'os')
            .agg(F.countDistinct('client_id'))
            .withColumnRenamed('count(DISTINCT client_id)', 'os_client_count')
            .join(client_counts, on='addon_id', how='left')
            .withColumn('os_pct', F.col('os_client_count')/F.col('total_clients'))
            .select('addon_id', 'os', 'os_pct')
            .groupBy('addon_id')
            .agg(
                F.collect_list('os').alias('os'),
                F.collect_list('os_pct').alias('os_pct')
            )
            .withColumn('os_dist', make_map(F.col('os'), F.col('os_pct').cast(ArrayType(DoubleType()))))
            .drop('os', 'os_pct')
        )
      
        ct_dist = (
            df
            .select('addon_id', 'country', 'client_id')
            .groupBy('addon_id', 'country')
            .agg(F.countDistinct('client_id').alias('country_client_count'))
            .join(client_counts, on='addon_id', how='left')
            .withColumn('country_pct', F.col('country_client_count') / F.col('total_clients'))
            .select('addon_id', 'country', 'country_pct')
            .groupBy('addon_id')
            .agg(
                F.collect_list('country').alias('country'),
                F.collect_list('country_pct').alias('country_pct')
            )
            .withColumn('country_dist', make_map(F.col('country'), F.col('country_pct')))
            .drop('country', 'country_pct')
        )
      
        combined_dist = os_dist.join(ct_dist, on='addon_id', how='outer')
      
        return combined_dist

###################################################
# engagement metrics - total hours and active hours
###################################################


def get_engagement_metrics(df):
    """
    Get total time spent on browser on avg, inactive or active
    :param df: addons_expanded
    :return: dataframe aggregated by addon_id
    """
    engagement_metrics = (
        addons_expanded
        .select('addon_id', 'client_id', 'Submission_date', 'subsession_length', 'active_ticks')
        .groupBy('addon_id','client_id','Submission_date')
        .agg(F.sum('active_ticks').alias('total_ticks'), F.sum('subsession_length').alias('daily_total'))
        .groupBy('addon_id')
        .agg(F.mean('total_ticks').alias('avg_time_active_ms'), F.mean('daily_total').alias('avg_time_total'))
        .withColumn('active_hours', F.col('avg_time_active_ms')/(12*60))
        .drop('avg_time_active_ms')
    )

    return engagement_metrics

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

##########################################################
# browser metrics - tabs, bookmarks, devtools opened count
##########################################################


def get_browser_metrics(df):
    """
    :param df: addons_expanded
    :return:
    """
    tab_counts = (
        addons_expanded
        .groupby('addon_id')
        .agg(F.avg('places_pages_count').alias('avg_tabs'), 
             F.avg('places_bookmarks_count').alias('avg_bookmarks'),
             F.avg('devtools_toolbox_opened_count').alias('avg_toolbox_opened_count'))
    )

    return tab_counts

#######################
# top ten other add ons
#######################


def get_top_ten_others(df):
    """
    :param df: this df should actually be main_summary, not addons_expanded
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
        .filter(lambda x: 'environment' in x)
        .filter(lambda x: 'addons' in x['environment'])
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


""" Trend Metrics """


#####
# DAU
#####


def get_dau(addons_expanded_df):
    """
    :param df: addons expanded from main summary
    :return:
    """
    df = (
        addons_expanded_df
        .filter("Submission_date >= (NOW() - INTERVAL 1 DAYS)")
    )

    dau = (
        df
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('dau'))
    )
    return dau

#####
# WAU
#####


def get_wau(addons_expanded_df):
    """
    :param df: addons expanded from main summary
    :return:
    """
    df = (
        addons_expanded_df
        .filter("Submission_date >= (NOW() - INTERVAL 7 DAYS)")
    )

    wau = (
        df
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('wau'))
    )
    return wau

#####
# MAU
#####


def get_mau(addons_expanded_df):
    """
    :param df: addons expanded from main summary
    :return:
    """
    df = (
        addons_expanded_df
        .filter("Submission_date >= (NOW() - INTERVAL 30 DAYS)")
    )

    mau = (
        df
        .filter("submission_date_s3 >= (NOW() - INTERVAL 28 DAY)")
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('mau'))
    )
    return mau

#####
# YAU
#####


def get_yau(addons_expanded_df):
    """
    :param df: main_summary addons expanded from last year
    :return:
    """
    yau = (
        addons_expanded_df
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('yau'))
    )

    return yau

#################
# Disabled addons
#################


def get_disabled(df):
    """
    Gets the number of distinct clients in df with the addon disabled
    """
    disabled_addons = (
        df
        .where(F.col('disabled_addons_ids').isNotNull())
        .withColumn('addon', F.explode('disabled_addons_ids'))
        .select('addon', 'client_id')
    )

    addons_disabled_count = (
        disabled_addons
        .groupBy('addon')
        .agg(F.countDistinct('client_id').alias('disabled'))
    )

    return addons_disabled_count



