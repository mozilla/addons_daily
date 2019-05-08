from .helpers import *
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql import SQLContext


# just so pycharm is not mad at me while I type this code
# these will be replaced later


# this script assumes we have the data from main_summary and the raw_pings
# already loaded and processed

###########################################################
# User Demographics - country distribution, os distribution
###########################################################


def get_user_demo_metrics(addons_expanded):
    """
    :param addons_expanded: addons_expanded dataframe
    :return: aggregated dataframe by addon_id
        with user demographic information including
        - distribution of users by operating system
        - distribution of users by country
    """
    client_counts = (
        addons_expanded
            .select('addon_id','client_id')
            .groupBy('addon_id')
            .agg(F.countDistinct('client_id').alias('total_clients'))
    )
      
    os_dist = (
        addons_expanded
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
        .withColumn(
            'os_dist', 
            make_map(F.col('os'), F.col('os_pct').cast(ArrayType(DoubleType())))
        )
        .drop('os', 'os_pct')
    )
  
    ct_dist = (
        addons_expanded
        .select('addon_id', 'country', 'client_id')
        .groupBy('addon_id', 'country')
        .agg(F.countDistinct('client_id').alias('country_client_count'))
        .join(client_counts, on='addon_id', how='left')
        .withColumn(
            'country_pct', 
            F.col('country_client_count') / F.col('total_clients')
        )
        .select('addon_id', 'country', 'country_pct')
        .groupBy('addon_id')
        .agg(
            F.collect_list('country').alias('country'),
            F.collect_list('country_pct').alias('country_pct')
        )
        .withColumn(
            'country_dist', 
            make_map(F.col('country'), F.col('country_pct'))
        )
        .drop('country', 'country_pct')
    )
  
    combined_dist = os_dist.join(ct_dist, on='addon_id', how='outer')
  
    return combined_dist

#####################################################################
# User Engagement Metrics - total hours, active hours, addon disabled
#####################################################################


def get_engagement_metrics(addons_expanded, main_summary):
    """
    :param addons_expanded: addons_expanded
    :param main_summary: main_summary
    :return: dataframe aggregated by addon_id
        with engagement metrics including 
        - average total hours spent per user
        - average total active ticks per user
        - number of clients with the addon disabled
    """
    engagement_metrics = (
        addons_expanded
        .select('addon_id', 'client_id', 'Submission_date', 'subsession_length', 'active_ticks')
        .groupBy('addon_id','client_id','Submission_date')
        .agg(
            F.sum('active_ticks').alias('total_ticks'),
            F.sum('subsession_length').alias('daily_total'))
        .groupBy('addon_id')
        .agg(
            F.mean('total_ticks').alias('avg_time_active_ms'),
            F.mean('daily_total').alias('avg_time_total'))
        .withColumn('active_hours', F.col('avg_time_active_ms')/(12*60))
        .drop('avg_time_active_ms')
    )
    
    disabled_addons = (
        main_summary
        .where(F.col('disabled_addons_ids').isNotNull())
        .withColumn('addon_id', F.explode('disabled_addons_ids'))
        .select('addon_id', 'client_id')
        .groupBy('addon_id')
        .agg(F.countDistinct('client_id').alias('disabled'))
    )
    
    engagement_metrics = engagement_metrics.join(disabled_addons, on='addon_id', how='outer')

    return engagement_metrics

####################################################################################
# Browser Metrics - avg tabs, avg bookmarks, avg devtools opened count, avg URI, and 
#   percent with tracking protection enabled
####################################################################################


def get_browser_metrics(addons_expanded):
    """
    :param addons_expanded: addons_expanded
    :return: dataframe aggregated by addon_id
        with browser-related metrics including
            - average number of tabs open
            - average number of bookmarks
            - average devtools opened
            - average URI
            - percent of users with tracking enabled
    """
    browser_metrics = (
        addons_expanded
        .groupby('addon_id')
        .agg(
            F.avg('places_pages_count').alias('avg_tabs'), 
            F.avg('places_bookmarks_count').alias('avg_bookmarks'),
            F.avg('devtools_toolbox_opened_count').alias('avg_toolbox_opened_count')
        )
    )

    avg_uri = (
        addons_expanded
        .select('addon_id', 'client_id', 
                'scalar_parent_browser_engagement_total_uri_count')
        .groupBy('addon_id', 'client_id')
        .agg(F.mean('scalar_parent_browser_engagement_total_uri_count').alias('avg_uri'))
        .groupBy('addon_id')
        .agg(F.mean('avg_uri').alias('avg_uri'))
    )
    
    tracking_enabled = (
        addons_expanded
        .where('histogram_parent_tracking_protection_enabled is not null')
        .select('addon_id', 'histogram_parent_tracking_protection_enabled.1',
                'histogram_parent_tracking_protection_enabled.0')
        .groupBy('addon_id')
        .agg(F.sum('1'), F.count('0'))
        .withColumn('total', F.col('sum(1)') + F.col('count(0)'))
        .withColumn(
            'pct_w_tracking_prot_enabled',
            F.col('sum(1)') / F.col('total')
        )
        .drop('sum(1)', 'count(0)', 'total')
    )
    
    browser_metrics = (
        browser_metrics
        .join(avg_uri, on='addon_id', how='outer')
        .join(tracking_enabled, on='addon_id', how='outer')
    )
    
    return browser_metrics

#######################
# top ten other add ons
#######################


def get_top_ten_others(df):
    """
    :param df: this df should actually be main_summary, not addons_expanded
    :return:
    """
    ttt = F.udf(take_top_ten, ArrayType(StringType()))
    str_to_list_udf = F.udf(str_to_list, ArrayType(StringType()))
    list_expander_udf = F.udf(list_expander, ArrayType(ArrayType(StringType())))

    other_addons_df = (
        df
        .filter('active_addons is not null')
        .select('client_id', F.explode('active_addons'))
        .select('client_id', 'col.addon_id')
        .groupBy('client_id')
        .agg(F.collect_set('addon_id').alias('addons_list'))
        .withColumn('test', F.explode(list_expander_udf(F.col('addons_list'))))
        .withColumn('addon_id', F.col('test').getItem(0))
        .withColumn('others', str_to_list_udf(F.col('test').getItem(1)))
        .withColumn('other_addon', F.explode('others'))
        .drop('others', 'addons_list', 'test')
        .groupBy('addon_id', 'other_addon')
        .agg(F.countDistinct('client_id').alias('seen_together'))
        .withColumn('others_counts', F.create_map(F.col('other_addon'), F.col('seen_together')))
        .drop('other_addon', 'seen_together')
        .groupBy('addon_id')
        .agg(F.collect_list('others_counts').alias('others_w_counts'))
        .withColumn('top_ten_others', ttt(F.col('others_w_counts')))
        .drop('others_w_counts')
    )

    return other_addons_df


###############################
# Trend Metrics - DAU, WAU, MAU
###############################

def get_trend_metrics(addons_expanded):
    """
    :param df: addons_expanded
    :return: aggregated dataframe by addon_id
        with trend metrics including
        - daily active users
        - weekly active users
        - monthly active users
    """
    # limit to last 30 days to calculate mau
    addons_expanded = addons_expanded.filter('Submission_date >= (NOW() - INTERVAL 30 DAYS)')
    mau = (
        addons_expanded
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('mau'))
    )

    # limit to last 7 days to calculate wau
    addons_expanded = addons_expanded.filter('Submission_date >= (NOW() - INTERVAL 7 DAYS)')
    wau = (
        addons_expanded
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('wau'))
    )

    # limit to last 1 day to calculate dau
    addons_expanded = addons_expanded.filter('Submission_date >= (NOW() - INTERVAL 1 DAYS)')
    dau = (
        addons_expanded
        .groupby('addon_id')
        .agg(F.countDistinct('client_id').alias('dau'))
    )

    trend_metrics = (
        mau
        .join(wau, on='addon_id', how='outer')
        .join(dau, on='addon_id', how='outer')
    )

    return trend_metrics
