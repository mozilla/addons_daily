from helpers import *
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
    :return: aggregated dataframe by addon_id,
    getting distribution of users with various OS
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
    same as get_os_dist, but get country distribution instead of os
    :param df: addons_expanded
    :return: dataframe of distributions of users across countries aggregated by addon_id
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
<<<<<<< HEAD
=======
    return yau

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
>>>>>>> a69643630cddab424e5844bde91f035fe38e2d13
