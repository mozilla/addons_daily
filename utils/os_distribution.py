import pyspark.sql.functions as F
from pyspark.sql.types import *
from helper_functions import make_map


def os_dist(df):
    """
    Given a spark dataframe containing the main summary data,
    return an aggregated dataframe of the form:
    addon-id : {os:pct, os:pct, os:pct}
    """

    # grab client counts by addon
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
