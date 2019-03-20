import pyspark.sql.functions as F
from helper_functions import make_map


def country_dist(df):
    """
    Given a spark dataframe containing the main summary data,
    return an aggregated dataframe of the form:
    addon-id : {country:pct, country:pct, country:pct}
    """
    
    # grab client counts by addon
    client_counts = (
        df
        .select("addon_id", "client_id")
        .groupBy("addon_id")
        .agg(F.countDistinct("client_id"))
        .withColumnRenamed("count(DISTINCT client_id)", "total_clients")
    )
    
    # count distinct clients for each addon:country pair,
    # join with total client counts on addon_id
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
