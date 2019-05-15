import pyspark.sql.functions as F
from pyspark.sql.types import *
from helpers import make_map, bucket_engine


def get_search_metrics(search_daily_df, addons_expanded):
    """
    :param search_daily_df: search clients daily dataframe
    :param addons_expanded: addons_expanded dataframe
    :return: aggregated dataframe by addon_id
        with metris from the search clients daily dataset
        - average sap searches by search engine
        - average tagged sap searches by search engine
        - average organic searches by search engine
        - total number of searches with ads by search engine
        - total number of ad clicks by search engine
    """
    search_daily_df = bucket_engine(search_daily_df)
    
    user_addon = addons_expanded.select('client_id', 'addon_id')
    user_addon_search = user_addon.join(search_daily_df, 'client_id')
    
    df = (
        user_addon_search.groupBy('addon_id', 'engine')
        .agg(
            F.sum('sap').alias('sap_searches'),
            F.sum('tagged_sap').alias('tagged_sap_searches'),
            F.sum('organic').alias('organic_searches'),
            F.sum('search_with_ads').alias('search_with_ads'),
            F.sum('ad_click').alias('ad_click')
        )
        .groupBy('addon_id')
        .agg(
            F.collect_list('engine').alias('engine'),
            F.collect_list('sap_searches').alias('sap_searches'),
            F.collect_list('tagged_sap_searches').alias('tagged_sap_searches'),
            F.collect_list('organic_searches').alias('organic_searches'),
            F.collect_list('search_with_ads').alias('search_with_ads'),
            F.collect_list('ad_click').alias('ad_click')
        )
        .withColumn(
            'sap_searches', 
            make_map(F.col('engine'), F.col('sap_searches').cast(ArrayType(DoubleType())))
        )
        .withColumn(
            'tagged_sap_searches', 
            make_map(F.col('engine'), F.col('tagged_sap_searches').cast(ArrayType(DoubleType())))
        )
        .withColumn(
            'organic_searches', 
            make_map(F.col('engine'), F.col('organic_searches').cast(ArrayType(DoubleType())))
        )
        .withColumn(
            'search_with_ads', 
            make_map(F.col('engine'), F.col('search_with_ads').cast(ArrayType(DoubleType())))
        )
        .withColumn(
            'ad_click', 
            make_map(F.col('ad_click'), F.col('ad_click').cast(ArrayType(DoubleType())))
        )
        .drop('engine')
    )

    return df
