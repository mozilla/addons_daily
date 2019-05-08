import pyspark.sql.functions as F
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
    """
    search_daily_df = bucket_engine(search_daily_df)
    
    user_addon = addons_expanded.select('client_id', 'addon_id')
    user_addon_search = user_addon.join(search_daily_df, 'client_id')
    
    df = (
        user_addon_search.groupBy('addon_id', 'engine')
        .agg(
            F.avg('sap').alias('avg_sap_searches'),
            F.avg('tagged_sap').alias('avg_tagged_sap_searches'),
            F.avg('organic').alias('avg_organic_searches'))
        .groupBy('addon_id')
        .agg(
            F.collect_list('engine').alias('engine'),
            F.collect_list('avg_sap_searches').alias('avg_sap_searches'),
            F.collect_list('avg_tagged_sap_searches').alias('avg_tagged_sap_searches'),
            F.collect_list('avg_organic_searches').alias('avg_organic_searches')
        )
        .withColumn(
            'avg_sap_searches', 
            make_map(F.col('engine'), F.col('avg_sap_searches').cast(ArrayType(DoubleType())))
        )
        .withColumn(
            'avg_tagged_sap_searches', 
            make_map(F.col('engine'), F.col('avg_tagged_sap_searches').cast(ArrayType(DoubleType())))
        )
        .withColumn(
            'avg_organic_searches', 
            make_map(F.col('engine'), F.col('avg_organic_searches').cast(ArrayType(DoubleType())))
        )
        .drop('engine')
    )

    return df
