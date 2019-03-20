import pyspark.sql.functions as F


def total_uri(df):
    """
    Given a dataframe of expanded add_ons from main_summary,
    return an aggregated dataframe by addon_id of avg uri count
    :param df: dataframe of addons expanded from main_summary
    :return: aggregated dataframe by addon_id
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
