import pyspark.sql.functions as F


def get_search_metrics(search_daily, addons_expanded):
    """
    """
    user_addon = addons_expanded.select("client_id", "addon_id")
    user_addon_search = user_addon.join(search_daily, "client_id")

    df = user_addon_search.groupBy("addon_id").agg(
        F.avg("sap").alias("avg_sap_searches"),
        F.avg("tagged_sap").alias("avg_tagged_sap_searches"),
        F.avg("organic").alias("avg_organic_searches"),
    )

    return df
