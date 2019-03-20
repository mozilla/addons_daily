import pyspark.sql.functions as F


def pct_tracking_prot_enabled(df):
    """
    aggregate dataframe by addon_id and take
    percentage of users with teacking protection enabled
    :param df: dataframe of expanded addons from main_summary
    :return: aggregated dataframe by addon_id
    """
    tracking_enabled = (
        df
        .where('histogram_parent_tracking_protection_enabled is not null')
        .select('addon_id', 'histogram_parent_tracking_protection_enabled.1',
                'histogram_parent_tracking_protection_enabled.0')
        .groupBy('addon_id')
        .agg(F.sum('1'), F.count('0'))
        .withColumn('total', F.col('sum(1)')+F.col('count(0)'))
        .withColumn('pct_w_tracking_prot_enabled', F.col('sum(1)')/F.col('total'))
        .drop('sum(1)', 'count(0)', 'total')
    )
    return tracking_enabled
