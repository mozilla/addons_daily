import pyspark.sql.functions as F


def devtools_count(df):
    """
    Given a datframes of expanded add-ons from main_summary,
    aggregate the df by addon_id and take average # of times
    the toolbox is opened. return aggregtaed dataframe
    :param df: expanded adddon dataframe from main_summary
    :return: aggregated dataframe by addon_id
    """
    dev_count = (
        df
        .groupBy("addon_id")
        .agg(F.avg("devtools_toolbox_open_count"))
        .withColumnRenamed("avg(devtools_toolbox_open_count)", "avg_toolbox_opened_count")
    )

    return dev_count
