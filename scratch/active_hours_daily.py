import pyspark.sql.functions as F


def active_hours_daily(df):
    """
    Given a pyspark dataframe drawn from main_summary with addons field expanded,
    return aggregated dataframe of the form:
    addon_id : avg_active_time_ms for an average user per day
    """
    
    # group dataframe by addon_id, client, and date to get daily user time spent active
    # sum of users subsessions length gives total time for a day.
    average_time = (
        df
        .select("addon_id", "client_id", "Submission_date", 'subsession_length')
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
