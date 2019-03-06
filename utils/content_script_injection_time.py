import pyspark.sql.functions as F
from helper_functions import get_hist_avg


def content_script_injection_time(just_keyed_hist):
    """
    Given an rdd of keyed histograms taken from raw pings, calculate the average content script injection
    time for each addon
    """
    content_script_injection_time_df = get_hist_avg("WEBEXT_CONTENT_SCRIPT_INJECTION_MS_BY_ADDONID",
                                             just_keyed_hist)

    content_script_injection_time_by_addon = (
        content_script_injection_time_df
        .groupBy("addon_id")
        .agg(F.mean("avg_WEBEXT_CONTENT_SCRIPT_INJECTION_MS_BY_ADDONID").alias("avg_content_script_injection_time"))
    )

    return content_script_injection_time_by_addon
