import pyspark.sql.functions as F
from helper_functions import get_hist_avg


def browser_popup_load_time(just_keyed_hist):
    """
    Given an rdd of keyed histograms taken from raw pings, calculate the average browser action popup load 
    time for each addon
    """
    browser_action_popup_time_df = get_hist_avg("WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID",
                                                just_keyed_hist)

    browser_action_popup_time_by_addon = (
        browser_action_popup_time_df
        .groupBy("addon_id")
        .agg(F.mean("avg_WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID").alias("avg_browser_popup_load_time"))
    )

    return browser_action_popup_time_by_addon
