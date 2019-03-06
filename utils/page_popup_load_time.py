import pyspark.sql.functions as F
from helper_functions import get_hist_avg


def page_popup_load_time(just_keyed_hist):
    """
    Given an rdd of keyed histograms taken from raw pings,
    """

    page_action_popup_time_df = get_hist_avg("WEBEXT_PAGEACTION_POPUP_OPEN_MS_BY_ADDONID",
                                             just_keyed_hist)

    page_action_popup_time_by_addon = (
        page_action_popup_time_df
        .groupBy("addon_id")
        .agg(F.mean("avg_WEBEXT_PAGEACTION_POPUP_OPEN_MS_BY_ADDONID").alias("avg_page_popup_load_time"))
    )

    return page_action_popup_time_by_addon
