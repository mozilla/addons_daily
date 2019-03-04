from helper_functions import get_hist_avg, dataframe_joiner


hist_list = ["histogram_parent_webext_background_page_load_ms_by_addonid",\
            "histogram_parent_webext_browseraction_popup_open_ms_by_addonid",\
            "histogram_parent_webext_browseraction_popup_preload_result_count_by_addonid",\
            "histogram_parent_webext_content_script_injection_ms_by_addonid",\
            "histogram_parent_webext_storage_local_get_ms_by_addonid",\
            "histogram_parent_webext_storage_local_set_ms_by_addonid",\
            "histogram_parent_webext_extension_startup_ms_by_addonid"]

def join_all_keyed_hists(hist_list,df):
    """
    Given a list of keyed histograms, and a pyspark dataframe of keyed_histograms,
    return an aggregated pyspark dataframe that is of the form:
    addon_id : mean(hist_1), mean(hist_2), etc for all hists in hist_list
    :param hist_list: list of histogram names, ie list of strings
    :param df: keyed_histogram dataframe
    :return: aggregated pyspark dataframe
    """
    all_dataframes = [get_hist_avg(hist,df) for hist in hist_list]
    return dataframe_joiner(all_dataframes)

