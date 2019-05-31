from addons_daily.utils.helpers import *
import pyspark.sql.functions as F
import pandas as pd
import re
from pyspark.sql import SQLContext


def agg_hist_keyed_by_addon(df, hist):
    new_hist = re.sub("_webext", "", hist.lower().split("by_addonid")[0])
    agg = get_hist_avg(hist, df)
    agg_by_addon = agg.groupBy("addon_id").agg(
        F.mean("avg_" + hist.lower()).alias("avg_" + new_hist)
    )
    return agg_by_addon


def get_storage_local_get_time(df):
    return agg_hist_keyed_by_addon(df, "WEBEXT_STORAGE_LOCAL_GET_MS_BY_ADDONID")


def get_storage_local_set_time(df):
    return agg_hist_keyed_by_addon(df, "WEBEXT_STORAGE_LOCAL_SET_MS_BY_ADDONID")


def get_startup_time(df):
    return agg_hist_keyed_by_addon(df, "WEBEXT_EXTENSION_STARTUP_MS_BY_ADDONID")


def get_bkgd_load_time(df):
    hist = "WEBEXT_BACKGROUND_PAGE_LOAD_MS_BY_ADDONID"
    return get_hist_avg(hist, df)


def get_ba_popup_load_time(df):
    return agg_hist_keyed_by_addon(df, "WEBEXT_BROWSERACTION_POPUP_OPEN_MS_BY_ADDONID")


def get_pa_popup_load_time(df):
    return agg_hist_keyed_by_addon(df, "WEBEXT_PAGEACTION_POPUP_OPEN_MS_BY_ADDONID")


def get_cs_injection_time(df):
    return agg_hist_keyed_by_addon(df, "WEBEXT_CONTENT_SCRIPT_INJECTION_MS_BY_ADDONID")
