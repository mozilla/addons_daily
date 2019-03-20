import pyspark.sql.functions as F

def storage_local_get(just_keyed_hist):
	"""
	
	"""
	storage_local_get_df = get_hist_avg('WEBEXT_STORAGE_LOCAL_GET_MS_BY_ADDONID', just_keyed_hist)

	storage_local_get_by_addon = (
	    storage_local_get_df
	    .groupBy('addon_id')
	    .agg(F.mean('avg_webext_storage_local_get_ms_by_addonid').alias('avg_storage_local_get_ms'))
	)

	return storage_local_get_by_addon