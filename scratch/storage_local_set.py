import pyspark.sql.functions as F

def storage_local_set(just_keyed_hist):
	"""
	"""
	storage_local_set_df = get_hist_avg('WEBEXT_STORAGE_LOCAL_SET_MS_BY_ADDONID')

	storage_local_set_by_addon = (
	    storage_local_set_df
	    .groupBy('addon_id')
	    .agg(F.mean('avg_webext_storage_local_set_ms_by_addonid').alias('avg_storage_local_set_ms'))
	)

	return storage_local_set_by_addon