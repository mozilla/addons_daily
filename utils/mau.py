def mau(summary_last_month):
	"""
	"""
	addon_client_date_expanded = (
	  summary_last_month
	  .select('active_addons', 'submission_date', 'client_id')
	  .withColumn('active_addons', F.explode('active_addons'))
	)

	addon_client_date = (
	  addon_client_date_expanded
	  .withColumn('addon_id', addon_client_date_expanded.active_addons['addon_id'])
	  .drop('active_addons')
	)

	mau = (
	  addon_client_date
	  .groupby('addon_id')
	  .agg(F.countDistinct('client_id').alias('mau'))
	)

	return mau
