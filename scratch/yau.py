def yau(summary_last_year):
	addon_client_date_expanded = (
	  summary_last_year
	  .select('active_addons', 'submission_date', 'client_id')
	  .withColumn('active_addons', F.explode('active_addons'))
	)

	addon_client_date = (
	  addon_client_date_expanded
	  .withColumn('addon_id', addon_client_date_expanded.active_addons['addon_id'])
	  .drop('active_addons')
	)

	yau = (
	  addon_client_date
	  .groupby('addon_id')
	  .agg(F.countDistinct('client_id').alias('yau'))
	)